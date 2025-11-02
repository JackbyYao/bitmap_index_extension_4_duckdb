#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/database.hpp"


#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"
#include "index/bitmap_idx_scan.hpp"

namespace duckdb {
//-----------------------------------------------------------------------------
// Plan rewriter
//-----------------------------------------------------------------------------
class BitmapIndexScanOptimizer : public OptimizerExtension {
public:
	BitmapIndexScanOptimizer() {
		optimize_function = BitmapIndexScanOptimizer::Optimize;
	}

	static void RewriteIndexExpression(Index &index, LogicalGet &get, Expression &expr, bool &rewrite_possible) {
		if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
			// bound column ref: rewrite to fit in the current set of bound column ids
			bound_colref.binding.table_index = get.table_index;
			auto &column_ids = index.GetColumnIds();
			auto &get_column_ids = get.GetColumnIds();
			column_t referenced_column = column_ids[bound_colref.binding.column_index];
			// search for the referenced column in the set of column_ids
			for (idx_t i = 0; i < get_column_ids.size(); i++) {
				if (get_column_ids[i].GetPrimaryIndex() == referenced_column) {
					bound_colref.binding.column_index = i;
					return;
				}
			}
			// column id not found in bound columns in the LogicalGet: rewrite not possible
			rewrite_possible = false;
			return;
		}
		// Recurse children
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { RewriteIndexExpression(index, get, child, rewrite_possible); });
	}

	// Rewrite expressions for a filter pushed into a LogicalGet (table filter). For bitmap we just
	// ensure the filter is on the indexed column and replace it with a bound reference at position 0
	static void RewriteIndexExpressionForFilter(Index &index, LogicalGet &get, unique_ptr<Expression> &expr,
	                                            idx_t filter_idx, bool &rewrite_possible) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_colref = expr->Cast<BoundColumnRefExpression>();

			auto &indexed_columns = index.GetColumnIds();
			if (indexed_columns.size() != 1) {
				// Only single column indexes are supported right now
				rewrite_possible = false;
				return;
			}

			const auto &duck_table = get.GetTable()->Cast<DuckTableEntry>();
			const auto &column_list = duck_table.GetColumns();

			auto &col = column_list.GetColumn(LogicalIndex(indexed_columns[0]));
			if (filter_idx != col.Physical().index) {
				// Bitmap index does not match the filter column
				rewrite_possible = false;
				return;
			}

			// this column matches the index column - turn it into a BoundReference at position 0
			expr = make_uniq_base<Expression, BoundReferenceExpression>(bound_colref.return_type, 0ULL);

			return;
		}
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			RewriteIndexExpressionForFilter(index, get, child, filter_idx, rewrite_possible);
		});
	}

	// Check whether the scalar function represents a bitmap-optimizable predicate:
	// equality or IN with the indexed column and a constant. Adapt set if your representation differs.
	static bool IsBitmapPredicate(const ScalarFunction &function, const unordered_set<string> &predicates) {
		if (predicates.find(function.name) == predicates.end()) {
			return false;
		}
		// For bitmap we typically support two-argument equality-like predicates
		if (function.arguments.size() < 2) {
			return false;
		}
		// Return type must be boolean
		if (function.return_type != LogicalType::BOOLEAN) {
			return false;
		}
		// Accept wide variety of scalar types as first argument (not limited to geometry)
		return true;
	}

	// Extract a constant value that represents the lookup value. For IN or array-style, user must adapt.
	// Returns true if we could obtain a constant value from the matched expression (bindings[2]).
	static bool TryGetLookupValue(const Value &value, Value &out_value) {
		// For simple equality the constant is already the lookup value
		out_value = value;
		return true;
	}

	
	static bool TryOptimize(Binder &binder, ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                        unique_ptr<LogicalOperator> &root) {
		auto &op = *plan;

		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = op.Cast<LogicalFilter>();

			if (filter.expressions.size() != 1) {
				// Only single expression supported here
				return false;
			}
			auto &filter_expr = filter.expressions[0];
			if (filter.children.front()->type != LogicalOperatorType::LOGICAL_GET) {
				return false;
			}
			auto &get_ptr = filter.children.front();
			return TryOptimizeGet(binder, context, get_ptr, root, filter, optional_idx(), filter_expr);
		}
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			for (auto &entry : get.table_filters.filters) {
				if (entry.second->filter_type != TableFilterType::EXPRESSION_FILTER) {
					continue;
				}
				auto &expr_filter = entry.second->Cast<ExpressionFilter>();
				if (TryOptimizeGet(binder, context, plan, root, nullptr, entry.first, expr_filter.expr)) {
					return true;
				}
			}
			return false;
		}
		return false;
	}

	// Core TryOptimizeGet: try to replace a LogicalGet (or LogicalGet+Filter) with a BitmapIndexScan
	static bool TryOptimizeGet(Binder &binder, ClientContext &context, unique_ptr<LogicalOperator> &get_ptr,
	                           unique_ptr<LogicalOperator> &root, optional_ptr<LogicalFilter> filter,
	                           optional_idx filter_column_idx, unique_ptr<Expression> &filter_expr) {
		auto &get = get_ptr->Cast<LogicalGet>();
		if (get.function.name != "seq_scan") {
			return false;
		}

		// do not optimize if dynamic filters are present
		if (get.dynamic_filters && get.dynamic_filters->HasFilters()) {
			return false;
		}

		auto &table = *get.GetTable();
		if (!table.IsDuckTable()) {
			return false;
		}

		auto &duck_table = table.Cast<DuckTableEntry>();
		auto &table_info = *table.GetStorage().GetDataTableInfo();
		unique_ptr<BitmapIndexScanBindData> bind_data = nullptr;

		// Bitmap-optimizable predicates: equality / IN. Adjust names to match your scalar function registry.
		unordered_set<string> bitmap_predicates = {"=", "eq", "IN", "Contains"}; // adjust as needed

		// Ensure indexes of type bitmap are bound and iterate
		table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);
		table_info.GetIndexes().Scan([&](Index &index) {
			if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
				return false;
			}

			auto &index_entry = index.Cast<BitmapIndex>();

			// Try rewriting the index expression to the GET's columns
			bool rewrite_possible = true;
			auto index_expr = index_entry.unbound_expressions[0]->Copy();
			if (filter_column_idx.IsValid()) {
				RewriteIndexExpressionForFilter(index_entry, get, index_expr, filter_column_idx.GetIndex(),
				                                rewrite_possible);
			} else {
				RewriteIndexExpression(index_entry, get, *index_expr, rewrite_possible);
			}
			if (!rewrite_possible) {
				return false;
			}

			// Build a matcher that matches: FUNCTION(index_expr, constant)
			FunctionExpressionMatcher matcher;
			matcher.function = make_uniq<ManyFunctionMatcher>(bitmap_predicates);
			matcher.expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::BOUND_FUNCTION);
			matcher.policy = SetMatcher::Policy::UNORDERED;

			// matcher: first child should equal index_expr, second should be a constant
			matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(*index_expr));
			matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());

			vector<reference<Expression>> bindings;
			if (!matcher.Match(*filter_expr, bindings)) {
				// not a bitmap-optimizable predicate for this index
				return false;
			}

			// Expect: bindings[0] = full function, bindings[1] = index_expr, bindings[2] = constant
			// Extract the constant value
			auto &const_expr = bindings[2].get().Cast<BoundConstantExpression>();
			Value lookup_value = const_expr.value;
			Value out_value;
			if (!TryGetLookupValue(lookup_value, out_value)) {
				return false;
			}

			// Construct bind data: the bitmap index, the column index (or column name), and the lookup value
			bind_data = make_uniq<BitmapIndexScanBindData>(duck_table, index_entry, out_value);

			// If the index expression refers to a specific physical column we might want to store that too:
			// (optional) bind_data->filter_column = <column name or physical index>
			return true;
		});

		if (!bind_data) {
			return false;
		}

		// Replace the seq_scan with a bitmap index scan function
		get.function = BitmapIndexScanFunction::GetFunction();
		const auto cardinality = get.function.cardinality(context, bind_data.get());
		get.has_estimated_cardinality = cardinality->has_estimated_cardinality;
		get.estimated_cardinality = cardinality->estimated_cardinality;
		get.bind_data = std::move(bind_data);

		// If there are no table filters pushed down, replacement is done
		if (get.table_filters.filters.empty()) {
			return true;
		}

		// If there are table filters, we need to pull them up and adapt projection ids
		if (!get.projection_ids.empty() && filter) {
			for (auto &id : filter->projection_map) {
				id = get.projection_ids[id];
			}
		}
		get.projection_ids.clear();
		get.types.clear();

		// Create a new LogicalFilter above the get that contains the old table filters rewritten against the index projection
		auto new_filter = make_uniq<LogicalFilter>();
		auto &column_ids = get.GetColumnIds();
		for (const auto &entry : get.table_filters.filters) {
			idx_t column_id = entry.first;
			auto &type = get.returned_types[column_id];
			bool found = false;
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i].GetPrimaryIndex() == column_id) {
					column_id = i;
					found = true;
					break;
				}
			}
			if (!found) {
				throw InternalException("Could not find column id for filter");
			}
			auto column = make_uniq<BoundColumnRefExpression>(type, ColumnBinding(get.table_index, column_id));
			new_filter->expressions.push_back(entry.second->ToExpression(*column));
		}
		new_filter->children.push_back(std::move(get_ptr));
		new_filter->ResolveOperatorTypes();
		get_ptr = std::move(new_filter);
		return true;
	}

/*
	static bool TryOptimize(Binder &binder, ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                        unique_ptr<LogicalOperator> &root) {
		// TODO: 实现索引扫描优化
		// 当前是框架实现，需要在以下功能完成后实现:
		// 1. BitmapIndex::TryInitializeScan() - 用于检查索引是否支持给定的 filter
		// 2. BitmapIndexScanFunction 的完整实现 - 用于执行索引扫描
		// 3. 访问表索引列表的公共 API

		// 基本思路:
		// - 匹配 LogicalFilter -> LogicalGet 模式
		// - 检查 Get 是否有 bitmap index
		// - 检查 filter 表达式是否可以使用 bitmap index
		// - 将 seq_scan 替换为 bitmap_index_scan
		// - 将 filter 表达式下推到 index scan


		// Look for a FILTER with a spatial predicate followed by a LOGICAL_GET table scan
		// OR for a seq_scan with an ExpressionFilter
		auto &op = *plan;

		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			// extract the filter from the filter node
			// Look for a spatial predicate
			auto &filter = op.Cast<LogicalFilter>();

			if (filter.expressions.size() != 1) {
				// We can only optimize if there is a single expression right now
				return false;
			}
			auto &filter_expr = filter.expressions[0];
			// Look for a table scan
			if (filter.children.front()->type != LogicalOperatorType::LOGICAL_GET) {
				return false;
			}
			auto &get_ptr = filter.children.front();
			return TryOptimizeGet(binder, context, get_ptr, root, filter, optional_idx(), filter_expr);
		}
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			// this is a LogicalGet - check if there is an ExpressionFilter
			auto &get = op.Cast<LogicalGet>();
			for (auto &entry : get.table_filters.filters) {
				if (entry.second->filter_type != TableFilterType::EXPRESSION_FILTER) {
					// not an expression filter
					continue;
				}
				auto &expr_filter = entry.second->Cast<ExpressionFilter>();
				if (TryOptimizeGet(binder, context, plan, root, nullptr, entry.first, expr_filter.expr)) {
					return true;
				}
			}
			return false;
		}
		return false;
	}*/

	static void OptimizeRecursive(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
	                              unique_ptr<LogicalOperator> &root) {
		if (!TryOptimize(input.optimizer.binder, input.context, plan, root)) {
			// No match: continue with the children
			for (auto &child : plan->children) {
				OptimizeRecursive(input, child, root);
			}
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		OptimizeRecursive(input, plan, plan);
	}
};

//-----------------------------------------------------------------------------
// Register
//-----------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndexPlanScan(ExtensionLoader &loader) {
	// Register the optimizer extension
	auto &db = loader.GetDatabaseInstance();
	db.config.optimizer_extensions.push_back(BitmapIndexScanOptimizer());
}

} // namespace duckdb
