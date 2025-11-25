#include "index/bitmap_idx_join_logical.hpp"
#include "index/bitmap_idx_join_physical.hpp"
#include "index/bitmap_idx_lookup.hpp"
#include "index/bitmap_idx.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "index/bitmap_idx_scan.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

namespace {

static LogicalGet *FindLogicalGetInSubtree(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return &op->Cast<LogicalGet>();
	}
	if (op->children.size() == 1) {
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
			return FindLogicalGetInSubtree(op->children[0].get());
		default:
			break;
		}
	}
	return nullptr;
}

static idx_t TraceBindingToLogicalGet(LogicalOperator &op, ColumnBinding binding, LogicalGet *target_get) {
	if (!target_get) {
		return DConstants::INVALID_INDEX;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (&get != target_get) {
			return DConstants::INVALID_INDEX;
		}
		if (get.table_index != binding.table_index) {
			return DConstants::INVALID_INDEX;
		}
		return binding.column_index;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index || binding.column_index >= projection.expressions.size()) {
			return DConstants::INVALID_INDEX;
		}
		auto &expr = projection.expressions[binding.column_index];
		if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
			return TraceBindingToLogicalGet(*projection.children[0], bound_colref.binding, target_get);
		}
		return DConstants::INVALID_INDEX;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		if (op.children.size() == 1) {
			return TraceBindingToLogicalGet(*op.children[0], binding, target_get);
		}
		return DConstants::INVALID_INDEX;
	default:
		return DConstants::INVALID_INDEX;
	}
}

// collect the column indices (for duckdb ColumnList) from projection map (operator column)
static bool CollectTableColumnIndices(LogicalOperator &child, const vector<idx_t> &projection_map,
                                      vector<idx_t> &result) {
    result.clear();
    auto logical_get = FindLogicalGetInSubtree(&child);
    if (!logical_get) {
        return false;
    }

    auto child_bindings = child.GetColumnBindings();
    auto &column_ids = logical_get->GetColumnIds();

    auto append_column = [&](idx_t child_output_idx) -> bool {
        if (child_output_idx >= child_bindings.size()) {
            return false;
        }
        auto binding = child_bindings[child_output_idx];
        auto mapped_idx = TraceBindingToLogicalGet(child, binding, logical_get);
        if (mapped_idx == DConstants::INVALID_INDEX || mapped_idx >= column_ids.size()) {
            return false;
        }
        auto column_id = column_ids[mapped_idx];
        if (column_id.IsVirtualColumn()) {
            return false;
        }
        result.push_back(column_id.GetPrimaryIndex());
        return true;
    };

    if (projection_map.empty()) {
        for (idx_t i = 0; i < child_bindings.size(); i++) {
            if (!append_column(i)) {
                return false;
            }
        }
        return true;
    }

    for (auto proj_idx : projection_map) {
        if (!append_column(proj_idx)) {
            return false;
        }
    }
    return true;
}

static void CollectFilterExpressions(LogicalOperator &op, vector<unique_ptr<Expression>> &filters) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			filters.push_back(expr->Copy());
		}
	}
	for (auto &child : op.children) {
		CollectFilterExpressions(*child, filters);
	}
}

static bool CollectTableFilterExpressions(LogicalGet &logical_get, DuckTableEntry &duck_table,
                                          vector<unique_ptr<Expression>> &filters) {
	if (logical_get.table_filters.filters.empty()) {
		return true;
	}
	auto &column_ids = logical_get.GetColumnIds();

	for (auto &entry : logical_get.table_filters.filters) {
		auto phys_column_id = entry.first;
		idx_t local_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == phys_column_id) {
				local_idx = i;
				break;
			}
		}
		if (local_idx == DConstants::INVALID_INDEX) {
			return false;
		}
		idx_t bound_idx = local_idx;
		if (!logical_get.projection_ids.empty()) {
			idx_t found = DConstants::INVALID_INDEX;
			for (idx_t pj = 0; pj < logical_get.projection_ids.size(); pj++) {
				if (logical_get.projection_ids[pj] == local_idx) {
					found = pj;
					break;
				}
			}
			if (found == DConstants::INVALID_INDEX) {
				for (idx_t pj = 0; pj < logical_get.projection_ids.size(); pj++) {
					auto proj_val = logical_get.projection_ids[pj];
					if (proj_val != DConstants::INVALID_INDEX && proj_val < column_ids.size()) {
						if (column_ids[proj_val].GetPrimaryIndex() == phys_column_id) {
							found = pj;
							break;
						}
					}
				}
			}
			if (found == DConstants::INVALID_INDEX) {
				return false;
			}
			bound_idx = found;
		}
		auto &column = duck_table.GetColumns().GetColumn(LogicalIndex(phys_column_id));
		auto column_ref =
		    make_uniq<BoundColumnRefExpression>(column.Type(), ColumnBinding(logical_get.table_index, bound_idx));
		filters.push_back(entry.second->ToExpression(*column_ref));
	}
	return true;
}

static void CollectColumnBindings(Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		bindings.push_back(colref.binding);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CollectColumnBindings(child, bindings); });
}

static bool TryMapBindingToTableColumn(LogicalOperator &child, LogicalGet &logical_get, const ColumnBinding &binding,
                                       idx_t &table_col_idx) {
	auto mapped_idx = TraceBindingToLogicalGet(child, binding, &logical_get);
	if (mapped_idx == DConstants::INVALID_INDEX) {
		return false;
	}
	auto &column_ids = logical_get.GetColumnIds();
	if (mapped_idx >= column_ids.size()) {
		return false;
	}
	auto column_id = column_ids[mapped_idx];
	if (column_id.IsVirtualColumn()) {
		return false;
	}
	table_col_idx = column_id.GetPrimaryIndex();
	return true;
}

static unique_ptr<Expression>
RemapFilterExpression(Expression &expr, LogicalOperator &child, LogicalGet &logical_get,
                      const unordered_map<idx_t, idx_t> &column_pos_map, const vector<LogicalType> &chunk_types) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		idx_t table_col_idx;
		if (!TryMapBindingToTableColumn(child, logical_get, colref.binding, table_col_idx)) {
			throw InvalidInputException("Bitmap index join filter references unsupported column");
		}
		auto entry = column_pos_map.find(table_col_idx);
		if (entry == column_pos_map.end()) {
			throw InvalidInputException("Bitmap index join filter column was not prepared for bitmap lookup");
		}
		auto chunk_idx = entry->second;
		D_ASSERT(chunk_idx < chunk_types.size());
		return make_uniq<BoundReferenceExpression>(chunk_types[chunk_idx], chunk_idx);
	}

	auto copy = expr.Copy();
	ExpressionIterator::EnumerateChildren(*copy,
	                                      [&](unique_ptr<Expression> &child_expr) {
		                                      child_expr = RemapFilterExpression(*child_expr, child, logical_get,
		                                                                          column_pos_map, chunk_types);
	                                      });
	return copy;
}

struct BitmapBuildPlanInfo {
	vector<idx_t> fetch_table_columns;
	vector<LogicalType> fetch_column_types;
	vector<idx_t> output_fetch_map;
	unique_ptr<Expression> filter_expression;
};

static bool PrepareBuildSideInfo(LogicalOperator &child, const vector<idx_t> &projection_map,
                                 DuckTableEntry &duck_table, BitmapBuildPlanInfo &info,
                                 const vector<unique_ptr<Expression>> *additional_filters) {
	auto logical_get = FindLogicalGetInSubtree(&child);
	if (!logical_get) {
		return false;
	}

	// Collect filter expressions inside the subtree (already pushed down)
	vector<unique_ptr<Expression>> filter_expressions;
	CollectFilterExpressions(child, filter_expressions);
	if (!CollectTableFilterExpressions(*logical_get, duck_table, filter_expressions)) {
		return false;
	}
	if (additional_filters) {
		for (auto &expr : *additional_filters) {
			filter_expressions.push_back(expr->Copy());
		}
	}

	// Map projection columns to table columns
	vector<ColumnBinding> child_bindings = child.GetColumnBindings();
	vector<idx_t> projected_indices;
	if (projection_map.empty()) {
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			projected_indices.push_back(i);
		}
	} else {
		projected_indices = projection_map;
	}

	vector<idx_t> output_table_cols;
	output_table_cols.reserve(projected_indices.size());
	for (auto proj_idx : projected_indices) {
		if (proj_idx >= child_bindings.size()) {
			return false;
		}
		idx_t table_col_idx;
		if (!TryMapBindingToTableColumn(child, *logical_get, child_bindings[proj_idx], table_col_idx)) {
			return false;
		}
		output_table_cols.push_back(table_col_idx);
	}

	// Collect all filter column ids
	vector<ColumnBinding> filter_bindings;
	for (auto &expr : filter_expressions) {
		CollectColumnBindings(*expr, filter_bindings);
	}
	vector<idx_t> filter_table_cols;
	filter_table_cols.reserve(filter_bindings.size());
	for (auto &binding : filter_bindings) {
		idx_t table_col_idx;
		if (!TryMapBindingToTableColumn(child, *logical_get, binding, table_col_idx)) {
			return false;
		}
		filter_table_cols.push_back(table_col_idx);
	}

	auto &columns = duck_table.GetColumns();

	// Capture bitmap_index_scan filters (if present)
	vector<pair<idx_t, Value>> bitmap_scan_filters;
	if (logical_get->function.name == "bitmap_index_scan" && logical_get->bind_data) {
		auto &bind = logical_get->bind_data->Cast<BitmapIndexScanBindData>();
		auto &bitmap_index = bind.index.Cast<BitmapIndex>();
		if (!bind.filter_value.IsNull()) {
			for (auto column_id : bitmap_index.GetColumnIds()) {
				if (column_id >= columns.LogicalColumnCount()) {
					continue;
				}
				bitmap_scan_filters.emplace_back(column_id, bind.filter_value);
				filter_table_cols.push_back(column_id);
			}
		}
	}

	// Build fetch column order (output columns first, then filter-only columns)
	unordered_set<idx_t> seen_cols;
	vector<idx_t> fetch_columns;
	auto add_column = [&](idx_t col_idx) {
		if (seen_cols.insert(col_idx).second) {
			fetch_columns.push_back(col_idx);
		}
	};
	for (auto col : output_table_cols) {
		add_column(col);
	}
	for (auto col : filter_table_cols) {
		add_column(col);
	}

	// Build types for fetch chunk
	vector<LogicalType> fetch_types;
	fetch_types.reserve(fetch_columns.size());
	for (auto col_idx : fetch_columns) {
		if (col_idx >= columns.LogicalColumnCount()) {
			return false;
		}
		auto &col = columns.GetColumn(LogicalIndex(col_idx));
		fetch_types.push_back(col.Type());
	}

	// Map columns to positions for copying/filtering
	unordered_map<idx_t, idx_t> column_pos_map;
	for (idx_t i = 0; i < fetch_columns.size(); i++) {
		column_pos_map[fetch_columns[i]] = i;
	}

	// Build output fetch map
	vector<idx_t> output_fetch_map;
	output_fetch_map.reserve(output_table_cols.size());
	for (auto col : output_table_cols) {
		output_fetch_map.push_back(column_pos_map[col]);
	}

	// Remap filter expressions to reference chunk positions
	vector<unique_ptr<Expression>> remapped_filters;
	for (auto &expr : filter_expressions) {
		remapped_filters.push_back(
		    RemapFilterExpression(*expr, child, *logical_get, column_pos_map, fetch_types));
	}
	for (auto &scan_filter : bitmap_scan_filters) {
		auto entry = column_pos_map.find(scan_filter.first);
		if (entry == column_pos_map.end()) {
			continue;
		}
		auto ref = make_uniq<BoundReferenceExpression>(fetch_types[entry->second], entry->second);
		auto constant = make_uniq<BoundConstantExpression>(scan_filter.second);
		auto cmp = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(ref),
		                                                std::move(constant));
		remapped_filters.push_back(std::move(cmp));
	}

	unique_ptr<Expression> filter_expression;
	if (!remapped_filters.empty()) {
		if (remapped_filters.size() == 1) {
			filter_expression = std::move(remapped_filters[0]);
		} else {
			auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &expr : remapped_filters) {
				conjunction->children.push_back(std::move(expr));
			}
			filter_expression = std::move(conjunction);
		}
	}

	info.fetch_table_columns = std::move(fetch_columns);
	info.fetch_column_types = std::move(fetch_types);
	info.output_fetch_map = std::move(output_fetch_map);
	info.filter_expression = std::move(filter_expression);
	return true;
}

} // namespace

LogicalBitmapIndexJoin::LogicalBitmapIndexJoin(JoinType join_type, vector<JoinCondition> conditions,
                                               string bitmap_index_schema, string bitmap_index_table_name,
                                               string bitmap_index_name, idx_t bitmap_index_table_index,
                                               bool probe_on_left, vector<idx_t> left_projection_map,
                                               vector<idx_t> right_projection_map,
                                               vector<unique_ptr<BaseStatistics>> join_stats,
                                               vector<unique_ptr<Expression>> left_filters_p,
                                               vector<unique_ptr<Expression>> right_filters_p)
    : LogicalExtensionOperator(), join_type(join_type), conditions(std::move(conditions)),
      bitmap_index_schema(std::move(bitmap_index_schema)),
      bitmap_index_table_name(std::move(bitmap_index_table_name)),
      bitmap_index_name(std::move(bitmap_index_name)), bitmap_index_table_index(bitmap_index_table_index),
      build_on_left(probe_on_left), left_projection_map(std::move(left_projection_map)),
      right_projection_map(std::move(right_projection_map)), join_stats(std::move(join_stats)),
      left_filters(std::move(left_filters_p)), right_filters(std::move(right_filters_p)) {
	// Add children (left and right)
	D_ASSERT(children.empty());
}

void LogicalBitmapIndexJoin::ResolveTypes() {
	// Mirror LogicalJoin::ResolveTypes()
	types = LogicalJoin::MapTypes(children[0]->types, left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.emplace_back(LogicalType::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	auto right_types = LogicalJoin::MapTypes(children[1]->types, right_projection_map);
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		types = right_types;
		return;
	}
	types.insert(types.end(), right_types.begin(), right_types.end());
}

vector<ColumnBinding> LogicalBitmapIndexJoin::GetColumnBindings() {
	// Mirror LogicalJoin::GetColumnBindings()
	auto left_bindings = LogicalJoin::MapBindings(children[0]->GetColumnBindings(), left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return left_bindings;
	}

	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side plus the MARK column
		left_bindings.emplace_back(0, 0); // TODO: proper mark_index
		return left_bindings;
	}
	// for other join types we project both the LHS and the RHS
	auto right_bindings = LogicalJoin::MapBindings(children[1]->GetColumnBindings(), right_projection_map);
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		return right_bindings;
	}
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalBitmapIndexJoin::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	// Follow the same pattern as LogicalComparisonJoin
	// First get the bindings of the LHS and resolve the LHS expressions
	res.VisitOperator(*children[0]);
	
	// Update expression types to match children[0]'s actual output types
	// This is necessary because subsequent optimizers may have changed the children's structure,
	// causing the expression's return_type to be out of sync with the actual column types
	auto &left_types = children[0]->types;
	for (auto &cond : conditions) {
		if (cond.left && cond.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = cond.left->Cast<BoundColumnRefExpression>();
			if (col_ref.binding.column_index < left_types.size()) {
				cond.left->return_type = left_types[col_ref.binding.column_index];
			}
		}
		res.VisitExpression(&cond.left);
	}
	for (auto &expr : left_filters) {
		res.VisitExpression(&expr);
	}
	
	// Then get the bindings of the RHS and resolve the RHS expressions
	res.VisitOperator(*children[1]);
	
	// Update expression types to match children[1]'s actual output types
	auto &right_types = children[1]->types;
	for (auto &cond : conditions) {
		if (cond.right && cond.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = cond.right->Cast<BoundColumnRefExpression>();
			if (col_ref.binding.column_index < right_types.size()) {
				cond.right->return_type = right_types[col_ref.binding.column_index];
			}
		}
		res.VisitExpression(&cond.right);
	}
	for (auto &expr : right_filters) {
		res.VisitExpression(&expr);
	}
	
	// Finally update the bindings with the result bindings of the join
	bindings = GetColumnBindings();
}

PhysicalOperator &LogicalBitmapIndexJoin::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Look up the table and bitmap index
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, "", bitmap_index_schema, bitmap_index_table_name);
	auto &duck_table = table_entry.Cast<DuckTableEntry>();
	
	// Find the bitmap index through DataTableInfo
	auto &storage = duck_table.GetStorage();
	auto &table_info = *storage.GetDataTableInfo();
	table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);
	
	BitmapIndex *bitmap_index = nullptr;
	table_info.GetIndexes().Scan([&](Index &index) {
		if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
			return false;
		}
		auto &bitmap_idx = index.Cast<BitmapIndex>();
		if (bitmap_idx.name == bitmap_index_name) {
			bitmap_index = &bitmap_idx;
			return true;
		}
		return false;
	});
	
	if (!bitmap_index) {
		throw InternalException("Bitmap index '%s' not found on table '%s'", bitmap_index_name, bitmap_index_table_name);
	}

	// Determine table column indices for both children (if possible)
	vector<idx_t> left_table_col_indices;
	vector<idx_t> right_table_col_indices;
	bool left_mapping_ok = CollectTableColumnIndices(*children[0], left_projection_map, left_table_col_indices);
	bool right_mapping_ok = CollectTableColumnIndices(*children[1], right_projection_map, right_table_col_indices);
	// Only the table that actually owns the bitmap index needs physical column mapping
	if ((bitmap_index_table_index == 0 && !left_mapping_ok) ||
	    (bitmap_index_table_index == 1 && !right_mapping_ok)) {
		throw InvalidInputException(
		    "Bitmap index join requires direct table column mapping on the indexed table (encountered complex subtree)");
	}

	BitmapBuildPlanInfo build_info;
	vector<LogicalType> build_fetch_types;
	vector<idx_t> build_output_fetch_map;
	unique_ptr<Expression> build_filter_expression;
	if (bitmap_index_table_index == 0) {
		const auto *extra_filters = left_filters.empty() ? nullptr : &left_filters;
		if (!PrepareBuildSideInfo(*children[0], left_projection_map, duck_table, build_info, extra_filters)) {
			throw InvalidInputException("Bitmap index join requires direct column mapping for indexed table (left)");
		}
		left_table_col_indices = build_info.fetch_table_columns;
	} else {
		const auto *extra_filters = right_filters.empty() ? nullptr : &right_filters;
		if (!PrepareBuildSideInfo(*children[1], right_projection_map, duck_table, build_info, extra_filters)) {
			throw InvalidInputException("Bitmap index join requires direct column mapping for indexed table (right)");
		}
		right_table_col_indices = build_info.fetch_table_columns;
	}
	build_fetch_types = build_info.fetch_column_types;
	build_output_fetch_map = build_info.output_fetch_map;
	build_filter_expression = std::move(build_info.filter_expression);
	auto left_output_types_copy = LogicalJoin::MapTypes(children[0]->types, left_projection_map);
	auto right_output_types_copy = LogicalJoin::MapTypes(children[1]->types, right_projection_map);

	// Create physical plans for children
	// For the build side (the table with bitmap index), create a virtual operator
	// that indicates data will be fetched directly using row IDs from the index
	PhysicalOperator *left = nullptr;
	PhysicalOperator *right = nullptr;
	
	if (bitmap_index_table_index == 0) {
		// Bitmap index is on left (build side)
		// Create virtual operator for left, real scan for right (probe side)
		vector<LogicalType> left_types;
		if (children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &left_get = children[0]->Cast<LogicalGet>();
			left_types = left_get.returned_types;
		} else {
			left_types = children[0]->types;
		}
		idx_t left_cardinality = children[0]->EstimateCardinality(context);
		left = &planner.Make<PhysicalBitmapIndexLookup>(left_types, duck_table, *bitmap_index, left_cardinality);
		
		// Right side is probe side - create normal plan
		right = &planner.CreatePlan(*children[1]);
	} else {
		// Bitmap index is on right (build side)
		// Left side is probe side - create normal plan
		left = &planner.CreatePlan(*children[0]);
		
		// Create virtual operator for right (build side)
		vector<LogicalType> right_types;
		if (children[1]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &right_get = children[1]->Cast<LogicalGet>();
			right_types = right_get.returned_types;
		} else {
			right_types = children[1]->types;
		}
		idx_t right_cardinality = children[1]->EstimateCardinality(context);
		right = &planner.Make<PhysicalBitmapIndexLookup>(right_types, duck_table, *bitmap_index, right_cardinality);
	}

	// Collect condition types
	vector<LogicalType> condition_types;
	for (auto &cond : conditions) {
		condition_types.push_back(cond.left->return_type);
	}

	// Determine actual build side after potential child rewrites/swaps.
	// The build side is the one replaced by the dummy bitmap lookup operator.
	bool physical_build_on_left = left->type == PhysicalOperatorType::DUMMY_SCAN;
	build_on_left = physical_build_on_left;

	// Create the physical operator
	// Note: Make automatically passes physical_plan as first argument
	// Pass the mapped table column indices so the physical join can fetch directly from storage
	auto &bitmap_join = planner.Make<PhysicalBitmapIndexJoin>(
	    *this, *left, *right, join_type, std::move(conditions), condition_types, std::move(left_output_types_copy),
	    std::move(right_output_types_copy), bitmap_index_table_index, bitmap_index, &duck_table, build_on_left,
	    left_projection_map, right_projection_map, std::move(left_table_col_indices), std::move(right_table_col_indices),
	    std::move(build_output_fetch_map), std::move(build_fetch_types), std::move(build_filter_expression));

	return bitmap_join;
}

void LogicalBitmapIndexJoin::Serialize(Serializer &writer) const {
	LogicalExtensionOperator::Serialize(writer);
	writer.WriteProperty(200, "join_type", join_type);
	writer.WriteProperty(201, "conditions", conditions);
	writer.WriteProperty(202, "bitmap_index_schema", bitmap_index_schema);
	writer.WriteProperty(203, "bitmap_index_table_name", bitmap_index_table_name);
	writer.WriteProperty(204, "bitmap_index_name", bitmap_index_name);
	writer.WriteProperty(205, "bitmap_index_table_index", bitmap_index_table_index);
	writer.WriteProperty(206, "probe_on_left", build_on_left);
	writer.WriteProperty(207, "left_projection_map", left_projection_map);
	writer.WriteProperty(208, "right_projection_map", right_projection_map);
	// Note: BaseStatistics serialization may need special handling
	// For now, we'll skip join_stats serialization or implement it later
}

unique_ptr<LogicalExtensionOperator> LogicalBitmapIndexJoin::Deserialize(Deserializer &reader) {
	throw NotImplementedException("LogicalBitmapIndexJoin::Deserialize not yet implemented");
}

} // namespace duckdb
