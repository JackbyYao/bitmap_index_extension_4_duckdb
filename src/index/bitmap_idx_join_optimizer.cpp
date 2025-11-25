#include "index/bitmap_idx_join_optimizer.hpp"
#include "index/bitmap_idx_join_logical.hpp"
#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static BitmapIndex *FindBitmapIndexOnColumn(ClientContext &context, TableCatalogEntry &table,
                                            const string &column_name) {
	// Get all indexes on this table
	auto &duck_table = table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	auto &table_info = *storage.GetDataTableInfo();

	// Bind indexes
	table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);

	BitmapIndex *found_index = nullptr;
	table_info.GetIndexes().Scan([&](Index &index) {
		if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
			return false;
		}
		auto &bitmap_index = index.Cast<BitmapIndex>();
		// Check if this index covers the column
		// For now, assume single-column indexes and check if column matches
		// TODO: Properly check if index covers the join key column
		auto &column_ids = bitmap_index.GetColumnIds();
		if (column_ids.size() == 1) {
			auto &col = table.GetColumns().GetColumn(LogicalIndex(column_ids[0]));
			if (col.Name() == column_name) {
				found_index = &bitmap_index;
				return true;
			}
		}
		return false;
	});

	return found_index;
}

// Helper function to recursively find LogicalGet in a subtree
// the reason is that DuckDB might dynamically add compression projections ahead LogicalGet
static LogicalGet* FindLogicalGet(LogicalOperator* op) {
	if (!op) {
		return nullptr;
	}
	
	// Direct LogicalGet
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return &op->Cast<LogicalGet>();
	}
	
	// Recursively search in children for operators that don't change the base table
	// These operators just transform the data but don't change which table we're scanning
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	    op->type == LogicalOperatorType::LOGICAL_FILTER ||
	    op->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		if (op->children.size() == 1) {
			return FindLogicalGet(op->children[0].get());
		}
	}
	
	return nullptr;
}

// Helper function to check if an expression references columns from a specific subtree
static bool ExpressionBelongsToSubtree(Expression &expr, LogicalOperator &subtree_root) {
	// Get all table_index values referenced by the expression
	unordered_set<idx_t> expr_bindings;
	LogicalJoin::GetExpressionBindings(expr, expr_bindings);
	
	// Get all table_index values produced by the subtree
	unordered_set<idx_t> subtree_bindings;
	LogicalJoin::GetTableReferences(subtree_root, subtree_bindings);
	
	// Check if expression references any column from the subtree
	for (auto &binding : expr_bindings) {
		if (subtree_bindings.find(binding) != subtree_bindings.end()) {
			return true;
		}
	}
	
	return false;
}

// Helper function to trace a ColumnBinding back to LogicalGet and get the column index
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
		// Found the target LogicalGet, return the column_index
		return binding.column_index;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index) {
			return DConstants::INVALID_INDEX;
		}
		if (binding.column_index >= projection.expressions.size()) {
			return DConstants::INVALID_INDEX;
		}
		auto &expr = projection.expressions[binding.column_index];
		if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			// Trace through to the child
			auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
			return TraceBindingToLogicalGet(*projection.children[0], bound_colref.binding, target_get);
		}
		return DConstants::INVALID_INDEX;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		// Pass-through operators - search in children
		if (op.children.size() == 1) {
			return TraceBindingToLogicalGet(*op.children[0], binding, target_get);
		}
		return DConstants::INVALID_INDEX;
	default:
		return DConstants::INVALID_INDEX;
	}
}

// Check if LogicalGet has bitmap index built on specific column
static BitmapIndex *FindBitmapIndexOnJoinKey(ClientContext &context, LogicalOperator &subtree_root, LogicalGet &get, Expression &key_expr) {
	// First check if the expression belongs to this subtree
	if (!ExpressionBelongsToSubtree(key_expr, subtree_root)) {
		return nullptr;
	}
	
	// Get the table
	auto table = get.GetTable();
	if (!table) {
		return nullptr;
	}

	// Check if the key expression is a BoundColumnRefExpression
	// pointing to a column. Extract the column name.
	// TODO: Handle more complex expressions
	if (key_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}

	auto &col_ref = key_expr.Cast<BoundColumnRefExpression>();
	
	// Trace the binding back to the LogicalGet to get the column index
	idx_t get_column_index = TraceBindingToLogicalGet(subtree_root, col_ref.binding, &get);
	
	if (get_column_index == DConstants::INVALID_INDEX) {
		return nullptr;
	}
	
	// Map LogicalGet output column index to actual table column index
	auto &column_ids = get.GetColumnIds();
	if (get_column_index >= column_ids.size()) {
		return nullptr;
	}
	
	auto base_column_id = column_ids[get_column_index];

	// Check if it's a virtual column
	if (base_column_id.IsVirtualColumn()) {
		return nullptr; // Virtual columns don't have bitmap indexes
	}

	// Get the actual table column index
	auto table_column_idx = base_column_id.GetPrimaryIndex();

	// Get column name from table
	auto &columns = table->GetColumns();
	if (table_column_idx >= columns.LogicalColumnCount()) {
		return nullptr;
	}
	auto &column = columns.GetColumn(LogicalIndex(table_column_idx));
	auto column_name = column.Name();

	// Find bitmap index on this column
	return FindBitmapIndexOnColumn(context, *table, column_name);
}

// Helper function to check if a join can be optimized with bitmap index join
// Returns true if optimization is possible, false otherwise
static bool CanOptimizeJoin(OptimizerExtensionInput &input, LogicalComparisonJoin &join, LogicalOperator &plan) {
	// Check if join has equality conditions
	idx_t range_count = 0;
	bool has_equality = join.HasEquality(range_count);
	if (!has_equality || range_count > 0) {
		return false;
	}

	// Check join type - only support certain types initially
	if (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT &&
	    join.join_type != JoinType::SEMI && join.join_type != JoinType::ANTI) {
		return false;
	}

	// Check if children are LogicalGet (table scans)
	if (plan.children.size() != 2) {
		return false;
	}

	// Recursively find LogicalGet in children (may be wrapped in Projection/Filter)
	auto left_get = FindLogicalGet(plan.children[0].get());
	auto right_get = FindLogicalGet(plan.children[1].get());

	if (!left_get || !right_get) {
		return false;
	}

	// Check for bitmap indexes on join keys
	// For now, handle single-key joins
	if (join.conditions.empty()) {
		return false;
	}

	auto &first_cond = join.conditions[0];
	BitmapIndex *left_index = nullptr;
	BitmapIndex *right_index = nullptr;

	if (first_cond.left && first_cond.right) {
		left_index = FindBitmapIndexOnJoinKey(input.context, *plan.children[0], *left_get, *first_cond.left);
		right_index = FindBitmapIndexOnJoinKey(input.context, *plan.children[1], *right_get, *first_cond.right);
	}

	// If neither table has bitmap index -> no optimization
	if (!left_index && !right_index) {
		return false;
	}

	// Get cardinalities for decision making
	idx_t left_cardinality = left_get->EstimateCardinality(input.context);
	idx_t right_cardinality = right_get->EstimateCardinality(input.context);

	// Check if optimization makes sense based on cardinalities
	if (left_index && !right_index) {
		// Only left has index - check if right is too large
		if (right_cardinality > left_cardinality * 10) {
			return false;
		}
	} else if (right_index && !left_index) {
		// Only right has index - check if left is too large
		if (left_cardinality > right_cardinality * 10) {
			return false;
		}
	}

	return true;
}

// Recursive function to detect if bitmap index join optimization can be applied
// If detected, disables CompressedMaterialization
static bool DisableCompressedMaterializationIfNeededRecursive(OptimizerExtensionInput &input, LogicalOperator &op) {
	// Check if this is a join we can optimize
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		
		// Check if we can optimize this join
		if (CanOptimizeJoin(input, join, op)) {
			// Disable CompressedMaterialization to avoid adding PROJECTIONs
			auto &config = DBConfig::GetConfig(input.context);
			config.options.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
			return true;
		}
	}
	
	// Recursively check children
	for (auto &child : op.children) {
		if (DisableCompressedMaterializationIfNeededRecursive(input, *child)) {
			return true;
		}
	}
	return false;
}

static void OptimizeRecursive(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	// Check if this is a join we can optimize
	if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		// LogicalDelimJoin inherits from LogicalComparisonJoin, so we can cast to base class
		auto &join = plan->Cast<LogicalComparisonJoin>();

		// Check if join has equality conditions
		idx_t range_count = 0;
		bool has_equality = join.HasEquality(range_count);
		if (!has_equality || range_count > 0) {
			// Only optimize equality joins without range conditions
			// Continue to children
			goto optimize_children;
		}

		// Check join type - only support certain types initially
		if (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT &&
		    join.join_type != JoinType::SEMI && join.join_type != JoinType::ANTI) {
			// Not supported yet
			goto optimize_children;
		}

		// Check if children are LogicalGet (table scans)
		if (plan->children.size() != 2) {
			return;
		}

		// Recursively find LogicalGet in children (may be wrapped in Projection/Filter)
		auto left_get = FindLogicalGet(plan->children[0].get());
		auto right_get = FindLogicalGet(plan->children[1].get());

		if (!left_get || !right_get) {
			// Could not find LogicalGet in children (may have other operators like Projection/Filter)
			goto optimize_children;
		}

		// Check for bitmap indexes on join keys
		// For now, handle single-key joins
		if (join.conditions.empty()) {
			return;
		}

		auto &first_cond = join.conditions[0];
		BitmapIndex *left_index = nullptr;
		BitmapIndex *right_index = nullptr;

		if (first_cond.left && first_cond.right) {
			left_index = FindBitmapIndexOnJoinKey(input.context, *plan->children[0], *left_get, *first_cond.left);
			right_index = FindBitmapIndexOnJoinKey(input.context, *plan->children[1], *right_get, *first_cond.right);
		}

		// Decision logic:
		// 1. If neither table has bitmap index -> no optimization
		if (!left_index && !right_index) {
			goto optimize_children;
		}

		// 2. Get cardinalities for decision making
		idx_t left_cardinality = left_get->EstimateCardinality(input.context);
		idx_t right_cardinality = right_get->EstimateCardinality(input.context);

		// 3. Decision: which side to use bitmap index, which side to build hash table
		idx_t bitmap_index_table_index = 0; // 0 -> index on left table; 1 -> index on right table
		bool probe_on_left = false;
		string bitmap_index_table_schema_name;
		string bitmap_index_table_name;
		string bitmap_index_name;

		if (left_index && right_index) {
			// Both have indexes: use bitmap index on smaller table (build side)
			// Probe side will scan the larger table
			if (left_cardinality < right_cardinality) {
				// Left is smaller - use bitmap index on left (build side)
				bitmap_index_table_index = 0; // using index of left table
				probe_on_left = false; // probe the right table
				auto left_table = left_get->GetTable();
				bitmap_index_table_schema_name = left_table->schema.name;
				bitmap_index_table_name = left_table->name;
				bitmap_index_name = left_index->name;
			} else {
				// Right is smaller - use bitmap index on right (build side)
				bitmap_index_table_index = 1; // right
				probe_on_left = true; // build on left (probe side)
				auto right_table = right_get->GetTable();
				bitmap_index_table_schema_name = right_table->schema.name;
				bitmap_index_table_name = right_table->name;
				bitmap_index_name = right_index->name;
			}
		} 
		else if (left_index) {
			// Only left has index
			bitmap_index_table_index = 0;
			probe_on_left = false; // build on right (no index)
			auto left_table = left_get->GetTable();
			bitmap_index_table_schema_name = left_table->schema.name;
			bitmap_index_table_name = left_table->name;
			bitmap_index_name = left_index->name;
		} 
		else if (right_index) {
			// Only right has index
			bitmap_index_table_index = 1;
			probe_on_left = true; // build on left (no index)
			auto right_table = right_get->GetTable();
			bitmap_index_table_schema_name = right_table->schema.name;
			bitmap_index_table_name = right_table->name;
			bitmap_index_name = right_index->name;
		}

		// Copy join statistics
		vector<unique_ptr<BaseStatistics>> join_stats_copy;
		for (auto &stat : join.join_stats) {
			if (stat) {
				join_stats_copy.push_back(stat->Copy().ToUnique());
			} else {
				join_stats_copy.push_back(nullptr);
			}
		}

		// Deep copy join conditions
		vector<JoinCondition> conditions_copy;
		conditions_copy.reserve(join.conditions.size());
		for (auto &cond : join.conditions) {
			JoinCondition new_cond;
			new_cond.comparison = cond.comparison;
			if (cond.left) {
				new_cond.left = cond.left->Copy();
			}
			if (cond.right) {
				new_cond.right = cond.right->Copy();
			}
			conditions_copy.push_back(std::move(new_cond));
		}

		// Create LogicalBitmapIndexJoin to replace the original join
		auto bitmap_join = make_uniq<LogicalBitmapIndexJoin>(
		    join.join_type, // join type
			std::move(conditions_copy), // join condition 
			bitmap_index_table_schema_name, // schema name of the indexed table
			bitmap_index_table_name, // name of the indexed table
			bitmap_index_name, // name of the index
		    bitmap_index_table_index, // left child -> 0, right child -> 0
			probe_on_left, 
			join.left_projection_map, // projections on left table [idx of columns]
			join.right_projection_map, // projections on right table [idx of columns]
		    std::move(join_stats_copy)
		);

		// Copy children
		bitmap_join->children = std::move(plan->children);
		plan = std::move(bitmap_join);

		return;
	}

optimize_children:
	// Recursively optimize children
	for (auto &child : plan->children) {
		OptimizeRecursive(input, child);
	}
}

void BitmapIndexJoinOptimizer::DisableCompressedMaterializationIfNeeded(OptimizerExtensionInput &input,
                                                                         unique_ptr<LogicalOperator> &plan) {
	if (plan) {
		DisableCompressedMaterializationIfNeededRecursive(input, *plan);
	}
}

void BitmapIndexJoinOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeRecursive(input, plan);
}

// Registration function
void BitmapIndexModule::RegisterBitmapIndexJoin(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	OptimizerExtension optimizer;
	// pre_optimize: 检测并禁用 CompressedMaterialization
	optimizer.pre_optimize_function = BitmapIndexJoinOptimizer::DisableCompressedMaterializationIfNeeded;
	// optimize: 应用 bitmap index join 优化（此时 projection_map 已设置）
	optimizer.optimize_function = BitmapIndexJoinOptimizer::Optimize;
	db.config.optimizer_extensions.push_back(std::move(optimizer));
}

} // namespace duckdb

