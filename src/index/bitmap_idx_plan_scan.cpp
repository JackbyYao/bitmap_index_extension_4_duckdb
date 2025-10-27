#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/function_matcher.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/main/database.hpp"

#include "index/bitmap_idx_module.hpp"

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
		throw NotImplementedException("RewriteIndexExpression() not implemented");
	}

	static void RewriteIndexExpressionForFilter(Index &index, LogicalGet &get, unique_ptr<Expression> &expr,
	                                            idx_t filter_idx, bool &rewrite_possible) {
		throw NotImplementedException("RewriteIndexExpressionForFilter() not implemented");
	}

	static bool TryOptimize(Binder &binder, ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                        unique_ptr<LogicalOperator> &root) {
		throw NotImplementedException("TryOptimize() not implemented");
	}

	static bool TryOptimizeGet(Binder &binder, ClientContext &context, unique_ptr<LogicalOperator> &get_ptr,
	                           unique_ptr<LogicalOperator> &root, optional_ptr<LogicalFilter> filter,
	                           optional_idx filter_column_idx, unique_ptr<Expression> &filter_expr) {
		throw NotImplementedException("TryOptimizeGet() not implemented");
	}

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
