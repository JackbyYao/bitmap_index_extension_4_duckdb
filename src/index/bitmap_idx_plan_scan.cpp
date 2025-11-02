#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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

		return false;
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
