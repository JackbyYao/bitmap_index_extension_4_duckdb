#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class BitmapIndexJoinOptimizer {
public:
	static void DisableCompressedMaterializationIfNeeded(OptimizerExtensionInput &input,
	                                                     unique_ptr<LogicalOperator> &plan);
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

