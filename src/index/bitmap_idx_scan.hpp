#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {
class DuckTableEntry;
class Index;

// This is created by the optimizer rule
struct BitmapIndexScanBindData final : public TableFunctionData {
	explicit BitmapIndexScanBindData(DuckTableEntry &table, Index &index)
	    : table(table), index(index){
	}

	//! The table to scan
	DuckTableEntry &table;

	//! The index to use
	Index &index;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BitmapIndexScanBindData>();
		return &other.table == &table;
	}
};

struct BitmapIndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb