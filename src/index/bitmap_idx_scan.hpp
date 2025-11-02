#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {
class DuckTableEntry;
class Index;

// This is created by the optimizer rule
struct BitmapIndexScanBindData final : public TableFunctionData {
	explicit BitmapIndexScanBindData(DuckTableEntry &table, Index &index, const Value &filter_value)
	    : table(table), index(index), filter_value(filter_value){
	}

	//! The table to scan
	DuckTableEntry &table;

	//! The index to use
	Index &index;

	Value filter_value;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BitmapIndexScanBindData>();
		return &table == &other.table && &index == &other.index && filter_value == other.filter_value;
	}
	/*
	unique_ptr<FunctionData> Copy() const override {
        return make_uniq<BitmapIndexScanBindData>(table, index, filter_value);
    }*/
};

struct BitmapIndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb