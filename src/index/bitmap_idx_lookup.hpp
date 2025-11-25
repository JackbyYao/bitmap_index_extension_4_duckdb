#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "index/bitmap_idx.hpp"

namespace duckdb {

//! PhysicalBitmapIndexLookup represents a virtual operator that indicates
//! data will be fetched directly from a table using row IDs from a bitmap index.
//! This operator doesn't actually execute - it's just for query plan visualization.
class PhysicalBitmapIndexLookup : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DUMMY_SCAN;

public:
	PhysicalBitmapIndexLookup(PhysicalPlan &physical_plan, vector<LogicalType> types, 
	                          DuckTableEntry &table, BitmapIndex &bitmap_index, 
	                          idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::DUMMY_SCAN, std::move(types), estimated_cardinality), // use DUMMY SCAN as its virtual
	      table(table), 
		  bitmap_index(bitmap_index) {
	}

	DuckTableEntry &table;
	BitmapIndex &bitmap_index;

public:
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
		// This operator should never be executed - data is fetched directly in PhysicalBitmapIndexJoin
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}

	bool IsSource() const override {
		return true;
	}

	string GetName() const override {
		return "BITMAP_INDEX_LOOKUP";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Table"] = table.name;
		result["Bitmap Index"] = bitmap_index.name;
		result["Note"] = "Data fetched directly using row IDs from bitmap index";
		SetEstimatedCardinality(result, estimated_cardinality);
		return result;
	}
};

} // namespace duckdb

