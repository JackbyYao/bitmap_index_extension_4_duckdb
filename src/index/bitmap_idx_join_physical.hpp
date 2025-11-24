#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "index/bitmap_idx.hpp"

namespace duckdb {

//! PhysicalBitmapIndexJoin represents a join using bitmap index for probe side
class PhysicalBitmapIndexJoin : public PhysicalComparisonJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::HASH_JOIN;

	struct JoinProjectionColumns {
		vector<idx_t> col_idxs;
		vector<LogicalType> col_types;
	};

public:
	PhysicalBitmapIndexJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left,
	                        PhysicalOperator &right, JoinType join_type, vector<JoinCondition> cond,
	                        vector<LogicalType> condition_types, idx_t bitmap_index_table_index,
	                        BitmapIndex *bitmap_index, DuckTableEntry *bitmap_index_table, bool build_on_left,
	                        vector<idx_t> left_projection_map, vector<idx_t> right_projection_map,
	                        vector<idx_t> right_table_col_indices = {});

	//! The types of the join keys
	vector<LogicalType> condition_types;

	//! The indices/types of the payload columns (from build side)
	JoinProjectionColumns payload_columns;
	//! The indices/types of the lhs columns that need to be output
	JoinProjectionColumns lhs_output_columns;
	//! The indices/types of the rhs columns that need to be output
	JoinProjectionColumns rhs_output_columns;

	//! Bitmap index reference
	BitmapIndex *bitmap_index;
	//! Table that has the bitmap index (for gathering rows)
	DuckTableEntry *bitmap_index_table;
	//! Which table has the bitmap index (0 = left, 1 = right)
	idx_t bitmap_index_table_index;
	//! Whether to build hash table on left side
	bool build_on_left;
	//! True if the probe side is the left child in the physical plan
	bool probe_side_is_left = true;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	
	// Override GetName to show this is a bitmap index join
	string GetName() const override {
		return "BITMAP_INDEX_JOIN";
	}

	bool ParallelOperator() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	// Operator Interface (for probe pipeline)
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;

protected:
	// CachingOperator Interface - receives LHS data in probe pipeline
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

private:
	// Forward declaration
	struct BitmapIndexJoinOperatorState;
	
	// Helper methods for ExecuteInternal
	OperatorResultType OutputBufferedMatches(ExecutionContext &context, DataChunk &chunk,
	                                         BitmapIndexJoinOperatorState &state) const;
	OperatorResultType OutputSemiJoinResult(DataChunk &input, DataChunk &chunk,
	                                        BitmapIndexJoinOperatorState &state) const;
	OperatorResultType OutputAntiJoinResult(DataChunk &input, DataChunk &chunk,
	                                       BitmapIndexJoinOperatorState &state) const;
	OperatorResultType OutputLeftJoinUnmatched(DataChunk &input, DataChunk &chunk,
	                                           BitmapIndexJoinOperatorState &state) const;

	bool ProbeSideIsLeft() const {
		return probe_side_is_left;
	}
};

// State classes for Execute (probe pipeline)
struct PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState : public CachingOperatorState {
	explicit BitmapIndexJoinOperatorState(ExecutionContext &context, const PhysicalBitmapIndexJoin &op);
	~BitmapIndexJoinOperatorState() override;

	// Expression executor for extracting join keys from probe side
	ExpressionExecutor probe_executor;
	// Buffer for join keys
	DataChunk join_keys;
	// Buffer for left side output
	DataChunk lhs_output;
	// Buffer for right side output (gathered from bitmap index)
	DataChunk rhs_output;
	// Row IDs from bitmap index lookup, grouped by left row index
	// Each entry is (left_row_index, vector<row_t>)
	vector<pair<idx_t, vector<row_t>>> row_id_matches;
	// Current position in row_id_matches
	idx_t match_pos = 0;
	// Current position within current match's row_ids
	idx_t row_id_pos = 0;
	// Track which left rows matched (for LEFT/SEMI/ANTI joins)
	vector<bool> left_row_matched;
	// Fetch state for gathering rows
	ColumnFetchState fetch_state;
	// Whether we have buffered matches to output
	bool has_buffered_matches = false;
};


} // namespace duckdb
