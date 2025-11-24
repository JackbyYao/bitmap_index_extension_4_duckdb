#include "index/bitmap_idx_join_physical.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include <unordered_set>
#include <algorithm>

namespace duckdb {

PhysicalBitmapIndexJoin::PhysicalBitmapIndexJoin(PhysicalPlan &physical_plan, LogicalOperator &op,
                                                 PhysicalOperator &left, PhysicalOperator &right, JoinType join_type,
                                                 vector<JoinCondition> cond, vector<LogicalType> condition_types,
                                                 idx_t bitmap_index_table_index, BitmapIndex *bitmap_index,
                                                 DuckTableEntry *bitmap_index_table, bool build_on_left,
                                                 vector<idx_t> left_projection_map, vector<idx_t> right_projection_map,
                                                 vector<idx_t> right_table_col_indices)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::HASH_JOIN, std::move(cond), join_type,
                             op.estimated_cardinality),
      condition_types(std::move(condition_types)), bitmap_index(bitmap_index),
      bitmap_index_table(bitmap_index_table), bitmap_index_table_index(bitmap_index_table_index),
      build_on_left(build_on_left) {
	// Disable chunk caching: bitmap lookup already produces compact batches
	this->caching_supported = false;

	children.push_back(left);
	children.push_back(right);

	// Determine which child represents the probe side
	if (children.size() == 2) {
		auto left_is_virtual = children[0].get().type == PhysicalOperatorType::DUMMY_SCAN;
		auto right_is_virtual = children[1].get().type == PhysicalOperatorType::DUMMY_SCAN;
		if (left_is_virtual != right_is_virtual) {
			probe_side_is_left = !left_is_virtual;
		} else {
			// Fallback to metadata if we cannot distinguish based on operator type
			probe_side_is_left = bitmap_index_table_index != 0;
		}
	}

	auto &lhs_input_types = children[0].get().GetTypes();
	auto &rhs_input_types = children[1].get().GetTypes();

	// Create projection maps for LHS - same as PhysicalHashJoin
	lhs_output_columns.col_idxs = left_projection_map;
	if (lhs_output_columns.col_idxs.empty()) {
		lhs_output_columns.col_idxs.reserve(lhs_input_types.size());
		for (idx_t i = 0; i < lhs_input_types.size(); i++) {
			lhs_output_columns.col_idxs.emplace_back(i);
		}
	}
	for (auto &lhs_col : lhs_output_columns.col_idxs) {
		lhs_output_columns.col_types.push_back(lhs_input_types[lhs_col]);
	}

	// For ANTI, SEMI and MARK join, we only need keys, so payload/RHS types are empty
	if (join_type == JoinType::ANTI || join_type == JoinType::SEMI || join_type == JoinType::MARK) {
		return;
	}

	// Create projection maps for RHS
	// If we have mapped table column indices and bitmap index is on right table, use them for data fetching
	if (!right_table_col_indices.empty() && bitmap_index_table_index == 1) {
		// Use mapped table column indices for fetching from table
		rhs_output_columns.col_idxs = right_table_col_indices;
		// Types should come from child operator, not table columns
		// Use right_projection_map to map from rhs_input_types
		// Note: right_projection_map should already be set by ColumnLifetimeAnalyzer in optimize_function stage
		for (auto &rhs_col : right_projection_map) {
			if (rhs_col < rhs_input_types.size()) {
				rhs_output_columns.col_types.push_back(rhs_input_types[rhs_col]);
				payload_columns.col_idxs.push_back(rhs_col);
				payload_columns.col_types.push_back(rhs_input_types[rhs_col]);
			}
		}
	} else {
		// Use child output column indices and types (same as PhysicalHashJoin)
		auto right_projection_map_copy = right_projection_map;
		if (right_projection_map_copy.empty()) {
			right_projection_map_copy.reserve(rhs_input_types.size());
			for (idx_t i = 0; i < rhs_input_types.size(); i++) {
				right_projection_map_copy.emplace_back(i);
			}
		}
		for (auto &rhs_col : right_projection_map_copy) {
			rhs_output_columns.col_idxs.push_back(rhs_col);
			rhs_output_columns.col_types.push_back(rhs_input_types[rhs_col]);
			payload_columns.col_idxs.push_back(rhs_col);
			payload_columns.col_types.push_back(rhs_input_types[rhs_col]);
		}
	}
}

InsertionOrderPreservingMap<string> PhysicalBitmapIndexJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);
	result["Bitmap Index"] = bitmap_index ? bitmap_index->name : "NULL";
	result["Index Table"] = bitmap_index_table_index == 0 ? "LEFT" : "RIGHT";
	result["Build On Left"] = build_on_left ? "true" : "false";
	result["Probe Side"] = probe_side_is_left ? "LEFT" : "RIGHT";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

unique_ptr<GlobalOperatorState> PhysicalBitmapIndexJoin::GetGlobalOperatorState(ClientContext &context) const {
	// Bitmap index is read-only, no global state needed
	return make_uniq<GlobalOperatorState>();
}

unique_ptr<OperatorState> PhysicalBitmapIndexJoin::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState>(context, *this);
}

// OperatorState for Execute (probe pipeline)
PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState::BitmapIndexJoinOperatorState(ExecutionContext &context,
                                                            const PhysicalBitmapIndexJoin &op)
    : probe_executor(context.client) {
	auto &allocator = BufferAllocator::Get(context.client);
	
	// Add expressions for extracting join keys from the probe side
	// Collect types from the actual expressions we're adding
	vector<LogicalType> probe_key_types;
	const bool probe_left = op.ProbeSideIsLeft();
	for (auto &cond : op.conditions) {
		auto &expr = probe_left ? *cond.left : *cond.right;
		probe_executor.AddExpression(expr);
		probe_key_types.push_back(expr.return_type);
	}
	
	// Initialize join_keys with the types of the expressions we actually added
	join_keys.Initialize(allocator, probe_key_types);
	
	if (!op.lhs_output_columns.col_types.empty()) {
		lhs_output.Initialize(allocator, op.lhs_output_columns.col_types);
	}
	if (!op.rhs_output_columns.col_types.empty()) {
		rhs_output.Initialize(allocator, op.rhs_output_columns.col_types);
	}
}

PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState::~BitmapIndexJoinOperatorState() {
}


void PhysicalBitmapIndexJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	
	// Follow the same pattern as PhysicalHashJoin::BuildJoinPipelines
	// Add this operator to the current pipeline (probe pipeline)
	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	
	// Continue building the current pipeline on the LHS (probe side)
	children[0].get().BuildPipelines(current, meta_pipeline);
	
	// No need to create child pipeline since we're a regular operator (not a source)
	// The right side (build side) is handled by PhysicalBitmapIndexLookup which is a virtual operator
}


OperatorResultType PhysicalBitmapIndexJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, GlobalOperatorState &gstate,
                                                            OperatorState &state_p) const {
	auto &state = state_p.Cast<PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState>();

	// If we have buffered matches, output them first
	// Continue processing even if input.size() == 0, as we have all needed data in lhs_output and row_id_matches
	if (state.has_buffered_matches && state.match_pos < state.row_id_matches.size()) {
		return OutputBufferedMatches(context, chunk, state);
	}

	// No buffered matches left: need more input
	if (input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Extract join keys from input
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

	// Store left side output for later
	state.lhs_output.ReferenceColumns(input, lhs_output_columns.col_idxs);

	// Initialize match tracking for this input chunk
	state.left_row_matched.assign(input.size(), false);
	state.row_id_matches.clear();
	state.match_pos = 0;
	state.row_id_pos = 0;

	// For each row in the input chunk, lookup bitmap index
	for (idx_t row = 0; row < state.join_keys.size(); row++) {
		// Get the key value(s) - for now, handle single key case
		if (state.join_keys.ColumnCount() == 0) {
			continue;
		}

		// Check for NULL - NULL keys don't match in equality joins
		if (FlatVector::IsNull(state.join_keys.data[0], row)) {
			continue;
		}

		// Get the key value
		Value key_value = state.join_keys.data[0].GetValue(row);

		// Convert to dictionary ID if VARCHAR
		int dict_id = -1;
		if (key_value.type().id() == LogicalTypeId::VARCHAR) {
			dict_id = bitmap_index->LookupValueId(key_value.GetValue<string>());
		} else {
			// For numeric types, try to convert to int
			try {
				dict_id = key_value.GetValue<int>();
			} catch (...) {
				// Skip if conversion fails
				continue;
			}
		}

		if (dict_id < 0) {
			// Key not found in index
			continue;
		}

		// Get row IDs for this key value
		vector<row_t> key_row_ids;
		bitmap_index->bitmap_table->GetRowsForValue(dict_id, key_row_ids);

		// Store matches for this left row
		if (!key_row_ids.empty()) {
			state.left_row_matched[row] = true;
			// Sort for efficient gathering
			std::sort(key_row_ids.begin(), key_row_ids.end());
			state.row_id_matches.emplace_back(row, std::move(key_row_ids));
		}
	}

	// Handle different join types
	if (join_type == JoinType::SEMI) {
		return OutputSemiJoinResult(input, chunk, state);
	} else if (join_type == JoinType::ANTI) {
		return OutputAntiJoinResult(input, chunk, state);
	}

	// INNER and LEFT joins: output matches
	if (!state.row_id_matches.empty()) {
		state.has_buffered_matches = true;
		return OutputBufferedMatches(context, chunk, state);
	}

	// No matches for this chunk
	// For LEFT join, output unmatched left rows with NULL right columns
	if (join_type == JoinType::LEFT) {
		return OutputLeftJoinUnmatched(input, chunk, state);
	}

	// INNER join with no matches: need more input
	chunk.SetCardinality(0);
	return OperatorResultType::NEED_MORE_INPUT;
}

// Helper method to output buffered matches
OperatorResultType PhysicalBitmapIndexJoin::OutputBufferedMatches(ExecutionContext &context, DataChunk &chunk,
                                                                 PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState &state) const {
	// Get current match
	auto &match = state.row_id_matches[state.match_pos];
	idx_t left_row_idx = match.first;
	auto &row_ids = match.second;

	// Prepare row ID vector for fetching
	idx_t remaining = row_ids.size() - state.row_id_pos;
	idx_t fetch_count = MinValue<idx_t>(remaining, NumericCast<idx_t>(STANDARD_VECTOR_SIZE));
	idx_t row_ids_to_process = fetch_count;  // 保存原始值，用于状态更新
	
	Vector row_id_vector(LogicalType::ROW_TYPE);
	auto row_id_data = FlatVector::GetData<row_t>(row_id_vector);
	for (idx_t i = 0; i < fetch_count; i++) {
		row_id_data[i] = row_ids[state.row_id_pos + i];
	}
	row_id_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetValidity(row_id_vector, ValidityMask(fetch_count));

	// Gather rows from bitmap index table
	auto &tx = DuckTransaction::Get(context.client, bitmap_index_table->catalog);
	auto &storage = bitmap_index_table->GetStorage();

	// Fetch right table columns
	if (!rhs_output_columns.col_idxs.empty() && bitmap_index_table_index == 1) {
		// Convert table column indices to storage indices
		vector<StorageIndex> storage_indices;
		auto &columns = bitmap_index_table->GetColumns();
		for (auto table_col_idx : rhs_output_columns.col_idxs) {
			if (table_col_idx < columns.LogicalColumnCount()) {
				auto physical_idx = columns.LogicalToPhysical(LogicalIndex(table_col_idx));
				storage_indices.push_back(StorageIndex(physical_idx.index));
			}
		}
		if (!storage_indices.empty()) {
			// Ensure rhs_output 的向量类型正确（Fetch 要求 FLAT_VECTOR）
			state.rhs_output.Reset();
			for (idx_t i = 0; i < state.rhs_output.ColumnCount(); i++) {
				state.rhs_output.data[i].SetVectorType(VectorType::FLAT_VECTOR);
			}
			
			storage.Fetch(tx, state.rhs_output, storage_indices, row_id_vector, fetch_count,
			             state.fetch_state);
			// Fetch 已经设置了正确的 cardinality（实际 fetch 到的行数）
			// 绝对不要覆盖它！使用 Fetch 返回的实际行数
			idx_t actual_fetched_count = state.rhs_output.size();
			
			if (actual_fetched_count == 0) {
				// 没有 fetch 到任何数据，跳过这些 row_ids，继续下一个
				state.row_id_pos += fetch_count;
				if (state.row_id_pos >= row_ids.size()) {
					state.match_pos++;
					state.row_id_pos = 0;
					if (state.match_pos >= state.row_id_matches.size()) {
						state.has_buffered_matches = false;
						return OperatorResultType::NEED_MORE_INPUT;
					}
				}
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			
			// 使用实际 fetch 到的行数
			fetch_count = actual_fetched_count;
		} else {
			// 如果没有需要 fetch 的列，设置 cardinality 为 0
			state.rhs_output.SetCardinality(0);
			fetch_count = 0;
		}
	} else {
		// 如果不需要 fetch right table 的列，设置 cardinality 为 0
		state.rhs_output.SetCardinality(0);
		fetch_count = 0;
	}

	// 如果 fetch_count 为 0，直接返回
	if (fetch_count == 0) {
		// 即使不需要 fetch right columns，我们仍然需要更新状态
		// 使用 row_ids_to_process 来更新状态，跳过当前这批 row_ids
		state.row_id_pos += row_ids_to_process;
		if (state.row_id_pos >= row_ids.size()) {
			// Move to next match
			state.match_pos++;
			state.row_id_pos = 0;
			if (state.match_pos >= state.row_id_matches.size()) {
				// All matches output
				state.has_buffered_matches = false;
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	// Build output chunk: left columns + right columns
	// fetch_count 已经是实际 fetch 到的行数（来自 rhs_output.size()）
	chunk.Reset();
	chunk.SetCardinality(fetch_count);

	// Ensure vectors are flat and properly initialized
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
	}

	// Copy left side columns - repeat left row for each match
	for (idx_t i = 0; i < lhs_output_columns.col_idxs.size(); i++) {
		auto left_value = state.lhs_output.data[i].GetValue(left_row_idx);
		for (idx_t j = 0; j < fetch_count; j++) {
			chunk.data[i].SetValue(j, left_value);
		}
	}

	// Copy right side columns via deep copy to chunk-owned buffers
	// fetch_count 是实际 fetch 到的行数，直接使用
	idx_t offset = lhs_output_columns.col_idxs.size();
	for (idx_t i = 0; i < rhs_output_columns.col_idxs.size(); i++) {
		chunk.data[offset + i].SetVectorType(VectorType::FLAT_VECTOR);
		VectorOperations::Copy(state.rhs_output.data[i], chunk.data[offset + i], fetch_count, 0, 0);
	}

	state.row_id_pos += fetch_count;
	if (state.row_id_pos >= row_ids.size()) {
		// Move to next match
		state.match_pos++;
		state.row_id_pos = 0;
		if (state.match_pos >= state.row_id_matches.size()) {
			// All matches output
			state.has_buffered_matches = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

// Helper method to output SEMI join results
OperatorResultType PhysicalBitmapIndexJoin::OutputSemiJoinResult(DataChunk &input, DataChunk &chunk,
                                                                 PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState &state) const {
	// SEMI join: output only left rows that matched (one row per match)
	chunk.Reset();
	// Ensure vectors are flat
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
	}
	idx_t output_count = 0;
	for (idx_t row = 0; row < input.size() && output_count < STANDARD_VECTOR_SIZE; row++) {
		if (state.left_row_matched[row]) {
			// Copy left row to output
			for (idx_t col = 0; col < lhs_output_columns.col_idxs.size(); col++) {
				chunk.data[col].SetValue(output_count, state.lhs_output.data[col].GetValue(row));
			}
			output_count++;
		}
	}
	chunk.SetCardinality(output_count);
	return output_count > 0 ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::NEED_MORE_INPUT;
}

// Helper method to output ANTI join results
OperatorResultType PhysicalBitmapIndexJoin::OutputAntiJoinResult(DataChunk &input, DataChunk &chunk,
                                                                  PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState &state) const {
	// ANTI join: output only left rows that didn't match
	chunk.Reset();
	// Ensure vectors are flat
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
	}
	idx_t output_count = 0;
	for (idx_t row = 0; row < input.size() && output_count < STANDARD_VECTOR_SIZE; row++) {
		if (!state.left_row_matched[row]) {
			// Copy left row to output
			for (idx_t col = 0; col < lhs_output_columns.col_idxs.size(); col++) {
				chunk.data[col].SetValue(output_count, state.lhs_output.data[col].GetValue(row));
			}
			output_count++;
		}
	}
	chunk.SetCardinality(output_count);
	return output_count > 0 ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::NEED_MORE_INPUT;
}

// Helper method to output LEFT join unmatched rows
OperatorResultType PhysicalBitmapIndexJoin::OutputLeftJoinUnmatched(DataChunk &input, DataChunk &chunk,
                                                                    PhysicalBitmapIndexJoin::BitmapIndexJoinOperatorState &state) const {
	// LEFT join: output unmatched left rows with NULL right columns
	chunk.Reset();
	// Ensure vectors are flat
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
	}
	idx_t output_count = 0;
	for (idx_t row = 0; row < input.size() && output_count < STANDARD_VECTOR_SIZE; row++) {
		if (!state.left_row_matched[row]) {
			// Copy left row
			for (idx_t col = 0; col < lhs_output_columns.col_idxs.size(); col++) {
				chunk.data[col].SetValue(output_count, state.lhs_output.data[col].GetValue(row));
			}
			// Set right columns to NULL
			idx_t offset = lhs_output_columns.col_idxs.size();
			for (idx_t col = 0; col < rhs_output_columns.col_idxs.size(); col++) {
				chunk.data[offset + col].SetValue(output_count, Value());
			}
			output_count++;
		}
	}
	chunk.SetCardinality(output_count);

	if (output_count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
