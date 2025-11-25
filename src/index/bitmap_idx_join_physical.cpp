#include "index/bitmap_idx_join_physical.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
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
#include "duckdb/common/limits.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include <unordered_set>
#include <algorithm>
#include <iostream>

namespace duckdb {

PhysicalBitmapIndexJoin::PhysicalBitmapIndexJoin(PhysicalPlan &physical_plan, LogicalOperator &op,
                                                 PhysicalOperator &left, PhysicalOperator &right, JoinType join_type,
                                                 vector<JoinCondition> cond, vector<LogicalType> condition_types,
                                                 vector<LogicalType> left_output_meta_p,
                                                 vector<LogicalType> right_output_meta_p, idx_t bitmap_index_table_index,
                                                 BitmapIndex *bitmap_index, DuckTableEntry *bitmap_index_table,
                                                 bool build_on_left, vector<idx_t> left_projection_map,
                                                 vector<idx_t> right_projection_map,
                                                 vector<idx_t> left_table_col_indices_p,
                                                 vector<idx_t> right_table_col_indices_p)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::HASH_JOIN, std::move(cond), join_type,
                             op.estimated_cardinality),
      condition_types(std::move(condition_types)), left_output_meta(std::move(left_output_meta_p)),
      right_output_meta(std::move(right_output_meta_p)), left_table_col_indices(std::move(left_table_col_indices_p)),
      right_table_col_indices(std::move(right_table_col_indices_p)), bitmap_index(bitmap_index),
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
			// Fallback to the build flag if we cannot distinguish based on operator type
			probe_side_is_left = !build_on_left;
		}
	}

	// Use types from this->types which were already resolved by the logical operator
	// The logical operator applied MapTypes with projection maps, so these are the correct output types
	// For non-SEMI/ANTI/MARK joins: this->types = [left_types..., right_types...]
	// For SEMI/ANTI/MARK joins: this->types = [left_types...] (or [left_types..., BOOLEAN] for MARK)
	
	// Calculate left output type count
	idx_t left_output_type_count = 0;
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI || join_type == JoinType::MARK) {
		left_output_type_count = left_output_meta.size();
	} else {
		left_output_type_count = left_output_meta.size();
	}
	D_ASSERT(left_output_type_count <= this->types.size());
	
	// Extract left and right types using provided metadata
	vector<LogicalType> left_output_types = left_output_meta;
	vector<LogicalType> right_output_types = right_output_meta;
	
	// Create projection maps for left side (logical left child)
	lhs_output_columns.col_idxs = left_projection_map;
	if (lhs_output_columns.col_idxs.empty()) {
		// No projection map - use all columns
		lhs_output_columns.col_idxs.reserve(left_output_types.size());
		for (idx_t i = 0; i < left_output_types.size(); i++) {
			lhs_output_columns.col_idxs.emplace_back(i);
		}
	}
	// Use the resolved types from logical operator (already applied projection map)
	lhs_output_columns.col_types = left_output_types;

	// For ANTI, SEMI and MARK join, we only need keys, so payload/RHS types are empty
	if (join_type == JoinType::ANTI || join_type == JoinType::SEMI || join_type == JoinType::MARK) {
		return;
	}

	// Create projection maps for right side
	auto right_projection_map_copy = right_projection_map;
	if (right_projection_map_copy.empty()) {
		// No projection map - use all columns
		right_projection_map_copy.reserve(right_output_types.size());
		for (idx_t i = 0; i < right_output_types.size(); i++) {
			right_projection_map_copy.emplace_back(i);
		}
	}
	// Use the resolved types from logical operator (already applied projection map)
	for (idx_t i = 0; i < right_projection_map_copy.size(); i++) {
		rhs_output_columns.col_idxs.push_back(right_projection_map_copy[i]);
		rhs_output_columns.col_types.push_back(right_output_types[i]);
	}

	// Determine which actual table column indices to use when fetching
	if (build_on_left) {
		if (left_table_col_indices.empty()) {
			left_table_col_indices = lhs_output_columns.col_idxs;
		}
		build_table_col_indices = left_table_col_indices;
	} else {
		if (right_table_col_indices.empty()) {
			right_table_col_indices = rhs_output_columns.col_idxs;
		}
		build_table_col_indices = right_table_col_indices;
	}

	// Payload columns correspond to the build side (only meaningful when there are RHS outputs)
	if (build_on_left) {
		payload_columns.col_idxs = lhs_output_columns.col_idxs;
		payload_columns.col_types = lhs_output_columns.col_types;
	} else {
		payload_columns.col_idxs = rhs_output_columns.col_idxs;
		payload_columns.col_types = rhs_output_columns.col_types;
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
	
	// Continue building the current pipeline on the probe side
	// Probe side is determined by probe_side_is_left, not always children[0]
	auto &probe_child = probe_side_is_left ? children[0] : children[1];
	probe_child.get().BuildPipelines(current, meta_pipeline);
	
	// No need to create child pipeline since we're a regular operator (not a source)
	// The build side is handled by PhysicalBitmapIndexLookup which is a virtual operator
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
	if (probe_side_is_left) {
		state.lhs_output.ReferenceColumns(input, lhs_output_columns.col_idxs);
		state.lhs_output.SetCardinality(input.size());
	} else {
		state.rhs_output.ReferenceColumns(input, rhs_output_columns.col_idxs);
		state.rhs_output.SetCardinality(input.size());
	}

	// Initialize match tracking for this input chunk
	state.left_row_matched.assign(input.size(), false);
	state.row_id_matches.clear();
	state.match_pos = 0;
	state.row_id_pos = 0;

	// Get the key value(s) - for now, handle single key case
	if (state.join_keys.ColumnCount() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Use UnifiedVectorFormat to safely handle all Vector types (FLAT, DICTIONARY, CONSTANT, etc.)
	// This is critical for release mode where D_ASSERT is disabled and FlatVector::IsNull() may fail
	// on non-FLAT vectors. Reference: null_operations.cpp uses ToUnifiedFormat for safe access.
	auto &key_vector = state.join_keys.data[0];
	const LogicalType *target_key_type = condition_types.empty() ? nullptr : &condition_types[0];
	if (target_key_type && key_vector.GetType() != *target_key_type) {
		if (!state.cast_key_vector || state.cast_key_vector->GetType() != *target_key_type) {
			state.cast_key_vector = make_uniq<Vector>(*target_key_type);
		}
		VectorOperations::Cast(context.client, key_vector, *state.cast_key_vector, state.join_keys.size());
		key_vector.Reference(*state.cast_key_vector);
	}

	UnifiedVectorFormat key_data;
	key_vector.ToUnifiedFormat(state.join_keys.size(), key_data);

	// For each row in the input chunk, lookup bitmap index
	for (idx_t row = 0; row < state.join_keys.size(); row++) {
		// Get the actual index using selection vector (handles DICTIONARY_VECTOR correctly)
		auto key_idx = key_data.sel->get_index(row);

		// Check for NULL - NULL keys don't match in equality joins
		// Use UnifiedVectorFormat's validity mask (works for all vector types)
		if (!key_data.validity.RowIsValid(key_idx)) {
			continue;
		}

		// Materialize the key value for type inspection/conversion
		Value key_value = key_vector.GetValue(key_idx);
		auto key_value_type = key_value.type();
		auto key_value_type_id = key_value_type.id();
		auto expected_type_id = target_key_type ? target_key_type->id() : key_value_type_id;

		// Convert to dictionary ID
		int dict_id = -1;
		
		switch (expected_type_id) {
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR: {
			auto string_value = key_value_type_id == LogicalTypeId::VARCHAR || key_value_type_id == LogicalTypeId::CHAR
			                        ? key_value
			                        : key_value.DefaultCastAs(LogicalType::VARCHAR);
			string str_value = string_value.GetValue<string>();
			dict_id = bitmap_index->LookupValueId(str_value);
			break;
		}
		case LogicalTypeId::ENUM: {
			string str_value;
			if (key_value_type_id == LogicalTypeId::ENUM) {
				str_value = EnumType::GetValue(key_value);
			} else {
				auto string_value = key_value.DefaultCastAs(LogicalType::VARCHAR);
				str_value = string_value.GetValue<string>();
			}
			dict_id = bitmap_index->LookupValueId(str_value);
			break;
		}
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT: {
			auto numeric_value = key_value_type_id == expected_type_id
			                         ? key_value
			                         : key_value.DefaultCastAs(LogicalType::BIGINT);
			int64_t sv = numeric_value.GetValue<int64_t>();
			if (sv < NumericLimits<int32_t>::Minimum() || sv > NumericLimits<int32_t>::Maximum()) {
				continue;
			}
			dict_id = static_cast<int32_t>(sv);
			break;
		}
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT: {
			auto numeric_value = key_value_type_id == expected_type_id
			                         ? key_value
			                         : key_value.DefaultCastAs(LogicalType::UBIGINT);
			uint64_t uv = numeric_value.GetValue<uint64_t>();
			if (uv > static_cast<uint64_t>(NumericLimits<int32_t>::Maximum())) {
				continue;
			}
			dict_id = static_cast<int32_t>(uv);
			break;
		}
		default:
			throw InvalidInputException("Bitmap index join does not support probe key type \"%s\"",
			                            target_key_type ? target_key_type->ToString() : key_value_type.ToString());
		}

		if (dict_id < 0) {
			throw InvalidInputException("Bitmap index \"%s\" does not contain value %s",
			                            bitmap_index ? bitmap_index->name : "UNKNOWN",
			                            key_value.ToString());
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
	// Get current match (row index corresponds to the probe input chunk)
	auto &match = state.row_id_matches[state.match_pos];
	idx_t probe_row_idx = match.first;
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

	// Determine build/probe chunks
	const bool build_is_left = build_on_left;
	auto &build_chunk = build_is_left ? state.lhs_output : state.rhs_output;
	auto &build_columns = build_is_left ? lhs_output_columns : rhs_output_columns;

	// Fetch build side columns from table when required
	if (!build_columns.col_idxs.empty()) {
		// Convert table column indices to storage indices
		vector<StorageIndex> storage_indices;
		auto &columns = bitmap_index_table->GetColumns();
		const auto &table_col_indices =
		    build_table_col_indices.empty() ? build_columns.col_idxs : build_table_col_indices;
		for (auto table_col_idx : table_col_indices) {
			if (table_col_idx < columns.LogicalColumnCount()) {
				auto physical_idx = columns.LogicalToPhysical(LogicalIndex(table_col_idx));
				storage_indices.push_back(StorageIndex(physical_idx.index));
			}
		}
		if (!storage_indices.empty()) {
			build_chunk.Reset();
			for (idx_t i = 0; i < build_chunk.ColumnCount(); i++) {
				build_chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
			}

			storage.Fetch(tx, build_chunk, storage_indices, row_id_vector, fetch_count, state.fetch_state);
			// Fetch 已经设置了正确的 cardinality（实际 fetch 到的行数）
			// 绝对不要覆盖它！使用 Fetch 返回的实际行数
			idx_t actual_fetched_count = build_chunk.size();
			
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
			build_chunk.SetCardinality(0);
			fetch_count = 0;
		}
	} else {
		// 如果不需要 fetch build table 的列，设置 cardinality 为 0
		build_chunk.SetCardinality(0);
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

	auto repeat_probe_row = [&](DataChunk &source_chunk, idx_t col_idx, Vector &dest) {
		auto &src = source_chunk.data[col_idx];
		SelectionVector sel(fetch_count);
		for (idx_t j = 0; j < fetch_count; j++) {
			sel.set_index(j, probe_row_idx);
		}
		dest.Reference(src);
		dest.Slice(sel, fetch_count);
	};

	// Copy left side columns
	for (idx_t i = 0; i < lhs_output_columns.col_idxs.size(); i++) {
		auto &dst = chunk.data[i];
		if (build_is_left) {
			VectorOperations::Copy(state.lhs_output.data[i], dst, fetch_count, 0, 0);
		} else {
			repeat_probe_row(state.lhs_output, i, dst);
		}
	}

	// Copy right side columns via deep copy to chunk-owned buffers
	idx_t offset = lhs_output_columns.col_idxs.size();
	for (idx_t i = 0; i < rhs_output_columns.col_idxs.size(); i++) {
		auto &dst = chunk.data[offset + i];
		dst.SetVectorType(VectorType::FLAT_VECTOR);
		if (!build_is_left) {
			VectorOperations::Copy(state.rhs_output.data[i], dst, fetch_count, 0, 0);
		} else {
			repeat_probe_row(state.rhs_output, i, dst);
		}
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
