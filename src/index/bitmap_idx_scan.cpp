#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"
#include "index/bitmap_idx_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include <algorithm>
#include <string>

namespace duckdb {

BindInfo BitmapIndexScanBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	return BindInfo(bind_data.table);
}

//-------------------------------------------------------------------------
// Global State
//-------------------------------------------------------------------------
struct BitmapIndexScanGlobalState final : public GlobalTableFunctionState {
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
	vector<idx_t> projection_ids;

	TableScanState local_storage_state;
	vector<StorageIndex> column_ids;

	// Index scan state
	unique_ptr<IndexScanState> index_state;
	Vector row_ids = Vector(LogicalType::ROW_TYPE);

	struct RowGroupBatch {
		idx_t row_group_index = DConstants::INVALID_INDEX;
		vector<row_t> row_ids;
	};
	vector<RowGroupBatch> batches;
	idx_t next_batch = 0;
};

static void BuildRowGroupBatches(BitmapIndexScanGlobalState &state, idx_t row_group_size, idx_t fetch_count) {
	state.batches.clear();
	state.next_batch = 0;
	if (fetch_count == 0) {
		return;
	}
	auto row_id_ptr = FlatVector::GetData<row_t>(state.row_ids);
	vector<row_t> sorted_ids(row_id_ptr, row_id_ptr + fetch_count);
	std::sort(sorted_ids.begin(), sorted_ids.end());

	idx_t idx = 0;
	while (idx < sorted_ids.size()) {
		auto row_id = sorted_ids[idx];
		idx_t group_idx = UnsafeNumericCast<idx_t>(row_id) / row_group_size;
		BitmapIndexScanGlobalState::RowGroupBatch batch;
		batch.row_group_index = group_idx;
		while (idx < sorted_ids.size()) {
			auto current = sorted_ids[idx];
			auto current_group = UnsafeNumericCast<idx_t>(current) / row_group_size;
			if (current_group != group_idx || batch.row_ids.size() >= STANDARD_VECTOR_SIZE) {
				break;
			}
			batch.row_ids.push_back(current);
			idx++;
		}
		state.batches.push_back(std::move(batch));
	}
}

static void GatherRows(RowGroup &row_group, TransactionData &transaction_data, const vector<StorageIndex> &column_ids,
                       const SelectionVector &sel, idx_t valid_count, idx_t local_vector_index, DataChunk &result,
                       idx_t offset) {
	// Boundary check: ensure we don't write beyond STANDARD_VECTOR_SIZE
	D_ASSERT(offset + valid_count <= STANDARD_VECTOR_SIZE);
	if (offset + valid_count > STANDARD_VECTOR_SIZE) {
		return;
	}
	
	// Safety check: ensure result.data is initialized and has enough columns
	if (result.data.empty() || result.ColumnCount() < column_ids.size()) {
		return;
	}
	
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto &column = column_ids[col_idx];
		auto &result_vector = result.data[col_idx];
		// Ensure the vector is flat and has enough capacity
		result_vector.Flatten(STANDARD_VECTOR_SIZE);
		D_ASSERT(result_vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto &col_data = row_group.GetColumnRef(column);

		ColumnScanState scan_state;
		scan_state.Initialize(col_data.type, column.GetChildIndexes(), nullptr);
		auto vector_start = row_group.start + local_vector_index * STANDARD_VECTOR_SIZE;
		col_data.InitializeScan(scan_state);
		col_data.InitializeScanWithOffset(scan_state, vector_start);

		Vector temp(col_data.type);
		auto global_vector_index = vector_start / STANDARD_VECTOR_SIZE;
		col_data.Scan(transaction_data, global_vector_index, scan_state, temp);
		// Copy directly using the selection vector - temp contains the full vector,
		// and sel contains the indices we want to copy
		VectorOperations::Copy(temp, result_vector, sel, valid_count, 0, offset, valid_count);
	}
}

static idx_t ConsumeBatch(DataTable &storage, DuckTransaction &transaction, const vector<StorageIndex> &column_ids,
                          BitmapIndexScanGlobalState::RowGroupBatch &batch, DataChunk &result, idx_t offset) {
	if (batch.row_group_index == DConstants::INVALID_INDEX || batch.row_ids.empty()) {
		return offset;
	}
	auto &collection = storage.GetRowGroups();
	auto row_group = collection.GetRowGroup(UnsafeNumericCast<int64_t>(batch.row_group_index));
	if (!row_group) {
		return offset;
	}
	TransactionData transaction_data(transaction);

	idx_t batch_offset = 0;
	while (batch_offset < batch.row_ids.size() && offset < STANDARD_VECTOR_SIZE) {
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t valid_count = 0;
		idx_t local_vector_index = DConstants::INVALID_INDEX;
		idx_t chunk_start = 0;

		while (batch_offset < batch.row_ids.size() && valid_count < STANDARD_VECTOR_SIZE &&
		       offset + valid_count < STANDARD_VECTOR_SIZE) {
			auto row_id = batch.row_ids[batch_offset];
			auto local_row = UnsafeNumericCast<idx_t>(row_id) - row_group->start;
			idx_t current_vector = local_row / STANDARD_VECTOR_SIZE;
			if (local_vector_index == DConstants::INVALID_INDEX) {
				local_vector_index = current_vector;
				chunk_start = local_vector_index * STANDARD_VECTOR_SIZE;
			} else if (current_vector != local_vector_index) {
				break;
			}
			if (row_group->Fetch(transaction_data, local_row)) {
				sel.set_index(valid_count++, UnsafeNumericCast<sel_t>(local_row - chunk_start));
			}
			batch_offset++;
		}

		if (valid_count == 0) {
			local_vector_index = DConstants::INVALID_INDEX;
			continue;
		}

		// Ensure we don't exceed STANDARD_VECTOR_SIZE before calling GatherRows
		D_ASSERT(offset + valid_count <= STANDARD_VECTOR_SIZE);
		if (offset + valid_count > STANDARD_VECTOR_SIZE) {
			break;
		}

		GatherRows(*row_group, transaction_data, column_ids, sel, valid_count, local_vector_index, result, offset);
		offset += valid_count;
	}
	return offset;
}

static unique_ptr<GlobalTableFunctionState> BitmapIndexScanInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BitmapIndexScanBindData>();
	auto result = make_uniq<BitmapIndexScanGlobalState>();

	// Setup the scan state for the local storage
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	result->column_ids.reserve(input.column_ids.size());

	// Figure out the storage column ids
	for (auto &id : input.column_ids) {
		storage_t col_id = id;
		if (id != DConstants::INVALID_INDEX) {
			col_id = bind_data.table.GetColumn(LogicalIndex(id)).StorageOid();
		}
		result->column_ids.emplace_back(col_id);
	}

	// Initialize local storage scan
	result->local_storage_state.Initialize(result->column_ids, context, input.filters);
	local_storage.InitializeScan(bind_data.table.GetStorage(),
	                             result->local_storage_state.local_state,
	                             input.filters);

	// Initialize the scan state for the index
	//result->index_state = bind_data.index.Cast<BitmapIndex>().InitializeScan();
	result->index_state = bind_data.index.Cast<BitmapIndex>().InitializeScan(&bind_data.filter_value);
	
	// We need this to project out what we scan from the underlying table.
	result->projection_ids = input.projection_ids;

	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	const auto &columns = duck_table.GetColumns();

	// Always initialize all_columns since we use it in BitmapIndexScanExecute
	// even when CanRemoveFilterColumns() returns false
	vector<LogicalType> scanned_types;
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	result->all_columns.Initialize(context, scanned_types);

	return std::move(result);
}

//-------------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------------
static void BitmapIndexScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<BitmapIndexScanBindData>();
	auto &state = data_p.global_state->Cast<BitmapIndexScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	auto &local_storage = LocalStorage::Get(transaction);

	auto ensure_batches = [&]() -> bool {
		if (state.next_batch < state.batches.size()) {
			return true;
		}
		state.batches.clear();
		state.next_batch = 0;
		auto row_count = bind_data.index.Cast<BitmapIndex>().Scan(*state.index_state, state.row_ids);
		if (row_count == 0) {
			return false;
		}
		auto row_group_size = storage.GetRowGroupSize();
		if (row_group_size == 0) {
			row_group_size = 1;
		}
		BuildRowGroupBatches(state, row_group_size, row_count);
		return !state.batches.empty();
	};

	auto &target_chunk = state.all_columns;
	
	// Use do-while loop similar to table_scan.cpp
	// The loop continues until we get data or confirm there's no more data
	// Unlike table_scan which uses NextParallelScan, we use ensure_batches() to get more data
	do {
		if (context.interrupted) {
			throw InterruptException();
		}
		
		target_chunk.Reset();
		idx_t produced = 0;
		
		// Try to get data from batches
		while (produced < STANDARD_VECTOR_SIZE) {
			if (!ensure_batches()) {
				// No more batches from index, break to try local_storage
				break;
			}
			
			while (state.next_batch < state.batches.size() && produced < STANDARD_VECTOR_SIZE) {
				auto &batch = state.batches[state.next_batch];
				produced = ConsumeBatch(storage, transaction, state.column_ids, batch, target_chunk, produced);
				state.next_batch++;
			}
		}
		
		target_chunk.SetCardinality(produced);
		
		// If we got data from batches, process and return
		if (produced > 0) {
			if (state.projection_ids.empty()) {
				output.Move(target_chunk);
			} else {
				// Ensure output is initialized with correct column count
				if (output.ColumnCount() != state.projection_ids.size()) {
					vector<LogicalType> output_types;
					output_types.reserve(state.projection_ids.size());
					for (auto &proj_id : state.projection_ids) {
						if (proj_id >= target_chunk.ColumnCount()) {
							throw InternalException("Projection index %llu out of range (column count: %llu)", 
							                        proj_id, target_chunk.ColumnCount());
						}
						output_types.push_back(target_chunk.data[proj_id].GetType());
					}
					output.Initialize(context, output_types);
				}
				output.ReferenceColumns(target_chunk, state.projection_ids);
			}
			if (output.size() > 0) {
				return;
			}
		}
		
		// No data from batches, try local_storage (only when batches are exhausted)
		// This follows the pattern from DuckIndexScanState::TableScanFunc
		if (produced == 0) {
			state.all_columns.Reset();
			if (state.projection_ids.empty()) {
				local_storage.Scan(state.local_storage_state.local_state, state.column_ids, output);
			} else {
				local_storage.Scan(state.local_storage_state.local_state, state.column_ids, state.all_columns);
				// Ensure output is initialized with correct column count
				if (output.ColumnCount() != state.projection_ids.size()) {
					vector<LogicalType> output_types;
					output_types.reserve(state.projection_ids.size());
					for (auto &proj_id : state.projection_ids) {
						if (proj_id >= state.all_columns.ColumnCount()) {
							throw InternalException("Projection index %llu out of range (column count: %llu)", 
							                        proj_id, state.all_columns.ColumnCount());
						}
						output_types.push_back(state.all_columns.data[proj_id].GetType());
					}
					output.Initialize(context, output_types);
				}
				output.ReferenceColumns(state.all_columns, state.projection_ids);
			}
			if (output.size() > 0) {
				return;
			}
		}
		
		// No data from either batches or local_storage, and no more batches available
		// This means we're done - return empty result
		// The function will be called again by the executor if there might be more data
		// We exit the loop here because ensure_batches() returned false, meaning no more index data
		return;
	} while (false); // Only execute once per function call, but use do-while for consistency with table_scan pattern
}

//-------------------------------------------------------------------------
// Statistics
//-------------------------------------------------------------------------
static unique_ptr<BaseStatistics> BitmapIndexScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                           column_t column_id) {
	auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);

	if (local_storage.Find(bind_data.table.GetStorage())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}

	return bind_data.table.GetStatistics(context, column_id);
}

//-------------------------------------------------------------------------
// Dependency
//-------------------------------------------------------------------------
void BitmapIndexScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	entries.AddDependency(bind_data.table);

	// TODO: Add dependency to index here?
}

//-------------------------------------------------------------------------
// Cardinality
//-------------------------------------------------------------------------
unique_ptr<NodeStatistics> BitmapIndexScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	const auto &storage = bind_data.table.GetStorage();

	// TODO: do we have to implement this function?
	idx_t table_rows = storage.GetTotalRows();
	idx_t estimated_cardinality = table_rows + local_storage.AddedRows(bind_data.table.GetStorage());
	return make_uniq<NodeStatistics>(table_rows, estimated_cardinality);
}

//-------------------------------------------------------------------------
// ToString
//-------------------------------------------------------------------------
static InsertionOrderPreservingMap<string> BitmapIndexScanToString(TableFunctionToStringInput &input) {
	D_ASSERT(input.bind_data);
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<BitmapIndexScanBindData>();
	result["Table"] = bind_data.table.name;
	result["Index"] = bind_data.index.GetIndexName();
	return result;
}

//-------------------------------------------------------------------------
// De/Serialize
//-------------------------------------------------------------------------
static void BitmapScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "index_name", bind_data.index.GetIndexName());
	serializer.WriteProperty(104, "filter_value", bind_data.filter_value);

}

static unique_ptr<FunctionData> BitmapScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();

	const auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	const auto schema = deserializer.ReadProperty<string>(101, "schema");
	const auto table = deserializer.ReadProperty<string>(102, "table");

	auto &catalog_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Could not find table %s.%s in catalog %s", schema, table, catalog);
	}

	// Read index name
	const auto index_name = deserializer.ReadProperty<string>(103, "index_name");

	//TODO, if we add in BitmapScanSerialize(), we have to deserialize here:

	auto &duck_table = catalog_entry.Cast<DuckTableEntry>();
	auto &table_info = *catalog_entry.GetStorage().GetDataTableInfo();

	unique_ptr<BitmapIndexScanBindData> result = nullptr;

	table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);
	table_info.GetIndexes().Scan([&](Index &index) {
		if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
			return false;
		}
		auto &bitmap_index = index.Cast<BitmapIndex>();
		if (bitmap_index.GetIndexName() == index_name) {
			    // Read filter value from serialized properties (default to NULL Value if not present)
				Value filter_value = deserializer.ReadPropertyWithExplicitDefault<Value>(104, "filter_value", Value());
			    result = make_uniq<BitmapIndexScanBindData>(duck_table, bitmap_index, filter_value);
			//TODO, if we add in BitmapScanSerialize(), we have to deserialize also here:
			return true;
		}
		return false;
	});

	if (!result) {
		throw SerializationException("Could not find bitmap index %s on table %s.%s",
		                             index_name, schema, table);
	}

	return std::move(result);

}

//-------------------------------------------------------------------------
// Get Function
//-------------------------------------------------------------------------
TableFunction BitmapIndexScanFunction::GetFunction() {
	TableFunction func("bitmap_index_scan", {}, BitmapIndexScanExecute);
	func.init_local = nullptr;
	func.init_global = BitmapIndexScanInitGlobal;
	func.statistics = BitmapIndexScanStatistics;
	func.dependency = BitmapIndexScanDependency;
	func.cardinality = BitmapIndexScanCardinality;
	func.pushdown_complex_filter = nullptr;
	func.to_string = BitmapIndexScanToString;
	func.table_scan_progress = nullptr;
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.get_bind_info = BitmapIndexScanBindInfo;
	func.serialize = BitmapScanSerialize;
	func.deserialize = BitmapScanDeserialize;
	return func;
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndexScan(ExtensionLoader &loader) {
	loader.RegisterFunction(BitmapIndexScanFunction::GetFunction());
}

} // namespace duckdb
