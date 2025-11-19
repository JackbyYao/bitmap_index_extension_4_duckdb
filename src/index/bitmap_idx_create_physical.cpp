/**
 * Execution engine
 */

#include "index/bitmap_idx_create_physical.hpp"
#include "index/bitmap_idx.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

//-------------------------------------------------------------
// Physical Create Bitmap Index
//-------------------------------------------------------------
PhysicalCreateBitmapIndex::PhysicalCreateBitmapIndex(PhysicalPlan &physical_plan, LogicalOperator &op,
                                                   TableCatalogEntry &table, const vector<column_t> &column_ids,
                                                   unique_ptr<CreateIndexInfo> info,
                                                   vector<unique_ptr<Expression>> unbound_expressions,
                                                   idx_t estimated_cardinality)
    // Declare this operators as a EXTENSION operator
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, estimated_cardinality),
      table(table.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)) {

	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
	// DUMMY: 构造函数完成，准备好接收数据
}

//-------------------------------------------------------------
// Local State (for parallel execution)
//-------------------------------------------------------------
class CreateBitmapIndexLocalSinkState final : public LocalSinkState {
public:
	//! Local index for this thread
	unique_ptr<BitmapIndex> local_index;
};

//-------------------------------------------------------------
// Global State
//-------------------------------------------------------------
class CreateBitmapIndexGlobalState final : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<BitmapIndex> bitmap_ptr;
	AllocatedData slice_buffer;

	idx_t entry_idx;
};

unique_ptr<GlobalSinkState> PhysicalCreateBitmapIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateBitmapIndexGlobalState>();

	// DUMMY: 创建全局索引对象
	// 参考ART索引的实现
	auto &storage = table.GetStorage();

	// 创建BitmapIndex对象（暂时空的，因为BitmapIndex构造函数已经是dummy）
	state->bitmap_ptr = make_uniq<BitmapIndex>(
		info->index_name,
		info->constraint_type,
		storage_ids,
		TableIOManager::Get(storage),
		unbound_expressions,
		storage.db,
		info->options,
		IndexStorageInfo(),
		estimated_cardinality
	);

	state->entry_idx = 0;

	return std::move(state);
}

//-------------------------------------------------------------
// Local Sink State
//-------------------------------------------------------------
unique_ptr<LocalSinkState> PhysicalCreateBitmapIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateBitmapIndexLocalSinkState>();

	// Create a local index for this thread (similar to ART implementation)
	auto &storage = table.GetStorage();

	state->local_index = make_uniq<BitmapIndex>(
		info->index_name,
		info->constraint_type,
		storage_ids,
		TableIOManager::Get(storage),
		unbound_expressions,
		storage.db,
		info->options,
		IndexStorageInfo(),
		estimated_cardinality
	);

	return std::move(state);
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------
SinkResultType PhysicalCreateBitmapIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	// Use local_index for parallel execution (no lock needed)
	auto &lstate = input.local_state.Cast<CreateBitmapIndexLocalSinkState>();
	auto &bitmap_index = *lstate.local_index;

	D_ASSERT(chunk.ColumnCount() >= 1);

	// key columns occupy everything except the final rowid column
	vector<LogicalType> key_types;
	vector<column_t> key_column_ids;
	const idx_t column_count = chunk.ColumnCount();
	const idx_t rowid_col_idx = column_count - 1;

	key_types.reserve(rowid_col_idx);
	key_column_ids.reserve(rowid_col_idx);
	for (idx_t col_idx = 0; col_idx < rowid_col_idx; col_idx++) {
		key_types.push_back(chunk.data[col_idx].GetType());
		key_column_ids.push_back(col_idx);
	}

	if (key_types.size() != 1) {
		throw NotImplementedException("Bitmap indexes currently support a single key column");
	}

	DataChunk key_chunk;
	key_chunk.Initialize(context.client, key_types);
	key_chunk.ReferenceColumns(chunk, key_column_ids);

	auto &rowid_vector = chunk.data[rowid_col_idx];

	// No lock needed for local_index (each thread has its own)
	IndexLock lock;
	bitmap_index.InitializeLock(lock);
	bitmap_index.Append(lock, key_chunk, rowid_vector);

	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Combine (merge local indexes into global index)
//-------------------------------------------------------------
SinkCombineResultType PhysicalCreateBitmapIndex::Combine(ExecutionContext &context,
                                                         OperatorSinkCombineInput &input) const {
	auto &g_state = input.global_state.Cast<CreateBitmapIndexGlobalState>();
	auto &l_state = input.local_state.Cast<CreateBitmapIndexLocalSinkState>();

	// Merge the local index into the global index
	// Get bitmap tables
	auto &global_bitmap_table = g_state.bitmap_ptr->bitmap_table;
	auto &local_bitmap_table = l_state.local_index->bitmap_table;

	if (!global_bitmap_table || !local_bitmap_table) {
		return SinkCombineResultType::FINISHED;
	}

	// Use the public MergeFrom method to merge local into global
	global_bitmap_table->MergeFrom(*local_bitmap_table);

	return SinkCombineResultType::FINISHED;
}

//-------------------------------------------------------------
// Bitmap Construction
//-------------------------------------------------------------

//TODO: change name of the function
static TaskExecutionResult BuildBitmapBottomUp(CreateBitmapIndexGlobalState &state, TaskExecutionMode mode,
                                              Event &event) {
	throw NotImplementedException("BuildBitmapBottomUp() not implemented");
	return TaskExecutionResult::TASK_FINISHED;
}

class BitmapIndexConstructionTask final : public ExecutorTask {
public:
	BitmapIndexConstructionTask(shared_ptr<Event> event_p, ClientContext &context, CreateBitmapIndexGlobalState &gstate,
	                           const PhysicalCreateBitmapIndex &op)
	    : ExecutorTask(context, std::move(event_p), op), state(gstate) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		return BuildBitmapBottomUp(state, mode, *event);
	}

private:
	CreateBitmapIndexGlobalState &state;
};

static void AddIndexToCatalog(ClientContext &context, CreateBitmapIndexGlobalState &gstate, CreateIndexInfo &info,
                              DuckTableEntry &table) {
    throw NotImplementedException("AddIndexToCatalog() not implemented");
}

class BitmapIndexConstructionEvent final : public BasePipelineEvent {
public:
	BitmapIndexConstructionEvent(CreateBitmapIndexGlobalState &gstate_p, Pipeline &pipeline_p, CreateIndexInfo &info_p,
	                            DuckTableEntry &table_p, const PhysicalCreateBitmapIndex &op_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), info(info_p), table(table_p), op(op_p) {
	}

	void Schedule() override {
		throw NotImplementedException("Schedule() not implemented");
	}

	void FinishEvent() override {
		AddIndexToCatalog(pipeline->GetClientContext(), gstate, info, table);
        throw NotImplementedException("FinishEvent() not implemented");
	}

private:
	CreateBitmapIndexGlobalState &gstate;
	CreateIndexInfo &info;
	DuckTableEntry &table;
	const PhysicalCreateBitmapIndex &op;
};

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------
SinkFinalizeType PhysicalCreateBitmapIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                    OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<CreateBitmapIndexGlobalState>();

	// DUMMY: 完成索引构建流程
	// 参考ART索引的Finalize实现，按照正确顺序：先检查，再修改状态

	// 1. 先做所有检查
	auto &storage = table.GetStorage();
	if (!storage.IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: cannot add an index to a table that has been altered or dropped");
	}

	auto &schema = table.schema;
	auto entry = schema.GetEntry(schema.GetCatalogTransaction(context), CatalogType::INDEX_ENTRY, info->index_name);
	if (entry) {
		if (info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
			throw CatalogException("Index with name \"%s\" already exists!", info->index_name);
		}
		// IF NOT EXISTS且索引已存在，直接返回
		return SinkFinalizeType::READY;
	}

	// 2. 所有检查通过后，执行维护操作
	IndexLock lock;
	state.bitmap_ptr->InitializeLock(lock);

	state.bitmap_ptr->Vacuum(lock);
	state.bitmap_ptr->VerifyAndToString(lock, true);
	state.bitmap_ptr->VerifyAllocations(lock);

	// 3. 更新info（在创建catalog entry之前）
	info->column_ids = storage_ids;

	// 4. 创建catalog entry
	auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(context), *info, table).get();
	D_ASSERT(index_entry);
	auto &index = index_entry->Cast<DuckIndexEntry>();
	index.initial_index_size = state.bitmap_ptr->GetInMemorySize(lock);

	// 5. 将索引添加到storage
	storage.AddIndex(std::move(state.bitmap_ptr));

	return SinkFinalizeType::READY;
}

} // namespace duckdb
