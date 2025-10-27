#include "index/bitmap_idx_create_physical.hpp"
#include "index/bitmap_idx.hpp"


#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/sort/sort.hpp"
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
    throw NotImplementedException("PhysicalCreateBitmapIndex::PhysicalCreateBitmapIndex() not implemented");
}

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
	throw NotImplementedException("PhysicalCreateBitmapIndex::GetGlobalSinkState() not implemented");
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------
SinkResultType PhysicalCreateBitmapIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	
    throw NotImplementedException("PhysicalCreateBitmapIndex::Sink() not implemented");
	return SinkResultType::NEED_MORE_INPUT;
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
	throw NotImplementedException("PhysicalCreateBitmapIndex::Finalize() not implemented");
	return SinkFinalizeType::READY;
}

} // namespace duckdb
