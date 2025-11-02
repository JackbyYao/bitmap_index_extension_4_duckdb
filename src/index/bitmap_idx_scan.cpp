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

namespace duckdb {

BindInfo BitmapIndexScanBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	//auto &bind_data = bind_data_p->Cast<BitmapIndexScanBindData>();
	throw NotImplementedException("BitmapIndexScanBindInfo() not implemented");
}

//-------------------------------------------------------------------------
// Global State
//-------------------------------------------------------------------------
struct BitmapIndexScanGlobalState final : public GlobalTableFunctionState {
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
	vector<idx_t> projection_ids;

	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<StorageIndex> column_ids;

	// Index scan state
	unique_ptr<IndexScanState> index_state;
	Vector row_ids = Vector(LogicalType::ROW_TYPE);
};

static unique_ptr<GlobalTableFunctionState> BitmapIndexScanInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	throw NotImplementedException("BitmapIndexScanInitGlobal() not implemented");
	return nullptr;
}

//-------------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------------
static void BitmapIndexScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    throw NotImplementedException("BitmapIndexScanExecute() not implemented");
}

//-------------------------------------------------------------------------
// Statistics
//-------------------------------------------------------------------------
static unique_ptr<BaseStatistics> BitmapIndexScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                           column_t column_id) {
	throw NotImplementedException("BitmapIndexScanStatistics() not implemented");
	return nullptr;
}

//-------------------------------------------------------------------------
// Dependency
//-------------------------------------------------------------------------
void BitmapIndexScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
    throw NotImplementedException("BitmapIndexScanDependency() not implemented");
}

//-------------------------------------------------------------------------
// Cardinality
//-------------------------------------------------------------------------
unique_ptr<NodeStatistics> BitmapIndexScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	throw NotImplementedException("BitmapIndexScanCardinality() not implemented");
}

//-------------------------------------------------------------------------
// ToString
//-------------------------------------------------------------------------
static InsertionOrderPreservingMap<string> BitmapIndexScanToString(TableFunctionToStringInput &input) {
	D_ASSERT(input.bind_data);
	InsertionOrderPreservingMap<string> result;
	throw NotImplementedException("BitmapIndexScanToString() not implemented");
	return result;
}

//-------------------------------------------------------------------------
// De/Serialize
//-------------------------------------------------------------------------
static void BitmapScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("BitmapScanSerialize() not implemented");
}

static unique_ptr<FunctionData> BitmapScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("BitmapScanDeserialize() not implemented");
	return nullptr;
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
	// DUMMY: 返回配置好的function，虽然所有回调都是dummy实现
	return func;
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndexScan(ExtensionLoader &loader) {
	loader.RegisterFunction(BitmapIndexScanFunction::GetFunction());
}

} // namespace duckdb
