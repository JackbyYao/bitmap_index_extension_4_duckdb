
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"

namespace duckdb {

// BIND
static unique_ptr<FunctionData> BitmapindexInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

// INIT GLOBAL
struct BitmapIndexInfoState final : public GlobalTableFunctionState {
	idx_t offset = 0;
	vector<reference<IndexCatalogEntry>> entries;
};

static unique_ptr<GlobalTableFunctionState> BitmapIndexInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<BitmapIndexInfoState>();

	// scan all the schemas for indexes and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			auto &index_entry = entry.Cast<IndexCatalogEntry>();
			if (index_entry.index_type == BitmapIndex::TYPE_NAME) {
				result->entries.push_back(index_entry);
			}
		});
	};
	return std::move(result);
}

// EXECUTE
static void BitmapIndexInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	throw NotImplementedException("BitmapIndexInfoExecute() not implemented"); 
}

static optional_ptr<BitmapIndex> TryGetIndex(ClientContext &context, const string &index_name) {
	throw NotImplementedException("TryGetIndex() not implemented"); 
}

//-------------------------------------------------------------------------
// Bitmap Index Dump
//-------------------------------------------------------------------------
// BIND
struct BitmapIndexDumpBindData final : public TableFunctionData {
	string index_name;
};

static unique_ptr<FunctionData> BitmapIndexDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<BitmapIndexDumpBindData>();

    throw NotImplementedException("BitmapIndexDumpBind() not implemented"); 
	return std::move(result);
}

// INIT
struct BitmapIndexDumpStackFrame {
	idx_t entry_idx = 0;
	BitmapIndexDumpStackFrame(idx_t entry_idx_p)
	    : entry_idx(entry_idx_p) {
	}
};

struct BitmapIndexDumpState final : public GlobalTableFunctionState {
	const BitmapIndex &index;
	//BitmapScanner scanner;

public:
	explicit BitmapIndexDumpState(const BitmapIndex &index) : index(index) {
	}
};

static unique_ptr<GlobalTableFunctionState> BitmapIndexDumpInit(ClientContext &context, TableFunctionInitInput &input) {
	throw NotImplementedException("BitmapIndexDumpInit() not implemented"); 
}

// EXECUTE
static void BitmapIndexDumpExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	throw NotImplementedException("BitmapIndexDumpExecute() not implemented"); 
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndexPragmas(ExtensionLoader &loader) {

	TableFunction info_function("pragma_bitmap_index_info", {}, BitmapIndexInfoExecute, BitmapindexInfoBind,
	                            BitmapIndexInfoInit);

	loader.RegisterFunction(info_function);

	TableFunction dump_function("bitmap_index_dump", {LogicalType::VARCHAR}, BitmapIndexDumpExecute, BitmapIndexDumpBind,
	                            BitmapIndexDumpInit);

	loader.RegisterFunction(dump_function);
}

} // namespace duckdb
