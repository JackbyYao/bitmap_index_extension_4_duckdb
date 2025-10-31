#include <cassert>

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/database.hpp"
//#include "index/bitmap_idx_table.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Bitmap Index Scan State
//------------------------------------------------------------------------------
class BitmapIndexScanState final : public IndexScanState {
public:
	;
};

//------------------------------------------------------------------------------
// Bitmap Configuration
//------------------------------------------------------------------------------

static BitmapConfig ParseOptions(const case_insensitive_map_t<Value> &options) {
	BitmapConfig config = {};
	//TODO : future expansion of any configs
	return config;
}

//------------------------------------------------------------------------------
// BitmapIndex Methods
//------------------------------------------------------------------------------

// Constructor
BitmapIndex::BitmapIndex(const string &name, IndexConstraintType index_constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db,
                       const case_insensitive_map_t<Value> &options, const IndexStorageInfo &info,
                       idx_t estimated_cardinality)
    : BoundIndex(name, TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db) {

	if (index_constraint_type != IndexConstraintType::NONE) {
		//we do not support unique or primary key because their high cardinality. 
		throw NotImplementedException("Bitmap indexes do not support unique or primary key constraints");
	}

	// configuration can be loaded here
	BitmapConfig bitmap_config = ParseOptions(options);
	this->bitmap_config.bitmap_cardinality = (int)estimated_cardinality;

	assert(this->bitmap_config.bitmap_cardinality > 0 );

	auto &block_manager = table_io_manager.GetIndexBlockManager();
	// TODO : figure out the estimate size after we defined on the actual storage
	// const auto max_alloc_size = 
	throw NotImplementedException("TODO: estimate the bitmap size");
	//TODO: this might need to be merged with BitmapConfig
	//Table_config bitmap_table_config;
	//bitmap_table_config.g_cardinality = this->bitmap_config.bitmap_cardinality;
	// instantiate the table
	//this->bitmap_table = make_uniq<BitmapTable>(block_manager,bitmap_table_config);
	if(info.IsValid()){
		;//TODO: is there any other things to be allocated?
	}
}

unique_ptr<IndexScanState> BitmapIndex::InitializeScan() const {
	return make_uniq<BitmapIndexScanState>();
}

idx_t BitmapIndex::Scan(IndexScanState &state, Vector &result) const {
	throw NotImplementedException("BitmapIndex::Scan() not implemented");
	return 0;
}

void BitmapIndex::CommitDrop(IndexLock &index_lock) {
	// TODO: Maybe we can drop these much earlier?
	throw NotImplementedException("BitmapIndex::CommitDrop() not implemented");
}

ErrorData BitmapIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	throw NotImplementedException("BitmapIndex::Insert() not implemented");
	return ErrorData {};
}

ErrorData BitmapIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	// For our simple in-memory table, Append behaves like Insert: set values for provided row ids.
	return Insert(lock, appended_data, row_identifiers);
}

void BitmapIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	throw NotImplementedException("BitmapIndex::Delete() not implemented");
	
}

IndexStorageInfo BitmapIndex::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	IndexStorageInfo info;
	throw NotImplementedException("BitmapIndex::SerializeToDisk() not implemented");
	return info;
}

IndexStorageInfo BitmapIndex::SerializeToWAL(const case_insensitive_map_t<Value> &options) {

	IndexStorageInfo info;
	throw NotImplementedException("BitmapIndex::SerializeToWAL() not implemented");
	return info;
}

idx_t BitmapIndex::GetInMemorySize(IndexLock &state) {
	throw NotImplementedException("BitmapIndex::GetInMemorySize() not implemented");
	return 0;
}

bool BitmapIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	throw NotImplementedException("BitmapIndex::MergeIndexes() not implemented");
}

void BitmapIndex::Vacuum(IndexLock &state) {
}

string BitmapIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	throw NotImplementedException("BitmapIndex::VerifyAndToString() not implemented");
}

void BitmapIndex::VerifyAllocations(IndexLock &state) {
	throw NotImplementedException("BitmapIndex::VerifyAllocations() not implemented");
}

void BitmapIndex::VerifyBuffers(IndexLock &l) {
	throw NotImplementedException("BitmapIndex::VerifyBuffers() not implemented");
}

//custom functions for _pragma:

idx_t BitmapIndex::GetInMemorySize() const {
	throw NotImplementedException("BitmapIndex::GetInMemorySize() not implemented");
};
idx_t BitmapIndex::GetIndexSize() const {
	throw NotImplementedException("BitmapIndex::GetIndexSize() not implemented");
};
idx_t BitmapIndex::GetCompressionRatio() const {
	throw NotImplementedException("BitmapIndex::GetCompressionRatio() not implemented");
};
vector<string> BitmapIndex::GetDistinctValues() const {
	throw NotImplementedException("BitmapIndex::GetDistinctValues() not implemented");
};


//------------------------------------------------------------------------------
// Register Index Type
//------------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndex(ExtensionLoader &loader) {

	IndexType index_type;

	index_type.name = BitmapIndex::TYPE_NAME;
	index_type.create_instance = BitmapIndex::Create;
	index_type.create_plan = BitmapIndex::CreatePlan;

	// Register the index type
	auto &db = loader.GetDatabaseInstance();
	db.config.GetIndexTypes().RegisterIndexType(index_type);
}

} // namespace duckdb