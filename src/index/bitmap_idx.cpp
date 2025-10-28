#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Bitmap Index Scan State
//------------------------------------------------------------------------------
class BitmapIndexScanState final : public IndexScanState {
public:
	//BitmapBounds query_bounds;
	//BitmapScanner scanner;
};

//------------------------------------------------------------------------------
// Bitmap Configuration
//------------------------------------------------------------------------------

static BitmapConfig ParseOptions(const case_insensitive_map_t<Value> &options) {
	BitmapConfig config = {};

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
		throw NotImplementedException("RTree indexes do not support unique or primary key constraints");
	}

	// Create the configuration from the options
	BitmapConfig config = ParseOptions(options);
	throw NotImplementedException("BitmapIndex::Constructor() not implemented");
	// Create the Bitmap
	auto &block_manager = table_io_manager.GetIndexBlockManager();
}

unique_ptr<IndexScanState> BitmapIndex::InitializeScan() const {
	throw NotImplementedException("BitmapIndex::InitializeScan() not implemented");
	return nullptr;
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
	
	throw NotImplementedException("BitmapIndex::Append() not implemented");
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