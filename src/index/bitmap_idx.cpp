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

	// DUMMY: 暂时不分配实际存储，只是初始化配置
	// 将来这里会创建BitmapTable对象
	auto &block_manager = table_io_manager.GetIndexBlockManager();

	// TODO: 当BitmapTable实现完成后，取消下面注释：
	//Table_config bitmap_table_config;
	//bitmap_table_config.g_cardinality = this->bitmap_config.bitmap_cardinality;
	//this->bitmap_table = make_uniq<BitmapTable>(block_manager, bitmap_table_config);

	if(info.IsValid()){
		// TODO: 从磁盘恢复索引数据
	}
}

unique_ptr<IndexScanState> BitmapIndex::InitializeScan() const {
	return make_uniq<BitmapIndexScanState>();
}

idx_t BitmapIndex::Scan(IndexScanState &state, Vector &result) const {
	// DUMMY: 返回0表示没有更多结果
	// 实际实现：从bitmap中读取匹配的row_ids填充到result向量
	return 0;
}

void BitmapIndex::CommitDrop(IndexLock &index_lock) {
	// DUMMY: 释放所有bitmap存储
	// 实际实现：bitmap_table.reset(); 或类似操作
}

ErrorData BitmapIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// DUMMY: 假装插入成功，不做任何操作
	// 实际实现：
	// 1. 遍历input chunk的每一行
	// 2. 提取索引列的值
	// 3. 在对应的bitmap中设置row_id位
	// 4. 检查约束冲突（如果需要）
	return ErrorData {};
}

ErrorData BitmapIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	// For our simple in-memory table, Append behaves like Insert: set values for provided row ids.
	return Insert(lock, appended_data, row_identifiers);
}

void BitmapIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// DUMMY: 假装删除成功
	// 实际实现：
	// 1. 遍历input chunk
	// 2. 在对应bitmap中清除row_id位
}

IndexStorageInfo BitmapIndex::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	// DUMMY: 返回空的存储信息（表示索引在内存中）
	// 实际实现：将bitmap数据序列化到block_manager
	IndexStorageInfo info;
	return info;
}

IndexStorageInfo BitmapIndex::SerializeToWAL(const case_insensitive_map_t<Value> &options) {
	// DUMMY: 返回空的存储信息
	// 实际实现：将操作写入WAL
	IndexStorageInfo info;
	return info;
}

idx_t BitmapIndex::GetInMemorySize(IndexLock &state) {
	// DUMMY: 返回占位值
	// 实际实现：计算bitmap_table的实际内存占用
	return 0;
}

bool BitmapIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	// DUMMY: 假装合并成功
	// 实际实现：
	// 1. 验证other_index也是BitmapIndex
	// 2. 对每个bitmap执行OR操作合并
	// 3. 检查约束冲突
	return true;
}

void BitmapIndex::Vacuum(IndexLock &state) {
	// DUMMY: Vacuum操作暂时为空
	// 实际实现：压缩bitmap、释放未使用的内存块
}

string BitmapIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	// DUMMY: 返回简单的索引描述
	// 实际实现：验证bitmap完整性，返回详细统计信息
	if (only_verify) {
		return "";
	}
	return StringUtil::Format("Bitmap Index %s (cardinality: %d)", name, bitmap_config.bitmap_cardinality);
}

void BitmapIndex::VerifyAllocations(IndexLock &state) {
	// DUMMY: 验证通过
	// 实际实现：检查allocator计数与实际bitmap数量是否匹配
}

void BitmapIndex::VerifyBuffers(IndexLock &l) {
	// DUMMY: 验证通过
	// 实际实现：检查buffer完整性
}

//custom functions for _pragma:

idx_t BitmapIndex::GetInMemorySize() const {
	// DUMMY: 返回占位值
	// 实际实现：计算所有bitmap的内存占用总和
	return 0;
}

idx_t BitmapIndex::GetIndexSize() const {
	// DUMMY: 返回占位值
	// 实际实现：返回压缩后的bitmap大小
	return 0;
}

idx_t BitmapIndex::GetCompressionRatio() const {
	// DUMMY: 返回1表示未压缩
	// 实际实现：return uncompressed_size / compressed_size
	return 1;
}

vector<string> BitmapIndex::GetDistinctValues() const {
	// DUMMY: 返回空列表
	// 实际实现：返回所有distinct values的字符串表示
	vector<string> result;
	return result;
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