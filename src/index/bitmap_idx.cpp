#include <cassert>

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"
#include "index/bitmap_idx_table.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/database.hpp"
// #include "duckdb/common/types/unified_vector_format.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Bitmap Index Scan State
//------------------------------------------------------------------------------
class BitmapIndexScanState final : public IndexScanState {
public:
	BitmapIndexScanState(const BitmapTable &table_p, vector<row_t> matches_p)
	    : table(table_p), matches(std::move(matches_p)) {
	}

	const BitmapTable &table;
	vector<row_t> matches;
	idx_t offset = 0;
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
	bitmap_config = ParseOptions(options);
	bitmap_config.bitmap_cardinality = MaxValue<idx_t>(1, estimated_cardinality);

	table_config.encoding = Table_config::EE;
	table_config.g_cardinality = static_cast<int>(bitmap_config.bitmap_cardinality);
	table_config.n_rows = 0;

	bitmap_table = make_uniq<BitmapTable>(&table_config);

	if(info.IsValid()){
		// TODO: 从磁盘恢复索引数据
	}
}

unique_ptr<IndexScanState> BitmapIndex::InitializeScan() const {
	if (!bitmap_table) {
		return nullptr;
	}
	// DUMMY implementation: gather all row_ids currently present
	vector<row_t> matches;
	bitmap_table->ForEachValue([&](row_t row_id, idx_t /*value*/) {
		matches.push_back(row_id);
		return true;
	});
	return make_uniq<BitmapIndexScanState>(*bitmap_table, std::move(matches));
}

idx_t BitmapIndex::Scan(IndexScanState &state, Vector &result) const {
	auto &scan_state = state.Cast<BitmapIndexScanState>();
	auto row_ids = FlatVector::GetData<row_t>(result);
	idx_t count = 0;
	while (scan_state.offset < scan_state.matches.size() && count < STANDARD_VECTOR_SIZE) {
		row_ids[count++] = scan_state.matches[scan_state.offset++];
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	return count;
}

void BitmapIndex::CommitDrop(IndexLock &index_lock) {
	bitmap_table.reset();
}

ErrorData BitmapIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	return Append(lock, input, rowid_vec);
}

ErrorData BitmapIndex::Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) {
	if (!bitmap_table) {
		return ErrorData {};
	}
	if (entries.ColumnCount() != 1) {
		throw NotImplementedException("Bitmap index currently supports single-column indexes");
	}

	auto count = entries.size();
	if (count == 0) {
		return ErrorData {};
	}

	auto &value_vector = entries.data[0]; // we only support single column index, so just pick data[0]
	UnifiedVectorFormat value_data;
	value_vector.ToUnifiedFormat(count, value_data);

	UnifiedVectorFormat rowid_data;
	row_identifiers.ToUnifiedFormat(count, rowid_data);
	auto value_type = value_vector.GetType().id();

	for (idx_t i = 0; i < count; i++) {
		// which row
		auto row_index = rowid_data.sel->get_index(i);
		auto rowid_ptr = reinterpret_cast<row_t *>(rowid_data.data);
		auto rowid = rowid_ptr[row_index];

		// if value not valid, clear the bitmap for that row
		if (!value_data.validity.RowIsValid(value_data.sel->get_index(i))) {
			bitmap_table->ClearRow(rowid);
			continue;
		}

		// set the bitmap for that row
		idx_t physical_index = value_data.sel->get_index(i);
		int64_t value = 0;
		switch (value_type) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
			value = reinterpret_cast<int8_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::UTINYINT:
			value = reinterpret_cast<uint8_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::SMALLINT:
			value = reinterpret_cast<int16_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::USMALLINT:
			value = reinterpret_cast<uint16_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::INTEGER:
			value = reinterpret_cast<int32_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::UINTEGER:
			value = reinterpret_cast<uint32_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::BIGINT:
			value = reinterpret_cast<int64_t *>(value_data.data)[physical_index];
			break;
		case LogicalTypeId::UBIGINT:
			value = static_cast<int64_t>(reinterpret_cast<uint64_t *>(value_data.data)[physical_index]);
			break;
		default:
			throw NotImplementedException("Bitmap index currently supports only integer-like types");
		}

		if (value < NumericLimits<int32_t>::Minimum() || value > NumericLimits<int32_t>::Maximum()) {
			throw OutOfRangeException("Bitmap index value %lld exceeds 32-bit storage bounds", value);
		}
		bitmap_table->SetRowValue(rowid, static_cast<int32_t>(value));
	}
	return ErrorData {};
}

void BitmapIndex::Delete(IndexLock &lock, DataChunk &entries, Vector &rowid_vec) {
	if (!bitmap_table) {
		return;
	}
	auto count = entries.size();
	if (count == 0) {
		return;
	}
	UnifiedVectorFormat rowid_data;
	rowid_vec.ToUnifiedFormat(count, rowid_data);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = rowid_data.sel->get_index(i);
		auto rowid = reinterpret_cast<row_t *>(rowid_data.data)[row_index];
		bitmap_table->ClearRow(rowid);
	}
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
	if (!bitmap_table) {
		return 0;
	}
	return bitmap_table->GetMemoryUsageBytes();
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
}

string BitmapIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	if (only_verify) {
		return "";
	}
	idx_t values = bitmap_table ? bitmap_table->GetDistinctValues().size() : 0;
	return StringUtil::Format("Bitmap Index %s (indexed values: %llu)", name, values);
}

void BitmapIndex::VerifyAllocations(IndexLock &state) {
}

void BitmapIndex::VerifyBuffers(IndexLock &l) {
}

//custom functions for _pragma:

idx_t BitmapIndex::GetInMemorySize() const {
	if (!bitmap_table) {
		return 0;
}
	return bitmap_table->GetMemoryUsageBytes();
}

idx_t BitmapIndex::GetIndexSize() const {
	if (!bitmap_table) {
		return 0;
	}
	return bitmap_table->GetTotalBitSize();
}

idx_t BitmapIndex::GetCompressionRatio() const {
	if (!bitmap_table) {
		return 1;
	}
	return bitmap_table->GetCompressionRatio();
}

std::vector<std::string> BitmapIndex::GetDistinctValues() const {
	if (!bitmap_table) {
		return {};
	}
	return bitmap_table->GetDistinctValues();
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
