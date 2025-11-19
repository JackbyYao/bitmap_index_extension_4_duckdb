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

// VARCHAR support
int BitmapIndex::GetOrAddValueId(const string &val) {
	std::lock_guard<std::mutex> guard(dict_lock);
	auto it = value_to_id.find(val);
	if (it != value_to_id.end()) return it->second;
	int id = static_cast<int>(id_to_value.size());
	id_to_value.push_back(val);
	value_to_id[val] = id;
	return id;
}

int BitmapIndex::LookupValueId(const string &val) const {
	std::lock_guard<std::mutex> guard(dict_lock);
	auto it = value_to_id.find(val);
	if (it == value_to_id.end()) return -1;
	return it->second;
}

string BitmapIndex::LookupValueString(int id) const {
	std::lock_guard<std::mutex> guard(dict_lock);
	if (id < 0 || id >= static_cast<int>(id_to_value.size())) {
		return std::to_string(id);
	}
	return id_to_value[id];
}

bool BitmapIndex::UsesDictionary() const {
	std::lock_guard<std::mutex> guard(dict_lock);
	return !id_to_value.empty();
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

unique_ptr<IndexScanState> BitmapIndex::InitializeScan(const Value *filter_value) const{
	// If no bitmap table present, nothing to do
    if (!bitmap_table) {
        return nullptr;
    }

	std::string fv = (!filter_value ? "<nullptr>" : filter_value->ToString());

	// No filter or NULL filter -> default full-scan
		if (!filter_value || filter_value->IsNull()) {
			return InitializeScan(); // calls existing full-scan implementation
		}

    try {
		// Optimize for VARCHAR and integer-like types
		auto type_id = filter_value->type().id();
		if (type_id == LogicalTypeId::VARCHAR) {
			// If index hasn't used the dictionary, fall back to full-scan
			if (!UsesDictionary()) {
				return InitializeScan();
			}
			// Resolve string -> id
			const string sval = filter_value->ToString();
			int id = LookupValueId(sval);
			vector<row_t> matches;
			if (id >= 0 && bitmap_table) {
				bitmap_table->GetRowsForValue(id, matches);
			}
			// Return a scan state backed by the bitmap_table and the explicit matches
			return make_uniq<BitmapIndexScanState>(*bitmap_table, std::move(matches));
		} else if (type_id == LogicalTypeId::BOOLEAN || type_id == LogicalTypeId::TINYINT ||
				   type_id == LogicalTypeId::UTINYINT || type_id == LogicalTypeId::SMALLINT ||
				   type_id == LogicalTypeId::USMALLINT || type_id == LogicalTypeId::INTEGER ||
				   type_id == LogicalTypeId::UINTEGER || type_id == LogicalTypeId::BIGINT ||
				   type_id == LogicalTypeId::UBIGINT) {
			// Handle integer-like filters by extracting numeric value and performing a lookup
			vector<row_t> matches;
			if (!bitmap_table) {
				return nullptr;
			}
			// Unsigned types: retrieve as uint64_t to check bounds
			if (type_id == LogicalTypeId::UTINYINT || type_id == LogicalTypeId::USMALLINT ||
				type_id == LogicalTypeId::UINTEGER || type_id == LogicalTypeId::UBIGINT) {
				uint64_t uv = filter_value->GetValue<uint64_t>();
				if (uv > static_cast<uint64_t>(NumericLimits<int32_t>::Maximum())) {
					// out of bounds -> no matches (or fallback to full scan)
					return InitializeScan();
				}
				int32_t key = static_cast<int32_t>(uv);
				bitmap_table->GetRowsForValue(key, matches);
				return make_uniq<BitmapIndexScanState>(*bitmap_table, std::move(matches));
			} else {
				// Signed numeric types and boolean
				int64_t sv = filter_value->GetValue<int64_t>();
				if (sv < NumericLimits<int32_t>::Minimum() || sv > NumericLimits<int32_t>::Maximum()) {
					return InitializeScan();
				}
				int32_t key = static_cast<int32_t>(sv);
				bitmap_table->GetRowsForValue(key, matches);
				return make_uniq<BitmapIndexScanState>(*bitmap_table, std::move(matches));
			}
		} else {
			// For other filter types, fall back to full-scan for now
			return InitializeScan();
		}
	} catch (...) {
		return InitializeScan();
	}
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

	// Collect all updates for batch processing
	std::vector<std::pair<uint64_t, int>> batch_updates;
	batch_updates.reserve(count);
	std::vector<uint64_t> rows_to_clear;

	for (idx_t i = 0; i < count; i++) {
		// which row
		auto row_index = rowid_data.sel->get_index(i);
		auto rowid_ptr = reinterpret_cast<row_t *>(rowid_data.data);
		auto rowid = rowid_ptr[row_index];

		// if value not valid, clear the bitmap for that row
		if (!value_data.validity.RowIsValid(value_data.sel->get_index(i))) {
			rows_to_clear.push_back(rowid);
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
		case LogicalTypeId::VARCHAR: {
			// VARCHAR support, For better performance, extract the underlying string_t from the
			// UnifiedVectorFormat data pointer. (since currenly we're creating key string on the fly)
			Value v = value_vector.GetValue(physical_index);
			string s = v.ToString();
			int id = GetOrAddValueId(s);
			value = id;
			break;
		}
		default:
			throw NotImplementedException("Bitmap index currently supports only integer-like types");
		}

		if (value < NumericLimits<int32_t>::Minimum() || value > NumericLimits<int32_t>::Maximum()) {
			throw OutOfRangeException("Bitmap index value %lld exceeds 32-bit storage bounds", value);
		}
		batch_updates.push_back({rowid, static_cast<int32_t>(value)});
	}

	// Process clears (use negative value to indicate clear in batch API)
	for (uint64_t rowid : rows_to_clear) {
		batch_updates.push_back({rowid, -1});
	}

	// Batch process all updates at once
	if (!batch_updates.empty()) {
		bitmap_table->SetRowValuesBatch(batch_updates);
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
	// If we haven't populated a dictionary (no VARCHAR support used), just
	// return the bitmap table's existing distinct-values (original behavior).
	{
		std::lock_guard<std::mutex> guard(dict_lock);
		if (id_to_value.empty()) {
			return bitmap_table->GetDistinctValues();
		}
	}
	// Otherwise, convert numeric ids (as returned by BitmapTable) to human-readable strings
	auto numeric_vals = bitmap_table->GetDistinctValues();
	std::vector<std::string> result;
	result.reserve(numeric_vals.size());
	for (auto &s : numeric_vals) {
		try {
			int id = std::stoi(s);
			result.push_back(LookupValueString(id));
		} catch (...) {
			// If parsing fails, just forward the raw string
			result.push_back(s);
		}
	}
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
