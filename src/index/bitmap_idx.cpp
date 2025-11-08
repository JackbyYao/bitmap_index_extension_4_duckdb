#include <cassert>

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"
#include "index/bitmap_idx_table.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
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
	index_dirty = false; // Initialize dirty flag
	dirty_bitmaps.clear();
	bitmap_locations.clear();
	
	// Start background serialization thread
	stop_serialization_thread = false;
	serialization_thread = make_uniq<std::thread>([this]() {
		this->SerializationThreadLoop();
	});

	if(info.IsValid()){
		// 从磁盘恢复索引数据
		auto &block_manager = table_io_manager.GetIndexBlockManager();
		
		if (!info.allocator_infos.empty()) {
			// 使用 FixedSizeAllocator 恢复数据
			const idx_t segment_size = 4096;
			bitmap_allocator = make_uniq<FixedSizeAllocator>(segment_size, block_manager);
			bitmap_allocator->Init(info.allocator_infos[0]);
			
			// 从 allocator 的 buffers 中读取所有数据
			// 由于 segments 是按顺序分配的，我们可以通过 buffer_id 和 offset 重建 IndexPointer
			auto &allocator_info = info.allocator_infos[0];
			
			// 收集所有数据到一个连续的 buffer
			vector<char> all_data;
			idx_t total_bytes = 0;
			
			// 计算总大小
			for (idx_t i = 0; i < allocator_info.buffer_ids.size(); i++) {
				total_bytes += allocator_info.allocation_sizes[i];
			}
			all_data.resize(total_bytes);
			
			// 从每个 buffer 读取数据
			// 注意：由于序列化时是按顺序写入的，segments 应该是连续的
			// 我们通过遍历 bitmask 来找到所有已分配的 segments
			idx_t data_offset = 0;
			for (idx_t i = 0; i < allocator_info.buffer_ids.size(); i++) {
				auto buffer_id = allocator_info.buffer_ids[i];
				auto segment_count = allocator_info.segment_counts[i];
				
				// 读取这个 buffer 的所有 segments
				// 由于 segments 在序列化时是按顺序分配的，我们假设它们是从 0 到 segment_count-1 连续的
				// 如果这不正确，我们需要遍历 bitmask 来找到所有已分配的 segments
				for (idx_t seg = 0; seg < segment_count; seg++) {
					// 构建 IndexPointer: buffer_id 和 segment offset
					// 注意：这里假设 segments 是连续的，实际实现中可能需要遍历 bitmask
					IndexPointer ptr(static_cast<uint32_t>(buffer_id), static_cast<uint32_t>(seg));
					auto handle = bitmap_allocator->GetHandle(ptr);
					auto ptr_data = handle.GetPtr();
					
					idx_t bytes_to_copy = MinValue<idx_t>(segment_size, total_bytes - data_offset);
					memcpy(all_data.data() + data_offset, ptr_data, bytes_to_copy);
					data_offset += bytes_to_copy;
				}
			}
			
			// 解析数据
			const char* read_ptr = all_data.data();
			BitmapTable::SerializedData data;
			
			// 读取元数据
			data.num_bitmaps = *reinterpret_cast<const int32_t*>(read_ptr);
			read_ptr += sizeof(int32_t);
			data.encoding = *reinterpret_cast<const int32_t*>(read_ptr);
			read_ptr += sizeof(int32_t);
			data.number_of_rows = *reinterpret_cast<const uint64_t*>(read_ptr);
			read_ptr += sizeof(uint64_t);
			
			// 读取每个 bitmap
			data.bitmap_data.resize(data.num_bitmaps);
			for (int32_t i = 0; i < data.num_bitmaps; i++) {
				uint32_t size = *reinterpret_cast<const uint32_t*>(read_ptr);
				read_ptr += sizeof(uint32_t);
				data.bitmap_data[i].resize(size);
				memcpy(data.bitmap_data[i].data(), read_ptr, size);
				read_ptr += size;
			}
			
			// 反序列化到 bitmap_table
			bitmap_table->Deserialize(data);
		} else if (info.root_block_ptr.IsValid()) {
			// 兼容旧格式：从 root_block_ptr 读取
			auto &metadata_manager = table_io_manager.GetMetadataManager();
			MetadataReader reader(metadata_manager, info.root_block_ptr);
			
			// 读取元数据
			int32_t num_bitmaps = reader.Read<int32_t>();
			int32_t encoding = reader.Read<int32_t>();
			uint64_t number_of_rows = reader.Read<uint64_t>();
			
			BitmapTable::SerializedData data;
			data.num_bitmaps = num_bitmaps;
			data.encoding = encoding;
			data.number_of_rows = number_of_rows;
			data.bitmap_data.resize(num_bitmaps);
			
			// 读取每个 bitmap
			for (int32_t i = 0; i < num_bitmaps; i++) {
				uint32_t serialized_size = reader.Read<uint32_t>();
				data.bitmap_data[i].resize(serialized_size);
				reader.ReadData(reinterpret_cast<data_ptr_t>(data.bitmap_data[i].data()), serialized_size);
			}
			
			// 反序列化到 bitmap_table
			bitmap_table->Deserialize(data);
		}
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
	// Stop background serialization thread
	if (serialization_thread) {
		stop_serialization_thread = true;
		queue_cv.notify_all();
		if (serialization_thread->joinable()) {
			serialization_thread->join();
		}
		serialization_thread.reset();
	}
	
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
			int32_t old_value = bitmap_table->get_value(rowid);
			bitmap_table->ClearRow(rowid);
			index_dirty = true; // Mark index as dirty
			if (old_value >= 0) {
				std::lock_guard<std::mutex> lock(serialization_mutex);
				dirty_bitmaps.insert(old_value);
			}
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
		int32_t bitmap_idx = static_cast<int32_t>(value);
		bitmap_table->SetRowValue(rowid, bitmap_idx);
		index_dirty = true; // Mark index as dirty
		// Mark this bitmap as dirty for incremental serialization
		{
			std::lock_guard<std::mutex> lock(serialization_mutex);
			dirty_bitmaps.insert(bitmap_idx);
		}
		// Add to pending updates for batch processing
		{
			std::lock_guard<std::mutex> lock(pending_mutex);
			// Serialize this bitmap to add to pending updates
			if (bitmap_idx >= 0 && bitmap_idx < static_cast<int32_t>(bitmap_table->bitmaps.size())) {
				const auto &bitmap = bitmap_table->bitmaps[bitmap_idx];
				size_t serialized_size = bitmap.getSizeInBytes();
				std::vector<char> bitmap_data(serialized_size);
				bitmap.write(reinterpret_cast<char*>(bitmap_data.data()));
				pending_updates.push_back({bitmap_idx, std::move(bitmap_data)});
				
				// Flush if threshold reached
				if (pending_updates.size() >= BATCH_UPDATE_THRESHOLD) {
					FlushPendingUpdates();
				}
			}
		}
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
		// Get the value before clearing to mark the bitmap as dirty
		int32_t old_value = bitmap_table->get_value(rowid);
		bitmap_table->ClearRow(rowid);
		index_dirty = true; // Mark index as dirty
		// Mark the bitmap that was cleared as dirty
		if (old_value >= 0) {
			std::lock_guard<std::mutex> lock(serialization_mutex);
			dirty_bitmaps.insert(old_value);
		}
	}
}

IndexStorageInfo BitmapIndex::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	if (!bitmap_table) {
		IndexStorageInfo info;
		return info;
	}

	// Flush any pending batch updates first
	FlushPendingUpdates();

	IndexStorageInfo info(name);
	info.options = options;	
	
	// Optimization: Skip serialization if index hasn't changed and allocator exists
	// This significantly improves performance when checkpoint is triggered frequently
	if (!index_dirty && bitmap_allocator && !bitmap_allocator->Empty()) {
		// Index hasn't changed - reuse existing allocator info
		info.allocator_infos.push_back(bitmap_allocator->GetInfo());
		return info;
	}
	
	// create allocator (buffer) for blockManager (disk writer)
	const idx_t segment_size = 4096;
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	
	// Optimization: Reuse existing allocator if it exists
	// FixedSizeBuffer::Serialize has early-out: if buffer is already on disk and not dirty, it returns immediately
	if (!bitmap_allocator) {
		bitmap_allocator = make_uniq<FixedSizeAllocator>(segment_size, block_manager);
	}
	
	std::lock_guard<std::mutex> lock(serialization_mutex);
	
	// Incremental serialization: only serialize dirty bitmaps if possible
	if (!dirty_bitmaps.empty() && !bitmap_allocator->Empty() && !bitmap_locations.empty()) {
		// Use incremental serialization
		SerializeDirtyBitmaps(context, *bitmap_allocator);
	} else {
		// Full serialization: serialize all bitmaps
		auto serialized_data = bitmap_table->Serialize();
		
		// Reset allocator if it's not empty (to write fresh data)
		if (!bitmap_allocator->Empty()) {
			bitmap_allocator->Reset();
			bitmap_locations.clear();
		}

		// Helper function to write data across multiple segments
		auto write_data = [&](const void* data, idx_t size) -> void {
			idx_t remaining = size;
			const char* src = reinterpret_cast<const char*>(data);
			
			while (remaining > 0) {
				// Allocate a new segment if needed
				auto index_ptr = bitmap_allocator->New();
				auto handle = bitmap_allocator->GetHandle(index_ptr);
				auto ptr = handle.GetPtr();
				handle.MarkModified();
				
				idx_t to_write = MinValue<idx_t>(remaining, segment_size);
				memcpy(ptr, src, to_write);
				
				src += to_write;
				remaining -= to_write;
			}
		};

		// Write metadata
		int32_t metadata[3];
		metadata[0] = serialized_data.num_bitmaps;
		metadata[1] = serialized_data.encoding;
		*reinterpret_cast<uint64_t*>(&metadata[2]) = serialized_data.number_of_rows;
		write_data(metadata, sizeof(int32_t) * 2 + sizeof(uint64_t));

		// Write each bitmap (size + data) and record location
		for (int32_t i = 0; i < static_cast<int32_t>(serialized_data.bitmap_data.size()); i++) {
			uint32_t size = static_cast<uint32_t>(serialized_data.bitmap_data[i].size());
			// Record the location before writing (we'll use the first segment pointer)
			// For simplicity, we store the pointer to the size field
			IndexPointer start_ptr = bitmap_allocator->New();
			auto handle = bitmap_allocator->GetHandle(start_ptr);
			auto ptr = handle.GetPtr();
			handle.MarkModified();
			bitmap_locations[i] = start_ptr;
			
			// Write size
			memcpy(ptr, &size, sizeof(uint32_t));
			
			// Write bitmap data
			write_data(serialized_data.bitmap_data[i].data(), serialized_data.bitmap_data[i].size());
		}
	}

	// Serialize buffers to disk using PartialBlockManager
	// FixedSizeBuffer::Serialize has early-out: if buffer is already on disk and not dirty, it returns immediately
	// However, since we just wrote new data, buffers will be dirty and need to be serialized
	PartialBlockManager partial_block_manager(context, block_manager, PartialBlockType::FULL_CHECKPOINT);
	bitmap_allocator->SerializeBuffers(partial_block_manager);
	partial_block_manager.FlushPartialBlocks();

	// Get allocator info and add to IndexStorageInfo
	info.allocator_infos.push_back(bitmap_allocator->GetInfo());
	
	// Mark index as clean after successful serialization
	index_dirty = false;
	dirty_bitmaps.clear();

	return info;
}

IndexStorageInfo BitmapIndex::SerializeToWAL(const case_insensitive_map_t<Value> &options) {
	if (!bitmap_table) {
		IndexStorageInfo info;
		return info;
	}

	IndexStorageInfo info(name);
	info.options = options;

	// serialize the table
	auto serialized_data = bitmap_table->Serialize();
	idx_t total_size = sizeof(int32_t) * 2 + sizeof(uint64_t); // metadata
	for (const auto& bitmap_bytes: serialized_data.bitmap_data) { // data
		total_size += sizeof(uint32_t) + bitmap_bytes.size(); // size + [raw_bytes]
	}

	// create allocator (buffer) for blockManager
	const idx_t segment_size = 4096;
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	if (!bitmap_allocator) {
		bitmap_allocator = make_uniq<FixedSizeAllocator>(segment_size, block_manager);
	} else {
		// Reset allocator if it already exists (for re-serialization)
		bitmap_allocator->Reset();
	}

	// Helper function to write data across multiple segments
	auto write_data = [&](const void* data, idx_t size) -> void {
		idx_t remaining = size;
		const char* src = reinterpret_cast<const char*>(data);
		
		while (remaining > 0) {
			// Allocate a new segment if needed
			auto index_ptr = bitmap_allocator->New();
			auto handle = bitmap_allocator->GetHandle(index_ptr);
			auto ptr = handle.GetPtr();
			handle.MarkModified();
			
			idx_t to_write = MinValue<idx_t>(remaining, segment_size);
			memcpy(ptr, src, to_write);
			
			src += to_write;
			remaining -= to_write;
		}
	};

	// Write metadata
	int32_t metadata[3];
	metadata[0] = serialized_data.num_bitmaps;
	metadata[1] = serialized_data.encoding;
	*reinterpret_cast<uint64_t*>(&metadata[2]) = serialized_data.number_of_rows;
	write_data(metadata, sizeof(int32_t) * 2 + sizeof(uint64_t));

	// Write each bitmap (size + data)
	for (const auto &bitmap_bytes: serialized_data.bitmap_data) {
		uint32_t size = static_cast<uint32_t>(bitmap_bytes.size());
		write_data(&size, sizeof(uint32_t));
		write_data(bitmap_bytes.data(), bitmap_bytes.size());
	}

	// Prepare for WAL serialization
	// buffers is vector<vector<IndexBufferInfo>>, so we need to wrap the result
	auto wal_buffers = bitmap_allocator->InitSerializationToWAL();
	info.buffers.push_back(std::move(wal_buffers));
	info.allocator_infos.push_back(bitmap_allocator->GetInfo());

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
// Background serialization thread loop
void BitmapIndex::SerializationThreadLoop() {
	while (!stop_serialization_thread) {
		std::unique_lock<std::mutex> lock(queue_mutex);
		queue_cv.wait(lock, [this] { 
			return stop_serialization_thread || !serialization_queue.empty(); 
		});
		
		if (stop_serialization_thread && serialization_queue.empty()) {
			break;
		}
		
		while (!serialization_queue.empty()) {
			auto task = std::move(serialization_queue.front());
			serialization_queue.pop();
			lock.unlock();
			
			// Execute serialization task
			task();
			
			lock.lock();
		}
	}
}

// Incremental serialization: serialize only dirty bitmaps
void BitmapIndex::SerializeDirtyBitmaps(QueryContext context, FixedSizeAllocator &allocator) {
	if (!bitmap_table || dirty_bitmaps.empty()) {
		return;
	}
	
	const idx_t segment_size = 4096;
	
	// Helper function to write data across multiple segments
	auto write_data = [&](const void* data, idx_t size) -> void {
		idx_t remaining = size;
		const char* src = reinterpret_cast<const char*>(data);
		
		while (remaining > 0) {
			auto index_ptr = allocator.New();
			auto handle = allocator.GetHandle(index_ptr);
			auto ptr = handle.GetPtr();
			handle.MarkModified();
			
			idx_t to_write = MinValue<idx_t>(remaining, segment_size);
			memcpy(ptr, src, to_write);
			
			src += to_write;
			remaining -= to_write;
		}
	};
	
	// Serialize only dirty bitmaps
	for (int32_t bitmap_idx : dirty_bitmaps) {
		if (bitmap_idx < 0 || bitmap_idx >= static_cast<int32_t>(bitmap_table->bitmaps.size())) {
			continue;
		}
		
		const auto &bitmap = bitmap_table->bitmaps[bitmap_idx];
		size_t serialized_size = bitmap.getSizeInBytes();
		std::vector<char> bitmap_data(serialized_size);
		bitmap.write(reinterpret_cast<char*>(bitmap_data.data()));
		
		// Update location if it exists, otherwise create new
		IndexPointer start_ptr = allocator.New();
		auto handle = allocator.GetHandle(start_ptr);
		auto ptr = handle.GetPtr();
		handle.MarkModified();
		bitmap_locations[bitmap_idx] = start_ptr;
		
		uint32_t size = static_cast<uint32_t>(bitmap_data.size());
		// Write size to the first segment
		memcpy(ptr, &size, sizeof(uint32_t));
		
		// Write bitmap data
		write_data(bitmap_data.data(), bitmap_data.size());
	}
}

// Batch update: flush pending updates
void BitmapIndex::FlushPendingUpdates() {
	std::lock_guard<std::mutex> lock(pending_mutex);
	if (pending_updates.empty()) {
		return;
	}
	
	// Process all pending updates
	// Note: This is a simplified version - in a real implementation,
	// we would batch these updates more efficiently
	for (const auto &update : pending_updates) {
		// Mark bitmap as dirty
		{
			std::lock_guard<std::mutex> serial_lock(serialization_mutex);
			dirty_bitmaps.insert(update.bitmap_idx);
		}
	}
	
	// Clear pending updates after processing
	pending_updates.clear();
}

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
