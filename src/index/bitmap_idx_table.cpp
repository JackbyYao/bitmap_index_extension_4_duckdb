
#include <fstream>
#include <algorithm>
#include <unordered_set>

#include "bitmap_idx_table.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/common/exception.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

using namespace duckdb;

bool run_merge = false;

void merge_func(BaseTable *table, int begin, int range, Table_config *config, std::mutex *bitmap_mutex)
{

    (void)table;
    (void)begin;
    (void)range;
    (void)config;
    (void)bitmap_mutex;

    while (run_merge) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

BitmapTable::BitmapTable(Table_config *config) : BaseTable(config), number_of_rows(config ? config->n_rows : 0)
{
    if (!config) {
        num_bitmaps = 0;
        return;
    }

    if (config->encoding == Table_config::EE) {
        num_bitmaps = config->g_cardinality;
    } else if (config->encoding == Table_config::RE) {
        num_bitmaps = std::max(0, config->g_cardinality - 1);
    }
    else {
        std::cerr << "Bitmap only supports EE and RE encoding schemes" << std::endl;
        assert(0);
    }

    bitmaps.resize(num_bitmaps);

    // If INDEX_PATH is set and on_disk was requested, we could implement file loading here.
    // For a starter implementation we keep everything in-memory.
}

int BitmapTable::append(int /*tid*/, int val)
{
    std::unique_lock<std::shared_mutex> guard(g_lock);

    if (!config) return -1;

    if (config->on_disk) {
        // Disk-backed behavior not implemented in this minimal version.
        (void)val;
        return -1;
    }

    if (config->encoding == Table_config::EE) {
        if (val < 0) {
            return -1;
        }
        EnsureBitmapForValue(val);
        // add row to bitmap
        bitmaps[val].add(number_of_rows);
        number_of_rows += 1;

    } else if (config->encoding == Table_config::RE) {
        if (val < 0) {
            return -1;
        }
        EnsureBitmapForValue(num_bitmaps > 0 ? num_bitmaps - 1 : val);
        // set bits from val..end at position number_of_rows
        for (int idx = val; idx < num_bitmaps; ++idx) {
            bitmaps[idx].add(number_of_rows);
        }
        number_of_rows += 1;

    } else {
        return -1;
    }

    return 0;
}

int BitmapTable::update(int /*tid*/, uint64_t rowid, int to_val)
{
    std::unique_lock<std::shared_mutex> guard(g_lock);
    if (!config) return -1;
    int from_val = get_value(rowid);
    if ((from_val == to_val) || (from_val == -1)) return -ENOENT;

    if (config->on_disk) {
        // Not implemented for disk-backed in this minimal version.
        (void)rowid; (void)to_val;
        return -1;
    }

    if (config->encoding == Table_config::EE) {
        if (from_val >= 0 && from_val < num_bitmaps) {
            bitmaps[from_val].remove(rowid);
        }
        if (to_val >= 0 && to_val < num_bitmaps) {
            EnsureBitmapForValue(to_val);
            bitmaps[to_val].add(rowid);
        }
    } else if (config->encoding == Table_config::RE) {
        int minv, maxv;
        if (to_val > from_val) { minv = from_val; maxv = to_val - 1; }
        else { minv = to_val; maxv = from_val - 1; }
        for (int idx = minv; idx <= maxv; ++idx) {
            if (bitmaps[idx].contains(rowid)) {
                bitmaps[idx].remove(rowid);
            } else {
                bitmaps[idx].add(rowid);
            }
        }
    }

    return 0;
}

int BitmapTable::remove(int /*tid*/, uint64_t rowid)
{
    if (!config) return -1;
    ClearRow(rowid);
    return 0;
}

int BitmapTable::evaluate(int /*tid*/, uint32_t val)
{
    roaring::Roaring tmp;
    {
        std::shared_lock<std::shared_mutex> guard(g_lock);
        if (val >= (uint32_t)num_bitmaps) return 0;
        tmp = bitmaps[val];
    }
    return static_cast<int>(tmp.cardinality());
}

void BitmapTable::_get_value(uint64_t rowid, int begin, int range, bool *flag, int *result)
{
    int ret = -1;
    for (int i = 0; i < range; ++i) {
        int curVal = begin + i;
        if (curVal < 0 || curVal >= num_bitmaps) continue;

        if (config->encoding == Table_config::EE) {
            if (__atomic_load_n(flag, __ATOMIC_SEQ_CST)) break;
        }

        // check bit
        bool bit = bitmaps[curVal].contains(rowid);
        if (bit) {
            if (config->encoding == Table_config::EE) {
                ret = curVal;
                __atomic_store_n(flag, true, __ATOMIC_SEQ_CST);
                break;
            } else if (config->encoding == Table_config::RE) {
                if (ret == -1) ret = curVal;
                else ret = (curVal < ret) ? curVal : ret;
            }
        }
    }

    __atomic_store_n(result, ret, __ATOMIC_RELEASE);
}

int BitmapTable::get_value(uint64_t rowid)
{
    bool flag = false;
    if (!config) return -1;
    int n_threads = (config->nThreads_for_getval > num_bitmaps) ? num_bitmaps : config->nThreads_for_getval;
    if (n_threads <= 0) n_threads = 1;
    int offset = num_bitmaps / n_threads;
    if (offset <= 0) offset = 1;

    std::vector<std::thread> threads;
    std::vector<int> local_results(n_threads, -1);

    for (int i = 0; i < n_threads; ++i) {
        int b = i * offset;
        int range = offset;
        if ((i == (n_threads - 1)) && (num_bitmaps > n_threads)) range += (num_bitmaps % n_threads);
        threads.emplace_back(&BitmapTable::_get_value, this, rowid, b, range, &flag, &local_results[i]);
    }

    int ret = -1;
    for (size_t t = 0; t < threads.size(); ++t) {
        threads[t].join();
        int tmp = __atomic_load_n(&local_results[t], __ATOMIC_RELAXED);
        if (tmp != -1) {
            if (config->encoding == Table_config::EE) {
                if (ret == -1) ret = tmp;
            } else if (config->encoding == Table_config::RE) {
                if (ret == -1) ret = tmp;
                else ret = (tmp < ret) ? tmp : ret;
            }
        }
    }

    return ret;
}

void BitmapTable::printMemory()
{
    uint64_t bytes = 0;
    std::shared_lock<std::shared_mutex> guard(g_lock);
    for (int i = 0; i < num_bitmaps; ++i) {
        bytes += bitmaps[i].getSizeInBytes();
    }
    std::cout << "M BM " << bytes << std::endl;
}

void BitmapTable::printUncompMemory()
{
    // For roaring map, uncompressed size would be max 'row * 8 bytes'
    uint64_t bytes = 0;
    std::shared_lock<std::shared_mutex> guard(g_lock);
    for (int i = 0; i < num_bitmaps; i++) {
        uint64_t max_row = bitmaps[i].maximum();
        if (max_row != 0) {
            bytes += ((max_row) + 63) / 64 * sizeof(uint64_t);
        }
    }
    std::cout << "U BM " << bytes << std::endl;
}

void BitmapTable::SetRowValue(uint64_t rowid, int to_val) {
    std::unique_lock<std::shared_mutex> guard(g_lock);
    if (!config) return;
    if (config->encoding == Table_config::EE) {
        if (to_val >= 0) {
            EnsureBitmapForValue(to_val);
        }
        // Use fast lookup table to find current value (O(1) instead of O(num_bitmaps))
        int from_val = -1;
        auto it = rowid_to_value.find(rowid);
        if (it != rowid_to_value.end()) {
            from_val = it->second;
        }
        
        // Only remove from the bitmap that actually contains rowid
        if (from_val >= 0 && from_val < num_bitmaps) {
            bitmaps[from_val].remove(rowid);
            rowid_to_value.erase(rowid);  // Remove old mapping
        }
        // Add to target bitmap if valid
        if (to_val >= 0 && to_val < num_bitmaps) {
            bitmaps[to_val].add(rowid);
            rowid_to_value[rowid] = to_val;  // Update mapping
        } else if (to_val < 0) {
            // If to_val is negative, remove from mapping
            rowid_to_value.erase(rowid);
        }
    } else if (config->encoding == Table_config::RE) {
        if (to_val >= 0) {
            EnsureBitmapForValue(to_val);
        }
        // For RE encoding, we still need to find current value to optimize range operations
        // Use lookup table if available, otherwise fall back to scanning
        int from_val = -1;
        auto it = rowid_to_value.find(rowid);
        if (it != rowid_to_value.end()) {
            from_val = it->second;
        }
        
        if (from_val >= 0) {
            // Only update the range that changed
            int minv = std::min(from_val, to_val);
            int maxv = std::max(from_val, to_val);
            for (int i = minv; i <= maxv; ++i) {
                if (i < to_val) {
                    if (bitmaps[i].contains(rowid)) {
                        bitmaps[i].remove(rowid);
                    }
                } else {
                    if (!bitmaps[i].contains(rowid)) {
                        bitmaps[i].add(rowid);
                    }
                }
            }
        } else {
            // No previous value, set all bits according to to_val
            for (int i = 0; i < num_bitmaps; ++i) {
                if (i < to_val) {
                    if (bitmaps[i].contains(rowid)) {
                        bitmaps[i].remove(rowid);
                    }
                } else {
                    if (!bitmaps[i].contains(rowid)) {
                        bitmaps[i].add(rowid);
                    }
                }
            }
        }
        
        // Update mapping for RE encoding
        if (to_val >= 0) {
            rowid_to_value[rowid] = to_val;
        } else {
            rowid_to_value.erase(rowid);
        }
    }
    // adjust number_of_rows if rowid extends beyond it
    if (rowid >= number_of_rows) number_of_rows = rowid + 1;
}

void BitmapTable::SetRowValuesBatch(const std::vector<std::pair<uint64_t, int>> &updates) {
    std::unique_lock<std::shared_mutex> guard(g_lock);
    if (!config || updates.empty()) return;

    // Ensure all required bitmaps exist
    int max_value = -1;
    for (const auto &update : updates) {
        if (update.second >= 0 && update.second > max_value) {
            max_value = update.second;
        }
    }
    if (max_value >= 0) {
        EnsureBitmapForValue(max_value);
    }

    if (config->encoding == Table_config::EE) {
        // Optimized batch processing for EE encoding:
        // 1. Group removes by from_value to reduce array access
        // 2. Group adds by to_value to reduce array access
        // 3. Cache bitmap references to avoid repeated array indexing
        // 4. Use reserve and emplace_back for memory efficiency
        
        // Estimate sizes to reduce reallocations
        const size_t updates_size = updates.size();
        const size_t estimated_values = std::min(static_cast<size_t>(num_bitmaps), updates_size / 4);
        
        // Map: from_value -> vector of rowids to remove
        std::unordered_map<int, std::vector<uint64_t>> removes_by_value;
        removes_by_value.reserve(estimated_values);
        // Map: to_value -> vector of rowids to add
        std::unordered_map<int, std::vector<uint64_t>> adds_by_value;
        adds_by_value.reserve(estimated_values);
        // Track rowids to remove from mapping (use set for faster lookup)
        std::unordered_set<uint64_t> rowids_to_clear_set;
        rowids_to_clear_set.reserve(updates_size);
        
        // First pass: collect all operations grouped by value
        for (const auto &update : updates) {
            uint64_t rowid = update.first;
            int to_val = update.second;
            
            // Find current value using lookup table
            int from_val = -1;
            auto it = rowid_to_value.find(rowid);
            if (it != rowid_to_value.end()) {
                from_val = it->second;
            }
            
            // Collect remove operation
            if (from_val >= 0 && from_val < num_bitmaps) {
                removes_by_value[from_val].emplace_back(rowid);
                rowids_to_clear_set.insert(rowid);
            }
            
            // Collect add operation
            if (to_val >= 0 && to_val < num_bitmaps) {
                adds_by_value[to_val].emplace_back(rowid);
            } else {
                // Negative value means clear
                if (from_val >= 0) {
                    rowids_to_clear_set.insert(rowid);
                }
            }
            
            // Update number_of_rows
            if (rowid >= number_of_rows) {
                number_of_rows = rowid + 1;
            }
        }
        
        // Reserve capacity for vectors to reduce reallocations
        for (auto &entry : removes_by_value) {
            entry.second.reserve(entry.second.size());
        }
        for (auto &entry : adds_by_value) {
            entry.second.reserve(entry.second.size());
        }
        
        // Second pass: batch remove operations (grouped by bitmap to reduce array access)
        for (const auto &entry : removes_by_value) {
            int value = entry.first;
            const auto &rowids = entry.second;
            // Cache bitmap reference to avoid repeated array access
            auto &bitmap = bitmaps[value];
            for (uint64_t rowid : rowids) {
                bitmap.remove(rowid);
            }
        }
        
        // Third pass: batch add operations (grouped by bitmap to reduce array access)
        for (const auto &entry : adds_by_value) {
            int value = entry.first;
            const auto &rowids = entry.second;
            // Cache bitmap reference to avoid repeated array access
            auto &bitmap = bitmaps[value];
            for (uint64_t rowid : rowids) {
                bitmap.add(rowid);
                rowid_to_value[rowid] = value;
            }
        }
        
        // Update mapping: remove cleared rowids (only if not being added)
        for (uint64_t rowid : rowids_to_clear_set) {
            // Check if rowid is being added
            bool being_added = false;
            for (const auto &entry : adds_by_value) {
                const auto &rowids = entry.second;
                // Use binary search if sorted, or linear search
                if (std::find(rowids.begin(), rowids.end(), rowid) != rowids.end()) {
                    being_added = true;
                    break;
                }
            }
            if (!being_added) {
                rowid_to_value.erase(rowid);
            }
        }
        
    } else if (config->encoding == Table_config::RE) {
        // Optimized batch processing for RE encoding:
        // 1. Group operations by bitmap index to reduce array access
        // 2. Use lookup table to find current values (O(1) lookup)
        // 3. Only update the range that changed for each rowid
        // 4. Cache bitmap references to avoid repeated array indexing
        // 5. Use reserve and emplace_back for memory efficiency
        
        const size_t updates_size = updates.size();
        const size_t estimated_bitmaps = std::min(static_cast<size_t>(num_bitmaps), updates_size);
        
        // Map: bitmap_index -> vector of (rowid, should_add)
        // should_add: true means add, false means remove
        std::unordered_map<int, std::vector<std::pair<uint64_t, bool>>> bitmap_ops;
        bitmap_ops.reserve(estimated_bitmaps);
        // Track rowids that need full initialization (no previous value)
        std::vector<std::pair<uint64_t, int>> full_init_updates;
        full_init_updates.reserve(updates_size / 2);  // Estimate: half might need full init
        
        // First pass: collect all operations, grouped by affected bitmaps
        for (const auto &update : updates) {
            uint64_t rowid = update.first;
            int to_val = update.second;
            
            // Use lookup table to find current value
            int from_val = -1;
            auto it = rowid_to_value.find(rowid);
            if (it != rowid_to_value.end()) {
                from_val = it->second;
            }
            
            if (from_val >= 0 && to_val >= 0) {
                // Only update the range that changed
                int minv = std::min(from_val, to_val);
                int maxv = std::max(from_val, to_val);
                for (int i = minv; i <= maxv; ++i) {
                    bool should_add = (i >= to_val);
                    bitmap_ops[i].emplace_back(rowid, should_add);
                }
            } else if (from_val < 0 && to_val >= 0) {
                // No previous value, need full initialization
                full_init_updates.emplace_back(rowid, to_val);
            } else if (to_val < 0) {
                // Clearing: remove from all bitmaps where it exists
                if (from_val >= 0) {
                    // Remove from all bitmaps from from_val to end
                    for (int i = from_val; i < num_bitmaps; ++i) {
                        bitmap_ops[i].emplace_back(rowid, false);
                    }
                }
            }
            
            // Update mapping
            if (to_val >= 0) {
                rowid_to_value[rowid] = to_val;
            } else {
                rowid_to_value.erase(rowid);
            }
            
            // Update number_of_rows
            if (rowid >= number_of_rows) {
                number_of_rows = rowid + 1;
            }
        }
        
        // Reserve capacity for vectors to reduce reallocations
        for (auto &entry : bitmap_ops) {
            entry.second.reserve(entry.second.size());
        }
        
        // Second pass: batch process operations grouped by bitmap (reduces array access)
        for (const auto &entry : bitmap_ops) {
            int bitmap_idx = entry.first;
            const auto &ops = entry.second;
            // Cache bitmap reference to avoid repeated array access
            auto &bitmap = bitmaps[bitmap_idx];
            
            for (const auto &op : ops) {
                uint64_t rowid = op.first;
                bool should_add = op.second;
                
                if (should_add) {
                    if (!bitmap.contains(rowid)) {
                        bitmap.add(rowid);
                    }
                } else {
                    if (bitmap.contains(rowid)) {
                        bitmap.remove(rowid);
                    }
                }
            }
        }
        
        // Third pass: handle full initialization (rowids with no previous value)
        for (const auto &update : full_init_updates) {
            uint64_t rowid = update.first;
            int to_val = update.second;
            
            // Set all bits according to to_val
            for (int i = 0; i < num_bitmaps; ++i) {
                auto &bitmap = bitmaps[i];
                if (i < to_val) {
                    if (bitmap.contains(rowid)) {
                        bitmap.remove(rowid);
                    }
                } else {
                    if (!bitmap.contains(rowid)) {
                        bitmap.add(rowid);
                    }
                }
            }
        }
    }
}

void BitmapTable::MergeFrom(const BitmapTable &other) {
    std::unique_lock<std::shared_mutex> guard(g_lock);
    if (!config || !other.config) return;

    // Ensure both have the same encoding
    if (config->encoding != other.config->encoding) {
        throw InternalException("Bitmap index encoding mismatch during merge");
    }

    // Ensure this table has enough bitmaps
    int max_bitmaps = std::max(num_bitmaps, other.num_bitmaps);
    if (max_bitmaps > num_bitmaps) {
        num_bitmaps = max_bitmaps;
        bitmaps.resize(max_bitmaps);
    }

    // Merge bitmaps: for each value, union the other bitmap into this bitmap
    for (int i = 0; i < other.num_bitmaps; ++i) {
        if (i < num_bitmaps) {
            // Union operation: this_bitmap |= other_bitmap
            bitmaps[i] |= other.bitmaps[i];
        } else {
            // Other has more bitmaps, copy them
            bitmaps.push_back(other.bitmaps[i]);
        }
    }

    // Merge rowid_to_value mappings
    // For conflicts (same rowid in both), use the other's value (other is more recent)
    for (const auto &entry : other.rowid_to_value) {
        uint64_t rowid = entry.first;
        int value = entry.second;
        rowid_to_value[rowid] = value;
    }

    // Update number_of_rows to maximum
    number_of_rows = std::max(number_of_rows, other.number_of_rows);
}

void BitmapTable::EnsureBitmapForValue(int value) {
    if (value < 0) {
        return;
    }
    if (value < num_bitmaps) {
        return;
    }
    auto old_count = num_bitmaps;
    num_bitmaps = value + 1;
    bitmaps.resize(num_bitmaps);
    if (config) {
        config->g_cardinality = std::max(config->g_cardinality, num_bitmaps);
    }
}

void BitmapTable::ClearRow(uint64_t rowid) {
    if (!config) {
        return;
    }
    if (config->on_disk) {
        // Disk-backed behaviour not implemented.
        return;
    }
    std::unique_lock<std::shared_mutex> guard(g_lock);
    for (int i = 0; i < num_bitmaps; ++i) {
        bitmaps[i].remove(rowid);
    }
}

uint64_t BitmapTable::GetMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> guard(g_lock);
    uint64_t bytes = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        bytes += bitmaps[i].getSizeInBytes();
    }
    return bytes;
}

uint64_t BitmapTable::GetTotalBitSize() const {
    std::shared_lock<std::shared_mutex> guard(g_lock);
    uint64_t bits = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        bits += bitmaps[i].cardinality();
    }
    return bits;
}

uint64_t BitmapTable::GetCompressionRatio() const {
    if (num_bitmaps == 0) {
        return 1;
    }
    std::shared_lock<std::shared_mutex> guard(g_lock);
    uint64_t compressed = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        compressed += bitmaps[i].getSizeInBytes();
    }
    uint64_t uncompressed = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        uint64_t max_row = bitmaps[i].maximum();
        if (max_row != 0) {
            uncompressed += ((max_row + 63) / 64) * sizeof(uint64_t);
        }
    }
    if (uncompressed == 0) return 1;
    return uncompressed / compressed;
}

std::vector<std::string> BitmapTable::GetDistinctValues() const {
    std::vector<std::string> result;
    std::shared_lock<std::shared_mutex> guard(g_lock);
    for (int value = 0; value < num_bitmaps; ++value) {
        if (bitmaps[value].cardinality() > 0) {
            result.push_back(std::to_string(value));
        }
    }
    return result;
}

void BitmapTable::GetRowsForValue(int value, std::vector<row_t> &out) const {
    std::shared_lock<std::shared_mutex> guard(g_lock);
    if (value < 0 || value >= num_bitmaps) return;
    // get values
    const auto &bitmap = bitmaps[value];
    // Use CRoaring bulk array export for better performance
    size_t card = bitmap.cardinality();
    out.clear();
    out.resize(card);
    if (card == 0) return;
    if (sizeof(row_t) == sizeof(uint32_t)) {
        // can write directly into the vector's buffer
        bitmap.toUint32Array(reinterpret_cast<uint32_t *>(out.data()));
    } else {
        // widening path: write into temporary 32-bit buffer then widen
        std::vector<uint32_t> tmp(card);
        bitmap.toUint32Array(tmp.data());
        for (size_t i = 0; i < card; ++i) {
            out[i] = static_cast<row_t>(tmp[i]);
        }
    }
}

size_t BitmapTable::GetRowsForValueChunk(int value, size_t offset, size_t limit, row_t *out_buf) const {
    std::lock_guard<std::mutex> guard(g_lock);
    if (value < 0 || value >= num_bitmaps) return 0;
    const auto &bitmap = bitmaps[value];
    size_t card = bitmap.cardinality();
    if (offset >= card) return 0;
    size_t to_copy = std::min(limit, card - offset);
    if (to_copy == 0) return 0;

    if (sizeof(row_t) == sizeof(uint32_t)) {
        bitmap.rangeUint32Array(reinterpret_cast<uint32_t *>(out_buf), offset, to_copy);
    } else {
        std::vector<uint32_t> tmp(to_copy);
        bitmap.rangeUint32Array(tmp.data(), offset, to_copy);
        for (size_t i = 0; i < to_copy; ++i) {
            out_buf[i] = static_cast<row_t>(tmp[i]);
        }
    }
    return to_copy;
}
