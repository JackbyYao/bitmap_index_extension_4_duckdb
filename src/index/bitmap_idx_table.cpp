
#include <fstream>
#include <algorithm>

#include "bitmap_idx_table.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"

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

// Helper: count bits in a 64-bit word
static inline int popcount64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcountll(x);
#else
    // Fallback naive
    int cnt = 0;
    while (x) { cnt += x & 1; x >>= 1; }
    return cnt;
#endif
}

// Helper: ensure a bitmap (vector<uint64_t>) can hold bits up to pos
static void ensure_size(std::vector<uint64_t> &bm, uint64_t pos_bits) {
    size_t words = (pos_bits + 63) / 64;
    if (bm.size() < words) bm.resize(words, 0);
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

    // initialize each bitmap with current number_of_rows bits
    for (int i = 0; i < num_bitmaps; ++i) {
        ensure_size(bitmaps[i], number_of_rows);
    }

    // If INDEX_PATH is set and on_disk was requested, we could implement file loading here.
    // For a starter implementation we keep everything in-memory.
}

int BitmapTable::append(int /*tid*/, int val)
{
    std::lock_guard<std::mutex> guard(g_lock);

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
        // set bit at number_of_rows in bitmap[val]
        ensure_size(bitmaps[val], number_of_rows + 1);
        bitmaps[val][number_of_rows / 64] |= (uint64_t(1) << (number_of_rows % 64));

        // grow other bitmaps as needed (they remain zero-extended)
        for (int i = 0; i < num_bitmaps; ++i) ensure_size(bitmaps[i], number_of_rows + 1);

        number_of_rows += 1;
    } else if (config->encoding == Table_config::RE) {
        if (val < 0) {
            return -1;
        }
        EnsureBitmapForValue(num_bitmaps > 0 ? num_bitmaps - 1 : val);
        // set bits from val..end at position number_of_rows
        for (int idx = val; idx < num_bitmaps; ++idx) {
            ensure_size(bitmaps[idx], number_of_rows + 1);
            bitmaps[idx][number_of_rows / 64] |= (uint64_t(1) << (number_of_rows % 64));
        }
        // ensure earlier bitmaps grow
        for (int idx = 0; idx < val; ++idx) ensure_size(bitmaps[idx], number_of_rows + 1);

        number_of_rows += 1;
    } else {
        return -1;
    }

    return 0;
}

int BitmapTable::update(int /*tid*/, uint64_t rowid, int to_val)
{
    std::lock_guard<std::mutex> guard(g_lock);
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
            ensure_size(bitmaps[from_val], rowid + 1);
            bitmaps[from_val][rowid / 64] &= ~(uint64_t(1) << (rowid % 64));
        }
        if (to_val >= 0 && to_val < num_bitmaps) {
            EnsureBitmapForValue(to_val);
            ensure_size(bitmaps[to_val], rowid + 1);
            bitmaps[to_val][rowid / 64] |= (uint64_t(1) << (rowid % 64));
        }
    } else if (config->encoding == Table_config::RE) {
        int minv, maxv;
        if (to_val > from_val) { minv = from_val; maxv = to_val - 1; }
        else { minv = to_val; maxv = from_val - 1; }
        for (int idx = minv; idx <= maxv; ++idx) {
            ensure_size(bitmaps[idx], rowid + 1);
            // toggle the bit
            bitmaps[idx][rowid / 64] ^= (uint64_t(1) << (rowid % 64));
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
    std::vector<uint64_t> tmp;
    {
        std::lock_guard<std::mutex> guard(g_lock);
        if (val >= (uint32_t)num_bitmaps) return 0;
        tmp = bitmaps[val];
    }
    // count bits
    uint64_t cnt = 0;
    for (uint64_t w : tmp) cnt += popcount64(w);
    return (int)cnt;
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
        bool bit = false;
        if (rowid / 64 < bitmaps[curVal].size()) {
            uint64_t w = bitmaps[curVal][rowid / 64];
            bit = ((w >> (rowid % 64)) & 1) != 0;
        }
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
    std::lock_guard<std::mutex> guard(g_lock);
    for (int i = 0; i < num_bitmaps; ++i) bytes += bitmaps[i].size() * sizeof(uint64_t);
    std::cout << "M BM " << bytes << std::endl;
}

void BitmapTable::printUncompMemory()
{
    // For this simple in-memory structure, compressed == uncompressed
    printMemory();
}

void BitmapTable::SetRowValue(uint64_t rowid, int to_val) {
    std::lock_guard<std::mutex> guard(g_lock);
    if (!config) return;
    if (config->encoding == Table_config::EE) {
        if (to_val >= 0) {
            EnsureBitmapForValue(to_val);
        }
        // clear any existing bit across all bitmaps at rowid
        for (int i = 0; i < num_bitmaps; ++i) {
            ensure_size(bitmaps[i], rowid + 1);
            bitmaps[i][rowid / 64] &= ~(uint64_t(1) << (rowid % 64));
        }
        if (to_val >= 0 && to_val < num_bitmaps) {
            ensure_size(bitmaps[to_val], rowid + 1);
            bitmaps[to_val][rowid / 64] |= (uint64_t(1) << (rowid % 64));
        }
    } else if (config->encoding == Table_config::RE) {
        if (to_val >= 0) {
            EnsureBitmapForValue(to_val);
        }
        // in RE, bits from val..end represent >= val. To set to_val, we need to
        // set bits in [to_val, end) and clear others below.
        for (int i = 0; i < num_bitmaps; ++i) ensure_size(bitmaps[i], rowid + 1);
        for (int i = 0; i < num_bitmaps; ++i) {
            if (i < to_val) {
                bitmaps[i][rowid / 64] &= ~(uint64_t(1) << (rowid % 64));
            } else {
                bitmaps[i][rowid / 64] |= (uint64_t(1) << (rowid % 64));
            }
        }
    }
    // adjust number_of_rows if rowid extends beyond it
    if (rowid >= number_of_rows) number_of_rows = rowid + 1;
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
    for (int i = old_count; i < num_bitmaps; ++i) {
        ensure_size(bitmaps[i], number_of_rows);
    }
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
    std::lock_guard<std::mutex> guard(g_lock);
    for (int i = 0; i < num_bitmaps; ++i) {
        if (rowid / 64 >= bitmaps[i].size()) {
            continue;
        }
        bitmaps[i][rowid / 64] &= ~(uint64_t(1) << (rowid % 64));
    }
}

uint64_t BitmapTable::GetMemoryUsageBytes() const {
    std::lock_guard<std::mutex> guard(g_lock);
    uint64_t bytes = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        bytes += bitmaps[i].size() * sizeof(uint64_t);
    }
    return bytes;
}

uint64_t BitmapTable::GetTotalBitSize() const {
    std::lock_guard<std::mutex> guard(g_lock);
    uint64_t bits = 0;
    for (int i = 0; i < num_bitmaps; ++i) {
        bits += bitmaps[i].size() * 64ULL;
    }
    return bits;
}

uint64_t BitmapTable::GetCompressionRatio() const {
    // No compression applied yet; return 1.
    return 1;
}

std::vector<std::string> BitmapTable::GetDistinctValues() const {
    std::vector<std::string> result;
    std::lock_guard<std::mutex> guard(g_lock);
    for (int value = 0; value < num_bitmaps; ++value) {
        const auto &bitmap = bitmaps[value];
        bool has_value = false;
        for (auto word : bitmap) {
            if (word != 0) {
                has_value = true;
                break;
            }
        }
        if (has_value) {
            result.push_back(std::to_string(value));
        }
    }
    return result;
}
