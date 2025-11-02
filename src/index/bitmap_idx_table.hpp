#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <sstream>
#include <thread>

#include "duckdb/common/common.hpp"

namespace duckdb {

struct Table_config {
    // encoding: "EE" (element encoding) or "RE" (run encoding). We expose
    // these as simple integer constants for convenience.
    enum Encoding { EE = 0, RE = 1 } encoding = EE;

    // logical cardinality / number of distinct values
    int g_cardinality = 0;

    // initial number of rows
    uint64_t n_rows = 0;

    int nThreads_for_getval = 1;
    std::string INDEX_PATH;
    bool enable_fence_pointer = false;
    bool on_disk = false;
    
};

class BaseTable {
public:
    BaseTable(Table_config *config) : config(config), cardinality(config->g_cardinality) {}

    Table_config *const config;

    virtual ~BaseTable() = default;

    virtual int update(int tid, uint64_t rowid, int to_val) { return -1; }
    virtual int remove(int tid, uint64_t rowid) { return -1; }
    virtual int append(int tid, int val) { return -1; }
    virtual int evaluate(int tid, uint32_t val) { return -1; }
    virtual void printMemory() { }
    virtual void printUncompMemory() { }

protected:
    const int32_t cardinality;

    std::string getBitmapName(int val) const {
        std::stringstream ss;
        if (config) {
            ss << config->INDEX_PATH << "/" << val << ".bm";
        } else {
            ss << val << ".bm";
        }
        return ss.str();
    }
};

//void merge_func(BaseTable *table, int begin, int range, Table_config *config, std::mutex *bitmap_mutex = nullptr);

//extern bool run_merge_func;

class BitmapTable : public BaseTable {
public:
    explicit BitmapTable(Table_config *config);

    int update(int tid, uint64_t rowid, int to_val) override;
    int remove(int tid, uint64_t rowid) override;
    int append(int tid, int val) override;
    int evaluate(int tid, uint32_t val) override;

    void _read_btv(int begin, int end);

    void printMemory() override;
    void printUncompMemory() override;

    int get_value(uint64_t rowid);

    // Expose current number of rows.
    uint64_t GetNumberOfRows() const { return number_of_rows; }

    // Set the value at an arbitrary row (create/overwrite). This sets the bitmap bits
    // for the requested value and clears other conflicting bits according to encoding.
    void SetRowValue(uint64_t rowid, int to_val);

    // In-memory bitmap storage. Each bitmap is a vector of 64-bit words.
    // TODO could use other better implementations.
    std::vector<std::vector<uint64_t>> bitmaps;
    int num_bitmaps = 0;

    uint64_t GetMemoryUsageBytes() const;
    uint64_t GetTotalBitSize() const;
    uint64_t GetOnDiskSizeBytes() const { return GetMemoryUsageBytes(); }
    uint64_t GetCompressionRatio() const;
    std::vector<std::string> GetDistinctValues() const;

    void ClearRow(uint64_t rowid);

    template <class FUN>
    void ForEachValue(FUN &&fun) const {
        std::lock_guard<std::mutex> guard(g_lock);
        for (int value = 0; value < num_bitmaps; value++) {
            const auto &bitmap = bitmaps[value];
            for (idx_t word_idx = 0; word_idx < bitmap.size(); word_idx++) {
                auto word = bitmap[word_idx];
                while (word) {
                    auto lsb = word & -word;
#if defined(__GNUC__) || defined(__clang__)
                    auto bit_offset = __builtin_ctzll(word);
#else
                    idx_t bit_offset = 0;
                    uint64_t tmp = word;
                    while ((tmp & 1) == 0) {
                        tmp >>= 1;
                        bit_offset++;
                    }
#endif
                    row_t row = static_cast<row_t>(word_idx * 64 + bit_offset);
                    if (!fun(row, static_cast<idx_t>(value))) {
                        return;
                    }
                    word &= ~lsb;
                }
            }
        }
    }

protected:
    // Global read-write lock to protect the whole bitmap index.
    mutable std::mutex g_lock;

    void EnsureBitmapForValue(int value);
    void _get_value(uint64_t rowid, int begin, int range, bool *flag, int *result);

    uint64_t number_of_rows = 0;
};

}
