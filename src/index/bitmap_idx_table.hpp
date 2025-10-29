#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <thread>

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

//void merge_func(BaseTable *table, int begin, int range, Table_config *config, std::shared_timed_mutex *bitmap_mutex = nullptr);

//extern bool run_merge_func;

class BitmapTable : public BaseTable {
public:
    BitmapTable(duckdb::BlockManager &block_manager,Table_config *config);

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

protected:
    // Global read-write lock to protect the whole bitmap index.
    std::shared_timed_mutex g_lock;

    void _get_value(uint64_t rowid, int begin, int range, bool *flag, int *result);

    uint64_t number_of_rows = 0;
};

}

