#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_NAME=$(basename "${SCRIPT_DIR%/*}")
DUCKDB_BIN="../${REPO_NAME}/build/release/duckdb"
EXTENSION_NAME="bitmap_index"
RESULT_DIR="./benchmark-results"
DATESTAMP=$(date +"%Y%m%d-%H%M%S")

mkdir -p "${RESULT_DIR}"

OUT_WITH_EXT="${RESULT_DIR}/benchmark-tpch-${EXTENSION_NAME}-${DATESTAMP}.out.txt"
OUT_BASELINE="${RESULT_DIR}/benchmark-tpch-baseline-${DATESTAMP}.out.txt"

EXTENSION_PATH="../${REPO_NAME}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension"

# -----------------------------
# Check that DuckDB exists
# -----------------------------
if [ ! -x "${DUCKDB_BIN}" ]; then
    echo "Error: DuckDB binary not found at ${DUCKDB_BIN}"
    echo "Please build DuckDB first (e.g., 'make release')."
    exit 1
fi

# -----------------------------
# Run TPC-H with the extension
# -----------------------------
echo "Running TPC-H with extension '${EXTENSION_NAME}'..."
"${DUCKDB_BIN}" -unsigned <<SQL | tee "${OUT_WITH_EXT}"
-- 1. Load the custom extension (Using .load for custom extensions)
LOAD '${EXTENSION_PATH}';

-- 2. Install and load the TPC-H extension (fixes 'dbgen' and 'tpch_scale_factor' errors)
INSTALL tpch;
LOAD tpch;

-- 3. Generate data (assuming a new in-memory database instance)
CALL dbgen(sf=0.01);
--PRAGMA tpch_scale_factor=1;

CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP(L_SUPPKEY);

-- 4. Set configuration
PRAGMA threads=4;

-- 5. Run benchmark

-- NOTE: Ensure your DuckDB build includes the 'benchmark' extension if needed,
-- or replace with the actual TPC-H queries if 'benchmark' is not a valid function.
SELECT now(), '--- TPC-H with ${EXTENSION_NAME} ---';
PRAGMA tpch(6);
SELECT now(), '--- TPC-H with ${EXTENSION_NAME} ---';

-- 6. report the state of index:
-- SELECT * FROM bitmap_index_dump(L_SUPPKEY_idx);
SQL

# -----------------------------
# Run baseline (no extension)
# -----------------------------
echo "Running TPC-H baseline (no extension)..."
"${DUCKDB_BIN}" -unsigned <<SQL | tee "${OUT_BASELINE}"
-- 1. Install and load the TPC-H extension (MANDATORY for baseline run too!)
INSTALL tpch;
LOAD tpch;

-- 2. Generate data
CALL dbgen(sf=0.01);
--PRAGMA tpch_scale_factor=1;

-- 3. Set configuration

PRAGMA threads=4;

-- 4. Run benchmark (using now() instead of current_timestamp())
SELECT now(), '--- TPC-H baseline ---';

-- NOTE: As above, verify 'benchmark' function availability.
PRAGMA tpch(6);
SELECT now(), '--- TPC-H baseline ---';
SQL

# -----------------------------
# Done
# -----------------------------
echo "Benchmark complete!"
echo "With extension: ${OUT_WITH_EXT}"
echo "Baseline:       ${OUT_BASELINE}"
