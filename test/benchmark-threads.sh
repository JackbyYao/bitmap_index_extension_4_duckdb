#!/usr/bin/env bash
set -euo pipefail

# Benchmark TPCH queries across different thread counts.
# Reuses the bulk of `test/benchmark-tpch.sh` but loops over thread counts.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB_BIN="${REPO_DIR}/build/release/duckdb"
EXTENSION_PATH="$(realpath "${REPO_DIR}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension")"
EXTENSION_NAME="bitmap_idx"

RESULT_BASE_DIR="./benchmark-results"

# Single timestamp for this sweep so results are grouped together
TS=$(date +"%Y%m%d-%H%M%S")
GROUP_DIR="${RESULT_BASE_DIR}/threads${TS}"
WITH_DIR="${GROUP_DIR}/with"
BASE_DIR="${GROUP_DIR}/baseline"

mkdir -p "${WITH_DIR}" "${BASE_DIR}"

# Thread counts to test (machine has up to 8 cores)
THREADS_LIST=(1 2 4 8)

# Clean DuckDB logs (if present)
rm -f duckdb_logs.txt duckdb_tmp_* || true

if [ ! -x "${DUCKDB_BIN}" ]; then
    echo "Error: DuckDB binary not found: ${DUCKDB_BIN}"
    exit 1
fi

if [ ! -f "${EXTENSION_PATH}" ]; then
    echo "Warning: Extension file not found: ${EXTENSION_PATH}. Extension runs will fail if attempted."
fi

echo "Benchmark: TPCH q1..22, SF=1, threads=${THREADS_LIST[*]}"

for threads in "${THREADS_LIST[@]}"; do
    echo "\n=== Running benchmarks with threads=${threads} ==="

    # with extension: write per-query files named q<id>-t<threads>.out
    for q in {1..22}; do
        outfile="${WITH_DIR}/q${q}-t${threads}.out"
        echo "Running Q${q} WITH extension (threads=${threads}) -> ${outfile}"
        timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${outfile}" 2>&1 || true
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);

-- indexes (use bitmap indexes)
CREATE INDEX C_MKTSEGMENT_idx ON CUSTOMER USING BITMAP (C_MKTSEGMENT);
CREATE INDEX O_ORDERPRIORITY_idx ON ORDERS USING BITMAP (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_idx ON ORDERS USING BITMAP (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_idx ON SUPPLIER USING BITMAP (S_NATIONKEY);
CREATE INDEX C_NATIONKEY_idx ON CUSTOMER USING BITMAP (C_NATIONKEY);
CREATE INDEX P_TYPE_idx ON PART USING BITMAP (P_TYPE);
CREATE INDEX PS_SUPPKEY_idx ON PARTSUPP USING BITMAP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_idx ON LINEITEM USING BITMAP (L_RETURNFLAG);

PRAGMA threads=${threads};
.timer on
PRAGMA tpch(${q});
EOF
    done

    # baseline (regular indexes)
    for q in {1..22}; do
        outfile="${BASE_DIR}/q${q}-t${threads}.out"
        echo "Running Q${q} BASELINE (threads=${threads}) -> ${outfile}"
        timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${outfile}" 2>&1 || true
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);

-- CUSTOMER table
CREATE INDEX C_MKTSEGMENT_art_idx ON CUSTOMER (C_MKTSEGMENT);
CREATE INDEX C_NATIONKEY_art_idx ON CUSTOMER (C_NATIONKEY);

-- ORDERS table
CREATE INDEX O_ORDERPRIORITY_art_idx ON ORDERS (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_art_idx ON ORDERS (O_ORDERSTATUS);

-- SUPPLIER table
CREATE INDEX S_NATIONKEY_art_idx ON SUPPLIER (S_NATIONKEY);

-- PART table
CREATE INDEX P_TYPE_art_idx ON PART (P_TYPE);

-- PARTSUPP table
CREATE INDEX PS_SUPPKEY_art_idx ON PARTSUPP (PS_SUPPKEY);

-- LINEITEM table
CREATE INDEX L_SUPPKEY_art_idx ON LINEITEM (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_art_idx ON LINEITEM (L_RETURNFLAG);
PRAGMA threads=${threads};
.timer on
PRAGMA tpch(${q});
EOF
    done

    echo "Completed thread=${threads} runs. Outputs in: ${GROUP_DIR}/with and ${GROUP_DIR}/baseline"
done

echo "\nAll thread benchmarks finished. Results under ${GROUP_DIR}/"
echo "Per-run files are named q<id>-t<threads>.out inside the with/ and baseline/ folders."
