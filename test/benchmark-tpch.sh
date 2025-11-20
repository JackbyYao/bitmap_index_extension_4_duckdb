#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_DIR="$(dirname "$SCRIPT_DIR")"
REPO_NAME=$(basename "$REPO_DIR")

DUCKDB_BIN="${REPO_DIR}/build/release/duckdb"
EXTENSION_PATH="$(realpath "${REPO_DIR}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension")"

EXTENSION_NAME="bitmap_idx"
RESULT_DIR="./benchmark-results"
DATESTAMP=$(date +"%Y%m%d-%H%M%S")

mkdir -p "${RESULT_DIR}"

OUT_WITH_EXT="${RESULT_DIR}/${DATESTAMP}-with-extension.txt"
OUT_BASELINE="${RESULT_DIR}/${DATESTAMP}-baseline.txt"

# aligned folders for per-query tests
WITH_DIR="${RESULT_DIR}/${DATESTAMP}-per-query-with"
BASE_DIR="${RESULT_DIR}/${DATESTAMP}-per-query-baseline"
mkdir -p "$WITH_DIR" "$BASE_DIR"

# Clean DuckDB logs (in case they exist)
rm -f duckdb_logs.txt duckdb_tmp_* || true


# -----------------------------
# Check DuckDB binary
# -----------------------------
if [ ! -x "${DUCKDB_BIN}" ]; then
    echo "Error: DuckDB binary not found: ${DUCKDB_BIN}"
    exit 1
fi

# Check extension
if [ ! -f "${EXTENSION_PATH}" ]; then
    echo "Error: Extension file missing: ${EXTENSION_PATH}"
    exit 1
fi

# -------------------------------------------------------
# Per-query benchmarks (1–22)
# Each executed separately with/without extension
# -------------------------------------------------------
echo "----------------------------------------------"
echo " Per-query benchmark 1..22"
echo "----------------------------------------------"

#10 , 21
for q in {1..22}; do
    echo "Running Q${q} WITH extension..."
    timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${WITH_DIR}/q${q}.out" 2>&1
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);

-- indexes (same as global run)
CREATE INDEX C_MKTSEGMENT_idx ON CUSTOMER USING BITMAP (C_MKTSEGMENT);
CREATE INDEX O_ORDERPRIORITY_idx ON ORDERS USING BITMAP (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_idx ON ORDERS USING BITMAP (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_idx ON SUPPLIER USING BITMAP (S_NATIONKEY);
CREATE INDEX C_NATIONKEY_idx ON CUSTOMER USING BITMAP (C_NATIONKEY);
CREATE INDEX P_TYPE_idx ON PART USING BITMAP (P_TYPE);
CREATE INDEX PS_SUPPKEY_idx ON PARTSUPP USING BITMAP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_idx ON LINEITEM USING BITMAP (L_RETURNFLAG);

PRAGMA threads=8;

.timer on
PRAGMA tpch(${q});
EOF

    echo "Running Q${q} BASELINE()..."
    timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${BASE_DIR}/q${q}.out" 2>&1
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);
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
PRAGMA threads=8;
.timer on
PRAGMA tpch(${q});
EOF

done

echo ""
echo "----------------------------------------------"
echo " Benchmark Finished!"
echo " Output:"
echo "  Global with extension: $OUT_WITH_EXT"
echo "  Global baseline:        $OUT_BASELINE"
echo "  Per-query with ext:     $WITH_DIR/"
echo "  Per-query baseline:     $BASE_DIR/"
echo "----------------------------------------------"