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


echo "----------------------------------------------"
echo " Running TPC-H with Bitmap Index Extension"
echo "----------------------------------------------"
"${DUCKDB_BIN}" ":memory:" <<SQL | tee "${OUT_WITH_EXT}"
-- Install & load required extensions
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';

INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);

-- Create bitmap indexes useful for TPC-H queries
-- Only equality predicates, numeric + VARCHAR supported
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

SELECT now(), '--- TPC-H with bitmap extension ---';
-- EXPLAIN ANALYZE PRAGMA tpch(6);    -- example query
PRAGMA tpch(2);
SELECT now(), '--- TPC-H with bitmap extension ---';
SQL


echo "----------------------------------------------"
echo " Running TPC-H Baseline (No Extension) , ART"
echo "----------------------------------------------"
"${DUCKDB_BIN}" ":memory:" <<SQL | tee "${OUT_BASELINE}"
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);

PRAGMA threads=8;

SELECT now(), '--- baseline ---';
--EXPLAIN ANALYZE PRAGMA tpch(6);
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

PRAGMA tpch(2);
SELECT now(), '--- baseline ---';
SQL


echo ""
echo "----------------------------------------------"
echo " Benchmark Finished!"
echo " Output:"
echo "  Global with extension: $OUT_WITH_EXT"
echo "  Global baseline:        $OUT_BASELINE"

echo "----------------------------------------------"