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


echo "----------------------------------------------"
echo " Running TPC-H with Bitmap Index Extension"
echo "----------------------------------------------"
"${DUCKDB_BIN}" ":memory:" <<SQL | tee "${OUT_WITH_EXT}"
-- Install & load required extensions
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';

INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.001);

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
EXPLAIN ANALYZE SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_returnflag = 'R'
    AND o_orderdate >= DATE '1993-10-01' -- Starting date of the quarter
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
SELECT now(), '--- TPC-H with bitmap extension ---';
SQL


echo "----------------------------------------------"
echo " Running TPC-H Baseline (No Extension)"
echo "----------------------------------------------"
"${DUCKDB_BIN}" ":memory:" <<SQL | tee "${OUT_BASELINE}"
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.001);

PRAGMA threads=8;

SELECT now(), '--- baseline ---';
--EXPLAIN ANALYZE PRAGMA tpch(6);
EXPLAIN ANALYZE SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_returnflag = 'R'
    AND o_orderdate >= DATE '1993-10-01' -- Starting date of the quarter
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
SELECT now(), '--- baseline ---';
SQL


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
    timeout 120s "${DUCKDB_BIN}" ":memory:" <<EOF > "${WITH_DIR}/q${q}.out" 2>&1
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.001);

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

    echo "Running Q${q} BASELINE..."
    timeout 120s "${DUCKDB_BIN}" ":memory:" <<EOF > "${BASE_DIR}/q${q}.out" 2>&1
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.001);

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