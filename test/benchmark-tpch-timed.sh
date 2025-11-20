#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_DIR="$(dirname "$SCRIPT_DIR")"
BRANCH_NAME=$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "detached")
BRANCH_TAG=$(echo "$BRANCH_NAME" | sed 's/[^A-Za-z0-9._-]/_/g')

DUCKDB_BIN="${REPO_DIR}/build/release/duckdb"
EXTENSION_PATH="$(realpath "${REPO_DIR}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension")"
EXTENSION_NAME="bitmap_idx"

RESULT_DIR="./benchmark-results"
DATESTAMP=$(date +"%Y%m%d-%H%M%S")
mkdir -p "$RESULT_DIR"

TIMED_WITH_DIR="${RESULT_DIR}/${DATESTAMP}-${BRANCH_TAG}-timed-per-query-with"
TIMED_BASE_DIR="${RESULT_DIR}/${DATESTAMP}-${BRANCH_TAG}-timed-per-query-baseline"
mkdir -p "$TIMED_WITH_DIR" "$TIMED_BASE_DIR"

if [ ! -x "$DUCKDB_BIN" ]; then
    echo "Error: DuckDB binary not found: $DUCKDB_BIN"
    exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
    echo "Error: Extension file missing: $EXTENSION_PATH"
    exit 1
fi

echo "----------------------------------------------"
echo " Timed per-query benchmark (1..22)"
echo "----------------------------------------------"

run_query() {
    local mode=$1
    local query=$2
    local outfile=$3

    TIMEFORMAT='Run Time (s): real %R user %U sys %S'

    if [ "$mode" = "with" ]; then
        {
            time "$DUCKDB_BIN" ":memory:" <<EOF
INSTALL '$EXTENSION_PATH';
LOAD '$EXTENSION_NAME';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.03);
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
.timer off
PRAGMA tpch(${query});
EOF
        } >> "$outfile" 2>&1
    else
        {
            time "$DUCKDB_BIN" ":memory:" <<EOF
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.03);
CREATE INDEX C_MKTSEGMENT_art_idx ON CUSTOMER (C_MKTSEGMENT);
CREATE INDEX C_NATIONKEY_art_idx ON CUSTOMER (C_NATIONKEY);
CREATE INDEX O_ORDERPRIORITY_art_idx ON ORDERS (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_art_idx ON ORDERS (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_art_idx ON SUPPLIER (S_NATIONKEY);
CREATE INDEX P_TYPE_art_idx ON PART (P_TYPE);
CREATE INDEX PS_SUPPKEY_art_idx ON PARTSUPP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_art_idx ON LINEITEM (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_art_idx ON LINEITEM (L_RETURNFLAG);
PRAGMA threads=8;
.timer off
PRAGMA tpch(${query});
EOF
        } >> "$outfile" 2>&1
    fi
}

for q in {1..22}; do
    echo "Running timed Q${q} WITH extension..."
    run_query "with" "${q}" "${TIMED_WITH_DIR}/q${q}.out"

    echo "Running timed Q${q} BASELINE..."
    run_query "baseline" "${q}" "${TIMED_BASE_DIR}/q${q}.out"
done

echo "Timed benchmark results saved under:"
echo "  With extension:    $TIMED_WITH_DIR"
echo "  Baseline:          $TIMED_BASE_DIR"
