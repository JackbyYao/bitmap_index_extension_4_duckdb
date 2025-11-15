#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Setup
###############################################################################
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_NAME=$(basename "${SCRIPT_DIR%/*}")
DUCKDB_BIN="../${REPO_NAME}/build/release/duckdb"
EXTENSION_PATH="../${REPO_NAME}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension"

TS=$(date +"%Y%m%d-%H%M%S")
RESULT_ROOT="benchmark-results/${TS}"
mkdir -p "${RESULT_ROOT}"

# basedir for per-query results
WITH_DIR="${RESULT_ROOT}/with"
BASE_DIR="${RESULT_ROOT}/baseline"
mkdir -p "${WITH_DIR}" "${BASE_DIR}"

###############################################################################
# Ensure DuckDB exists
###############################################################################
if [ ! -x "${DUCKDB_BIN}" ]; then
    echo "Error: DuckDB binary not found at ${DUCKDB_BIN}"
    exit 1
fi

###############################################################################
# Function to run a single TPC-H query (1–22)
###############################################################################
run_query_with_extension() {
    local q=$1
    local out_file="${WITH_DIR}/q$(printf "%02d" ${q})-with.out"
    local time_file="${WITH_DIR}/q$(printf "%02d" ${q})-with.time"

    "${DUCKDB_BIN}" -unsigned <<SQL > "${out_file}" 2> /dev/null
.timer on
LOAD '${EXTENSION_PATH}';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.1);

-- Create bitmap indexes
CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP(L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_idx ON LINEITEM USING BITMAP(L_RETURNFLAG);
CREATE INDEX L_LINESTATUS_idx ON LINEITEM USING BITMAP(L_LINESTATUS);
CREATE INDEX O_ORDERSTATUS_idx ON ORDERS USING BITMAP(O_ORDERSTATUS);
CREATE INDEX PS_SUPPKEY_idx ON PARTSUPP USING BITMAP(PS_SUPPKEY);
CREATE INDEX P_TYPE_idx ON PART USING BITMAP(P_TYPE);
CREATE INDEX P_SIZE_idx ON PART USING BITMAP(P_SIZE);
CREATE INDEX C_MKTSEGMENT_idx ON CUSTOMER USING BITMAP(C_MKTSEGMENT);

-- Run query
PRAGMA tpch($q);
.timer off
SQL

    # Extract timing from output
    grep "Run Time" -m1 "${out_file}" | sed 's/^/TIME: /' > "${time_file}"
}

run_query_without_extension() {
    local q=$1
    local out_file="${BASE_DIR}/q$(printf "%02d" ${q})-base.out"
    local time_file="${BASE_DIR}/q$(printf "%02d" ${q})-base.time"

    "${DUCKDB_BIN}" -unsigned <<SQL > "${out_file}" 2> /dev/null
.timer on
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.1);

PRAGMA tpch($q);
.timer off
SQL

    grep "Run Time" -m1 "${out_file}" | sed 's/^/TIME: /' > "${time_file}"
}

###############################################################################
# Main loop
###############################################################################
echo "Running per-query benchmark WITH extension..."
for q in $(seq 1 22); do
    echo "Running query $q (with extension)..."
    run_query_with_extension $q
done

echo "Running per-query benchmark WITHOUT extension..."
for q in $(seq 1 22); do
    echo "Running query $q (baseline)..."
    run_query_without_extension $q
done

###############################################################################
# Compare outputs
###############################################################################
echo "Comparing outputs..."

for q in $(seq 1 22); do
    w="${WITH_DIR}/q$(printf "%02d" ${q})-with.out"
    b="${BASE_DIR}/q$(printf "%02d" ${q})-base.out"

    diff_out="${RESULT_ROOT}/q$(printf "%02d" ${q})-diff.txt"

    if diff -q <(grep -v "Run Time" "$w") <(grep -v "Run Time" "$b") > /dev/null; then
        echo "Q$q: OK" > "${diff_out}"
    else
        echo "Q$q: MISMATCH" > "${diff_out}"
        diff <(grep -v "Run Time" "$w") <(grep -v "Run Time" "$b") >> "${diff_out}"
    fi
done

###############################################################################
# Cleanup: remove timing strings from output logs
###############################################################################
find "${RESULT_ROOT}" -type f -name "*.out" -exec sed -i '/Run Time/d' {} \;

echo "Done."
echo "Results stored in: ${RESULT_ROOT}"