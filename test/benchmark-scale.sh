#!/usr/bin/env bash
set -euo pipefail

# Benchmark TPCH at multiple scale-factors (sf) and collect per-query results.
# Defaults tuned for a machine with 32GB RAM and 8 cores.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
REPO_DIR="$(dirname "$SCRIPT_DIR")"
REPO_NAME=$(basename "$REPO_DIR")

DUCKDB_BIN="${REPO_DIR}/build/release/duckdb"
# Use an array so we can include flags safely
DUCKDB_CMD=("${DUCKDB_BIN}" -unsigned)

EXTENSION_PATH="$(realpath "${REPO_DIR}/build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension")"
EXTENSION_NAME="bitmap_idx"

# Default scale factors (space-separated). Override by passing SFs as args.
DEFAULT_SFS=(0.01 0.1 1 5 10 25)
#DEFAULT_SFS=(0.001 0.01 0.1 )
SFS=()
if [ "$#" -gt 0 ]; then
    SFS=("$@")
else
    SFS=("${DEFAULT_SFS[@]}")
fi

# Queries to run (default 1..22). Set QUERIES env to override.
if [ -z "${QUERIES-}" ]; then
    QUERIES=( {1..22} )
else
    read -r -a QUERIES <<<"$QUERIES"
fi

# Timeouts and resources
TIMEOUT_S=${TIMEOUT_S-3600}   # seconds per query (default 1 hour)
THREADS=${THREADS-8}
MEM_LIMIT=${MEM_LIMIT-32GB}

RESULT_BASE_DIR="./benchmark-results"
DATESTAMP=$(date +"%Y%m%d-%H%M%S")

# Group directory for this scale sweep
GROUP_DIR="${RESULT_BASE_DIR}/scale${DATESTAMP}"
WITH_DIR="${GROUP_DIR}/with"
BASE_DIR="${GROUP_DIR}/baseline"
mkdir -p "${WITH_DIR}" "${BASE_DIR}"
mkdir -p "${RESULT_BASE_DIR}"

echo "Benchmarking TPCH across scale-factors: ${SFS[*]}"
echo "Queries: ${QUERIES[*]}"
echo "Threads: ${THREADS}, Memory limit: ${MEM_LIMIT}"
echo "Results will be written to: ${RESULT_BASE_DIR}"

# Sanity checks
if [ ! -x "${DUCKDB_BIN}" ]; then
    echo "Error: DuckDB binary not found or not executable: ${DUCKDB_BIN}"
    exit 1
fi
if [ ! -f "${EXTENSION_PATH}" ]; then
    echo "Warning: Extension file not found: ${EXTENSION_PATH}"
    echo "The script will still attempt the baseline (no bitmap extension) runs."
fi

# Helper to run a sequence of queries for a given SF and mode
run_queries_for_sf() {
    local sf=$1
    local mode=$2   # "with" or "baseline"
    local outdir="$3"

    mkdir -p "${outdir}"

    for q in "${QUERIES[@]}"; do
        echo "[sf=${sf}] Running Q${q} (${mode})..."
        local outfile="${outdir}/q${q}-sf${sf}.out"

        # Build the SQL script for this invocation
        if [ "${mode}" = "with" ]; then
            cat > /tmp/tpch_run_${sf}_${q}.sql <<SQL
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${sf});

-- create example bitmap indexes used by the extension
CREATE INDEX C_MKTSEGMENT_idx ON CUSTOMER USING BITMAP (C_MKTSEGMENT);
CREATE INDEX O_ORDERPRIORITY_idx ON ORDERS USING BITMAP (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_idx ON ORDERS USING BITMAP (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_idx ON SUPPLIER USING BITMAP (S_NATIONKEY);
CREATE INDEX C_NATIONKEY_idx ON CUSTOMER USING BITMAP (C_NATIONKEY);
CREATE INDEX P_TYPE_idx ON PART USING BITMAP (P_TYPE);
CREATE INDEX PS_SUPPKEY_idx ON PARTSUPP USING BITMAP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_idx ON LINEITEM USING BITMAP (L_RETURNFLAG);

PRAGMA threads=${THREADS};
PRAGMA memory_limit='${MEM_LIMIT}';
.timer on
PRAGMA tpch(${q});
SQL
        else
            # baseline: no bitmap extension; use classical indexes
            cat > /tmp/tpch_run_${sf}_${q}.sql <<SQL
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${sf});

-- baseline indexes (artificial, regular B-tree style indexes)
CREATE INDEX C_MKTSEGMENT_art_idx ON CUSTOMER (C_MKTSEGMENT);
CREATE INDEX C_NATIONKEY_art_idx ON CUSTOMER (C_NATIONKEY);
CREATE INDEX O_ORDERPRIORITY_art_idx ON ORDERS (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_art_idx ON ORDERS (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_art_idx ON SUPPLIER (S_NATIONKEY);
CREATE INDEX P_TYPE_art_idx ON PART (P_TYPE);
CREATE INDEX PS_SUPPKEY_art_idx ON PARTSUPP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_art_idx ON LINEITEM (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_art_idx ON LINEITEM (L_RETURNFLAG);

PRAGMA threads=${THREADS};
PRAGMA memory_limit='${MEM_LIMIT}';
.timer on
PRAGMA tpch(${q});
SQL
        fi

        # Run the command (use array expansion so flags are preserved)
        timeout ${TIMEOUT_S}s "${DUCKDB_CMD[@]}" ":memory:" < /tmp/tpch_run_${sf}_${q}.sql > "${outfile}" 2>&1 || true

        # Stamp which SF and mode produced this file
        echo "# SF=${sf} MODE=${mode} QUERY=${q}" >> "${outfile}"
    done
}

# Main loop over SFs
for sf in "${SFS[@]}"; do
    sf_label="sf_${sf//./_}"

    # BASELINE runs (run original B-tree/art indices first)
    run_queries_for_sf "${sf}" baseline "${BASE_DIR}"

    # WITH extension (run bitmap extension second if available)
    if [ -f "${EXTENSION_PATH}" ]; then
        run_queries_for_sf "${sf}" with "${WITH_DIR}"
    else
        echo "Skipping 'with' runs for sf=${sf}: extension not found: ${EXTENSION_PATH}"
    fi

    echo "Completed runs for sf=${sf}. Results in: ${GROUP_DIR}"
done

# Create simple global marker files so `benchmark-diff-all.sh` can find this run by timestamp
echo "scale run: ${DATESTAMP}" > "${RESULT_BASE_DIR}/${DATESTAMP}-with-extension.txt"
echo "scale baseline run: ${DATESTAMP}" > "${RESULT_BASE_DIR}/${DATESTAMP}-baseline.txt"

echo "\nAll scale-factor runs finished. Results directory: ${GROUP_DIR}"
echo "Per-sf outputs are under: ${GROUP_DIR}/with and ${GROUP_DIR}/baseline"
echo "Top-level marker files: ${RESULT_BASE_DIR}/${DATESTAMP}-with-extension.txt"
ls -la "${GROUP_DIR}"

echo "Done. Use the per-query outputs under each sf folder for analysis."
