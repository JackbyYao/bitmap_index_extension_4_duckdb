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
RESULT_BASE_DIR="./benchmark-results"
DATESTAMP=$(date +"%Y%m%d-%H%M%S")

GROUP_DIR="${RESULT_BASE_DIR}/tpcds${DATESTAMP}"
WITH_DIR="${GROUP_DIR}/with"
BASE_DIR="${GROUP_DIR}/baseline"
mkdir -p "${WITH_DIR}" "${BASE_DIR}" "${RESULT_BASE_DIR}"

OUT_WITH_EXT="${RESULT_BASE_DIR}/${DATESTAMP}-with-extension.txt"
OUT_BASELINE="${RESULT_BASE_DIR}/${DATESTAMP}-baseline.txt"

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

# Prefer a locally-built tpcds extension if available to avoid attempting
# to download from the DuckDB extension server (which can 404).
TPCDS_LOCAL_EXT="${REPO_DIR}/build/release/extension/tpcds/tpcds.duckdb_extension"
if [ -f "${TPCDS_LOCAL_EXT}" ]; then
	TPCDS_INSTALL_SQL="LOAD '${TPCDS_LOCAL_EXT}';"
else
	TPCDS_INSTALL_SQL=$'INSTALL tpcds;\nLOAD tpcds;'
fi

# -------------------------------------------------------
# Per-query benchmarks (TPC-DS 1..99)
# Each executed separately with/without extension
# -------------------------------------------------------
echo "----------------------------------------------"
echo " Per-query TPC-DS benchmark 1..99"
echo "----------------------------------------------"


for q in {1..99}; do
    echo "Running Q${q} BASELINE()..."
    timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${BASE_DIR}/q${q}.out" 2>&1
${TPCDS_INSTALL_SQL}
CALL dsdgen(sf=0.0001);

-- FACT TABLE INDEXES
CREATE INDEX ss_store_sk_art_idx ON store_sales (ss_store_sk);
CREATE INDEX ss_promo_sk_art_idx ON store_sales (ss_promo_sk);
CREATE INDEX cs_ship_mode_sk_art_idx ON catalog_sales (cs_ship_mode_sk);
CREATE INDEX ws_ship_mode_sk_art_idx ON web_sales (ws_ship_mode_sk);
CREATE INDEX ws_promo_sk_art_idx ON web_sales (ws_promo_sk);

-- CREATE INDEX ss_customer_sk_art_idx ON store_sales (ss_customer_sk);
-- CREATE INDEX cs_bill_customer_sk_art_idx ON catalog_sales (cs_bill_customer_sk);
-- CREATE INDEX cs_warehouse_sk_art_idx ON catalog_sales (cs_warehouse_sk);
-- CREATE INDEX ws_warehouse_sk_art_idx ON web_sales (ws_warehouse_sk);

-- DIMENSION TABLE INDEXES
CREATE INDEX i_category_art_idx ON item (i_category);
CREATE INDEX i_class_art_idx ON item (i_class);
CREATE INDEX d_moy_art_idx ON date_dim (d_moy);
CREATE INDEX hd_income_band_sk_art_idx ON household_demographics (hd_income_band_sk);

-- CREATE INDEX i_brand_art_idx ON item (i_brand);
-- CREATE INDEX d_dow_art_idx ON date_dim (d_dow);
-- CREATE INDEX d_qoy_art_idx ON date_dim (d_qoy);
-- CREATE INDEX d_year_art_idx ON date_dim (d_year);
-- CREATE INDEX c_current_cdemo_sk_art_idx ON customer (c_current_cdemo_sk);
-- CREATE INDEX c_current_hdemo_sk_art_idx ON customer (c_current_hdemo_sk);
-- CREATE INDEX c_current_addr_sk_art_idx ON customer (c_current_addr_sk);
-- CREATE INDEX hd_buy_potential_art_idx ON household_demographics (hd_buy_potential);
-- CREATE INDEX s_store_name_art_idx ON store (s_store_name);

PRAGMA threads=8;
.timer on
PRAGMA tpcds(${q});
EOF

    echo "Running Q${q} WITH extension..."
    timeout 1200s "${DUCKDB_BIN}" ":memory:" <<EOF > "${WITH_DIR}/q${q}.out" 2>&1
INSTALL '${EXTENSION_PATH}';
LOAD '${EXTENSION_NAME}';
${TPCDS_INSTALL_SQL}
CALL dsdgen(sf=0.0001);

-- FACT TABLES
CREATE INDEX ss_store_sk_idx ON store_sales USING BITMAP (ss_store_sk);
CREATE INDEX ss_promo_sk_idx ON store_sales USING BITMAP (ss_promo_sk);
CREATE INDEX cs_ship_mode_sk_idx ON catalog_sales USING BITMAP (cs_ship_mode_sk);
CREATE INDEX ws_ship_mode_sk_idx ON web_sales USING BITMAP (ws_ship_mode_sk);
CREATE INDEX ws_promo_sk_idx ON web_sales USING BITMAP (ws_promo_sk);

-- CREATE INDEX ss_customer_sk_idx ON store_sales USING BITMAP (ss_customer_sk);
-- CREATE INDEX cs_bill_customer_sk_idx ON catalog_sales USING BITMAP (cs_bill_customer_sk);

-- DIMENSIONS
CREATE INDEX i_category_idx ON item USING BITMAP (i_category);
CREATE INDEX i_class_idx ON item USING BITMAP (i_class);
CREATE INDEX d_moy_idx ON date_dim USING BITMAP (d_moy);
CREATE INDEX hd_income_band_sk_idx ON household_demographics USING BITMAP (hd_income_band_sk);

-- CREATE INDEX i_brand_idx ON item USING BITMAP (i_brand);
-- CREATE INDEX d_dow_idx ON date_dim USING BITMAP (d_dow);
-- CREATE INDEX d_qoy_idx ON date_dim USING BITMAP (d_qoy);
-- CREATE INDEX d_year_idx ON date_dim USING BITMAP (d_year);
-- CREATE INDEX c_current_cdemo_sk_idx ON customer USING BITMAP (c_current_cdemo_sk);
-- CREATE INDEX c_current_hdemo_sk_idx ON customer USING BITMAP (c_current_hdemo_sk);
-- CREATE INDEX hd_buy_potential_idx ON household_demographics USING BITMAP (hd_buy_potential);
-- CREATE INDEX s_store_name_idx ON store USING BITMAP (s_store_name);

PRAGMA threads=8;

.timer on
PRAGMA tpcds(${q});
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

# Create simple global marker files so `benchmark-diff-all.sh` can find this run by timestamp
echo "tpcds run: ${DATESTAMP}" > "$OUT_WITH_EXT"
echo "tpcds baseline run: ${DATESTAMP}" > "$OUT_BASELINE"

