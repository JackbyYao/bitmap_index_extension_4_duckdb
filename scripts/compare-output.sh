#!/usr/bin/env bash
set -euo pipefail

RESULT_DIR="./benchmark-results"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
SUMMARY_FILE="${RESULT_DIR}/summary-${TIMESTAMP}.txt"

# Most recent result directories (the last per-query run)
WITH_DIR=$(ls -dt ${RESULT_DIR}/*-per-query-with | head -n 1)
BASE_DIR=$(ls -dt ${RESULT_DIR}/*-per-query-baseline | head -n 1)

if [ ! -d "$WITH_DIR" ] || [ ! -d "$BASE_DIR" ]; then
    echo "Error: Could not find per-query with/baseline directories."
    exit 1
fi

echo "Comparing:" | tee "$SUMMARY_FILE"
echo "  WITH extension:  $WITH_DIR" | tee -a "$SUMMARY_FILE"
echo "  BASELINE:        $BASE_DIR" | tee -a "$SUMMARY_FILE"
echo "" | tee -a "$SUMMARY_FILE"

function extract_time() {
    local file="$1"
    # Returns: real user sys OR "" if missing
    grep "Run Time (s)" "$file" | \
        sed -E 's/.*real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+).*/\1 \2 \3/' || echo ""
}

for q in {1..22}; do
    FILE_WITH="${WITH_DIR}/q${q}.out"
    FILE_BASE="${BASE_DIR}/q${q}.out"

    if [ ! -f "$FILE_WITH" ] || [ ! -f "$FILE_BASE" ]; then
        continue
    fi

    echo "========================= Query ${q} =========================" | tee -a "$SUMMARY_FILE"

    # ------------------------------
    # 1) DIFF query results (excluding timing lines)
    # ------------------------------
    diff <(grep -v "Run Time (s)" "$FILE_WITH") \
         <(grep -v "Run Time (s)" "$FILE_BASE") \
         > /tmp/q${q}.diff || true

    if [ -s /tmp/q${q}.diff ]; then
        echo "[DIFF] Output differs!" | tee -a "$SUMMARY_FILE"
    else
        echo "[OK] Output identical." | tee -a "$SUMMARY_FILE"
    fi

    # ------------------------------
    # 2) Extract timings
    # ------------------------------
    TIME_WITH=$(extract_time "$FILE_WITH")
    TIME_BASE=$(extract_time "$FILE_BASE")

    if [ -z "$TIME_WITH" ] || [ -z "$TIME_BASE" ]; then
        echo "Missing timing information." | tee -a "$SUMMARY_FILE"
        echo "" | tee -a "$SUMMARY_FILE"
        continue
    fi

    read real_w user_w sys_w <<< "$TIME_WITH"
    read real_b user_b sys_b <<< "$TIME_BASE"

    # compute diffs
    real_diff=$(awk "BEGIN {print $real_w - $real_b}")
    user_diff=$(awk "BEGIN {print $user_w - $user_b}")
    sys_diff=$(awk "BEGIN {print $sys_w - $sys_b}")

    echo "Timing:" | tee -a "$SUMMARY_FILE"
    echo "  WITH:     real $real_w   user $user_w   sys $sys_w" | tee -a "$SUMMARY_FILE"
    echo "  BASELINE: real $real_b   user $user_b   sys $sys_b" | tee -a "$SUMMARY_FILE"
    echo "  DIFF:     real $real_diff   user $user_diff   sys $sys_diff" | tee -a "$SUMMARY_FILE"
    echo "" | tee -a "$SUMMARY_FILE"

done

echo "Summary written to: $SUMMARY_FILE"