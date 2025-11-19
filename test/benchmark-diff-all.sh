#!/usr/bin/env bash
set -euo pipefail

# Compare benchmark-results pairs that share the same timestamp.
#
# Usage:
#   ./test/benchmark-diff-all.sh [BENCHMARK_DIR]
#
# If no directory is provided, the script defaults to the repository's
# `benchmark-results` sibling directory of `test/`.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
BENCH_DIR_INPUT=${1:-"$SCRIPT_DIR/../benchmark-results"}
BENCH_DIR=$(realpath "$BENCH_DIR_INPUT")

if [ ! -d "$BENCH_DIR" ]; then
    echo "Benchmark directory not found: $BENCH_DIR"
    exit 1
fi

echo "Using benchmark dir: $BENCH_DIR"

# Use a temporary file to collect timestamps (avoid associative arrays for older bash)
timestamps_file=$(mktemp)
trap 'rm -f "$timestamps_file"' EXIT

# Parse a timing line into seconds (prefer real/elapsed wall-clock time)
# Accept formats like: "real 0m0.123s", "0m0.123s", "Time: 0.123s", "0.123s", "123ms", or fallback to last numeric token.
parse_seconds() {
    line="$1"
    [ -z "$line" ] && { echo ""; return; }

    # real 0m0.123s or 0m0.123s
    if echo "$line" | grep -Eqi '([[:space:]]|^)real[[:space:]]+[0-9]+m[0-9]+(\.[0-9]+)?s'; then
        parts=$(echo "$line" | sed -E 's/.*real[[:space:]]+([0-9]+)m([0-9]+(\.[0-9]+)?)s.*/\1 \2/gi')
        min=$(printf "%s" "$parts" | awk '{print $1}')
        sec=$(printf "%s" "$parts" | awk '{print $2}')
        awk -v m="$min" -v s="$sec" 'BEGIN{printf "%.6f", m*60 + s}'
        return
    fi

    if echo "$line" | grep -Eo '[0-9]+m[0-9]+(\.[0-9]+)?s' >/dev/null 2>&1; then
        parts=$(echo "$line" | sed -E 's/.*([0-9]+)m([0-9]+(\.[0-9]+)?)s.*/\1 \2/')
        min=$(printf "%s" "$parts" | awk '{print $1}')
        sec=$(printf "%s" "$parts" | awk '{print $2}')
        awk -v m="$min" -v s="$sec" 'BEGIN{printf "%.6f", m*60 + s}'
        return
    fi

    # Seconds with 's'
    if echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?s' >/dev/null 2>&1; then
        val=$(echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?s' | tail -n1)
        num=$(echo "$val" | sed 's/s$//')
        printf "%s" "$num"
        return
    fi

    # Milliseconds
    if echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?ms' >/dev/null 2>&1; then
        val=$(echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?ms' | tail -n1)
        num=$(echo "$val" | sed 's/ms$//')
        awk -v n="$num" 'BEGIN{printf "%.6f", n/1000}'
        return
    fi

    # Fallback: last numeric token
    num=$(echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?' | tail -n1 || true)
    printf "%s" "$num"
}

# Find candidate timestamps from global files like: 20251119-025917-with-extension.txt
for f in "$BENCH_DIR"/*-with-extension.txt; do
    [ -e "$f" ] || continue
    bn=$(basename "$f")
    if [[ $bn =~ ^([0-9]{8}-[0-9]{6})-with-extension\.txt$ ]]; then
        ts=${BASH_REMATCH[1]}
        printf "%s\n" "$ts" >> "$timestamps_file"
    fi
done

# Also accept timestamps derived from per-query directories when globals are absent
for d in "$BENCH_DIR"/*-per-query-with; do
    [ -e "$d" ] || continue
    bn=$(basename "$d")
    if [[ $bn =~ ^([0-9]{8}-[0-9]{6})-per-query-with$ ]]; then
        ts=${BASH_REMATCH[1]}
        printf "%s\n" "$ts" >> "$timestamps_file"
    fi
done

if [ ! -s "$timestamps_file" ]; then
    echo "No '-with-extension.txt' files or '*-per-query-with' directories found in $BENCH_DIR"
    rm -f "$timestamps_file"
    exit 0
fi

found_pairs=0
total_diffs=0
nonempty_diffs=0

# Iterate unique timestamps (sorted) from the temporary file
while IFS=$'\n' read -r ts; do
    base_file="$BENCH_DIR/${ts}-baseline.txt"
    with_file="$BENCH_DIR/${ts}-with-extension.txt"
    with_dir="$BENCH_DIR/${ts}-per-query-with"
    base_dir="$BENCH_DIR/${ts}-per-query-baseline"

    has_global=false
    has_perquery=false

    if [ -f "$base_file" ] && [ -f "$with_file" ]; then
        has_global=true
    fi

    if [ -d "$with_dir" ] && [ -d "$base_dir" ]; then
        has_perquery=true
    fi

    # If we don't have anything to compare for this timestamp, skip it
    if [ "$has_global" = false ] && [ "$has_perquery" = false ]; then
        continue
    fi

    found_pairs=$((found_pairs+1))

    # Print a header for this timestamp
    echo
    echo "==================== Timestamp: $ts ===================="

    # Global diff (only if both files exist). Exclude last line from comparison.
    if [ "$has_global" = true ]; then
        echo "-- Global diff (excluding last line) --"
        if diff -u <(sed '$d' "$base_file") <(sed '$d' "$with_file"); then
            echo "[global] no differences"
        else
            nonempty_diffs=$((nonempty_diffs+1))
        fi
        total_diffs=$((total_diffs+1))
    else
        echo "[global] missing global files; skipped"
    fi

    # Prepare runtime temporary file for per-query summary (avoid associative arrays)
    runtime_tmp=$(mktemp)
    # runtime_tmp will be removed after the summary for this timestamp

    # Per-query diffs: look for matching per-query dirs and compare excluding last line
    if [ "$has_perquery" = true ]; then
        echo "-- Per-query diffs (excluding last line) --"
        # iterate per-query files (q1.out ... qN.out)
        for wf in "$with_dir"/*; do
            [ -e "$wf" ] || continue
            name=$(basename "$wf")
            bf="$base_dir/$name"

            if [ ! -f "$bf" ]; then
                echo "[per-query] $name: missing baseline; skipped"
                continue
            fi

            # Collect runtimes (last lines) and parse real/wall-clock seconds
            baseline_last=$(tail -n1 "$bf" 2>/dev/null || echo "")
            with_last=$(tail -n1 "$wf" 2>/dev/null || echo "")
            baseline_real=$(parse_seconds "$baseline_last")
            with_real=$(parse_seconds "$with_last")
            printf "%s\t%s\t%s\n" "$name" "$baseline_real" "$with_real" >> "$runtime_tmp"

            # Print a small header for the query
            echo "--- $name ---"

            # Diff excluding last line (so timing lines excluded)
            if diff -u <(sed '$d' "$bf") <(sed '$d' "$wf"); then
                echo "[no differences]"
            else
                nonempty_diffs=$((nonempty_diffs+1))
            fi
            total_diffs=$((total_diffs+1))
        done
    else
        echo "[per-query] per-query directories missing; skipped"
    fi

    # Print runtime summary for this timestamp
    echo "-- Runtime summary --"
    # columns: query, baseline(s), with(s), delta(s)
    printf "%-12s %-12s %-12s %-12s %-8s\n" "query" "baseline(s)" "with(s)" "delta(s)" "pct(%)"

    # Build unique list of names from runtime_tmp and sort numerically by the first numeric token in the name
    all_names=$(cut -f1 "$runtime_tmp" | sort -u)
    sorted_names=$(printf "%s\n" "$all_names" | awk '{ name=$0; match(name, /[0-9]+/); if (RSTART) { num=substr(name,RSTART,RLENGTH); printf "%s\t%010d\n", name, num } else { printf "%s\t%010d\n", name, 0 } }' | sort -t$'\t' -k2,2n | cut -f1)

    for name in $sorted_names; do
        # extract baseline and with real seconds from runtime_tmp (fields 2 and 3)
        line=$(awk -F"\t" -v n="$name" '$1==n { print $2 "\t" $3; exit }' "$runtime_tmp" || true)
        bnum=$(printf "%s" "$line" | cut -f1)
        wnum=$(printf "%s" "$line" | cut -f2)
        delta=""
        if [ -n "$bnum" ] && [ -n "$wnum" ]; then
            delta=$(awk -v a="$wnum" -v b="$bnum" 'BEGIN{printf "%.6f", (a - b)}')
        fi
        # format display values
        if [ -n "$bnum" ]; then bdisp=$(awk -v x="$bnum" 'BEGIN{printf "%.6fs", x}'); else bdisp="-"; fi
        if [ -n "$wnum" ]; then wdisp=$(awk -v x="$wnum" 'BEGIN{printf "%.6fs", x}'); else wdisp="-"; fi
        if [ -z "$delta" ]; then ddisp="-"; else ddisp=$(printf "%.6fs" "$delta"); fi
        pct_disp="-"
        if [ -n "$bnum" ] && [ "$bnum" != "0" ]; then
            pct=$(awk -v a="$wnum" -v b="$bnum" 'BEGIN{printf "%.2f", ((a-b)/b)*100}')
            case "$pct" in
                -*) pct_disp="${pct}%" ;;
                *) pct_disp="+${pct}%" ;;
            esac
        fi
        printf "%-12s %-12s %-12s %-12s %-8s\n" "$name" "$bdisp" "$wdisp" "$ddisp" "$pct_disp"
    done
    echo "=============================================================="

    # cleanup runtime tmp for this timestamp
    rm -f "$runtime_tmp"
    unset runtime_tmp
done < <(sort -u "$timestamps_file")

if [ "$found_pairs" -eq 0 ]; then
    echo "No matching timestamp pairs (with-extension + baseline) found in $BENCH_DIR"
    exit 0
fi

echo
echo "Summary: pairs found: $found_pairs; diffs created: $total_diffs; non-empty diffs: $nonempty_diffs"
echo "Diff directories: $BENCH_DIR/*-diff/"

exit 0
