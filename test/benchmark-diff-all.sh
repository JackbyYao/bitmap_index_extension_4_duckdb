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


declare -A runs=()

# Find candidate timestamps from global files like: 20251119-025917-branch-with-extension.txt
for f in "$BENCH_DIR"/*-with-extension.txt; do
    [ -e "$f" ] || continue
    bn=$(basename "$f")
    if [[ $bn =~ ^([0-9]{8}-[0-9]{6})-([A-Za-z0-9._-]+)-with-extension\.txt$ ]]; then
        ts=${BASH_REMATCH[1]}
        branch=${BASH_REMATCH[2]}
        key="${ts}|${branch}"
        runs["$key"]=1
    fi
done

# Also accept timestamps derived from per-query directories when globals are absent
for d in "$BENCH_DIR"/*-per-query-with; do
    [ -e "$d" ] || continue
    bn=$(basename "$d")
    if [[ $bn =~ ^([0-9]{8}-[0-9]{6})-([A-Za-z0-9._-]+)-per-query-with$ ]]; then
        ts=${BASH_REMATCH[1]}
        branch=${BASH_REMATCH[2]}
        key="${ts}|${branch}"
        runs["$key"]=1
    fi
done

if [ ${#runs[@]} -eq 0 ]; then
    echo "No '-with-extension.txt' files or '*-per-query-with' directories found in $BENCH_DIR"
    exit 0
fi

found_pairs=0
total_diffs=0
nonempty_diffs=0

for key in "${!runs[@]}"; do
    ts="${key%%|*}"
    branch="${key##*|}"
    base_file="$BENCH_DIR/${ts}-${branch}-baseline.txt"
    with_file="$BENCH_DIR/${ts}-${branch}-with-extension.txt"
    with_dir="$BENCH_DIR/${ts}-${branch}-per-query-with"
    base_dir="$BENCH_DIR/${ts}-${branch}-per-query-baseline"

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
    echo "==================== Timestamp: $ts  (branch: $branch) ===================="

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

    # Prepare runtime collection for per-query summary
    declare -A baseline_rt=()
    declare -A with_rt=()

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

            # Collect runtimes (last lines) for the summary
            baseline_last=$(tail -n1 "$bf" 2>/dev/null || echo "")
            with_last=$(tail -n1 "$wf" 2>/dev/null || echo "")
            baseline_rt["$name"]="$baseline_last"
            with_rt["$name"]="$with_last"

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
    echo "-- Runtime summary (last line of each file) --"
    printf "%-20s %-25s %-25s %-12s\n" "query" "baseline_last_line" "with_last_line" "delta(s)"

    # Build unique list of names and sort numerically by the first numeric token in the name
    all_names=$(printf "%s\n" "${!baseline_rt[@]}" "${!with_rt[@]}" | sort -u)
    sorted_names=$(printf "%s\n" "$all_names" | awk '{ name=$0; match(name, /[0-9]+/); if (RSTART) { num=substr(name,RSTART,RLENGTH); printf "%s\t%010d\n", name, num } else { printf "%s\t%010d\n", name, 0 } }' | sort -t$'\t' -k2,2n | cut -f1)

    for name in $sorted_names; do
        b=${baseline_rt["$name"]-}
        w=${with_rt["$name"]-}

        # Try to extract a numeric value (last number) from the last line
        bnum=$(echo "$b" | grep -Eo '[0-9]+(\.[0-9]+)?' | tail -n1 || true)
        wnum=$(echo "$w" | grep -Eo '[0-9]+(\.[0-9]+)?' | tail -n1 || true)
        delta=""
        if [ -n "$bnum" ] && [ -n "$wnum" ]; then
            # compute delta = with - baseline
            delta=$(awk -v a="$wnum" -v b="$bnum" 'BEGIN{printf "%.6f", (a - b)}')
        fi

        printf "%-20s %-25s %-25s %-12s\n" "$name" "$b" "$w" "$delta"
    done
    echo "=============================================================="
done

if [ "$found_pairs" -eq 0 ]; then
    echo "No matching timestamp pairs (with-extension + baseline) found in $BENCH_DIR"
    exit 0
fi

echo
echo "Summary: pairs found: $found_pairs; diffs created: $total_diffs; non-empty diffs: $nonempty_diffs"
echo "Diff directories: $BENCH_DIR/*-diff/"

exit 0
