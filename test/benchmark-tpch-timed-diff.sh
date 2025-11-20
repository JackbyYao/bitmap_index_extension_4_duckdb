#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
BENCH_DIR_INPUT=${1:-"$SCRIPT_DIR/../benchmark-results"}
BENCH_DIR=$(realpath "$BENCH_DIR_INPUT")

if [ ! -d "$BENCH_DIR" ]; then
    echo "Benchmark directory not found: $BENCH_DIR"
    exit 1
fi

echo "Using benchmark dir: $BENCH_DIR"

declare -A runs

for d in "$BENCH_DIR"/*-timed-per-query-with; do
    [ -d "$d" ] || continue
    bn=$(basename "$d")
    if [[ $bn =~ ^([0-9]{8}-[0-9]{6})-([A-Za-z0-9._-]+)-timed-per-query-with$ ]]; then
        ts=${BASH_REMATCH[1]}
        branch=${BASH_REMATCH[2]}
        key="${ts}|${branch}"
        runs["$key"]=1
    fi
done

if [ ${#runs[@]} -eq 0 ]; then
    echo "No timed benchmark directories found"
    exit 0
fi

found=0
diff_count=0
for key in "${!runs[@]}"; do
    ts="${key%%|*}"
    branch="${key##*|}"
    with_dir="$BENCH_DIR/${ts}-${branch}-timed-per-query-with"
    base_dir="$BENCH_DIR/${ts}-${branch}-timed-per-query-baseline"

    if [ ! -d "$with_dir" ] || [ ! -d "$base_dir" ]; then
        continue
    fi

    found=$((found+1))
    echo
    echo "==================== Timed diff: $ts (branch: $branch) ===================="

    echo "-- Per-query diffs (excluding time line) --"
    for wf in "$with_dir"/*.out; do
        [ -f "$wf" ] || continue
        name=$(basename "$wf")
        bf="$base_dir/$name"
        if [ ! -f "$bf" ]; then
            echo "$name: baseline missing"
            continue
        fi
        echo "--- $name ---"
        if diff -u <(sed '$d' "$bf") <(sed '$d' "$wf"); then
            echo "[no differences]"
        else
            diff_count=$((diff_count + 1))
        fi
    done

    echo "-- Runtime comparison --"
    printf "%-10s %-25s %-25s %-10s %-10s\n" "query" "baseline_time" "with_time" "delta" "ratio"

    for file in "$with_dir"/*.out; do
        [ -f "$file" ] || continue
        name=$(basename "$file")
        bf="$base_dir/$name"
        if [ ! -f "$bf" ]; then
            continue
        fi
        bline=$(grep -E "^Run Time" "$bf" | tail -n1)
        wline=$(grep -E "^Run Time" "$file" | tail -n1)
        btime=$(echo "$bline" | awk '{print $5}')
        wtime=$(echo "$wline" | awk '{print $5}')
        if [ -z "$btime" ] || [ -z "$wtime" ]; then
            continue
        fi
        delta=$(awk -v w="$wtime" -v b="$btime" 'BEGIN{printf "%.6f", w - b}')
        if [ "$(awk -v b="$btime" 'BEGIN{print (b == 0 ? 1 : 0)}')" -eq 1 ]; then
            ratio="inf"
        else
            ratio=$(awk -v w="$wtime" -v b="$btime" 'BEGIN{printf "%.4f", (w - b) / b}')
        fi
        printf "%-10s %-25s %-25s %-10s %-10s\n" "$name" "$btime" "$wtime" "$delta" "$ratio"
    done
    echo "==================================================================="
done

if [ $found -eq 0 ]; then
    echo "No matched with/baseline timed directories found"
else
    echo
    echo "Total non-empty diffs: $diff_count"
fi
