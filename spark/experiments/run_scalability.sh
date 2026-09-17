#!/usr/bin/env bash
# Chạy toàn bộ lưới thực nghiệm scalability (WORKPLAN.md §Thực nghiệm).
# Mỗi combo 1 tiến trình Python riêng — xem lý do trong scalability_bench.py.
set -euo pipefail
cd "$(dirname "$0")/.."   # cd vao spark/
source .venv/bin/activate

CSV_OUT="../docs/experiments_data/scalability_results.csv"
rm -f "$CSV_OUT"

SIZES=(100000 1000000 8000000)
EXECUTORS=(1 2 4)

for n in "${SIZES[@]}"; do
  for e in "${EXECUTORS[@]}"; do
    python3 experiments/scalability_bench.py --n "$n" --executors "$e"
  done
done

python3 experiments/scalability_bench.py --plot-only
