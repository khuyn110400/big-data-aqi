#!/usr/bin/env bash
# Đo scalability của Pha 2 với executor thật trên Spark Standalone (3 kích thước dữ liệu x 1/2/4 executor).
#
# Script dựng 4 worker tạm (mỗi worker 1 core) cạnh container spark-master đang chạy, rồi chạy
# spark/experiments/scalability_bench.py cho từng tổ hợp. spark.cores.max giới hạn ứng dụng ở
# 1, 2 hoặc 4 core, tương ứng 1, 2, 4 executor. Sau mỗi lần chạy, script đếm dòng "Executor added"
# trong log để xác nhận số executor thực tế đúng bằng số yêu cầu, sai thì dừng.
# Tất cả worker là container Docker trên cùng một máy, không phải cụm nhiều máy.
#
# Yêu cầu: stack Docker đang chạy (spark-master, spark-worker). Chạy từ bất kỳ thư mục nào:
#   bash scripts/run_scalability_distributed.sh
# Kết quả ghi vào final_results/scalability/ (CSV thời gian, CSV speedup, xác nhận executor, summary.md).
# Lúc kết thúc (kể cả khi lỗi) script xoá 4 worker tạm và bật lại spark-worker.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT="$ROOT/final_results/scalability"
LOGS="$OUT/logs"
mkdir -p "$LOGS"

echo "=== Benchmark scalability Pha 2 trên Spark Standalone ==="

python3 -m py_compile spark/experiments/scalability_bench.py

echo
echo "=== Chuẩn bị mã nguồn trong spark-master ==="
docker exec spark-master mkdir -p /tmp/aqi/spark/experiments /tmp/aqi/spark/jobs
docker cp spark/experiments/scalability_bench.py \
  spark-master:/tmp/aqi/spark/experiments/scalability_bench.py
docker cp spark/jobs/phase2_aqi.py \
  spark-master:/tmp/aqi/spark/jobs/phase2_aqi.py
docker cp spark/aqi_core \
  spark-master:/tmp/aqi/spark/

NET="$(docker inspect spark-master --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' | head -n1)"
IMG="$(docker inspect spark-worker --format '{{.Config.Image}}')"

echo "network=$NET"
echo "image=$IMG"

echo
echo "=== Kiểm tra mount thư mục spark ==="
docker run --rm \
  --network "$NET" \
  -v "$ROOT/spark:/tmp/aqi/spark:ro" \
  "$IMG" \
  sh -lc 'test -f /tmp/aqi/spark/aqi_core/breakpoints_vn.json && echo "AQI CORE JSON MOUNT OK"'

cleanup() {
  echo
  echo "=== Dọn 4 worker tạm ==="
  for i in 1 2 3 4; do
    docker rm -f "spark-bench-w$i" >/dev/null 2>&1 || true
  done
  docker start spark-worker >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo
echo "=== Khởi động 4 worker 1 core ==="
docker stop spark-worker >/dev/null 2>&1 || true

for i in 1 2 3 4; do
  docker rm -f "spark-bench-w$i" >/dev/null 2>&1 || true
  docker run -d \
    --name "spark-bench-w$i" \
    --network "$NET" \
    -v "$ROOT/spark:/tmp/aqi/spark:ro" \
    -e SPARK_MODE=worker \
    -e SPARK_MASTER_URL=spark://spark-master:7077 \
    -e SPARK_WORKER_CORES=1 \
    -e SPARK_WORKER_MEMORY=1536M \
    -e HOME=/tmp \
    -e JAVA_TOOL_OPTIONS=-Duser.home=/tmp \
    "$IMG" >/dev/null
done

sleep 8
docker ps --format 'table {{.Names}}\t{{.Status}}' \
  | grep -E 'NAMES|spark-bench|spark-master'

rm -f "$OUT/scalability_results.csv" \
      "$OUT/scalability_speedup.csv" \
      "$OUT/executor_verification.txt"
rm -rf "$LOGS"
mkdir -p "$LOGS"
docker exec spark-master rm -f /tmp/aqi/scalability_results.csv

echo
echo "=== Chạy 100K / 1M / 8M x 1 / 2 / 4 executor ==="

for n in 100000 1000000 8000000; do
  for e in 1 2 4; do
    log="$LOGS/${n}_${e}exec.log"

    echo
    echo ">>> rows=$n executors=$e"

    docker exec \
      -u root \
      -e USER=root \
      -e LOGNAME=root \
      -e HADOOP_USER_NAME=root \
      -e SCALABILITY_SPARK_MASTER=spark://spark-master:7077 \
      -e PYTHONPATH=/tmp/aqi/spark:/tmp/aqi/spark/jobs \
      spark-master \
      /opt/bitnami/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 2g \
      --executor-memory 768m \
      --conf "spark.cores.max=$e" \
      --conf "spark.executor.cores=1" \
      --conf "spark.sql.adaptive.enabled=false" \
      --conf "spark.executorEnv.PYTHONPATH=/tmp/aqi/spark:/tmp/aqi/spark/jobs" \
      /tmp/aqi/spark/experiments/scalability_bench.py \
      --n "$n" \
      --executors "$e" \
      --csv-out /tmp/aqi/scalability_results.csv \
      2>&1 | tee "$log"

    count="$(
      grep 'Executor added:' "$log" \
        | sed -E 's/.*Executor added: ([^ ]+).*/\1/' \
        | sort -u \
        | wc -l \
        | tr -d ' '
    )"

    printf 'rows=%s requested_executors=%s actual_executors=%s\n' \
      "$n" "$e" "$count" \
      | tee -a "$OUT/executor_verification.txt"

    if [ "$count" -ne "$e" ]; then
      echo "ERROR: requested $e executors but observed $count"
      exit 2
    fi
  done
done

docker cp spark-master:/tmp/aqi/scalability_results.csv \
  "$OUT/scalability_results.csv" >/dev/null

echo
echo "=== Tính speedup và ghi báo cáo ==="
python3 - "$OUT" <<'PY'
import csv
import sys
from pathlib import Path

out = Path(sys.argv[1])
src = out / "scalability_results.csv"

with src.open(encoding="utf-8", newline="") as f:
    rows = list(csv.DictReader(f))

data = sorted(
    (
        int(r["n_rows"]),
        int(r["n_executors"]),
        float(r["seconds"]),
    )
    for r in rows
)

if len(data) != 9:
    raise SystemExit(f"Expected 9 benchmark rows, got {len(data)}")

by = {(n, e): sec for n, e, sec in data}
sizes = [100000, 1000000, 8000000]
execs = [1, 2, 4]

with (out / "scalability_speedup.csv").open(
    "w", encoding="utf-8", newline=""
) as f:
    w = csv.writer(f)
    w.writerow(["n_rows", "n_executors", "seconds", "speedup_vs_1"])
    for n in sizes:
        base = by[n, 1]
        for e in execs:
            w.writerow([n, e, by[n, e], round(base / by[n, e], 4)])

lines = [
    "# Phase 2 scalability benchmark",
    "",
    "| Rows | 1 executor | 2 executors | 4 executors |",
    "|---:|---:|---:|---:|",
]
for n in sizes:
    lines.append(
        f"| {n:,} | {by[n,1]:.3f}s | {by[n,2]:.3f}s | {by[n,4]:.3f}s |"
    )

lines += [
    "",
    "| Rows | Speedup 2 executors | Speedup 4 executors |",
    "|---:|---:|---:|",
]
for n in sizes:
    base = by[n,1]
    lines.append(
        f"| {n:,} | {base/by[n,2]:.3f}x | {base/by[n,4]:.3f}x |"
    )

(out / "summary.md").write_text(
    "\n".join(lines) + "\n",
    encoding="utf-8",
)

print((out / "summary.md").read_text(encoding="utf-8"))
PY

echo
echo "=== Kết quả ==="
cat "$OUT/executor_verification.txt"
echo
cat "$OUT/summary.md"

echo
echo "Hoàn tất benchmark scalability"
