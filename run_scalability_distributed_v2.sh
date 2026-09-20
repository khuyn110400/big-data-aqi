#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

OUT="$ROOT/final_results/scalability"
LOGS="$OUT/logs"
mkdir -p "$LOGS"

echo "=== FIX: DISTRIBUTED BENCHMARK V2 ==="

# Keep the already-correct patch: local[N] -> env-selectable Spark master.
python3 - <<'PY'
from pathlib import Path
p = Path("spark/experiments/scalability_bench.py")
s = p.read_text(encoding="utf-8")
old = '.master(f"local[{n_executors}]")'
new = '.master(os.environ.get("SCALABILITY_SPARK_MASTER", f"local[{n_executors}]"))'
if old in s:
    s = s.replace(old, new, 1)
    p.write_text(s, encoding="utf-8", newline="\n")
    print("patched Spark master selector")
elif new in s:
    print("Spark master selector already patched")
else:
    raise SystemExit("Unexpected scalability_bench.py master line")
PY
python3 -m py_compile spark/experiments/scalability_bench.py

echo
echo "=== PREPARE DRIVER SOURCE ==="
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
echo "=== VERIFY HOST MOUNT BEFORE BENCHMARK ==="
docker run --rm \
  --network "$NET" \
  -v "$ROOT/spark:/tmp/aqi/spark:ro" \
  "$IMG" \
  sh -lc 'test -f /tmp/aqi/spark/aqi_core/breakpoints_vn.json && echo "AQI CORE JSON MOUNT OK"'

cleanup() {
  echo
  echo "=== CLEANUP TEMP WORKERS ==="
  for i in 1 2 3 4; do
    docker rm -f "spark-bench-w$i" >/dev/null 2>&1 || true
  done
  docker start spark-worker >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo
echo "=== START 4 REAL 1-CORE WORKERS ==="
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
echo "=== RUN 100K / 1M / 8M x 1 / 2 / 4 EXECUTORS ==="

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
echo "=== BUILD SPEEDUP + REPORT ==="
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
echo "=== RESULTS ==="
cat "$OUT/executor_verification.txt"
echo
cat "$OUT/summary.md"

echo
echo "SCALABILITY BENCHMARK DONE"
