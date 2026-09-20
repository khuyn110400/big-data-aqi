"""
Thực nghiệm scalability — bắt buộc cho đồ án big data (xem WORKPLAN.md §Thực nghiệm).

Đo thời gian chạy Pha 2 (compute_iaqi_hour — phần tốn CPU nhất: UDF Nowcast + iaqi_hour
trên từng dòng) với 3 kích thước dữ liệu x 3 số executor. Chế độ local[N] chỉ dùng cho
dev; benchmark chính thức chạy Spark Standalone với worker/executor thật, 1 core/executor
có 8 core vật lý nên local[1]/[2]/[4] không bị oversubscribe).

Dữ liệu sinh trực tiếp bằng Spark (spark.range + F.rand), KHÔNG ghi file JSON trung
gian — mục đích là đo chi phí TÍNH TOÁN (UDF/window) của Pha 2, không lẫn với chi phí
đọc file. Vì vậy con số ở đây không thay thế cho benchmark I/O thật trên HDFS.

Mỗi combo (size, executor) chạy trong 1 TIẾN TRÌNH PYTHON RIÊNG — đã thử gộp nhiều
SparkSession start/stop trong cùng 1 process (calibration run) và gặp lỗi ngẫu nhiên
"EOF reached before Python server acknowledged" (PythonAccumulatorV2) khi tạo lại
SparkContext liên tiếp. Không ảnh hưởng kết quả lần đó, nhưng không đáng tin cậy cho
9 lần chạy dài (8M dòng) — nên bash loop gọi script 1 lần/combo (script tự append CSV).

Chạy (xem experiments/run_scalability.sh):
  python experiments/scalability_bench.py --n 100000 --executors 1
  python experiments/scalability_bench.py --n 100000 --executors 2
  ... (9 lần, mỗi lần append 1 dòng vào CSV)
  python experiments/scalability_bench.py --plot-only   # đọc CSV, in bảng + vẽ 2 biểu đồ

Output: docs/experiments_data/scalability_results.csv + docs/images/scalability_*.png
"""
import argparse
import csv
import os
import sys
import time
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(0, _SPARK_ROOT)
sys.path.insert(0, os.path.join(_SPARK_ROOT, "jobs"))
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from phase2_aqi import compute_iaqi_hour

N_STATIONS = 200  # khớp quy mô backfill thật dự kiến trong WORKPLAN (200 điểm)

POLLUTANT_RANGES = {
    "pm2_5": (0, 150), "pm10": (0, 200), "o3": (0, 150),
    "no2": (0, 150), "so2": (0, 100), "co": (0, 3000),
}


def make_synthetic_df(spark, n_rows: int, n_stations: int = N_STATIONS):
    """Sinh n_rows bản ghi tổng hợp đúng shape input của compute_iaqi_hour()."""
    hours_per_station = max(1, n_rows // n_stations)
    total = hours_per_station * n_stations

    df = (
        spark.range(total)
        .withColumn("station_idx", (F.col("id") % n_stations).cast("int"))
        .withColumn("hour_idx", (F.col("id") / n_stations).cast("long"))
        .withColumn("station_id", F.concat(F.lit("BENCH_"), F.col("station_idx")))
        .withColumn("ts_epoch", F.lit(1_700_000_000) + F.col("hour_idx") * 3600)
    )
    for i, (col, (lo, hi)) in enumerate(POLLUTANT_RANGES.items()):
        df = df.withColumn(col, F.round(F.rand(seed=i) * (hi - lo) + lo, 2))

    return df.select("station_id", "ts_epoch", *POLLUTANT_RANGES.keys())


def run_once(n_rows: int, n_executors: int) -> float:
    spark = (
        SparkSession.builder.appName(f"bench_{n_rows}_{n_executors}ex")
        .master(os.environ.get("SCALABILITY_SPARK_MASTER", f"local[{n_executors}]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(max(4, n_executors * 4)))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = make_synthetic_df(spark, n_rows)
        result = compute_iaqi_hour(df)

        t0 = time.perf_counter()
        result.agg(F.sum("aqi"), F.count("aqi")).collect()  # ep thuc thi toan bo UDF
        elapsed = time.perf_counter() - t0
    finally:
        spark.stop()
    return elapsed


FIELDNAMES = ["n_rows", "n_executors", "seconds"]


def append_csv_row(row: dict, path: str):
    """Ghi thêm 1 dòng — mỗi combo chạy trong 1 tiến trình Python riêng (xem lý do ở main()),
    nên không thể gom hết rồi ghi 1 lần như save_csv() (không dùng nữa, giữ lại cho tham khảo)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def load_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return [
            {"n_rows": int(r["n_rows"]), "n_executors": int(r["n_executors"]), "seconds": float(r["seconds"])}
            for r in csv.DictReader(f)
        ]


def print_table(rows: list[dict]):
    sizes = sorted({r["n_rows"] for r in rows})
    execs = sorted({r["n_executors"] for r in rows})
    by_key = {(r["n_rows"], r["n_executors"]): r["seconds"] for r in rows}
    print("\n=== Bảng kết quả ===")
    print(f"{'Kích thước':>12} | " + " | ".join(f"{e} executor".rjust(12) for e in execs))
    for n in sizes:
        vals = [f"{by_key.get((n, e), float('nan')):.2f}s" for e in execs]
        print(f"{n:>12,} | " + " | ".join(v.rjust(12) for v in vals))


def plot_charts(rows: list[dict], out_dir: str):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(out_dir, exist_ok=True)
    sizes = sorted({r["n_rows"] for r in rows})
    execs = sorted({r["n_executors"] for r in rows})
    by_key = {(r["n_rows"], r["n_executors"]): r["seconds"] for r in rows}

    # Bieu do 1: thoi gian chay theo kich thuoc, 1 duong/so executor
    fig, ax = plt.subplots(figsize=(7, 5))
    for e in execs:
        ys = [by_key[(s, e)] for s in sizes]
        ax.plot(sizes, ys, marker="o", label=f"{e} executor" + ("s" if e > 1 else ""))
    ax.set_xscale("log")
    ax.set_xlabel("Số bản ghi")
    ax.set_ylabel("Thời gian chạy (giây)")
    ax.set_title("Thời gian chạy Pha 2 theo kích thước dữ liệu")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "scalability_runtime.png"), dpi=150)
    plt.close(fig)

    # Bieu do 2: speedup (so voi 1 executor), 1 duong/kich thuoc
    fig, ax = plt.subplots(figsize=(7, 5))
    base_execs = min(execs)
    for s in sizes:
        base_time = by_key[(s, base_execs)]
        ys = [base_time / by_key[(s, e)] for e in execs]
        ax.plot(execs, ys, marker="o", label=f"{s:,} bản ghi")
    ax.plot(execs, execs, linestyle="--", color="gray", label="Speedup lý tưởng")
    ax.set_xlabel("Số executor")
    ax.set_ylabel(f"Speedup (so với {base_execs} executor)")
    ax.set_title("Speedup theo số executor")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "scalability_speedup.png"), dpi=150)
    plt.close(fig)


DEFAULT_CSV = os.path.join(_REPO_ROOT, "docs/experiments_data/scalability_results.csv")
DEFAULT_IMAGES = os.path.join(_REPO_ROOT, "docs/images")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, help="Chạy đúng 1 combo: số bản ghi (dùng cùng --executors)")
    ap.add_argument("--executors", type=int, help="Số executor cho combo đơn (dùng cùng --n)")
    ap.add_argument("--plot-only", action="store_true", help="Không chạy gì, chỉ đọc CSV có sẵn để in bảng + vẽ biểu đồ")
    ap.add_argument("--csv-out", default=DEFAULT_CSV)
    ap.add_argument("--images-out", default=DEFAULT_IMAGES)
    args = ap.parse_args()

    if args.plot_only:
        rows = load_csv(args.csv_out)
        print_table(rows)
        plot_charts(rows, args.images_out)
        print(f"Da ghi bieu do: {args.images_out}/scalability_runtime.png, scalability_speedup.png")
        return

    if args.n is None or args.executors is None:
        ap.error("Cần --n và --executors (chạy 1 combo/tiến trình — xem docstring đầu file lý do)")

    print(f"--- Chạy {args.n:,} bản ghi, {args.executors} executor ---", flush=True)
    secs = run_once(args.n, args.executors)
    print(f"    -> {secs:.2f}s", flush=True)
    append_csv_row({"n_rows": args.n, "n_executors": args.executors, "seconds": round(secs, 3)}, args.csv_out)


if __name__ == "__main__":
    main()
