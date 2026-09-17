"""
Xuất dữ liệu ra CSV để chạy Tầng 3 (DBSCAN/HDBSCAN, CNN-LSTM — nhánh mở rộng M4) TRÊN
KAGGLE. Máy dev đã nhiều lần OOM/treo ở tầng 1+2 (RAM cạn kiệt), CNN-LSTM sẽ còn nặng
hơn nhiều — Kaggle cho GPU miễn phí + môi trường không bị app khác giành RAM.

Kaggle KHÔNG có sẵn PySpark/Java (cài thêm tốn thời gian mỗi session mới), nên chạy
Spark Ở ĐÂY để xuất dữ liệu nhỏ gọn ra CSV, rồi 2 script `kaggle_dbscan_hdbscan.py` /
`kaggle_cnn_lstm.py` (thuần Python, KHÔNG PySpark) tự đọc CSV trên Kaggle.

Input : /air-quality/aqi/ (output Pha 2, schema C3)
Output: kaggle_exports/city_month_features.csv           (cho kaggle_dbscan_hdbscan.py)
        kaggle_exports/sequence_train.csv, sequence_test.csv  (cho kaggle_cnn_lstm.py)

Chạy:
  python jobs/export_for_kaggle.py --input /tmp/aqi --output kaggle_exports

Sau khi có file: tạo Kaggle Dataset từ thư mục kaggle_exports/, rồi tạo Kaggle Notebook,
paste nội dung 2 file kaggle_*.py vào, trỏ input dataset vừa upload, chạy (bật GPU cho
kaggle_cnn_lstm.py). Kết quả (RMSE/MAE/R²/silhouette) chép tay về docs/experiments.md
mục 5 — Kaggle không tự đồng bộ ngược lại máy này.
"""
import argparse
import glob
import os
import shutil
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)
sys.path.insert(0, os.path.join(_SPARK_ROOT, "jobs"))
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

from aqi_core.iaqi import align_hourly_window
from ext_clustering import build_city_month_features
from ext_forecast import TARGET_COL, time_based_split

SEQUENCE_WINDOW_HOURS = 24


def build_sequence_windows(df, window_hours=SEQUENCE_WINDOW_HOURS):
    """
    Chuỗi window_hours giá trị AQI liên tiếp gần nhất (kết thúc ở giờ hiện tại) + target
    (AQI 24h sau) — dùng cho CNN-LSTM (tier 3), KHÁC lag rời rạc 1h/3h/24h của
    `build_forecast_features()` (tier 1/2): CNN-LSTM cần chuỗi thật liên tục để phát huy
    đúng kiến trúc (Conv1D bắt pattern cục bộ, LSTM bắt phụ thuộc dài hạn).

    Tái dùng `aqi_core.align_hourly_window()` để căn đúng vị trí giờ theo timestamp
    (không dựa vào vị trí mảng — an toàn khi có gap) — cùng cơ chế đã dùng cho Nowcast
    trong `phase2_aqi.py`, không viết lại logic căn chỉnh.

    seq_0 = giờ CŨ NHẤT (t - window_hours + 1) ... seq_{window_hours-1} = giờ HIỆN TẠI (t)
    — thứ tự thời gian tự nhiên (oldest -> newest), chuẩn cho input LSTM.

    Loại bỏ dòng nào có giờ trong chuỗi bị null (mạng neural không nhận NaN, không tự bịa
    số điền vào) hoặc thiếu target (rìa cuối chuỗi mỗi trạm — không đủ tương lai để có nhãn).
    """
    w = Window.partitionBy("station_id").orderBy("ts_epoch")
    w_seq = w.rowsBetween(-(window_hours - 1), 0)

    df = df.withColumn("ts_epoch", F.unix_timestamp("ts_utc"))
    df = df.withColumn("chunk_month", F.date_trunc("month", "ts_utc"))
    df = df.withColumn(TARGET_COL, F.lead("aqi", 24).over(w))

    packed = F.collect_list(F.struct(F.col("ts_epoch").alias("ts_epoch"), F.col("aqi").alias("v"))).over(w_seq)

    @F.udf(returnType=ArrayType(DoubleType()))
    def _seq_udf(cur_ts, pairs):
        aligned = align_hourly_window(cur_ts, ((r["ts_epoch"], r["v"]) for r in pairs), window_hours)
        return list(reversed(aligned))  # align_hourly_window: idx0=hien tai -> dao lai thanh oldest-first

    df = df.withColumn("sequence", _seq_udf(F.col("ts_epoch"), packed))

    seq_cols = [F.col("sequence")[i].alias(f"seq_{i}") for i in range(window_hours)]
    out = df.select("station_id", "chunk_month", *seq_cols, TARGET_COL)
    return out.na.drop()


def _write_single_csv(df, out_path):
    """Ghi ra ĐÚNG 1 file CSV tên rõ ràng — Spark mặc định ghi ra thư mục + part-*.csv,
    bất tiện khi upload lên Kaggle. Ghi vào thư mục tạm rồi move file ra ngoài."""
    tmp_dir = out_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    if os.path.exists(out_path):
        os.remove(out_path)
    shutil.move(part_file, out_path)
    shutil.rmtree(tmp_dir)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default="kaggle_exports")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("export_for_kaggle")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    os.makedirs(args.output, exist_ok=True)

    # 1. City-month features -> DBSCAN/HDBSCAN
    city_month = build_city_month_features(df)
    n_city_month = city_month.count()
    _write_single_csv(city_month, os.path.join(args.output, "city_month_features.csv"))
    print(f"city_month_features.csv: {n_city_month} dong")

    # 2. Sequence windows -> CNN-LSTM
    seq = build_sequence_windows(df).cache()
    n_seq = seq.count()
    print(f"sequence windows (du {SEQUENCE_WINDOW_HOURS}h lich su + target): {n_seq} dong")

    train_months, test_months = time_based_split(seq)
    train_df = seq.filter(F.col("chunk_month").isin(train_months)).drop("chunk_month")
    test_df = seq.filter(F.col("chunk_month").isin(test_months)).drop("chunk_month")

    _write_single_csv(train_df, os.path.join(args.output, "sequence_train.csv"))
    _write_single_csv(test_df, os.path.join(args.output, "sequence_test.csv"))
    print(f"sequence_train.csv: {train_df.count()} dong | sequence_test.csv: {test_df.count()} dong")

    print(f"\nDa xuat xong vao {args.output}/ — tao Kaggle Dataset tu thu muc nay de chay tang 3.")
    spark.stop()


if __name__ == "__main__":
    main()
