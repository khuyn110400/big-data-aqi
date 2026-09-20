"""
Pha 2: tính AQI theo giờ cho từng trạm (giai đoạn 2 trong mô hình của El Fazziki et al., 2015).

Mapper : với mỗi bản ghi (station_id, ts_utc), tính Nowcast 12 giờ cho PM2.5 và PM10,
         rồi gọi aqi_core.iaqi_hour() để có IAQI của từng chất.
Reducer: AQI = max(IAQI), gán aqi_level, aqi_label và dominant_pollutant. Phần này nằm
         sẵn trong aqi_core.iaqi_hour(), Pha 2 không tính lại công thức.

Đầu vào : /air-quality/clean/  (đầu ra Pha 1)
Đầu ra  : /air-quality/aqi/    (schema C3)

Chạy local không cần cluster:
  python jobs/phase2_aqi.py --input /tmp/clean --output /tmp/aqi

Nowcast được tính ở Pha 2 chứ không phải Pha 1, vì nó là một phần của công thức AQI giờ
(QĐ 1459, mục 2.2.1a), còn Pha 1 chỉ làm sạch dữ liệu.

Pha 1 giữ lại cột owm_aqi chỉ để đối chiếu ở bước này. Cột đó không có trong schema C3
nên không ghi vào đầu ra; bảng đối chiếu được in ra console và có thể ghi ra CSV bằng
--comparison-output. Muốn thêm cột vào schema dùng chung thì phải sửa CONTRACTS.md trước.
"""
import argparse
import os
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)  # để driver import được aqi_core dù chạy trực tiếp file này
# UDF chạy trong tiến trình Python worker riêng (kể cả với local[*]). Worker không kế thừa
# sys.path đã sửa trong driver mà chỉ kế thừa biến môi trường; nếu không đặt PYTHONPATH,
# worker sẽ báo "ModuleNotFoundError: No module named 'aqi_core'" khi giải nén UDF.
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from aqi_core.iaqi import align_hourly_window, iaqi_hour, nowcast

NOWCAST_WINDOW_HOURS = 12
IAQI_COLS = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]

_HOUR_RESULT_SCHEMA = StructType([
    StructField("aqi", DoubleType()),
    StructField("aqi_level", IntegerType()),
    StructField("aqi_label", StringType()),
    StructField("dominant_pollutant", StringType()),
    StructField("iaqi_pm2_5", DoubleType()),
    StructField("iaqi_pm10", DoubleType()),
    StructField("iaqi_o3", DoubleType()),
    StructField("iaqi_no2", DoubleType()),
    StructField("iaqi_so2", DoubleType()),
    StructField("iaqi_co", DoubleType()),
])


@F.udf(returnType=DoubleType())
def _nowcast_udf(current_ts, window_rows):
    """Xếp 12 điểm trong window vào đúng vị trí giờ bằng aqi_core.align_hourly_window()
    (streaming_aqi.py dùng chung hàm này), rồi gọi aqi_core.nowcast()."""
    if window_rows is None:
        return None
    hourly = align_hourly_window(
        current_ts, ((row["ts_epoch"], row["v"]) for row in window_rows), NOWCAST_WINDOW_HOURS
    )
    return nowcast(hourly)


@F.udf(returnType=_HOUR_RESULT_SCHEMA)
def _iaqi_hour_udf(o3, no2, so2, co, pm2_5_nowcast, pm10_nowcast):
    """Gọi aqi_core.iaqi_hour(), không tính lại công thức ở đây."""
    out = iaqi_hour({
        "o3": o3, "no2": no2, "so2": so2, "co": co,
        "pm2_5": pm2_5_nowcast, "pm10": pm10_nowcast,
    })
    isubs = out["iaqi"]

    def _f(x):
        # round() trả int nhưng schema khai DoubleType. Phải ép kiểu tường minh, nếu không
        # UDF thường của PySpark (không dùng Arrow) sẽ âm thầm cho ra null.
        return float(x) if x is not None else None

    return (
        _f(out["aqi"]),
        int(out["aqi_level"]) if out["aqi_level"] is not None else None,
        out["aqi_label"],
        out["dominant_pollutant"],
        _f(isubs.get("pm2_5")), _f(isubs.get("pm10")), _f(isubs.get("o3")),
        _f(isubs.get("no2")), _f(isubs.get("so2")), _f(isubs.get("co")),
    )


def add_nowcast(df, pollutant: str):
    """Nowcast 12 giờ cho pm2_5 và pm10. Chỉ áp dụng cho hai chất này (QĐ 1459, mục 2.2.1a)."""
    w = Window.partitionBy("station_id").orderBy("ts_epoch").rowsBetween(-(NOWCAST_WINDOW_HOURS - 1), 0)
    packed = F.collect_list(F.struct(F.col("ts_epoch").alias("ts_epoch"), F.col(pollutant).alias("v"))).over(w)
    return df.withColumn(f"{pollutant}_nowcast", _nowcast_udf(F.col("ts_epoch"), packed))


def compute_iaqi_hour(df):
    df = add_nowcast(df, "pm2_5")
    df = add_nowcast(df, "pm10")

    result = _iaqi_hour_udf(
        F.col("o3"), F.col("no2"), F.col("so2"), F.col("co"),
        F.col("pm2_5_nowcast"), F.col("pm10_nowcast"),
    )
    return df.withColumn("_r", result).select("*", "_r.*").drop("_r")


def print_owm_comparison(df):
    """Bảng đối chiếu AQI tự tính (VN_AQI, 0-500) với owm_aqi (thang 1-5 của OpenWeather).
    Chỉ dùng cho báo cáo; owm_aqi không phải AQI của đồ án."""
    total = df.filter(F.col("aqi").isNotNull() & F.col("owm_aqi").isNotNull()).count()
    print("\n=== Đối chiếu AQI tự tính vs owm_aqi (OpenWeather, thang 1-5) ===")
    print(f"Số bản ghi có cả 2 giá trị: {total}")

    by_owm = (
        df.filter(F.col("aqi").isNotNull() & F.col("owm_aqi").isNotNull())
        .groupBy("owm_aqi")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("aqi"), 1).alias("avg_aqi_tinh"),
            F.round(F.min("aqi"), 1).alias("min_aqi_tinh"),
            F.round(F.max("aqi"), 1).alias("max_aqi_tinh"),
        )
        .orderBy("owm_aqi")
    )
    by_owm.show()
    print("=" * 65 + "\n")
    return by_owm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--comparison-output", default=None,
                     help="(tuỳ chọn) ghi bảng đối chiếu owm_aqi ra CSV, ví dụ docs/owm_comparison")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("phase2_aqi")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")  # xem ghi chú trong phase1_clean.py
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    df = compute_iaqi_hour(df)

    standard = os.environ.get("AQI_STANDARD", "VN_1459")
    df = df.withColumn("standard", F.lit(standard))

    comparison = print_owm_comparison(df)
    if args.comparison_output:
        comparison.coalesce(1).write.mode("overwrite").option("header", True).csv(args.comparison_output)

    # Schema C3 (CONTRACTS.md) — KHÔNG thêm owm_aqi vào đây, xem docstring đầu file.
    out_cols = (
        ["station_id", "city", "country", "lat", "lon", "ts_utc"]
        + IAQI_COLS
        + [f"iaqi_{p}" for p in IAQI_COLS]
        + ["aqi", "aqi_level", "aqi_label", "dominant_pollutant", "standard", "dt"]
    )
    (
        df.select(*out_cols)
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("country", "dt")
        .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
