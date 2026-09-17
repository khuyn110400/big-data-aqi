"""
PHA 2 — TÍNH AQI  (El Fazziki 2015, pha 2). NGƯỜI B · M2
*** ĐÂY LÀ TRÁI TIM CỦA ĐỒ ÁN ***

Mapper : với mỗi bản ghi (station_id, ts_utc), tính Nowcast 12h cho PM2.5/PM10
         rồi gọi aqi_core.iaqi_hour() -> IAQI từng chất
Reducer: aqi_core.iaqi_hour() đã tự làm — AQI = max(IAQI); gán aqi_level,
         aqi_label, dominant_pollutant (không viết lại công thức ở đây)

Input : /air-quality/clean/   (output Pha 1)   Output: /air-quality/aqi/  (schema C3)

Chạy local không cần cluster:
  python jobs/phase2_aqi.py --input /tmp/clean --output /tmp/aqi

Nowcast (12h) chỉ tính được ở đây, KHÔNG phải Pha 1 — vì Pha 1 chỉ có nhiệm vụ
làm sạch, còn Nowcast là một phần của công thức AQI GIỜ (QĐ 1459 mục 2.2.1a),
thuộc trách nhiệm Pha 2 theo đúng cách chia Mapper/Reducer.

owm_aqi được Pha 1 giữ lại (không phải phần của C3) chỉ để đối chiếu ở đây —
KHÔNG ghi owm_aqi vào output /air-quality/aqi/ vì C3 trong CONTRACTS.md không
có cột này (đổi schema dùng chung phải sửa CONTRACTS.md + báo Người A trước).
Bảng đối chiếu in ra console + ghi riêng vào docs/ cho báo cáo.
"""
import argparse
import os
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)  # để driver `import aqi_core` được dù gọi trực tiếp
# UDF chạy trong tiến trình worker RIÊNG (kể cả local[*]) -> không kế thừa sys.path đã sửa
# trong bộ nhớ của driver, chỉ kế thừa biến môi trường. Không set PYTHONPATH thì worker
# unpickle UDF sẽ lỗi "ModuleNotFoundError: No module named 'aqi_core'".
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
    """Ghép 12 điểm trong window thành đúng vị trí giờ (aqi_core.align_hourly_window(),
    DÙNG CHUNG với streaming_aqi.py) rồi gọi aqi_core.nowcast() thật — không viết lại
    công thức Nowcast ở đây."""
    if window_rows is None:
        return None
    hourly = align_hourly_window(
        current_ts, ((row["ts_epoch"], row["v"]) for row in window_rows), NOWCAST_WINDOW_HOURS
    )
    return nowcast(hourly)


@F.udf(returnType=_HOUR_RESULT_SCHEMA)
def _iaqi_hour_udf(o3, no2, so2, co, pm2_5_nowcast, pm10_nowcast):
    """Gọi thẳng aqi_core.iaqi_hour() — QUY TẮC VÀNG: không copy công thức."""
    out = iaqi_hour({
        "o3": o3, "no2": no2, "so2": so2, "co": co,
        "pm2_5": pm2_5_nowcast, "pm10": pm10_nowcast,
    })
    isubs = out["iaqi"]

    def _f(x):
        # round() trả int, còn schema khai DoubleType -> ép kiểu tường minh,
        # nếu không PySpark UDF (non-Arrow) serialize sai kiểu thành null âm thầm.
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
    """Nowcast(12h) cho pm2_5/pm10 — chỉ đúng cho 2 chất này (QĐ 1459 mục 2.2.1a)."""
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
    """Bảng đối chiếu aqi tự tính (VN_AQI 0-500) vs owm_aqi (thang 1-5 của OpenWeather).
    CHỈ để báo cáo — owm_aqi không bao giờ được coi là AQI của đồ án (xem README §1)."""
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
