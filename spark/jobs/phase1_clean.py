"""
PHA 1 — LÀM SẠCH  (El Fazziki 2015, pha 1). NGƯỜI B · M1

Mapper : key = (station_id, ts_utc), value = bản ghi thô
Reducer: khử trùng lặp, loại ngoại lai, nội suy điểm thiếu

Input : hdfs:///air-quality/raw/**/*.jsonl.gz   (hoặc data/samples/ khi dev local)
Output: hdfs:///air-quality/clean/  parquet, partition country/dt

Chạy local không cần cluster:
  python jobs/phase1_clean.py --input ../data/samples/air_quality_sample.jsonl --output /tmp/clean

Rule gap (xem data/samples/README.md, đã kiểm chứng trên dữ liệu thật):
  - Thiếu <= 3 giờ liên tiếp -> nội suy tuyến tính theo trạm.
  - Thiếu > 3 giờ (khối nguyên ngày) -> KHÔNG nội suy, giữ null, đánh qc_flag
    "missing" để Pha 2/3 loại ngày đó khỏi AQI ngày.

Ngày AQI theo QĐ 1459 (mục 2.2.2a) là khối 01:00 -> 00:00 hôm sau, KHÁC với
ngày dương lịch 00:00->23:00 dùng để partition output — vì vậy có 2 cột ngày
riêng: 'dt' (ngày UTC, dùng để partition theo C2) và 'aqi_day' (dùng để tính
TB24h PM2.5/PM10 và tỉ lệ đầy đủ dữ liệu theo đúng định nghĩa QĐ 1459).
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # để `import aqi_core` chạy được dù gọi trực tiếp

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from aqi_core.iaqi import load_standard

POLLUTANTS = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]
MAX_GAP_HOURS_TO_INTERPOLATE = 3
MIN_DAY_COMPLETENESS = 0.75  # 18/24h — chuẩn US EPA; QĐ 1459 không ghi rõ số % trong mục đã có

RAW_SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("station_id", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts_utc", StringType()),
    StructField("ts_epoch", LongType()),
    StructField("dt_raw", LongType()),
    StructField("ingest_mode", StringType()),
    StructField("owm_aqi", IntegerType()),
    StructField("components", StructType(
        [StructField(p, DoubleType()) for p in ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]]
    )),
])


def _max_plausible() -> dict:
    """Trần loại ngoại lai = mốc nồng độ ứng với I=500 trong breakpoint VN_1459 (đã verify)."""
    std = load_standard("VN_1459")["pollutants"]
    return {
        "pm2_5": std["pm2_5"]["bp"][-1],
        "pm10": std["pm10"]["bp"][-1],
        "so2": std["so2"]["bp"][-1],
        "no2": std["no2"]["bp"][-1],
        "co": std["co"]["bp"][-1],
        "o3": std["o3_1h"]["bp"][-1],
    }


def load_raw(spark: SparkSession, path: str):
    df = spark.read.schema(RAW_SCHEMA).json(path)
    # owm_aqi giữ lại (không dùng để làm sạch) để Pha 2 đối chiếu với AQI tự tính — yêu cầu M2.
    cols = ["station_id", "city", "country", "lat", "lon", "ts_epoch", "owm_aqi"]
    cols += [F.col(f"components.{p}").alias(p) for p in POLLUTANTS]
    return df.select(*cols).dropDuplicates(["station_id", "ts_epoch"])


def clip_outliers(df):
    """Nồng độ âm hoặc vượt trần vật lý -> null cho ĐÚNG cột đó, không drop cả dòng."""
    max_plausible = _max_plausible()
    for p in POLLUTANTS:
        df = df.withColumn(
            p,
            F.when((F.col(p) < 0) | (F.col(p) > max_plausible[p]), None).otherwise(F.col(p)),
        )
    return df


def build_hourly_grid(df):
    """Dựng lưới giờ liên tục mỗi trạm để lộ ra giờ thiếu HẲN (không chỉ giờ có mặt trong input)."""
    meta = df.groupBy("station_id").agg(
        F.first("city").alias("city"),
        F.first("country").alias("country"),
        F.first("lat").alias("lat"),
        F.first("lon").alias("lon"),
        F.min("ts_epoch").alias("min_ts"),
        F.max("ts_epoch").alias("max_ts"),
    )
    grid = meta.select(
        "station_id", "city", "country", "lat", "lon",
        F.explode(F.sequence(F.col("min_ts"), F.col("max_ts"), F.lit(3600))).alias("ts_epoch"),
    )
    return grid.join(df.drop("city", "country", "lat", "lon"), ["station_id", "ts_epoch"], "left")


def interpolate_short_gaps(df):
    """Nội suy tuyến tính nếu khoảng thiếu <= MAX_GAP_HOURS_TO_INTERPOLATE giờ; giữ null nếu dài hơn."""
    w_asc = Window.partitionBy("station_id").orderBy("ts_epoch")
    w_prev = w_asc.rowsBetween(Window.unboundedPreceding, 0)
    w_next = w_asc.rowsBetween(0, Window.unboundedFollowing)

    for p in POLLUTANTS:
        is_valid = F.col(p).isNotNull()
        prev_ts = F.last(F.when(is_valid, F.col("ts_epoch")), ignorenulls=True).over(w_prev)
        prev_val = F.last(F.when(is_valid, F.col(p)), ignorenulls=True).over(w_prev)
        next_ts = F.first(F.when(is_valid, F.col("ts_epoch")), ignorenulls=True).over(w_next)
        next_val = F.first(F.when(is_valid, F.col(p)), ignorenulls=True).over(w_next)

        gap_hours = (next_ts - prev_ts) / 3600
        interpolated = prev_val + (next_val - prev_val) * (F.col("ts_epoch") - prev_ts) / (next_ts - prev_ts)
        can_interpolate = prev_ts.isNotNull() & next_ts.isNotNull() & (gap_hours <= MAX_GAP_HOURS_TO_INTERPOLATE)

        df = df.withColumn(
            f"{p}_qc",
            F.when(is_valid, F.lit("measured"))
             .when(can_interpolate, F.lit("interpolated"))
             .otherwise(F.lit("missing")),
        )
        df = df.withColumn(p, F.when(is_valid, F.col(p)).when(can_interpolate, interpolated).otherwise(None))

    return df


def add_day_aggregates(df):
    """TB24h PM2.5/PM10 + tỉ lệ đầy đủ theo 'ngày AQI' (01:00 -> 00:00 hôm sau, QĐ 1459 mục 2.2.2a)."""
    df = df.withColumn("ts_utc", F.to_timestamp(F.from_unixtime(F.col("ts_epoch"))))
    df = df.withColumn("dt", F.to_date("ts_utc"))
    df = df.withColumn("aqi_day", F.to_date(F.col("ts_utc") - F.expr("INTERVAL 1 HOUR")))

    day_stats = df.groupBy("station_id", "aqi_day").agg(
        F.avg("pm2_5").alias("pm2_5_day_avg"),
        F.avg("pm10").alias("pm10_day_avg"),
        F.count(F.when(F.col("pm2_5").isNotNull() | F.col("pm10").isNotNull(), 1)).alias("valid_hours"),
    )
    day_stats = day_stats.withColumn(
        "day_qc_flag",
        F.when(F.col("valid_hours") >= 24 * MIN_DAY_COMPLETENESS, F.lit("valid")).otherwise(F.lit("insufficient_data")),
    ).drop("valid_hours")

    return df.join(day_stats, ["station_id", "aqi_day"], "left")


def print_quality_report(df):
    total = df.count()
    print(f"\n=== Báo cáo chất lượng dữ liệu (Pha 1) ===")
    print(f"Tổng số giờ (sau dựng lưới đầy đủ): {total}")
    for p in POLLUTANTS:
        missing = df.filter(F.col(f"{p}_qc") == "missing").count()
        interpolated = df.filter(F.col(f"{p}_qc") == "interpolated").count()
        print(f"  {p:<6}: thiếu={missing:>5} ({missing / total:.1%})  "
              f"đã nội suy={interpolated:>5} ({interpolated / total:.1%})")

    days = df.select("station_id", "aqi_day", "day_qc_flag").dropDuplicates(["station_id", "aqi_day"])
    total_days = days.count()
    valid_days = days.filter(F.col("day_qc_flag") == "valid").count()
    print(f"Ngày hợp lệ cho AQI ngày (>= {MIN_DAY_COMPLETENESS:.0%} dữ liệu): "
          f"{valid_days}/{total_days} ({valid_days / total_days:.1%})")
    print("=" * 43 + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("phase1_clean")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # BẮT BUỘC: to_date()/from_unixtime() quy đổi theo timezone của session (mặc định
        # theo máy chạy job), không phải UTC -> dt/aqi_day sẽ lệch ngày nếu không set (đã
        # phát hiện qua test: máy dev ở +07 làm sai lệch ranh giới ngày UTC yêu cầu ở C1).
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = load_raw(spark, args.input)
    df = clip_outliers(df)
    df = build_hourly_grid(df)
    df = interpolate_short_gaps(df)
    df = add_day_aggregates(df)

    print_quality_report(df)

    out_cols = (
        ["station_id", "city", "country", "lat", "lon", "ts_utc", "ts_epoch", "dt", "aqi_day", "owm_aqi"]
        + POLLUTANTS
        + ["pm2_5_day_avg", "pm10_day_avg", "day_qc_flag"]
        + [f"{p}_qc" for p in POLLUTANTS]
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
