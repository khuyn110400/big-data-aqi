#!/usr/bin/env python3
"""
Phase 1 clean - resumable / bounded-window version.

Stages are intentionally separate Spark jobs:
  grid        raw -> clipped hourly grid checkpoint
  interpolate grid -> bounded-window interpolation checkpoint
  final       interpolation checkpoint -> daily aggregates -> clean parquet
  quality     read materialized clean parquet -> quality report

This preserves the existing cleaning semantics while making completed stages reusable.
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType
)

from aqi_core.iaqi import max_plausible

POLLUTANTS = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]
MAX_GAP_HOURS_TO_INTERPOLATE = 3
MIN_DAY_COMPLETENESS = 0.75

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
        [StructField(p, DoubleType())
         for p in ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]]
    )),
])


def _max_plausible():
    return max_plausible("VN_1459")


def load_raw(spark, path):
    df = spark.read.schema(RAW_SCHEMA).json(path)
    cols = ["station_id", "city", "country", "lat", "lon", "ts_epoch", "owm_aqi"]
    cols += [F.col(f"components.{p}").alias(p) for p in POLLUTANTS]
    return df.select(*cols).dropDuplicates(["station_id", "ts_epoch"])


def clip_outliers(df):
    limits = _max_plausible()
    for p in POLLUTANTS:
        df = df.withColumn(
            p,
            F.when(
                (F.col(p) < 0) | (F.col(p) > limits[p]),
                F.lit(None).cast("double"),
            ).otherwise(F.col(p)),
        )
    return df


def build_hourly_grid(df):
    meta = df.groupBy("station_id").agg(
        F.first("city", ignorenulls=True).alias("city"),
        F.first("country", ignorenulls=True).alias("country"),
        F.first("lat", ignorenulls=True).alias("lat"),
        F.first("lon", ignorenulls=True).alias("lon"),
        F.min("ts_epoch").alias("min_ts"),
        F.max("ts_epoch").alias("max_ts"),
    )

    grid = meta.select(
        "station_id", "city", "country", "lat", "lon",
        F.explode(
            F.sequence(F.col("min_ts"), F.col("max_ts"), F.lit(3600))
        ).alias("ts_epoch"),
    )

    return grid.join(
        df.drop("city", "country", "lat", "lon"),
        ["station_id", "ts_epoch"],
        "left",
    )


def interpolate_short_gaps_bounded(df):
    """
    Same interpolation decision as the existing Phase 1, but bounded and
    computed in one Window pass for all pollutants.

    Because the dataframe is already an hourly grid and the accepted endpoint
    span is <= MAX_GAP_HOURS_TO_INTERPOLATE, no lookup outside +/- N rows can
    ever affect a row that is eligible for interpolation.
    """
    n = MAX_GAP_HOURS_TO_INTERPOLATE
    w = Window.partitionBy("station_id").orderBy("ts_epoch")
    w_prev = w.rowsBetween(-n, 0)
    w_next = w.rowsBetween(0, n)

    helper_exprs = []
    for p in POLLUTANTS:
        is_valid = F.col(p).isNotNull()
        helper_exprs.extend([
            F.last(
                F.when(is_valid, F.col("ts_epoch")),
                ignorenulls=True,
            ).over(w_prev).alias(f"__{p}_prev_ts"),
            F.last(
                F.when(is_valid, F.col(p)),
                ignorenulls=True,
            ).over(w_prev).alias(f"__{p}_prev_val"),
            F.first(
                F.when(is_valid, F.col("ts_epoch")),
                ignorenulls=True,
            ).over(w_next).alias(f"__{p}_next_ts"),
            F.first(
                F.when(is_valid, F.col(p)),
                ignorenulls=True,
            ).over(w_next).alias(f"__{p}_next_val"),
        ])

    # All 24 endpoint expressions share the same station/time ordering.
    # Building them together lets Spark plan one Window operator instead of a
    # long chain of withColumn/window transformations.
    df = df.select("*", *helper_exprs)

    projected = []
    base_cols = [c for c in df.columns if not c.startswith("__")]

    for c in base_cols:
        if c not in POLLUTANTS:
            projected.append(F.col(c))

    for p in POLLUTANTS:
        original = F.col(p)
        prev_ts = F.col(f"__{p}_prev_ts")
        prev_val = F.col(f"__{p}_prev_val")
        next_ts = F.col(f"__{p}_next_ts")
        next_val = F.col(f"__{p}_next_val")

        gap_hours = (next_ts - prev_ts) / F.lit(3600.0)
        can_interpolate = (
            original.isNull()
            & prev_ts.isNotNull()
            & next_ts.isNotNull()
            & (next_ts > prev_ts)
            & (gap_hours <= F.lit(float(MAX_GAP_HOURS_TO_INTERPOLATE)))
        )

        interpolated = (
            prev_val
            + (next_val - prev_val)
            * (F.col("ts_epoch") - prev_ts)
            / (next_ts - prev_ts)
        )

        projected.append(
            F.when(original.isNotNull(), original)
             .when(can_interpolate, interpolated)
             .otherwise(F.lit(None).cast("double"))
             .alias(p)
        )
        projected.append(
            F.when(original.isNotNull(), F.lit("measured"))
             .when(can_interpolate, F.lit("interpolated"))
             .otherwise(F.lit("missing"))
             .alias(f"{p}_qc")
        )

    return df.select(*projected)


def add_day_aggregates_window(df):
    """
    Equivalent output to groupBy + join, but avoids materializing a separate
    aggregate dataframe and joining it back to every hourly row.
    """
    df = df.withColumn(
        "ts_utc",
        F.to_timestamp(F.from_unixtime(F.col("ts_epoch"))),
    )
    df = df.withColumn("dt", F.to_date("ts_utc"))
    df = df.withColumn(
        "aqi_day",
        F.to_date(F.col("ts_utc") - F.expr("INTERVAL 1 HOUR")),
    )

    w_day = Window.partitionBy("station_id", "aqi_day")

    df = df.withColumn(
        "pm2_5_day_avg",
        F.avg("pm2_5").over(w_day),
    )
    df = df.withColumn(
        "pm10_day_avg",
        F.avg("pm10").over(w_day),
    )
    df = df.withColumn(
        "_valid_hours",
        F.sum(
            F.when(
                F.col("pm2_5").isNotNull() | F.col("pm10").isNotNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).over(w_day),
    )
    df = df.withColumn(
        "day_qc_flag",
        F.when(
            F.col("_valid_hours") >= F.lit(24 * MIN_DAY_COMPLETENESS),
            F.lit("valid"),
        ).otherwise(F.lit("insufficient_data")),
    ).drop("_valid_hours")

    return df


def print_quality_report(df):
    agg_exprs = [F.count(F.lit(1)).alias("total")]

    for p in POLLUTANTS:
        agg_exprs.extend([
            F.sum(
                F.when(F.col(f"{p}_qc") == "missing", 1).otherwise(0)
            ).alias(f"{p}_missing"),
            F.sum(
                F.when(F.col(f"{p}_qc") == "interpolated", 1).otherwise(0)
            ).alias(f"{p}_interpolated"),
        ])

    stats = df.agg(*agg_exprs).first().asDict()
    total = int(stats["total"])

    print("\n=== Báo cáo chất lượng dữ liệu (Pha 1) ===")
    print(f"Tổng số giờ (sau dựng lưới đầy đủ): {total}")

    for p in POLLUTANTS:
        missing = int(stats.get(f"{p}_missing") or 0)
        interpolated = int(stats.get(f"{p}_interpolated") or 0)
        print(
            f"  {p:<6}: thiếu={missing:>5} ({missing / total:.1%})  "
            f"đã nội suy={interpolated:>5} ({interpolated / total:.1%})"
        )

    days = (
        df.select("station_id", "aqi_day", "day_qc_flag")
        .dropDuplicates(["station_id", "aqi_day"])
    )

    day_stats = days.agg(
        F.count(F.lit(1)).alias("total_days"),
        F.sum(
            F.when(F.col("day_qc_flag") == "valid", 1).otherwise(0)
        ).alias("valid_days"),
    ).first().asDict()

    total_days = int(day_stats["total_days"])
    valid_days = int(day_stats.get("valid_days") or 0)
    ratio = valid_days / total_days if total_days else 0.0

    print(
        f"Ngày hợp lệ cho AQI ngày (>= {MIN_DAY_COMPLETENESS:.0%} dữ liệu): "
        f"{valid_days}/{total_days} ({ratio:.1%})"
    )
    print("=" * 43 + "\n")


def success_exists(spark, path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    success = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_SUCCESS")
    fs = success.getFileSystem(hconf)
    return bool(fs.exists(success))


def stage_path(workdir, name):
    return workdir.rstrip("/") + "/" + name


def build_spark():
    spark = (
        SparkSession.builder
        .appName("phase1_clean_resumable_v4")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True,
                    choices=["grid", "interpolate", "final", "quality"])
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--partitions", type=int, default=96)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    spark = build_spark()
    grid_path = stage_path(args.workdir, "grid")
    interp_path = stage_path(args.workdir, "interpolated")

    try:
        if args.stage == "grid":
            if args.resume and success_exists(spark, grid_path):
                print(f"=== STAGE_GRID_ALREADY_COMPLETE: {grid_path} ===", flush=True)
                return

            df = load_raw(spark, args.input)
            df = clip_outliers(df)
            df = build_hourly_grid(df)

            (
                df.repartition(args.partitions, "station_id")
                  .write.mode("overwrite")
                  .parquet(grid_path)
            )
            print(f"=== STAGE_GRID_COMPLETE: {grid_path} ===", flush=True)

        elif args.stage == "interpolate":
            if args.resume and success_exists(spark, interp_path):
                print(
                    f"=== STAGE_INTERPOLATE_ALREADY_COMPLETE: {interp_path} ===",
                    flush=True,
                )
                return

            if not success_exists(spark, grid_path):
                raise RuntimeError(
                    f"Grid checkpoint is missing: {grid_path}/_SUCCESS"
                )

            df = spark.read.parquet(grid_path)
            df = (
                df.repartition(args.partitions, "station_id")
                  .sortWithinPartitions("station_id", "ts_epoch")
            )
            df = interpolate_short_gaps_bounded(df)

            df.write.mode("overwrite").parquet(interp_path)
            print(
                f"=== STAGE_INTERPOLATE_COMPLETE: {interp_path} ===",
                flush=True,
            )

        elif args.stage == "final":
            if args.resume and success_exists(spark, args.output):
                print(
                    f"=== CLEAN_ALREADY_COMPLETE: {args.output} ===",
                    flush=True,
                )
                return

            if not success_exists(spark, interp_path):
                raise RuntimeError(
                    f"Interpolation checkpoint is missing: {interp_path}/_SUCCESS"
                )

            df = spark.read.parquet(interp_path)
            df = add_day_aggregates_window(df)

            out_cols = (
                ["station_id", "city", "country", "lat", "lon",
                 "ts_utc", "ts_epoch", "dt", "aqi_day", "owm_aqi"]
                + POLLUTANTS
                + ["pm2_5_day_avg", "pm10_day_avg", "day_qc_flag"]
                + [f"{p}_qc" for p in POLLUTANTS]
            )

            (
                df.select(*out_cols)
                  .repartition(args.partitions, "country", "dt")
                  .write.mode("overwrite")
                  .partitionBy("country", "dt")
                  .parquet(args.output)
            )
            print("\n=== CLEAN_WRITE_COMPLETE ===", flush=True)
            print("=== QUALITY_REPORT_DEFERRED ===", flush=True)

        elif args.stage == "quality":
            if not success_exists(spark, args.output):
                raise RuntimeError(
                    f"Clean output is missing: {args.output}/_SUCCESS"
                )
            clean_df = spark.read.parquet(args.output)
            print_quality_report(clean_df)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
