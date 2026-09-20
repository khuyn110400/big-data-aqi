"""
PHA 3 — TỔNG HỢP & XẾP HẠNG  (mẫu 3 job của Iris-pot/AQI_analysis). NGƯỜI B · M3

Input : /air-quality/aqi/    (output Pha 2, schema C3)
Output: /air-quality/agg/daily/    — trung bình/max AQI + phân bố 6 mức theo (city, dt)
        /air-quality/agg/ranking/ — xếp hạng thành phố theo avg_aqi trong ngày (C5 /aqi/ranking)

Chạy local không cần cluster:
  python jobs/phase3_aggregate.py --input /tmp/aqi --output /tmp/agg

Quyết định thiết kế (đã chốt, ghi lại để khỏi phải hỏi lại):
  - "Điểm tổng hợp" xếp hạng = avg_aqi trong ngày. KHÔNG dùng composite score có
    trọng số tự chọn — QĐ 1459 không định nghĩa cách xếp hạng thành phố, tự bịa
    công thức sẽ không giải thích được nguồn gốc khi bảo vệ đồ án.
  - Rank 1 = ô nhiễm NHẤT (avg_aqi cao nhất) trong ngày, giống kiểu dashboard AQI
    công khai (IQAir...).
  - AQIClassify (phân bố 6 mức) GỘP vào agg/daily/ làm cột đếm thêm (count_level_1..6),
    KHÔNG tách folder riêng — giữ đúng 2 đường dẫn đã chốt ở C2 (CONTRACTS.md), khỏi
    phải sửa contract chung / báo Người A.
  - Rank tính TOÀN CỤC theo dt (không tách riêng theo từng country). Nếu Query/API
    Bridge cần rank riêng theo country khi filter, Người A tự re-rank ở tầng API.

worst_pollutant = dominant_pollutant xuất hiện NHIỀU GIỜ NHẤT trong ngày đó của thành
phố (mode) — dominant_pollutant đã được Pha 2 tính đúng theo từng giờ (chất có IAQI cao
nhất giờ đó), lấy mode tránh phải tính lại IAQI trung bình ngày cho từng chất ở đây.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

AQI_LEVELS = range(1, 7)
GROUP_COLS = ["city", "country", "dt"]


def _mode_dominant_pollutant(df, group_cols):
    """Chất xuất hiện làm dominant_pollutant nhiều giờ nhất trong nhóm -> 'worst_pollutant'."""
    counts = (
        df.filter(F.col("dominant_pollutant").isNotNull())
        .groupBy(*group_cols, "dominant_pollutant")
        .count()
    )
    w = Window.partitionBy(*group_cols).orderBy(F.col("count").desc())
    return (
        counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(*group_cols, F.col("dominant_pollutant").alias("worst_pollutant"))
    )


def compute_daily(df):
    """AQI + AQIClassify gộp: trung bình/max/min AQI + đếm phân bố 6 mức theo (city, dt)."""
    level_counts = [
        F.count(F.when(F.col("aqi_level") == lvl, 1)).alias(f"count_level_{lvl}") for lvl in AQI_LEVELS
    ]
    daily = df.groupBy(*GROUP_COLS).agg(
        F.round(F.avg("aqi"), 1).alias("avg_aqi"),
        F.max("aqi").alias("max_aqi"),
        F.min("aqi").alias("min_aqi"),
        F.count(F.when(F.col("aqi").isNotNull(), 1)).alias("n_hours"),
        *level_counts,
    )
    worst = _mode_dominant_pollutant(df, GROUP_COLS)
    return daily.join(worst, GROUP_COLS, "left")


def compute_ranking(daily_df):
    """Xếp hạng thành phố theo avg_aqi trong ngày — rank 1 = ô nhiễm nhất (toàn cục theo dt)."""
    w = Window.partitionBy("dt").orderBy(F.col("avg_aqi").desc())
    return (
        daily_df.withColumn("rank", F.row_number().over(w))
        .select("rank", "city", "country", "dt", "avg_aqi", "max_aqi", "worst_pollutant")
    )


def print_top_ranking(ranking_df, n=5):
    latest_dt = ranking_df.select(F.max("dt")).first()[0]
    print(f"\n=== Top {n} thành phố ô nhiễm nhất ngày {latest_dt} ===")
    ranking_df.filter(F.col("dt") == latest_dt).orderBy("rank").limit(n).show(truncate=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("phase3_aggregate")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    daily = compute_daily(df)
    ranking = compute_ranking(daily)

    daily.repartition(48, "country", "dt").write.mode("overwrite").partitionBy("country", "dt").parquet(f"{args.output}/daily")
    ranking.repartition(48, "dt").write.mode("overwrite").partitionBy("dt").parquet(f"{args.output}/ranking")

    print_top_ranking(ranking)

    spark.stop()


if __name__ == "__main__":
    main()
