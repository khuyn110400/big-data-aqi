"""
Pha 3: tổng hợp theo ngày và xếp hạng thành phố (theo mô hình 3 job của Iris-pot/AQI_analysis).

Đầu vào : /air-quality/aqi/  (đầu ra Pha 2, schema C3)
Đầu ra  : /air-quality/agg/daily/    AQI trung bình, lớn nhất, nhỏ nhất và số giờ ở từng
                                     mức, theo (city, dt)
          /air-quality/agg/ranking/  xếp hạng thành phố theo avg_aqi trong ngày

Chạy local không cần cluster:
  python jobs/phase3_aggregate.py --input /tmp/aqi --output /tmp/agg

Các quyết định thiết kế:
  - Điểm để xếp hạng là avg_aqi trong ngày. Không dùng điểm tổng hợp có trọng số tự đặt,
    vì QĐ 1459 không định nghĩa cách xếp hạng thành phố.
  - Hạng 1 là thành phố ô nhiễm nhất (avg_aqi cao nhất) trong ngày, giống các dashboard
    AQI công khai.
  - Số giờ ở từng mức (6 mức) được gộp vào agg/daily/ thành các cột count_level_1..6 thay
    vì tách thư mục riêng, để giữ đúng hai đường dẫn đã ghi ở C2 (CONTRACTS.md).
  - Hạng được tính chung cho mọi quốc gia trong cùng một ngày dt. Muốn xếp hạng riêng theo
    quốc gia thì xếp lại ở tầng API.

worst_pollutant là dominant_pollutant xuất hiện nhiều giờ nhất trong ngày của thành phố
(mode). Pha 2 đã tính dominant_pollutant theo từng giờ; lấy mode để khỏi phải tính lại IAQI
trung bình ngày cho từng chất ở đây.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

AQI_LEVELS = range(1, 7)
GROUP_COLS = ["city", "country", "dt"]


def _mode_dominant_pollutant(df, group_cols):
    """Chất xuất hiện làm dominant_pollutant nhiều giờ nhất trong nhóm, đặt tên là worst_pollutant."""
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
    """Trung bình, lớn nhất, nhỏ nhất của AQI và số giờ ở từng mức (count_level_1..6) theo (city, dt)."""
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
    """Xếp hạng thành phố theo avg_aqi trong ngày: hạng 1 là ô nhiễm nhất, xếp chung theo dt."""
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
