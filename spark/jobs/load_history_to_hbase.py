"""
NẠP LỊCH SỬ — parquet Pha 2 (/air-quality/aqi/) -> HBase air_quality. NGƯỜI B · M3

FastAPI/Grafana chỉ đọc HBase, mà làn streaming chỉ sinh dữ liệu mới từ lúc bật. Job này nạp
kết quả batch (Pha 2) vào HBase để dashboard có lịch sử. Row key + 16 cột do
sinks/hbase_sink.py định nghĩa, dùng chung với streaming_aqi.py.

NẠP LỊCH SỬ TRƯỚC, BẬT STREAMING SAU: streaming lấy 11 giờ trước đó của từng trạm từ HBase
để tính Nowcast PM2.5/PM10; HBase trống thì giờ live đầu tiên thiếu cửa sổ.

Chỉ nạp dòng có AQI. Nên dùng --since (vd 90 ngày gần nhất) cho dashboard; nạp toàn bộ ~8,5
triệu dòng qua Thrift chạy lâu và không cần thiết.

Chạy trong container spark-master (xem docs/B_TO_A_RUNBOOK.md):
  spark-submit --master local[*] spark/jobs/load_history_to_hbase.py \\
      --input hdfs://namenode:9000/air-quality/aqi --since 2026-06-01 --hbase-host hbase-thrift
Thử không ghi gì (chỉ đếm + in mẫu): thêm --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)
# foreachPartition chạy trong tiến trình Python worker riêng -> cần PYTHONPATH để import `sinks`
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sinks import hbase_sink

HISTORY_COLUMNS = [
    "station_id", "city", "country", "lat", "lon",
    "pm2_5", "pm10", "o3", "no2", "so2", "co",
    "aqi", "aqi_level", "aqi_label", "dominant_pollutant", "standard",
]


def select_history(spark, input_path: str, since: str | None = None, until: str | None = None):
    """Đọc parquet Pha 2, giữ dòng có AQI, thêm ts_epoch.

    ts_epoch lấy bằng cast("long") ngay trong Spark: đi qua datetime Python sẽ ra giờ theo
    múi giờ máy (xem hbase_sink.epoch_of)."""
    df = spark.read.parquet(input_path)
    if since:
        df = df.filter(F.col("dt") >= F.lit(since).cast("date"))
    if until:
        df = df.filter(F.col("dt") <= F.lit(until).cast("date"))
    return df.filter(F.col("aqi").isNotNull()).select(
        *HISTORY_COLUMNS, F.col("ts_utc").cast("long").alias("ts_epoch")
    )


def write_partition(rows, host: str, port: int, table_name: str, batch_size: int, connect=hbase_sink.connect) -> int:
    connection = connect(host, port)
    try:
        table = connection.table(table_name)
        return hbase_sink.put_rows(table, (row.asDict() for row in rows), batch_size)
    finally:
        connection.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="thư mục parquet Pha 2, vd hdfs://namenode:9000/air-quality/aqi")
    ap.add_argument("--since", default=None, help="chỉ nạp dt >= YYYY-MM-DD (UTC)")
    ap.add_argument("--until", default=None, help="chỉ nạp dt <= YYYY-MM-DD (UTC)")
    ap.add_argument("--hbase-host", default=os.environ.get("HBASE_THRIFT_HOST", "hbase-thrift"))
    ap.add_argument("--hbase-port", type=int, default=int(os.environ.get("HBASE_THRIFT_PORT", "9090")))
    ap.add_argument("--hbase-table", default=os.environ.get("HBASE_TABLE", hbase_sink.TABLE))
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--partitions", type=int, default=8, help="số luồng ghi song song (mỗi partition 1 kết nối Thrift)")
    ap.add_argument("--dry-run", action="store_true", help="chỉ đếm và in mẫu, không ghi HBase")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("load_history_to_hbase")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = select_history(spark, args.input, args.since, args.until)

    if args.dry_run:
        print(f"[DRY RUN] {df.count()} dòng sẽ được nạp vào {args.hbase_table}")
        df.orderBy("station_id", "ts_epoch").show(5, truncate=False)
        spark.stop()
        return

    written = spark.sparkContext.accumulator(0)
    host, port, table_name, batch_size = args.hbase_host, args.hbase_port, args.hbase_table, args.batch_size

    def _write(rows):
        written.add(write_partition(rows, host, port, table_name, batch_size))

    df.coalesce(args.partitions).foreachPartition(_write)

    print(f"Đã nạp {written.value} dòng vào HBase bảng {args.hbase_table} ({args.hbase_host}:{args.hbase_port})")
    spark.stop()


if __name__ == "__main__":
    main()
