"""
Streaming: Kafka -> tính AQI -> HBase và HDFS.

Đọc topic air-quality-raw, parse theo schema C1, tính AQI bằng aqi_core (cùng module với
Pha 2) rồi ghi ra:
  - HBase: bảng air_quality (row key và cột theo C4, qua sinks/hbase_sink.py)
  - HDFS : /air-quality/raw/ingest_mode=live/country=XX/dt=YYYY-MM-DD/, để batch xử lý lại được
Bản ghi sai schema được đẩy sang topic air-quality-dlq, không làm dừng job.

Thiết kế:
  - Lượng dữ liệu live nhỏ (khoảng 200 bản ghi mỗi 30-60 phút) nên mỗi micro-batch được gom
    về driver (foreachBatch + collect) rồi xử lý ở đó. Nhờ vậy chỉ driver cần happybase,
    executor chỉ đọc Kafka.
  - Nowcast PM2.5 và PM10 cần 11 giờ trước đó của từng trạm. Job đọc các giờ này từ HBase
    (hbase_sink.read_pm_history) cộng với các giờ nằm cùng batch, rồi gọi align_hourly_window(),
    nowcast() và iaqi_hour(), đúng ba hàm mà Pha 2 gọi, không viết lại công thức.
  - Ghi HBase theo row key nên idempotent: batch chạy lại sau khi restart chỉ ghi đè cùng
    dòng. Ghi HDFS là append nên batch chạy lại có thể trùng dòng; Pha 1 đã dropDuplicates
    theo (station_id, ts_epoch).
  - Bản ghi đến muộn được ghi đúng dòng của nó nhưng các giờ sau đó không được tính lại.
    Không dùng watermark vì job không có state hay aggregation của Spark.
  - Khác Pha 1 ở một điểm: không nội suy khoảng thiếu ngắn (chưa có dữ liệu tương lai).
    Nowcast tự chịu được việc thiếu giờ, chỉ cần ít nhất 2 trong 3 giờ gần nhất.

Chạy (trong container spark-master, xem docs/huong-dan-chay.md):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<đúng bản Spark> \\
      spark/jobs/streaming_aqi.py --starting-offsets earliest --once
--once xử lý hết dữ liệu đang có trong Kafka rồi thoát (dùng để kiểm thử); bỏ --once để chạy liên tục.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)

from aqi_core.iaqi import align_hourly_window, iaqi_hour, max_plausible, nowcast
from sinks import hbase_sink

logger = logging.getLogger("streaming_aqi")

SCHEMA_VERSION = "1.0"
POLLUTANTS = ("pm2_5", "pm10", "o3", "no2", "so2", "co")
NOWCAST_WINDOW_HOURS = 12
_REQUIRED = ("station_id", "city", "country", "lat", "lon", "ts_epoch")


class InvalidRecord(ValueError):
    """Bản ghi Kafka sai schema C1 -> đẩy sang air-quality-dlq."""


def parse_record(raw: str) -> dict:
    try:
        record = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidRecord(f"JSON hỏng: {exc}") from exc

    if not isinstance(record, dict):
        raise InvalidRecord("message không phải JSON object")
    if record.get("schema_version") != SCHEMA_VERSION:
        raise InvalidRecord(f"schema_version lạ: {record.get('schema_version')!r}")

    missing = [f for f in _REQUIRED if record.get(f) in (None, "")]
    if missing:
        raise InvalidRecord(f"thiếu trường bắt buộc: {missing}")
    if not isinstance(record.get("components"), dict):
        raise InvalidRecord("thiếu components")

    try:
        record["ts_epoch"] = int(record["ts_epoch"])
    except (TypeError, ValueError) as exc:
        raise InvalidRecord(f"ts_epoch không phải số: {record['ts_epoch']!r}") from exc
    return record


def clean_components(components: dict, limits: dict) -> dict:
    """Nồng độ âm hoặc vượt trần vật lý thì đặt None cho đúng chất đó (giống clip_outliers của Pha 1)."""
    cleaned = {}
    for pollutant in POLLUTANTS:
        value = components.get(pollutant)
        ok = isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value <= limits[pollutant]
        cleaned[pollutant] = float(value) if ok else None
    return cleaned


def compute_hour(series: dict, ts_epoch: int, standard: str = "VN_1459") -> dict:
    """AQI giờ tại ts_epoch. series = {ts_epoch: {pollutant: value}} của MỘT trạm, gồm các
    giờ trước đó (từ HBase) và giờ hiện tại. Trả kết quả của aqi_core.iaqi_hour()."""
    current = series[ts_epoch]

    def nowcast_of(pollutant: str):
        pairs = ((ts, comps.get(pollutant)) for ts, comps in series.items())
        return nowcast(align_hourly_window(ts_epoch, pairs, NOWCAST_WINDOW_HOURS))

    return iaqi_hour(
        {
            "o3": current.get("o3"),
            "no2": current.get("no2"),
            "so2": current.get("so2"),
            "co": current.get("co"),
            "pm2_5": nowcast_of("pm2_5"),
            "pm10": nowcast_of("pm10"),
        },
        standard,
    )


def process_records(records: list[dict], table, standard: str = "VN_1459", batch_size: int = 1000) -> dict:
    """Danh sách bản ghi C1 đã parse -> tính AQI -> ghi HBase. Trả thống kê."""
    limits = max_plausible(standard)

    # cùng (trạm, giờ) xuất hiện nhiều lần (live poll chồng nhau) -> giữ bản fetch mới nhất
    latest: dict[tuple, dict] = {}
    for record in records:
        key = (record["station_id"], record["ts_epoch"])
        current = latest.get(key)
        if current is None or (record.get("fetched_at_utc") or "") >= (current.get("fetched_at_utc") or ""):
            latest[key] = record

    by_station: dict[str, list[dict]] = defaultdict(list)
    for (station_id, _), record in latest.items():
        by_station[station_id].append(record)

    out_rows = []
    for station_id, station_records in by_station.items():
        station_records.sort(key=lambda r: r["ts_epoch"])
        first, last = station_records[0]["ts_epoch"], station_records[-1]["ts_epoch"]

        series = dict(hbase_sink.read_pm_history(table, station_id, first - hbase_sink.LOOKBACK_HOURS * 3600, last))
        cleaned = {r["ts_epoch"]: clean_components(r["components"], limits) for r in station_records}
        series.update(cleaned)

        for record in station_records:
            ts = record["ts_epoch"]
            result = compute_hour(series, ts, standard)
            out_rows.append({
                "station_id": station_id,
                "city": record["city"],
                "country": record["country"],
                "lat": record["lat"],
                "lon": record["lon"],
                "ts_epoch": ts,
                **cleaned[ts],
                "aqi": result["aqi"],
                "aqi_level": result["aqi_level"],
                "aqi_label": result["aqi_label"],
                "dominant_pollutant": result["dominant_pollutant"],
                "standard": standard,
            })

    written = hbase_sink.put_rows(table, out_rows, batch_size)
    return {"received": len(records), "distinct": len(latest), "written": written}


# ---------------------------------------------------------------------------
# Phần Spark: chỉ là lớp mỏng quanh logic thuần ở trên
# ---------------------------------------------------------------------------
def _utc_day(ts_epoch: int) -> str:
    return datetime.fromtimestamp(ts_epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def write_live_raw(spark, good: list[tuple[str, dict]], hdfs_raw: str) -> None:
    """Giữ nguyên dòng JSON C1 gốc, xếp theo layout C2: ingest_mode=live/country=/dt=."""
    if not good:
        return
    rows = [(rec["country"], _utc_day(rec["ts_epoch"]), raw) for raw, rec in good]
    (
        spark.createDataFrame(rows, "country string, dt string, value string")
        .write.mode("append")
        .option("compression", "gzip")
        .partitionBy("country", "dt")
        .text(f"{hdfs_raw.rstrip('/')}/ingest_mode=live")
    )


def write_dlq(spark, bad: list[tuple], bootstrap: str, topic: str) -> None:
    if not bad:
        return
    rows = [
        (key, json.dumps({"error": error, "station_id": key, "raw_data": raw}, ensure_ascii=False))
        for key, raw, error in bad
    ]
    (
        spark.createDataFrame(rows, "key string, value string")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", topic)
        .save()
    )


def make_batch_handler(spark, args):
    from pyspark.sql import functions as F

    standard = os.environ.get("AQI_STANDARD", "VN_1459")

    def handle(batch_df, batch_id):
        rows = batch_df.select(
            F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value")
        ).collect()
        if not rows:
            return

        good, bad = [], []
        for row in rows:
            try:
                good.append((row["value"], parse_record(row["value"])))
            except InvalidRecord as exc:
                bad.append((row["key"], row["value"], str(exc)))

        connection = hbase_sink.connect(args.hbase_host, args.hbase_port)
        try:
            stats = process_records(
                [record for _, record in good], connection.table(args.hbase_table), standard, args.batch_size
            )
        finally:
            connection.close()

        write_live_raw(spark, good, args.hdfs_raw)
        write_dlq(spark, bad, args.kafka_bootstrap, args.dlq_topic)
        logger.info("batch %s: %s, dlq=%d", batch_id, stats, len(bad))

    return handle


def main():
    hdfs_uri = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092"),
                    help="trong Docker network dùng kafka:19092 (localhost:9092 chỉ đúng từ máy host)")
    ap.add_argument("--topic", default="air-quality-raw")
    ap.add_argument("--dlq-topic", default="air-quality-dlq")
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--hbase-host", default=os.environ.get("HBASE_THRIFT_HOST", "hbase-thrift"))
    ap.add_argument("--hbase-port", type=int, default=int(os.environ.get("HBASE_THRIFT_PORT", "9090")))
    ap.add_argument("--hbase-table", default=os.environ.get("HBASE_TABLE", hbase_sink.TABLE))
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--hdfs-raw", default=f"{hdfs_uri}/air-quality/raw")
    ap.add_argument("--checkpoint", default=f"{hdfs_uri}/air-quality/_checkpoints/streaming_aqi")
    ap.add_argument("--trigger-seconds", type=int, default=30)
    ap.add_argument("--once", action="store_true", help="xử lý hết dữ liệu hiện có rồi thoát")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("streaming_aqi")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    writer = (
        stream.writeStream.foreachBatch(make_batch_handler(spark, args))
        .option("checkpointLocation", args.checkpoint)
    )
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(
        processingTime=f"{args.trigger_seconds} seconds"
    )
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
