"""
Ghi và đọc bảng HBase `air_quality` theo C4 (CONTRACTS.md).

Đây là nơi duy nhất định nghĩa row key và ánh xạ cột, dùng chung cho
  - load_history_to_hbase.py  (nạp lịch sử từ parquet Pha 2)
  - streaming_aqi.py          (làn streaming)
để hai đường ghi không thể lệch nhau. FastAPI (serving/app/main.py) đọc đúng định dạng này:
  row key = {station_id}#{9999999999 - ts_epoch:010d}, giá trị là chuỗi UTF-8.

Module không import pyspark hay happybase ở mức module, để test chạy được mà không cần
HBase và worker Spark chỉ cần happybase khi thật sự mở kết nối.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Iterable, Optional

TABLE = "air_quality"
COLUMN_FAMILY = "d"
REVERSE_TS_MAX = 9_999_999_999
LOOKBACK_HOURS = 11  # Nowcast dùng c1..c12 = giờ hiện tại + 11 giờ trước

# trường trong bản ghi -> qualifier HBase (C4)
COLUMN_MAP = {
    "city": "city",
    "country": "country",
    "lat": "lat",
    "lon": "lon",
    "ts_utc": "ts_utc",
    "pm2_5": "pm25",
    "pm10": "pm10",
    "o3": "o3",
    "no2": "no2",
    "so2": "so2",
    "co": "co",
    "aqi": "aqi",
    "aqi_level": "level",
    "aqi_label": "label",
    "dominant_pollutant": "dom",
    "standard": "std",
}


def row_key(station_id: str, ts_epoch: int) -> bytes:
    return f"{station_id}#{REVERSE_TS_MAX - int(ts_epoch):010d}".encode("utf-8")


def epoch_from_key(key: bytes) -> int:
    return REVERSE_TS_MAX - int(key.rsplit(b"#", 1)[1])


def key_range(station_id: str, start_epoch: int, end_epoch: int) -> tuple[bytes, bytes]:
    """Khoảng row key phủ mọi giờ trong [start_epoch, end_epoch] (row_stop exclusive).
    Cùng công thức với row_bounds() của FastAPI: reverse_ts giảm khi thời gian tăng."""
    start_rev = REVERSE_TS_MAX - int(end_epoch)
    stop_rev = REVERSE_TS_MAX - int(start_epoch) + 1
    return (
        f"{station_id}#{start_rev:010d}".encode("utf-8"),
        f"{station_id}#{stop_rev:010d}".encode("utf-8"),
    )


def epoch_of(record: dict) -> int:
    """Lấy ts_epoch nếu có, không thì suy từ ts_utc.

    ts_utc dạng chuỗi ISO được coi là UTC theo hợp đồng C1. Datetime không kèm múi giờ bị từ
    chối: PySpark trả TimestampType về datetime theo múi giờ của máy chứ không phải UTC, đoán
    là UTC sẽ lệch giờ mà không báo lỗi. Với dữ liệu từ Spark, hãy truyền ts_epoch (cast("long"))."""
    if record.get("ts_epoch") is not None:
        return int(record["ts_epoch"])

    ts = record["ts_utc"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    elif ts.tzinfo is None:
        raise ValueError("ts_utc là datetime naive, không rõ múi giờ — truyền ts_epoch thay vì ts_utc")
    return int(ts.timestamp())


def _fmt(value) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        if value.is_integer():
            value = int(value)
    return str(value).encode("utf-8")


def _to_float(value: Optional[bytes]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value.decode("utf-8"))
    except ValueError:
        return None


def is_writable(record: dict) -> bool:
    """Chỉ ghi bản ghi có AQI. FastAPI /aqi/latest lấy dòng mới nhất theo prefix, nên một
    dòng rỗng sẽ che mất dòng có số."""
    return _fmt(record.get("aqi")) is not None


def to_hbase_row(record: dict) -> tuple[bytes, dict[bytes, bytes]]:
    epoch = epoch_of(record)
    data: dict[bytes, bytes] = {}
    for field, qualifier in COLUMN_MAP.items():
        if field == "ts_utc":
            value = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            value = record.get(field)
        encoded = _fmt(value)
        if encoded is not None:
            data[f"{COLUMN_FAMILY}:{qualifier}".encode("utf-8")] = encoded
    return row_key(record["station_id"], epoch), data


def put_rows(table, records: Iterable[dict], batch_size: int = 1000) -> int:
    """Ghi các bản ghi có AQI vào HBase, trả về số dòng đã ghi. `table` là happybase.Table."""
    written = 0
    with table.batch(batch_size=batch_size) as batch:
        for record in records:
            if not is_writable(record):
                continue
            key, data = to_hbase_row(record)
            batch.put(key, data)
            written += 1
    return written


def read_pm_history(table, station_id: str, start_epoch: int, end_epoch: int) -> dict[int, dict]:
    """{ts_epoch: {"pm2_5": v, "pm10": v}} trong [start_epoch, end_epoch] — dữ liệu đầu vào
    của Nowcast 12 giờ cho làn streaming."""
    row_start, row_stop = key_range(station_id, start_epoch, end_epoch)
    history: dict[int, dict] = {}
    for key, data in table.scan(row_start=row_start, row_stop=row_stop, columns=[b"d:pm25", b"d:pm10"]):
        history[epoch_from_key(key)] = {
            "pm2_5": _to_float(data.get(b"d:pm25")),
            "pm10": _to_float(data.get(b"d:pm10")),
        }
    return history


def connect(host: str, port: int = 9090):
    import happybase

    return happybase.Connection(host=host, port=port, timeout=30_000)
