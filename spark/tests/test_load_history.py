"""Test load_history_to_hbase.py.

select_history() chạy trên parquet dựng đúng layout của Pha 2 (partitionBy country, dt);
write_partition() chạy với bảng HBase giả."""
from pyspark.sql import Row
from pyspark.sql import functions as F

from load_history_to_hbase import select_history, write_partition
from sinks import hbase_sink as hs
from tests.fake_hbase import FakeTable

DAY = 86_400
D1 = 1_788_998_400  # 2026-09-10T00:00:00Z
D2 = D1 + DAY       # 2026-09-11
D3 = D1 + 2 * DAY   # 2026-09-12

SCHEMA = (
    "station_id string, city string, country string, lat double, lon double, ts_epoch long, "
    "pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double, "
    "aqi double, aqi_level int, aqi_label string, dominant_pollutant string, standard string"
)


def _row(station, country, epoch, aqi):
    return (station, "City", country, 10.0, 106.0, epoch, 14.4, 18.0, 20.0, 8.0, 1.0, 300.0,
            aqi, 2, "Trung bình", "pm2_5", "VN_1459")


def _write_pha2_parquet(spark, tmp_path, rows):
    df = spark.createDataFrame(rows, schema=SCHEMA)
    # ts_utc dựng từ epoch -> đúng thời điểm bất kể múi giờ máy; dt tính theo UTC (session tz)
    df = df.withColumn("ts_utc", F.col("ts_epoch").cast("timestamp")).drop("ts_epoch")
    df = df.withColumn("dt", F.to_date("ts_utc"))
    out = str(tmp_path / "aqi")
    df.write.partitionBy("country", "dt").parquet(out)
    return out


def test_select_history_loc_theo_since_va_bo_dong_khong_co_aqi(spark, tmp_path):
    path = _write_pha2_parquet(spark, tmp_path, [
        _row("VN_A", "VN", D1 + 3600, 50.0),   # trước --since
        _row("VN_A", "VN", D2 + 3600, 60.0),
        _row("VN_A", "VN", D2 + 7200, None),   # không có aqi -> bỏ
        _row("IN_B", "IN", D3 + 3600, 70.0),
    ])
    out = select_history(spark, path, since="2026-09-11").collect()
    assert sorted((r["station_id"], r["ts_epoch"]) for r in out) == [
        ("IN_B", D3 + 3600), ("VN_A", D2 + 3600),
    ]


def test_select_history_ts_epoch_khop_thoi_diem_goc(spark, tmp_path):
    path = _write_pha2_parquet(spark, tmp_path, [_row("VN_A", "VN", D2 + 5 * 3600, 60.0)])
    (row,) = select_history(spark, path).collect()
    assert row["ts_epoch"] == D2 + 5 * 3600


def test_select_history_giu_du_cot_cho_hbase(spark, tmp_path):
    path = _write_pha2_parquet(spark, tmp_path, [_row("VN_A", "VN", D2, 60.0)])
    (row,) = select_history(spark, path).collect()
    key, data = hs.to_hbase_row(row.asDict())
    assert key == hs.row_key("VN_A", D2)
    assert len(data) == 16  # đủ cả 16 cột C4


class _FakeConnection:
    def __init__(self, table):
        self._table = table
        self.closed = False

    def table(self, name):
        return self._table

    def close(self):
        self.closed = True


def test_write_partition_ghi_dung_va_dong_ket_noi():
    table = FakeTable()
    conn = _FakeConnection(table)
    rows = [Row(**dict(zip(SCHEMA_FIELDS, _row("VN_A", "VN", D2, 60.0)))),
            Row(**dict(zip(SCHEMA_FIELDS, _row("VN_A", "VN", D2 + 3600, None))))]

    n = write_partition(rows, "h", 9090, "air_quality", 100, connect=lambda host, port: conn)

    assert n == 1  # dòng không có aqi không được ghi
    assert list(table.rows) == [hs.row_key("VN_A", D2)]
    assert conn.closed


SCHEMA_FIELDS = [
    "station_id", "city", "country", "lat", "lon", "ts_epoch",
    "pm2_5", "pm10", "o3", "no2", "so2", "co",
    "aqi", "aqi_level", "aqi_label", "dominant_pollutant", "standard",
]
