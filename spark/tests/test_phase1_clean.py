"""Unit test cho phase1_clean.py.

Test từng hàm riêng lẻ trên DataFrame nhỏ dựng tay. Không chạy cả job trên data/samples/ (phần
đó đã kiểm tra thủ công bằng cách chạy CLI trên data/samples/)."""
import pytest
from pyspark.sql import functions as F

from phase1_clean import (
    _max_plausible,
    add_day_aggregates,
    build_hourly_grid,
    clip_outliers,
    interpolate_short_gaps,
)

BASE_TS = 1_800_000_000  # mốc epoch bất kỳ, chia hết cho 3600 cho đơn giản


def _h(n: int) -> int:
    return BASE_TS + n * 3600


POLLUTANT_SCHEMA = "station_id string, ts_epoch long, pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double"


def test_clip_outliers_loai_am(spark):
    df = spark.createDataFrame([("S1", _h(0), -5.0, 10.0, 10.0, 10.0, 10.0, 10.0)], schema=POLLUTANT_SCHEMA)
    out = clip_outliers(df).collect()[0]
    assert out["pm2_5"] is None
    assert out["pm10"] == 10.0


def test_clip_outliers_vuot_tran_thanh_null(spark):
    cap = float(_max_plausible()["pm2_5"])
    df = spark.createDataFrame(
        [("S1", _h(0), cap + 1.0, 10.0, 10.0, 10.0, 10.0, 10.0), ("S1", _h(1), cap, 10.0, 10.0, 10.0, 10.0, 10.0)],
        schema=POLLUTANT_SCHEMA,
    )
    out = {r["ts_epoch"]: r["pm2_5"] for r in clip_outliers(df).collect()}
    assert out[_h(0)] is None  # vượt trần -> null
    assert out[_h(1)] == cap  # đúng bằng trần -> vẫn giữ (không loại)


def test_build_hourly_grid_lo_ra_gio_thieu(spark):
    schema = "station_id string, city string, country string, lat double, lon double, ts_epoch long, pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double"
    df = spark.createDataFrame(
        [
            ("S1", "City", "VN", 1.0, 1.0, _h(0), 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ("S1", "City", "VN", 1.0, 1.0, _h(3), 3.0, 3.0, 3.0, 3.0, 3.0, 3.0),
        ],
        schema=schema,
    )
    grid = build_hourly_grid(df).orderBy("ts_epoch").collect()
    assert [r["ts_epoch"] for r in grid] == [_h(0), _h(1), _h(2), _h(3)]
    assert grid[1]["pm2_5"] is None and grid[2]["pm2_5"] is None  # 2 giờ ở giữa bị lộ ra là thiếu
    assert grid[0]["city"] == "City"  # metadata của trạm được gán cho cả giờ thiếu


def test_interpolate_gap_ngan_duoc_noi_suy(spark):
    # thiếu đúng 1 giờ (h=1) giữa 2 điểm đo thì phải nội suy tuyến tính
    rows = [
        ("S1", _h(0), 0.0, None, None, None, None, None),
        ("S1", _h(1), None, None, None, None, None, None),
        ("S1", _h(2), 10.0, None, None, None, None, None),
    ]
    df = spark.createDataFrame(rows, schema=POLLUTANT_SCHEMA)
    out = {r["ts_epoch"]: r for r in interpolate_short_gaps(df).collect()}
    assert out[_h(1)]["pm2_5_qc"] == "interpolated"
    assert out[_h(1)]["pm2_5"] == pytest.approx(5.0, abs=0.01)


def test_interpolate_gap_dai_giu_null(spark):
    # thiếu 4 giờ liên tiếp (h=1..4) thì không nội suy, giữ null và qc = missing
    rows = [("S1", _h(0), 0.0, None, None, None, None, None)]
    rows += [("S1", _h(h), None, None, None, None, None, None) for h in range(1, 5)]
    rows += [("S1", _h(5), 20.0, None, None, None, None, None)]
    df = spark.createDataFrame(rows, schema=POLLUTANT_SCHEMA)
    out = {r["ts_epoch"]: r for r in interpolate_short_gaps(df).collect()}
    for h in range(1, 5):
        assert out[_h(h)]["pm2_5_qc"] == "missing"
        assert out[_h(h)]["pm2_5"] is None


def test_add_day_aggregates_ranh_gioi_01h_den_00h(spark):
    # QĐ 1459 mục 2.2.2a: ngày AQI là khối 01:00 đến 00:00 hôm sau, lệch so với ngày dương lịch
    schema = "station_id string, ts_epoch long, pm2_5 double, pm10 double"
    ts_0100 = BASE_TS - 7 * 3600  # BASE_TS là 08:00 UTC (đã kiểm chứng) -> trừ 7 giờ ra đúng 01:00 UTC
    ts_0000_next = ts_0100 + 23 * 3600  # 00:00 hôm sau, vẫn thuộc cùng một aqi_day với 01:00
    df = spark.createDataFrame(
        [("S1", ts_0100, 10.0, 10.0), ("S1", ts_0000_next, 30.0, 30.0)], schema=schema
    )
    out = add_day_aggregates(df).collect()
    aqi_days = {r["aqi_day"] for r in out}
    assert len(aqi_days) == 1, "01:00 va 00:00 hom sau phai cung 1 aqi_day theo QD 1459"


def test_add_day_aggregates_flag_khong_du_du_lieu(spark):
    schema = "station_id string, ts_epoch long, pm2_5 double, pm10 double"
    # chỉ 1 giờ có dữ liệu trong cả ngày nên chắc chắn dưới 75%
    df = spark.createDataFrame([("S1", _h(0), 10.0, 10.0)], schema=schema)
    out = add_day_aggregates(df).collect()[0]
    assert out["day_qc_flag"] == "insufficient_data"
