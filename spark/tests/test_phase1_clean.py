"""
Unit test cho phase1_clean.py — NGƯỜI B.

Test từng hàm riêng lẻ trên DataFrame nhỏ dựng tay, không chạy toàn bộ job trên
data/samples/ (việc đó đã verify thủ công qua phase1_dev.ipynb + chạy CLI).
"""
import pytest
from pyspark.sql import functions as F

from phase1_clean import (
    _max_plausible,
    add_day_aggregates,
    build_hourly_grid,
    clip_outliers,
    interpolate_short_gaps,
)

BASE_TS = 1_800_000_000  # moc epoch bat ky, chia het cho 3600 de don gian


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
    assert out[_h(0)] is None  # vuot tran -> null
    assert out[_h(1)] == cap  # dung bang tran -> van giu (khong loai)


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
    assert grid[1]["pm2_5"] is None and grid[2]["pm2_5"] is None  # 2 gio giua bi lo ra la thieu
    assert grid[0]["city"] == "City"  # metadata tram duoc gan cho ca gio thieu


def test_interpolate_gap_ngan_duoc_noi_suy(spark):
    # thieu dung 1 gio (h=1) giua 2 diem do -> phai noi suy tuyen tinh
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
    # thieu 4 gio lien tiep (h=1..4) -> khong noi suy, giu null + qc=missing
    rows = [("S1", _h(0), 0.0, None, None, None, None, None)]
    rows += [("S1", _h(h), None, None, None, None, None, None) for h in range(1, 5)]
    rows += [("S1", _h(5), 20.0, None, None, None, None, None)]
    df = spark.createDataFrame(rows, schema=POLLUTANT_SCHEMA)
    out = {r["ts_epoch"]: r for r in interpolate_short_gaps(df).collect()}
    for h in range(1, 5):
        assert out[_h(h)]["pm2_5_qc"] == "missing"
        assert out[_h(h)]["pm2_5"] is None


def test_add_day_aggregates_ranh_gioi_01h_den_00h(spark):
    # QD 1459 muc 2.2.2a: ngay AQI la khoi 01:00 -> 00:00 hom sau, LECH so voi ngay duong lich
    schema = "station_id string, ts_epoch long, pm2_5 double, pm10 double"
    ts_0100 = BASE_TS - 7 * 3600  # BASE_TS la 08:00 UTC (da kiem chung) -> tru 7h ra dung 01:00 UTC
    ts_0000_next = ts_0100 + 23 * 3600  # 00:00 hom sau, VAN thuoc cung 1 aqi_day voi 01:00
    df = spark.createDataFrame(
        [("S1", ts_0100, 10.0, 10.0), ("S1", ts_0000_next, 30.0, 30.0)], schema=schema
    )
    out = add_day_aggregates(df).collect()
    aqi_days = {r["aqi_day"] for r in out}
    assert len(aqi_days) == 1, "01:00 va 00:00 hom sau phai cung 1 aqi_day theo QD 1459"


def test_add_day_aggregates_flag_khong_du_du_lieu(spark):
    schema = "station_id string, ts_epoch long, pm2_5 double, pm10 double"
    # chi 1 gio co du lieu trong ca ngay -> chac chan duoi 75%
    df = spark.createDataFrame([("S1", _h(0), 10.0, 10.0)], schema=schema)
    out = add_day_aggregates(df).collect()[0]
    assert out["day_qc_flag"] == "insufficient_data"
