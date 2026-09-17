"""
Unit test cho phase3_aggregate.py — NGƯỜI B.

Test compute_daily()/compute_ranking() trên DataFrame nhỏ dựng tay, đối chiếu với
các quyết định thiết kế đã chốt: điểm xếp hạng = avg_aqi, rank 1 = ô nhiễm nhất,
worst_pollutant = dominant_pollutant xuất hiện nhiều giờ nhất.
"""
import pytest

from phase3_aggregate import compute_daily, compute_ranking

AQI_SCHEMA = "city string, country string, dt string, aqi double, aqi_level int, dominant_pollutant string"


def test_compute_daily_trung_binh_max_min_dung(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", "VN", "2026-06-15", 10.0, 1, "o3"),
            ("Hanoi", "VN", "2026-06-15", 20.0, 1, "o3"),
        ],
        schema=AQI_SCHEMA,
    )
    out = compute_daily(df).collect()[0]
    assert out["avg_aqi"] == 15.0
    assert out["max_aqi"] == 20.0
    assert out["min_aqi"] == 10.0
    assert out["n_hours"] == 2


def test_compute_daily_dem_dung_phan_bo_muc(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", "VN", "2026-06-15", 10.0, 1, "o3"),
            ("Hanoi", "VN", "2026-06-15", 60.0, 2, "o3"),
            ("Hanoi", "VN", "2026-06-15", 60.0, 2, "o3"),
        ],
        schema=AQI_SCHEMA,
    )
    out = compute_daily(df).collect()[0]
    assert out["count_level_1"] == 1
    assert out["count_level_2"] == 2
    assert out["count_level_3"] == 0
    assert out["count_level_1"] + out["count_level_2"] == out["n_hours"]


def test_compute_daily_worst_pollutant_la_mode(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", "VN", "2026-06-15", 10.0, 1, "pm2_5"),
            ("Hanoi", "VN", "2026-06-15", 20.0, 1, "pm2_5"),
            ("Hanoi", "VN", "2026-06-15", 15.0, 1, "o3"),
        ],
        schema=AQI_SCHEMA,
    )
    out = compute_daily(df).collect()[0]
    assert out["worst_pollutant"] == "pm2_5"


def test_compute_daily_thieu_du_lieu_van_giu_dong_null(spark):
    # aqi=None cho ca ngay -> avg/max/min phai la None, KHONG bi loai khoi output
    df = spark.createDataFrame([("Hanoi", "VN", "2026-06-15", None, None, None)], schema=AQI_SCHEMA)
    out = compute_daily(df).collect()[0]
    assert out["avg_aqi"] is None
    assert out["n_hours"] == 0


def test_compute_ranking_rank1_la_o_nhiem_nhat(spark):
    daily_schema = "city string, country string, dt string, avg_aqi double, max_aqi double, worst_pollutant string"
    daily = spark.createDataFrame(
        [
            ("City_Sach", "VN", "2026-06-15", 50.0, 60.0, "o3"),
            ("City_O_Nhiem", "IN", "2026-06-15", 150.0, 200.0, "pm2_5"),
        ],
        schema=daily_schema,
    )
    out = {r["city"]: r["rank"] for r in compute_ranking(daily).collect()}
    assert out["City_O_Nhiem"] == 1
    assert out["City_Sach"] == 2


def test_compute_ranking_doc_lap_theo_tung_ngay(spark):
    daily_schema = "city string, country string, dt string, avg_aqi double, max_aqi double, worst_pollutant string"
    daily = spark.createDataFrame(
        [
            ("A", "VN", "2026-06-15", 100.0, 100.0, "o3"),
            ("B", "VN", "2026-06-15", 50.0, 50.0, "o3"),
            ("A", "VN", "2026-06-16", 10.0, 10.0, "o3"),
            ("B", "VN", "2026-06-16", 90.0, 90.0, "o3"),
        ],
        schema=daily_schema,
    )
    ranking = {(r["city"], r["dt"]): r["rank"] for r in compute_ranking(daily).collect()}
    assert ranking[("A", "2026-06-15")] == 1  # ngay 15: A o nhiem hon
    assert ranking[("B", "2026-06-16")] == 1  # ngay 16: B o nhiem hon -> rank tinh doc lap tung ngay
