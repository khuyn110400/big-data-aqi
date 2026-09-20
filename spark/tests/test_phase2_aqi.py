"""Unit test cho phase2_aqi.py.

Test add_nowcast() và compute_iaqi_hour() trên DataFrame nhỏ dựng tay, đối chiếu với ví dụ
QĐ 1459 mục 2.3b đã kiểm tra trong test_iaqi.py."""
import pytest

from phase2_aqi import add_nowcast, compute_iaqi_hour

BASE_TS = 1_800_000_000


def _h(n: int) -> int:
    return BASE_TS + n * 3600


IAQI_SCHEMA = "station_id string, ts_epoch long, pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double"


def test_add_nowcast_12_gio_bang_nhau_ra_dung_gia_tri_do(spark):
    # 12 giờ liên tiếp cùng một giá trị thì Nowcast phải ra đúng giá trị đó (trọng số không làm lệch)
    rows = [("S1", _h(h), 20.3, None, None, None, None, None) for h in range(12)]
    df = spark.createDataFrame(rows, schema=IAQI_SCHEMA)
    out = add_nowcast(df, "pm2_5").orderBy("ts_epoch").collect()
    assert out[-1]["pm2_5_nowcast"] == pytest.approx(20.3, abs=0.01)


def test_add_nowcast_thieu_qua_nhieu_tra_none(spark):
    # giờ đầu tiên chỉ có 1 giá trị (c1) trong 3 giá trị đầu, chưa đủ 2 trên 3 nên phải trả None
    df = spark.createDataFrame([("S1", _h(0), 15.0, None, None, None, None, None)], schema=IAQI_SCHEMA)
    out = add_nowcast(df, "pm2_5").collect()[0]
    assert out["pm2_5_nowcast"] is None


def test_compute_iaqi_hour_doi_chieu_muc_2_3b(spark):
    # Nowcast PM2.5 cố định = 20.3 bằng cách cho 12 giờ giống hệt nhau (đơn giản hoá test),
    # giờ cuối cùng thêm O3 và NO2 theo đúng ví dụ 2.3b của QĐ 1459.
    rows = [("S1", _h(h), 20.3, None, None, None, None, None) for h in range(11)]
    rows.append(("S1", _h(11), 20.3, None, 136.1, 118.7, None, None))
    df = spark.createDataFrame(rows, schema=IAQI_SCHEMA)

    out = compute_iaqi_hour(df).orderBy("ts_epoch").collect()[-1]

    assert out["iaqi_o3"] == pytest.approx(43, abs=0.5)
    assert out["iaqi_no2"] == pytest.approx(59.35, abs=0.01)  # xem ghi chú về sai lệch trong test_iaqi.py
    assert out["iaqi_pm2_5"] == pytest.approx(41, abs=0.5)
    assert out["aqi"] == 59
    assert out["dominant_pollutant"] == "no2"


def test_compute_iaqi_hour_thieu_het_du_lieu_tra_null(spark):
    df = spark.createDataFrame([("S1", _h(0), None, None, None, None, None, None)], schema=IAQI_SCHEMA)
    out = compute_iaqi_hour(df).collect()[0]
    assert out["aqi"] is None
    assert out["dominant_pollutant"] is None
