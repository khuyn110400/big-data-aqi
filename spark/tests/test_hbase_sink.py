"""Unit test cho sinks/hbase_sink.py.

Các test này chốt hợp đồng với FastAPI (serving/app/main.py): sai một ký tự trong row key hoặc
tên cột thì API đọc ra rỗng mà không báo lỗi. Vì vậy các giá trị mẫu dưới đây lấy từ
CONTRACTS.md (C4) và main.py."""
import calendar
from datetime import datetime, timezone

import pytest

from sinks import hbase_sink as hs
from tests.fake_hbase import FakeTable

BASE = 1_789_023_600  # = 2026-09-10T07:00:00Z (ví dụ C1 ghi nhầm ngày 13/09 cho epoch này)


def _record(**over):
    rec = {
        "station_id": "VN_HCM_01", "city": "Ho Chi Minh City", "country": "VN",
        "lat": 10.8231, "lon": 106.6297, "ts_epoch": BASE,
        "pm2_5": 14.4, "pm10": 18.14, "o3": 19.73, "no2": 8.68, "so2": 1.4, "co": 498.18,
        "aqi": 57.0, "aqi_level": 2, "aqi_label": "Trung bình",
        "dominant_pollutant": "pm2_5", "standard": "VN_1459",
    }
    rec.update(over)
    return rec


def test_row_key_dung_vi_du_hop_dong():
    # 9999999999 - 1789023600 = 8210976399
    assert hs.row_key("VN_HCM_01", BASE) == b"VN_HCM_01#8210976399"


def test_row_key_ban_ghi_moi_hon_nam_truoc():
    assert hs.row_key("S1", BASE + 3600) < hs.row_key("S1", BASE)


def test_epoch_from_key_la_nghich_dao_cua_row_key():
    assert hs.epoch_from_key(hs.row_key("VN_HCM_01", BASE)) == BASE


def test_to_hbase_row_du_16_cot_dung_ten_va_dinh_dang():
    key, data = hs.to_hbase_row(_record())
    assert key == b"VN_HCM_01#8210976399"
    assert set(data) == {
        b"d:city", b"d:country", b"d:lat", b"d:lon", b"d:ts_utc",
        b"d:pm25", b"d:pm10", b"d:o3", b"d:no2", b"d:so2", b"d:co",
        b"d:aqi", b"d:level", b"d:label", b"d:dom", b"d:std",
    }
    assert data[b"d:pm25"] == b"14.4"
    assert data[b"d:aqi"] == b"57"  # 57.0 -> "57", FastAPI as_number() ra int
    assert data[b"d:level"] == b"2"
    assert data[b"d:ts_utc"] == b"2026-09-10T07:00:00Z"
    assert data[b"d:label"] == "Trung bình".encode("utf-8")
    assert data[b"d:dom"] == b"pm2_5"


def test_to_hbase_row_bo_qua_cot_none_va_nan():
    _, data = hs.to_hbase_row(_record(o3=None, no2=float("nan")))
    assert b"d:o3" not in data and b"d:no2" not in data
    assert b"d:pm25" in data


def test_epoch_tu_ts_utc_chuoi_iso():
    rec = _record(ts_epoch=None, ts_utc="2026-09-13T07:00:00Z")
    assert hs.epoch_of(rec) == calendar.timegm((2026, 9, 13, 7, 0, 0))


def test_epoch_tu_datetime_co_mui_gio():
    rec = _record(ts_epoch=None, ts_utc=datetime(2026, 9, 13, 7, 0, tzinfo=timezone.utc))
    assert hs.epoch_of(rec) == calendar.timegm((2026, 9, 13, 7, 0, 0))


def test_epoch_tu_dong_datetime_naive_phai_bao_loi():
    # PySpark trả TimestampType thành datetime naive theo múi giờ máy -> không được đoán UTC
    rec = _record(ts_epoch=None, ts_utc=datetime(2026, 9, 13, 7, 0))
    with pytest.raises(ValueError):
        hs.epoch_of(rec)


@pytest.mark.parametrize("aqi", [None, float("nan")])
def test_is_writable_false_khi_khong_co_aqi(aqi):
    assert hs.is_writable(_record(aqi=aqi)) is False


def test_put_rows_bo_qua_ban_ghi_khong_aqi_va_tra_so_dong_da_ghi():
    table = FakeTable()
    records = [_record(), _record(ts_epoch=BASE + 3600, aqi=None)]
    assert hs.put_rows(table, records) == 1
    assert list(table.rows) == [hs.row_key("VN_HCM_01", BASE)]


def test_key_range_phu_dung_cac_gio_trong_khoang():
    table = FakeTable()
    hs.put_rows(table, [_record(ts_epoch=BASE + h * 3600) for h in range(-2, 5)])  # 7 giờ
    # lấy [BASE, BASE+2h] -> đúng 3 giờ, cả hai đầu mút đều được gồm
    start, stop = hs.key_range("VN_HCM_01", BASE, BASE + 2 * 3600)
    got = sorted(hs.epoch_from_key(k) for k, _ in table.scan(row_start=start, row_stop=stop))
    assert got == [BASE, BASE + 3600, BASE + 2 * 3600]


def test_key_range_khong_lan_sang_tram_khac():
    table = FakeTable()
    hs.put_rows(table, [_record(station_id="S1"), _record(station_id="S2")])
    start, stop = hs.key_range("S1", BASE, BASE)
    assert [k for k, _ in table.scan(row_start=start, row_stop=stop)] == [hs.row_key("S1", BASE)]


def test_read_pm_history_tra_dung_gio_va_gia_tri():
    table = FakeTable()
    hs.put_rows(table, [_record(ts_epoch=BASE - h * 3600, pm2_5=10.0 + h, pm10=None) for h in range(5)])
    hist = hs.read_pm_history(table, "VN_HCM_01", BASE - 3 * 3600, BASE)
    assert sorted(hist) == [BASE - 3 * 3600, BASE - 2 * 3600, BASE - 3600, BASE]
    assert hist[BASE - 2 * 3600] == {"pm2_5": 12.0, "pm10": None}
