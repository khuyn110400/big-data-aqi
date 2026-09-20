"""Test streaming_aqi.py.

Quan trọng nhất là test_streaming_cung_ket_qua_voi_pha2: cùng một chuỗi giờ đi qua Pha 2 (batch,
Spark) và qua streaming (từng giờ một, đọc lookback từ HBase giả) phải cho cùng AQI. Test này cho
thấy hai làn thật sự dùng chung một lõi tính toán."""
import json

import pytest

from aqi_core.iaqi import iaqi_hour, max_plausible
from sinks import hbase_sink as hs
from streaming_aqi import (
    InvalidRecord,
    clean_components,
    parse_record,
    process_records,
    write_live_raw,
)
from tests.fake_hbase import FakeTable

BASE = 1_788_998_400  # 2026-09-10T00:00:00Z


def _h(n):
    return BASE + n * 3600


def _c1(station="S1", hour=0, fetched="2026-09-10T12:00:00Z", **components):
    comps = {"co": None, "no": None, "no2": None, "o3": None, "so2": None,
             "pm2_5": None, "pm10": None, "nh3": None}
    comps.update(components)
    return {
        "schema_version": "1.0", "station_id": station, "city": "Test City", "country": "VN",
        "lat": 10.8231, "lon": 106.6297, "ts_utc": "x", "ts_epoch": _h(hour), "dt_raw": _h(hour),
        "source": "openweather.air_pollution", "ingest_mode": "live",
        "fetched_at_utc": fetched, "owm_aqi": 2, "components": comps,
    }


def _aqi_map(table, station="S1"):
    """{ts_epoch: aqi} của các dòng đã ghi trong bảng HBase giả."""
    out = {}
    for key, data in table.scan(row_start=f"{station}#".encode(), row_stop=f"{station}$".encode()):
        out[hs.epoch_from_key(key)] = int(data[b"d:aqi"])
    return out


# --- parse_record -----------------------------------------------------------
def test_parse_record_hop_le():
    rec = parse_record(json.dumps(_c1(pm2_5=10.0)))
    assert rec["ts_epoch"] == BASE and rec["components"]["pm2_5"] == 10.0


@pytest.mark.parametrize("raw", [
    "khong phai json",
    "[1, 2]",
    json.dumps({**_c1(), "schema_version": "9.9"}),
    json.dumps({k: v for k, v in _c1().items() if k != "station_id"}),
    json.dumps({**_c1(), "components": None}),
    json.dumps({**_c1(), "ts_epoch": "abc"}),
])
def test_parse_record_sai_schema_bao_loi(raw):
    with pytest.raises(InvalidRecord):
        parse_record(raw)


# --- clean_components -------------------------------------------------------
def test_clean_components_cat_am_va_vuot_tran_dung_chat():
    limits = max_plausible()
    out = clean_components({"pm2_5": -3.0, "pm10": limits["pm10"] + 1, "o3": 50.0, "no2": None,
                            "so2": "x", "co": limits["co"]}, limits)
    assert out == {"pm2_5": None, "pm10": None, "o3": 50.0, "no2": None, "so2": None, "co": float(limits["co"])}


# --- process_records --------------------------------------------------------
def test_process_records_dung_lookback_hbase_de_tinh_nowcast():
    table = FakeTable()
    # 11 giờ trước đó đã có trong HBase (như sau khi nạp lịch sử): pm2_5 = 20.3, đủ để có Nowcast
    hs.put_rows(table, [
        {"station_id": "S1", "ts_epoch": _h(h), "pm2_5": 20.3, "aqi": 50} for h in range(11)
    ])
    stats = process_records([_c1(hour=11, pm2_5=20.3)], table)

    expected = iaqi_hour({"pm2_5": 20.3})["aqi"]
    assert stats["written"] == 1
    assert _aqi_map(table)[_h(11)] == expected


def test_process_records_khong_co_lich_su_thi_nowcast_thieu_khong_ghi_aqi():
    # giờ đầu tiên, chỉ có 1 giá trị PM2.5 -> Nowcast None (cần >= 2 trong 3 giờ gần nhất)
    table = FakeTable()
    stats = process_records([_c1(hour=0, pm2_5=20.3)], table)
    assert stats["written"] == 0 and not table.rows


def test_process_records_cung_batch_nhieu_gio_dung_thu_tu_thoi_gian():
    table = FakeTable()
    records = [_c1(hour=h, pm2_5=15.0 + h, o3=40.0) for h in (2, 0, 1)]  # cố tình đảo thứ tự
    process_records(records, table)
    assert set(_aqi_map(table)) == {_h(0), _h(1), _h(2)}


def test_process_records_trung_lap_giu_ban_fetch_moi_nhat():
    table = FakeTable()
    old = _c1(hour=0, fetched="2026-09-10T01:00:00Z", o3=30.0)
    new = _c1(hour=0, fetched="2026-09-10T02:00:00Z", o3=90.0)
    stats = process_records([new, old], table)  # thứ tự đến không quyết định bản nào thắng
    assert stats == {"received": 2, "distinct": 1, "written": 1}
    assert table.row(hs.row_key("S1", _h(0)))[b"d:o3"] == b"90"


def test_process_records_ngoai_lai_bi_cat_khong_lam_hong_ca_dong():
    limits = max_plausible()
    table = FakeTable()
    process_records([_c1(hour=0, pm2_5=limits["pm2_5"] * 10, o3=60.0)], table)
    row = table.row(hs.row_key("S1", _h(0)))
    assert b"d:pm25" not in row          # pm2_5 phi thực tế -> bỏ
    assert row[b"d:dom"] == b"o3"        # AQI vẫn tính được từ O3
    assert row[b"d:std"] == b"VN_1459"


def test_process_records_ban_ghi_khong_co_du_lieu_khong_ghi():
    table = FakeTable()
    assert process_records([_c1(hour=0)], table)["written"] == 0


# --- parity với Pha 2 ---------------------------------------------------------
def _series(n_hours):
    """Chuỗi giờ có khoảng thiếu ở PM2.5 (2 giờ liền), O3 và PM10 để thử cả các nhánh None."""
    rows = []
    for h in range(n_hours):
        pm25 = None if h in (5, 6) else 8.0 + (h * 7) % 23
        pm10 = None if h == 15 else 12.0 + (h * 5) % 31
        o3 = None if h == 10 else 30.0 + (h * 3) % 40
        rows.append({"hour": h, "pm2_5": pm25, "pm10": pm10, "o3": o3, "no2": 12.0 + h % 7,
                     "so2": 3.0 + h % 4, "co": 300.0 + 20 * (h % 9)})
    return rows


PHASE2_SCHEMA = ("station_id string, ts_epoch long, pm2_5 double, pm10 double, "
                 "o3 double, no2 double, so2 double, co double")


def _phase2_aqi(spark, series):
    from phase2_aqi import compute_iaqi_hour

    rows = [("S1", _h(r["hour"]), r["pm2_5"], r["pm10"], r["o3"], r["no2"], r["so2"], r["co"]) for r in series]
    out = compute_iaqi_hour(spark.createDataFrame(rows, schema=PHASE2_SCHEMA)).collect()
    return {row["ts_epoch"]: int(row["aqi"]) for row in out if row["aqi"] is not None}


def _c1_of(r):
    return _c1(hour=r["hour"], pm2_5=r["pm2_5"], pm10=r["pm10"], o3=r["o3"], no2=r["no2"], so2=r["so2"], co=r["co"])


def test_streaming_cung_ket_qua_voi_pha2_khi_den_tung_gio(spark):
    series = _series(30)
    table = FakeTable()
    for r in series:                       # mô phỏng live: mỗi giờ 1 batch
        process_records([_c1_of(r)], table)
    assert _aqi_map(table) == _phase2_aqi(spark, series)


def test_streaming_cung_ket_qua_voi_pha2_khi_den_cung_luc(spark):
    series = _series(30)
    table = FakeTable()
    process_records([_c1_of(r) for r in series], table)   # mô phỏng bắt kịp: cả chuỗi trong 1 batch
    assert _aqi_map(table) == _phase2_aqi(spark, series)


# --- HDFS raw live ------------------------------------------------------------
def test_write_live_raw_giu_nguyen_dong_json_va_xep_dung_layout(spark, tmp_path):
    raw1 = json.dumps(_c1(hour=0, o3=30.0))
    raw2 = json.dumps(_c1(hour=30, o3=31.0))   # sang ngày UTC hôm sau
    write_live_raw(spark, [(raw1, json.loads(raw1)), (raw2, json.loads(raw2))], str(tmp_path))

    base = tmp_path / "ingest_mode=live" / "country=VN"
    assert sorted(p.name for p in base.iterdir() if p.is_dir()) == ["dt=2026-09-10", "dt=2026-09-11"]

    lines = [r.value for r in spark.read.text(str(base / "dt=2026-09-10")).collect()]
    assert lines == [raw1]                      # dòng gốc còn nguyên, kể cả trường country
