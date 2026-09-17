"""
Unit test cho aqi_core — NGƯỜI B viết ở M1.

Toàn bộ số kỳ vọng lấy trực tiếp từ QĐ 1459/QĐ-TCMT (mục 2.3 — Ví dụ tính mẫu).
Đây là bằng chứng duy nhất chứng minh công thức AQI của đồ án đúng.
"""
import pytest
from aqi_core.iaqi import align_hourly_window, iaqi, iaqi_day, iaqi_hour, level_of, nowcast


def test_iaqi_dung_moc_duoi():
    # PM2.5 = 0 -> IAQI = 0
    assert iaqi(0, "pm2_5") == 0


def test_iaqi_dung_moc_tren_khoang_1():
    # PM2.5 = 25 (mốc trên khoảng đầu) -> IAQI = 50
    assert iaqi(25, "pm2_5") == pytest.approx(50, abs=0.5)


def test_iaqi_noi_suy_giua_khoang():
    # O3(1h) = 136.1 -> IAQI = (50-0)/(160-0)*(136.1-0)+0 = 42.53125 (QĐ 1459 mục 2.3b)
    assert iaqi(136.1, "o3_1h") == pytest.approx(42.53125, abs=0.01)


def test_thieu_du_lieu_tra_none():
    assert iaqi_hour({})["aqi"] is None


def test_khong_duoc_dung_owm_aqi():
    """Chốt chặn: owm_aqi (1-5) không bao giờ được coi là AQI của đồ án."""
    out = iaqi_hour({"pm2_5": 80})
    assert out["aqi"] > 5


# ---------------------------------------------------------------------------
# Nowcast — mục 2.2.1a / 2.3a
# ---------------------------------------------------------------------------

def test_nowcast_doi_chieu_muc_2_3a():
    # Bảng gốc liệt kê PM2.5 từ 09:00 -> 20:00; c1 (hiện tại) = 20:00 nên phải đảo ngược.
    pm25_09h_to_20h = [26.9, 24.7, 20.5, 23.5, 19.5, 16.5, 19.0, 16.5, 20.3, 22.4, 19.6, 20.6]
    c1_to_c12 = list(reversed(pm25_09h_to_20h))
    assert nowcast(c1_to_c12) == pytest.approx(20.3, abs=0.1)


def test_nowcast_thieu_du_lieu_tra_none():
    assert nowcast([None, None, 10.0] + [None] * 9) is None


# ---------------------------------------------------------------------------
# AQI giờ — Công thức 1/2, mục 2.2.1b / ví dụ 2.3b
#
# LƯU Ý: văn bản gốc QĐ 1459 ghi AQI(NO2)=60, nhưng đúng công thức 1:
#   (100-50)/(200-100)*(118.7-100)+50 = 59.35 -> làm tròn chuẩn = 59, không phải 60.
# Đã đối chiếu chéo với ví dụ AQI ngày (2.3c, NO2=130.8 -> 65.4 -> văn bản ghi đúng 65)
# để loại trừ khả năng VN_AQI dùng luật làm tròn lên (ceiling): nếu có, ví dụ ngày
# phải ra 66, nhưng văn bản ghi 65. Kết luận: ví dụ giờ (2.3b) là lỗi số học trong
# chính văn bản gốc — test dưới đây theo ĐÚNG công thức (59), không ép theo số 60.
# ---------------------------------------------------------------------------

def test_iaqi_hour_doi_chieu_muc_2_3b():
    out = iaqi_hour({"o3": 136.1, "no2": 118.7, "pm2_5": 20.3})
    assert out["iaqi"]["o3"] == pytest.approx(43, abs=0.5)
    assert out["iaqi"]["no2"] == pytest.approx(59.35, abs=0.01)
    assert out["iaqi"]["pm2_5"] == pytest.approx(41, abs=0.5)
    assert out["aqi"] == 59
    assert out["dominant_pollutant"] == "no2"


# ---------------------------------------------------------------------------
# AQI ngày — Công thức 1, mục 2.2.2b / ví dụ 2.3c
# ---------------------------------------------------------------------------

def test_iaqi_day_doi_chieu_muc_2_3c():
    out = iaqi_day({
        "no2": 130.8,
        "pm2_5": 55.7,
        "o3_1h_max": 114.6,
        "o3_8h_max": 89.3,
    })
    assert out["iaqi"]["no2"] == pytest.approx(65.4, abs=0.01)
    assert out["iaqi"]["pm2_5"] == pytest.approx(110, abs=0.5)
    assert out["iaqi"]["o3"] == pytest.approx(45, abs=0.5)  # max(O3 8h=45, O3 1h=36)
    assert out["aqi"] == 110
    assert out["dominant_pollutant"] == "pm2_5"


def test_iaqi_day_o3_bo_qua_8h_khi_vuot_400():
    # TB8h ngày > 400 -> theo mục 2.2.2b phải bỏ nhánh 8h, chỉ dùng O3(1h)
    out_khi_vuot = iaqi_day({"o3_1h_max": 114.6, "o3_8h_max": 450})
    out_thieu_8h = iaqi_day({"o3_1h_max": 114.6, "o3_8h_max": None})
    assert out_khi_vuot["iaqi"]["o3"] == out_thieu_8h["iaqi"]["o3"]
    assert out_khi_vuot["iaqi"]["o3"] == pytest.approx(iaqi(114.6, "o3_1h"), abs=0.001)


def test_level_of_dung_bang_hien_thi():
    assert level_of(30) == {"level": 1, "label": "Tốt"}
    assert level_of(75) == {"level": 2, "label": "Trung bình"}
    assert level_of(450) == {"level": 6, "label": "Nguy hại"}


# ---------------------------------------------------------------------------
# align_hourly_window() — dùng chung giữa Pha 2 (batch) và streaming_aqi.py
# ---------------------------------------------------------------------------

def test_align_hourly_window_dung_vi_tri():
    current_ts = 1_800_000_000
    pairs = [
        (current_ts, 10.0),                 # gio hien tai -> offset 0
        (current_ts - 3600, 9.0),           # 1 gio truoc -> offset 1
        (current_ts - 5 * 3600, 5.0),       # 5 gio truoc -> offset 5
    ]
    out = align_hourly_window(current_ts, pairs)
    assert out[0] == 10.0
    assert out[1] == 9.0
    assert out[5] == 5.0
    assert out[2] is None and out[3] is None and out[4] is None
    assert len(out) == 12


def test_align_hourly_window_bo_qua_ngoai_cua_so():
    current_ts = 1_800_000_000
    pairs = [(current_ts - 20 * 3600, 99.0)]  # 20 gio truoc -> ngoai window 12h
    out = align_hourly_window(current_ts, pairs)
    assert all(v is None for v in out)


def test_align_hourly_window_ket_qua_giong_het_nowcast_truc_tiep():
    # dam bao align_hourly_window() + nowcast() cho cung ket qua nhu goi nowcast()
    # truc tiep voi mang da xep san -> chinh la dieu can "kiem chung" batch vs streaming
    current_ts = 1_800_000_000
    values_c1_to_c12 = [20.6, 19.6, 22.4, 20.3, 16.5, 19.0, 16.5, 19.5, 23.5, 20.5, 24.7, 26.9]
    pairs = [(current_ts - i * 3600, v) for i, v in enumerate(values_c1_to_c12)]

    aligned = align_hourly_window(current_ts, pairs)
    assert nowcast(aligned) == pytest.approx(nowcast(values_c1_to_c12), abs=1e-9)
