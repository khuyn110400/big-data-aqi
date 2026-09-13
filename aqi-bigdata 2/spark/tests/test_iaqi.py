"""
Unit test cho aqi_core — NGƯỜI B viết ở M1.

QUAN TRỌNG: phải có ít nhất 3 case tính tay đối chiếu với ví dụ trong QĐ 1459.
Đây là bằng chứng duy nhất chứng minh công thức AQI của đồ án đúng.
"""
import pytest
from aqi_core.iaqi import iaqi, aqi, level_of


def test_iaqi_dung_moc_duoi():
    # PM2.5 = 0 -> IAQI = 0
    assert iaqi(0, "pm2_5") == 0


def test_iaqi_dung_moc_tren_khoang_1():
    # PM2.5 = 25 (mốc trên khoảng đầu) -> IAQI = 50
    assert iaqi(25, "pm2_5") == pytest.approx(50, abs=0.5)


def test_iaqi_noi_suy_giua_khoang():
    # TODO(B): tính tay một giá trị giữa khoảng rồi điền số kỳ vọng
    ...


def test_aqi_lay_max_va_chat_troi():
    out = aqi({"pm2_5": 80, "pm10": 10, "o3": 10})
    assert out["dominant_pollutant"] == "pm2_5"


def test_thieu_du_lieu_tra_none():
    assert aqi({})["aqi"] is None


def test_khong_duoc_dung_owm_aqi():
    """Chốt chặn: owm_aqi (1-5) không bao giờ được coi là AQI của đồ án."""
    out = aqi({"pm2_5": 80})
    assert out["aqi"] > 5
