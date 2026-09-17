"""
Chuẩn hoá response OpenWeather -> message schema C1 trong CONTRACTS.md. NGƯỜI A · M1

Đây là hàm QUAN TRỌNG NHẤT của Người A: mọi thứ downstream phụ thuộc vào nó
đúng schema. Sai một tên trường là Spark job của Người B chết.

Response OpenWeather có dạng:
  {"coord": {...}, "list": [{"main": {"aqi": 2},
                             "components": {"co":..., "pm2_5":..., ...},
                             "dt": 1789023600}]}

TODO(A):
  [x] map sang đúng 100% schema C1 (đọc lại CONTRACTS.md, đừng nhớ theo trí nhớ)
  [x] ts_utc phải tròn giờ và có hậu tố Z
  [x] validate: thiếu trường bắt buộc -> đẩy sang topic air-quality-dlq, KHÔNG im lặng bỏ
      (normalize() raise NormalizeError; caller là người bắt lỗi và đẩy DLQ)
  [x] station_id sinh từ config/cities.json, ổn định giữa các lần chạy
      (station_id không sinh ở đây - lấy nguyên từ station dict do caller truyền vào)
"""
from datetime import datetime, timezone

SCHEMA_VERSION = "1.0"

_COMPONENT_KEYS = ("co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3")
_VALID_INGEST_MODES = ("history", "live")


class NormalizeError(Exception):
    """Bản ghi thiếu trường bắt buộc hoặc sai định dạng -> caller đẩy sang DLQ."""


def normalize(owm_item: dict, station: dict, ingest_mode: str) -> dict:
    if ingest_mode not in _VALID_INGEST_MODES:
        raise NormalizeError(f"ingest_mode không hợp lệ: {ingest_mode!r}")

    try:
        dt_raw = int(owm_item["dt"])
        components_raw = owm_item["components"]
        owm_aqi = owm_item["main"]["aqi"]
    except (KeyError, TypeError) as exc:
        raise NormalizeError(f"owm_item thiếu trường bắt buộc: {exc}") from exc

    try:
        station_id = station["station_id"]
        city = station["city"]
        country = station["country"]
        # R2: lat/lon LUÔN lấy từ cities.json, KHÔNG bao giờ lấy từ coord của response
        lat = round(float(station["lat"]), 4)
        lon = round(float(station["lon"]), 4)
    except (KeyError, TypeError) as exc:
        raise NormalizeError(f"station thiếu trường bắt buộc: {exc}") from exc

    # R1: history đã tròn giờ, current thì không -> luôn làm tròn XUỐNG giờ
    # để live và history của cùng một giờ sinh ra cùng ts_epoch (khớp key, dedup đúng).
    ts_epoch = dt_raw - (dt_raw % 3600)
    ts_utc = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    components = {}
    for key in _COMPONENT_KEYS:
        value = components_raw.get(key)
        components[key] = float(value) if value is not None else None

    return {
        "schema_version": SCHEMA_VERSION,
        "station_id": station_id,
        "city": city,
        "country": country,
        "lat": lat,
        "lon": lon,
        "ts_utc": ts_utc,
        "ts_epoch": ts_epoch,
        "dt_raw": dt_raw,
        "source": "openweather.air_pollution",
        "ingest_mode": ingest_mode,
        "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "owm_aqi": owm_aqi,
        "components": components,
    }
