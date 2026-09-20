"""
Chuẩn hoá response của OpenWeather thành bản ghi theo schema C1 (CONTRACTS.md).

Các job Spark phía sau đọc thẳng theo schema này nên tên trường phải khớp tuyệt đối.
Bản ghi thiếu trường bắt buộc làm normalize() raise NormalizeError; nơi gọi chịu trách
nhiệm bắt lỗi và đẩy bản ghi sang topic air-quality-dlq. station_id không được sinh ở
đây mà lấy nguyên từ dict station (collector/config/cities.json).

Response của OpenWeather có dạng:
  {"coord": {...}, "list": [{"main": {"aqi": 2},
                             "components": {"co":..., "pm2_5":..., ...},
                             "dt": 1789023600}]}
"""
from datetime import datetime, timezone

SCHEMA_VERSION = "1.0"

_COMPONENT_KEYS = ("co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3")
_VALID_INGEST_MODES = ("history", "live")


class NormalizeError(Exception):
    """Bản ghi thiếu trường bắt buộc hoặc sai định dạng -> nơi gọi đẩy sang DLQ."""


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
        # lat/lon luôn lấy từ cities.json, không lấy từ coord trong response
        # (API trả toạ độ lệch nhẹ so với toạ độ đã gửi đi)
        lat = round(float(station["lat"]), 4)
        lon = round(float(station["lon"]), 4)
    except (KeyError, TypeError) as exc:
        raise NormalizeError(f"station thiếu trường bắt buộc: {exc}") from exc

    # Endpoint history trả dt đã tròn giờ, endpoint hiện tại thì không. Luôn làm tròn xuống
    # giờ để live và history của cùng một giờ có cùng ts_epoch, nhờ đó dedup và row key
    # HBase khớp nhau.
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
