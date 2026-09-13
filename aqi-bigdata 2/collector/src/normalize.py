"""
Chuẩn hoá response OpenWeather -> message schema C1 trong CONTRACTS.md. NGƯỜI A · M1

Đây là hàm QUAN TRỌNG NHẤT của Người A: mọi thứ downstream phụ thuộc vào nó
đúng schema. Sai một tên trường là Spark job của Người B chết.

Response OpenWeather có dạng:
  {"coord": {...}, "list": [{"main": {"aqi": 2},
                             "components": {"co":..., "pm2_5":..., ...},
                             "dt": 1789023600}]}

TODO(A):
  [ ] map sang đúng 100% schema C1 (đọc lại CONTRACTS.md, đừng nhớ theo trí nhớ)
  [ ] ts_utc phải tròn giờ và có hậu tố Z
  [ ] validate: thiếu trường bắt buộc -> đẩy sang topic air-quality-dlq, KHÔNG im lặng bỏ
  [ ] station_id sinh từ config/cities.json, ổn định giữa các lần chạy
"""
SCHEMA_VERSION = "1.0"


def normalize(owm_item: dict, station: dict, ingest_mode: str) -> dict:
    raise NotImplementedError("TODO(A) M1")
