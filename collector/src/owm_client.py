"""
Wrapper gọi OpenWeather Air Pollution API. NGƯỜI A · M1

Endpoint (gói Free, 60 calls/phút · 1.000.000 calls/tháng):
  hiện tại : GET /data/2.5/air_pollution?lat=&lon=&appid=
  lịch sử  : GET /data/2.5/air_pollution/history?lat=&lon=&start=&end=&appid=
  dự báo   : GET /data/2.5/air_pollution/forecast?lat=&lon=&appid=

Lịch sử có từ 27/11/2020, độ phân giải theo giờ, toàn cầu.
start/end là Unix timestamp UTC (giây).

TODO(A):
  [ ] retry với exponential backoff cho 429 / 5xx
  [ ] throttle <= 1 call/giây (an toàn dưới hạn 60/phút)
  [ ] 401 -> báo rõ "key chưa active, chờ 10 phút - 2 tiếng", đừng retry vô hạn
  [ ] log số call đã dùng để không đụng hạn tháng
"""
import os

BASE = "https://api.openweathermap.org/data/2.5/air_pollution"


class OwmClient:
    def __init__(self, api_key: str | None = None, rate_limit_per_sec: float = 1.0):
        self.api_key = api_key or os.environ["OWM_API_KEY"]
        raise NotImplementedError("TODO(A) M1")

    def current(self, lat: float, lon: float) -> dict: ...
    def history(self, lat: float, lon: float, start: int, end: int) -> list[dict]: ...
