"""
Client gọi OpenWeather Air Pollution API.

Endpoint (gói Free: 60 call/phút, 1.000.000 call/tháng):
  hiện tại : GET /data/2.5/air_pollution?lat=&lon=&appid=
  lịch sử  : GET /data/2.5/air_pollution/history?lat=&lon=&start=&end=&appid=
  dự báo   : GET /data/2.5/air_pollution/forecast?lat=&lon=&appid=

Dữ liệu lịch sử có từ 27/11/2020, độ phân giải theo giờ. start/end là Unix timestamp UTC (giây).

Client giới hạn tối đa 1 call/giây, retry với exponential backoff khi gặp 429 hoặc 5xx,
báo lỗi rõ khi gặp 401 (key sai hoặc chưa active, thường phải chờ 10 phút đến vài giờ sau
khi tạo key) và ghi log số call đã dùng để theo dõi hạn mức tháng.
"""
import logging
import os
import time

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

BASE = "https://api.openweathermap.org/data/2.5/air_pollution"

logger = logging.getLogger(__name__)


class OwmAuthError(Exception):
    """401 - key sai hoặc chưa active trên OpenWeather. Không nên retry vô hạn."""


class OwmRateLimitError(Exception):
    """429 hoặc 5xx sau khi hết số lần retry cho phép."""


class OwmClient:
    def __init__(self, api_key: str | None = None, rate_limit_per_sec: float = 1.0):
        self.api_key = api_key or os.environ["OWM_API_KEY"]
        self._min_interval = 1.0 / rate_limit_per_sec
        self._last_call_ts = 0.0
        self.call_count = 0

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_call_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.monotonic()

    @retry(
        retry=retry_if_exception_type(OwmRateLimitError),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _get(self, url: str, params: dict) -> dict:
        self._throttle()
        resp = requests.get(url, params={**params, "appid": self.api_key}, timeout=30)
        self.call_count += 1

        if resp.status_code == 401:
            raise OwmAuthError(
                "401 Invalid API key - key mới tạo có thể chưa active. "
                "Chờ 10 phút - 2 tiếng rồi thử lại, ĐỪNG debug code trong lúc chờ."
            )
        if resp.status_code == 429 or resp.status_code >= 500:
            logger.warning(
                "OWM %s -> HTTP %s, sẽ retry (đã dùng %d call)", url, resp.status_code, self.call_count
            )
            raise OwmRateLimitError(f"HTTP {resp.status_code} từ {url}")

        resp.raise_for_status()

        if self.call_count % 100 == 0:
            logger.info("OWM: đã gọi %d call kể từ khi khởi tạo client", self.call_count)

        return resp.json()

    def current(self, lat: float, lon: float) -> dict:
        return self._get(BASE, {"lat": lat, "lon": lon})

    def history(self, lat: float, lon: float, start: int, end: int) -> list[dict]:
        data = self._get(f"{BASE}/history", {"lat": lat, "lon": lon, "start": start, "end": end})
        return data.get("list", [])
