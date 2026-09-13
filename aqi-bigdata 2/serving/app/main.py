"""
Query/API Bridge — FastAPI đọc HBase, trả JSON cho Grafana. NGƯỜI A · M3

Grafana KHÔNG nối thẳng HBase (theo Chương 5 Thiết kế v1). Lớp này tách dashboard
khỏi chi tiết HBase và chuẩn hoá response.

5 endpoint bắt buộc — schema chốt ở CONTRACTS.md §C5:
  GET /health
  GET /stations?country=
  GET /aqi/latest?country=&limit=
  GET /aqi/timeseries?station_id=&from=&to=
  GET /aqi/ranking?dt=&country=

TODO(A):
  [ ] happybase connection pool (đừng mở connection mỗi request)
  [ ] /aqi/timeseries: scan theo prefix row key {station_id}# — nhanh nhờ thiết kế row key ở C4
  [ ] cache 60s cho /aqi/latest và /aqi/ranking (Grafana refresh liên tục)
  [ ] CORS cho Grafana
  [ ] timestamp trả về LUÔN là ISO-8601 UTC
"""
from fastapi import FastAPI

app = FastAPI(title="AQI Query Bridge")


@app.get("/health")
def health():
    return {"status": "ok"}
