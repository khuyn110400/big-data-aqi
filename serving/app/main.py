"""
Query/API Bridge: FastAPI đọc HBase và trả JSON cho Grafana.

Contract: CONTRACTS.md §C5 (các endpoint /ext/* theo §C8).
"""

from __future__ import annotations

from functools import wraps

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from threading import RLock

import happybase
from thriftpy2.transport.base import TTransportException
from cachetools import TTLCache
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


ROOT = Path(__file__).resolve().parents[2]
CITIES_FILE = ROOT / "collector" / "config" / "cities.json"
EXT_DIR = ROOT / "final_results" / "json"
SAMPLE_DIR = ROOT / "data" / "samples"

CLUSTERS_FILE = EXT_DIR / "ext_clusters.json"
FORECAST_FILE = EXT_DIR / "ext_forecast_backtest.json"

CLUSTERS_SAMPLE = SAMPLE_DIR / "ext_clusters_sample.json"
FORECAST_SAMPLE = SAMPLE_DIR / "ext_forecast_backtest_sample.json"

load_dotenv(ROOT / ".env")

HBASE_HOST = os.getenv("HBASE_THRIFT_HOST", "127.0.0.1")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
HBASE_TABLE = os.getenv("HBASE_TABLE", "air_quality")

REVERSE_TS_MAX = 9_999_999_999


app = FastAPI(
    title="AQI Query Bridge",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


pool = happybase.ConnectionPool(
    size=5,
    host=HBASE_HOST,
    port=HBASE_PORT,
    timeout=5000,
)

latest_cache = TTLCache(maxsize=64, ttl=60)
ranking_cache = TTLCache(maxsize=128, ttl=60)
cache_lock = RLock()


def retry_hbase_transport(func):
    """Thử lại một lần khi HBase Thrift dùng lại socket đã cũ (stale)."""
    @wraps(func)
    def wrapped(*args, **kwargs):
        for attempt in range(2):
            try:
                return func(*args, **kwargs)
            except TTransportException:
                if attempt == 1:
                    raise
        raise RuntimeError("unreachable")

    return wrapped



def load_stations() -> list[dict]:
    with CITIES_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data["stations"]


def station_map() -> dict[str, dict]:
    return {
        row["station_id"]: row
        for row in load_stations()
    }


def decode(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def as_float(value):
    value = decode(value)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_number(value):
    value = as_float(value)
    if value is None:
        return None
    if value.is_integer():
        return int(value)
    return value


def parse_utc(value: str, field: str) -> datetime:
    raw = value.strip()

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{field} phải là ISO-8601 datetime",
        ) from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def reverse_ts(epoch: int) -> int:
    return REVERSE_TS_MAX - epoch


def row_bounds(
    station_id: str,
    start: datetime,
    end: datetime,
) -> tuple[bytes, bytes]:
    """
    HBase scan dùng row_stop exclusive.

    Row key:
      {station_id}#{9999999999-ts_epoch}

    Vì reverse_ts giảm khi thời gian tăng:
      row_start = reverse(end)
      row_stop  = reverse(start) + 1
    """
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())

    start_rev = reverse_ts(end_epoch)
    stop_rev = reverse_ts(start_epoch) + 1

    row_start = (
        f"{station_id}#{start_rev:010d}"
    ).encode("utf-8")

    row_stop = (
        f"{station_id}#{stop_rev:010d}"
    ).encode("utf-8")

    return row_start, row_stop


def row_to_measurement(row: dict) -> dict:
    return {
        "ts_utc": decode(row.get(b"d:ts_utc")),
        "aqi": as_number(row.get(b"d:aqi")),
        "pm2_5": as_float(row.get(b"d:pm25")),
        "pm10": as_float(row.get(b"d:pm10")),
        "o3": as_float(row.get(b"d:o3")),
        "no2": as_float(row.get(b"d:no2")),
        "so2": as_float(row.get(b"d:so2")),
        "co": as_float(row.get(b"d:co")),
        "dominant_pollutant": decode(row.get(b"d:dom")),
    }


@retry_hbase_transport
def scan_station_range(
    station_id: str,
    start: datetime,
    end: datetime,
) -> list[dict]:
    row_start, row_stop = row_bounds(
        station_id,
        start,
        end,
    )

    columns = [
        b"d:ts_utc",
        b"d:aqi",
        b"d:pm25",
        b"d:pm10",
        b"d:o3",
        b"d:no2",
        b"d:so2",
        b"d:co",
        b"d:dom",
    ]

    rows = []

    with pool.connection() as connection:
        table = connection.table(HBASE_TABLE)

        for _, data in table.scan(
            row_start=row_start,
            row_stop=row_stop,
            columns=columns,
        ):
            rows.append(row_to_measurement(data))

    # Nhờ reverse_ts, HBase trả dữ liệu từ mới đến cũ.
    # API timeseries trả theo thứ tự thời gian tăng dần.
    rows.sort(key=lambda x: x["ts_utc"] or "")

    return rows


@retry_hbase_transport
def latest_station(station: dict) -> dict | None:
    station_id = station["station_id"]

    prefix = f"{station_id}#".encode("utf-8")

    columns = [
        b"d:city",
        b"d:country",
        b"d:lat",
        b"d:lon",
        b"d:ts_utc",
        b"d:aqi",
        b"d:level",
        b"d:label",
        b"d:dom",
    ]

    with pool.connection() as connection:
        table = connection.table(HBASE_TABLE)

        scan = table.scan(
            row_prefix=prefix,
            columns=columns,
            limit=1,
        )

        item = next(scan, None)

    if item is None:
        return None

    _, row = item

    return {
        "station_id": station_id,
        "city": decode(row.get(b"d:city")) or station["city"],
        "lat": as_float(row.get(b"d:lat")) or station["lat"],
        "lon": as_float(row.get(b"d:lon")) or station["lon"],
        "ts_utc": decode(row.get(b"d:ts_utc")),
        "aqi": as_number(row.get(b"d:aqi")),
        "aqi_level": as_number(row.get(b"d:level")),
        "aqi_label": decode(row.get(b"d:label")),
        "dominant_pollutant": decode(row.get(b"d:dom")),
    }

def load_extension_json(primary: Path, sample: Path) -> dict:
    path = primary if primary.exists() else sample

    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Extension data not found: {primary.name}",
        )

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Cannot read {path.name}",
        ) from exc

@app.get("/health")
def health():
    # Giữ response đúng CONTRACTS.md §C5.
    return {"status": "ok"}


@app.get("/stations")
def stations(
    country: str | None = Query(
        default=None,
        description="ISO country code, ví dụ VN hoặc IN",
    )
):
    rows = load_stations()

    if country:
        country = country.upper().strip()
        rows = [
            row
            for row in rows
            if row["country"].upper() == country
        ]

    return [
        {
            "station_id": row["station_id"],
            "city": row["city"],
            "country": row["country"],
            "lat": row["lat"],
            "lon": row["lon"],
        }
        for row in rows
    ]


@app.get("/aqi/latest")
def aqi_latest(
    country: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
):
    country_norm = country.upper().strip() if country else None
    cache_key = (country_norm, limit)

    with cache_lock:
        cached = latest_cache.get(cache_key)

    if cached is not None:
        return cached

    stations_data = load_stations()

    if country_norm:
        stations_data = [
            s
            for s in stations_data
            if s["country"].upper() == country_norm
        ]

    result = []

    for station in stations_data:
        row = latest_station(station)
        if row is not None:
            result.append(row)

    result.sort(
        key=lambda x: (
            x["aqi"] is not None,
            x["aqi"] if x["aqi"] is not None else -1,
        ),
        reverse=True,
    )

    result = result[:limit]

    with cache_lock:
        latest_cache[cache_key] = result

    return result


@app.get("/aqi/timeseries")
def aqi_timeseries(
    station_id: str = Query(...),
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    stations_by_id = station_map()

    if station_id not in stations_by_id:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown station_id: {station_id}",
        )

    start = parse_utc(from_, "from")
    end = parse_utc(to, "to")

    if start > end:
        raise HTTPException(
            status_code=400,
            detail="from phải <= to",
        )

    return scan_station_range(
        station_id,
        start,
        end,
    )


@app.get("/aqi/ranking")
def aqi_ranking(
    dt: str = Query(..., description="UTC date YYYY-MM-DD"),
    country: str | None = Query(default=None),
):
    try:
        target_date = date.fromisoformat(dt)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="dt phải là YYYY-MM-DD",
        ) from exc

    country_norm = country.upper().strip() if country else None
    cache_key = (dt, country_norm)

    with cache_lock:
        cached = ranking_cache.get(cache_key)

    if cached is not None:
        return cached

    day_start = datetime.combine(
        target_date,
        time.min,
        tzinfo=timezone.utc,
    )

    day_end = (
        day_start
        + timedelta(days=1)
        - timedelta(seconds=1)
    )

    stations_data = load_stations()

    if country_norm:
        stations_data = [
            s
            for s in stations_data
            if s["country"].upper() == country_norm
        ]

    ranking = []

    for station in stations_data:
        rows = scan_station_range(
            station["station_id"],
            day_start,
            day_end,
        )

        valid = [
            row
            for row in rows
            if row["aqi"] is not None
        ]

        if not valid:
            continue

        avg_aqi = sum(
            float(row["aqi"])
            for row in valid
        ) / len(valid)

        worst_row = max(
            valid,
            key=lambda x: float(x["aqi"]),
        )

        ranking.append({
            "city": station["city"],
            "avg_aqi": round(avg_aqi, 2),
            "max_aqi": worst_row["aqi"],
            "worst_pollutant": (
                worst_row["dominant_pollutant"]
            ),
        })

    ranking.sort(
        key=lambda x: (
            x["avg_aqi"],
            x["max_aqi"],
            x["city"],
        ),
        reverse=True,
    )

    result = [
        {
            "rank": rank,
            **row,
        }
        for rank, row in enumerate(
            ranking,
            start=1,
        )
    ]

    with cache_lock:
        ranking_cache[cache_key] = result

    return result

@app.get("/ext/clusters")
def ext_clusters(
    month: int | None = Query(default=None, ge=1, le=12),
    country: str | None = Query(default=None),
):
    data = load_extension_json(
        CLUSTERS_FILE,
        CLUSTERS_SAMPLE,
    )

    rows = data.get("rows", [])

    if month is not None:
        rows = [
            row for row in rows
            if row.get("month") == month
        ]

    if country:
        country_norm = country.upper().strip()
        rows = [
            row for row in rows
            if str(row.get("country", "")).upper()
            == country_norm
        ]

    return {
        **{
            k: v
            for k, v in data.items()
            if k != "rows"
        },
        "rows": rows,
    }

@app.get("/ext/forecast")
def ext_forecast(
    station_id: str | None = Query(default=None),
):
    data = load_extension_json(
        FORECAST_FILE,
        FORECAST_SAMPLE,
    )

    rows = data.get("rows", [])

    if station_id:
        rows = [
            row for row in rows
            if row.get("station_id") == station_id
        ]

    return {
        **{
            k: v
            for k, v in data.items()
            if k != "rows"
        },
        "rows": rows,
    }
