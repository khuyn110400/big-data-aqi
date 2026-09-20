"""
Sinh file dashboard Grafana grafana/dashboards/aqi-overview.json bằng Python, để dễ chỉnh sửa hơn
so với viết tay JSON dài.

Các panel dùng Infinity datasource gọi FastAPI (http://serving-api:8000): danh sách trạm, bản đồ,
AQI mới nhất, xếp hạng, chuỗi thời gian AQI và nồng độ các chất, phân cụm theo tháng và dự báo AQI
sau 24 giờ.

Chạy từ thư mục gốc repo: python scripts/generate_grafana_dashboard.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path("grafana/dashboards/aqi-overview.json")

DS = {
    "type": "yesoreyeram-infinity-datasource",
    "uid": "aqi-fastapi",
}

BASE = "http://serving-api:8000"


def col(selector, title, type_, timestamp_format=None):
    x = {
        "selector": selector,
        "text": title,
        "type": type_,
    }

    if timestamp_format:
        x["timestampFormat"] = timestamp_format

    return x


def query(ref_id, url, columns, fmt="table", root_selector=""):
    return {
        "refId": ref_id,
        "datasource": DS,
        "type": "json",
        "source": "url",
        "format": fmt,
        "parser": "backend",
        "url": url,
        "url_options": {
            "method": "GET",
            "data": "",
        },
        "root_selector": root_selector,
        "columns": columns,
        "filters": [],
        "computed_columns": [],
        "filterExpression": "",
        "summarizeExpression": "",
        "global_query_id": "",
    }


station_columns = [
    col("station_id", "Station ID", "string"),
    col("city", "City", "string"),
    col("country", "Country", "string"),
    col("lat", "Latitude", "number"),
    col("lon", "Longitude", "number"),
]

latest_columns = [
    col("station_id", "Station ID", "string"),
    col("city", "City", "string"),
    col("lat", "Latitude", "number"),
    col("lon", "Longitude", "number"),
    col(
        "ts_utc",
        "Time",
        "timestamp",
        "2006-01-02T15:04:05Z07:00",
    ),
    col("aqi", "AQI", "number"),
    col("aqi_level", "Level", "number"),
    col("aqi_label", "Label", "string"),
    col(
        "dominant_pollutant",
        "Dominant Pollutant",
        "string",
    ),
]

ranking_columns = [
    col("rank", "Rank", "number"),
    col("city", "City", "string"),
    col("avg_aqi", "Average AQI", "number"),
    col("max_aqi", "Max AQI", "number"),
    col("worst_pollutant", "Worst Pollutant", "string"),
]

aqi_ts_columns = [
    col(
        "ts_utc",
        "Time",
        "timestamp",
        "2006-01-02T15:04:05Z07:00",
    ),
    col("aqi", "AQI", "number"),
]

pollutant_columns = [
    col(
        "ts_utc",
        "Time",
        "timestamp",
        "2006-01-02T15:04:05Z07:00",
    ),
    col("pm2_5", "PM2.5", "number"),
    col("pm10", "PM10", "number"),
    col("o3", "O3", "number"),
    col("no2", "NO2", "number"),
]


cluster_columns = [
    col("city", "City", "string"),
    col("country", "Country", "string"),
    col("month", "Month", "number"),
    col("avg_pm2_5", "Avg PM2.5", "number"),
    col("avg_pm10", "Avg PM10", "number"),
    col("avg_o3", "Avg O3", "number"),
    col("avg_no2", "Avg NO2", "number"),
    col("cluster", "Cluster", "number"),
]

forecast_columns = [
    col(
        "ts_utc",
        "Time",
        "timestamp",
        "2006-01-02T15:04:05Z07:00",
    ),
    col("actual", "Actual AQI", "number"),
    col("sgd", "SGD T1", "number"),
    col("rf", "Random Forest T2", "number"),
    col("cnn_lstm", "CNN-LSTM T3", "number"),
]


dashboard = {
    "id": None,
    "uid": "aqi-big-data-overview",
    "title": "AQI Big Data Overview",
    "tags": [
        "AQI",
        "Big Data",
        "OpenWeather",
        "HBase",
        "FastAPI",
    ],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "1m",
    "editable": True,
    "graphTooltip": 1,
    "time": {
        "from": "now-24h",
        "to": "now",
    },
    "timepicker": {},
    "annotations": {
        "list": [],
    },
    "templating": {
        "list": [
            {
                "name": "country",
                "label": "Country",
                "type": "custom",
                "query": (
                    "VN,IN,CN,JP,US,CA,"
                    "GB,DE,AU,BR,ZA,AE"
                ),
                "current": {
                    "selected": True,
                    "text": "VN",
                    "value": "VN",
                },
                "includeAll": True,
                "allValue": "",
                "multi": False,
                "options": [],
                "hide": 0,
            },
            {
                "name": "station_id",
                "label": "Station ID",
                "type": "textbox",
                "query": "VN_HCM_01",
                "current": {
                    "selected": True,
                    "text": "VN_HCM_01",
                    "value": "VN_HCM_01",
                },
                "hide": 0,
            },
            {
                "name": "ranking_dt",
                "label": "Ranking Date (UTC)",
                "type": "textbox",
                "query": "2026-09-18",
                "current": {
                    "selected": True,
                    "text": "2026-09-18",
                    "value": "2026-09-18",
                },
                "hide": 0,
            },
            {
                "name": "cluster_month",
                "label": "Cluster Month",
                "type": "textbox",
                "query": "8",
                "current": {
                    "selected": True,
                    "text": "8",
                    "value": "8",
                },
                "hide": 0,
            },
        ]
    },
    "panels": [
        {
            "id": 1,
            "title": "Station Directory",
            "type": "table",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 0,
                "y": 0,
            },
            "targets": [
                query(
                    "A",
                    f"{BASE}/stations?country=${{country}}",
                    station_columns,
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "showHeader": True,
                "cellHeight": "sm",
            },
        },
        {
            "id": 2,
            "title": "Station Coverage Map",
            "type": "geomap",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 12,
                "y": 0,
            },
            "targets": [
                query(
                    "A",
                    f"{BASE}/stations?country=${{country}}",
                    station_columns,
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "view": {
                    "id": "fit",
                    "lat": 16.0,
                    "lon": 106.0,
                    "zoom": 4,
                },
                "controls": {
                    "showZoom": True,
                    "mouseWheelZoom": True,
                    "showAttribution": True,
                    "showScale": True,
                    "showMeasure": False,
                    "showDebug": False,
                },
                "tooltip": {
                    "mode": "details",
                },
                "basemap": {
                    "type": "xyz",
                    "name": "OpenStreetMap",
                    "config": {
                        "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
                    }
                },
                "layers": [
                    {
                        "type": "markers",
                        "name": "Stations",
                        "location": {
                            "mode": "coords",
                            "latitude": "Latitude",
                            "longitude": "Longitude",
                        },
                        "config": {
                            "showLegend": False,
                            "style": {
                                "size": {
                                    "fixed": 7,
                                },
                                "opacity": 0.75,
                            },
                        },
                    }
                ],
            },
        },
        {
            "id": 3,
            "title": "Latest AQI",
            "description": (
                "Populated when processed AQI rows "
                "are available in HBase."
            ),
            "type": "table",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 0,
                "y": 9,
            },
            "targets": [
                query(
                    "A",
                    (
                        f"{BASE}/aqi/latest"
                        "?country=${country}&limit=200"
                    ),
                    latest_columns,
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "showHeader": True,
                "cellHeight": "sm",
            },
        },
        {
            "id": 4,
            "title": "AQI Ranking",
            "description": (
                "Daily ranking from processed HBase data."
            ),
            "type": "table",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 12,
                "y": 9,
            },
            "targets": [
                query(
                    "A",
                    (
                        f"{BASE}/aqi/ranking"
                        "?dt=${ranking_dt}"
                        "&country=${country}"
                    ),
                    ranking_columns,
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "showHeader": True,
                "cellHeight": "sm",
            },
        },
        {
            "id": 5,
            "title": "AQI Time Series — ${station_id}",
            "type": "timeseries",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 0,
                "y": 18,
            },
            "targets": [
                query(
                    "A",
                    (
                        f"{BASE}/aqi/timeseries"
                        "?station_id=${station_id}"
                        "&from=${__from:date:iso}"
                        "&to=${__to:date:iso}"
                    ),
                    aqi_ts_columns,
                    fmt="timeseries",
                )
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "none",
                },
                "overrides": [],
            },
            "options": {
                "legend": {
                    "displayMode": "list",
                    "placement": "bottom",
                },
                "tooltip": {
                    "mode": "single",
                },
            },
        },
        {
            "id": 6,
            "title": "Pollutants — ${station_id}",
            "type": "timeseries",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 12,
                "y": 18,
            },
            "targets": [
                query(
                    "A",
                    (
                        f"{BASE}/aqi/timeseries"
                        "?station_id=${station_id}"
                        "&from=${__from:date:iso}"
                        "&to=${__to:date:iso}"
                    ),
                    pollutant_columns,
                    fmt="timeseries",
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "legend": {
                    "displayMode": "list",
                    "placement": "bottom",
                },
                "tooltip": {
                    "mode": "multi",
                },
            },
        },
        {
            "id": 7,
            "title": "AQI Clustering — Month ${cluster_month}",
            "description": "Cluster assignment and monthly pollutant averages.",
            "type": "table",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 0,
                "y": 27,
            },
            "targets": [
                query(
                    "A",
                    f"{BASE}/ext/clusters?month=${{cluster_month}}",
                    cluster_columns,
                    root_selector="rows",
                )
            ],
            "fieldConfig": {
                "defaults": {},
                "overrides": [],
            },
            "options": {
                "showHeader": True,
                "cellHeight": "sm",
            },
        },
        {
            "id": 8,
            "title": "AQI Forecast +24h — ${station_id}",
            "description": "Actual AQI versus SGD, Random Forest and CNN-LSTM backtest.",
            "type": "timeseries",
            "datasource": DS,
            "gridPos": {
                "h": 9,
                "w": 12,
                "x": 12,
                "y": 27,
            },
            "targets": [
                query(
                    "A",
                    f"{BASE}/ext/forecast?station_id=${{station_id}}",
                    forecast_columns,
                    fmt="timeseries",
                    root_selector="rows",
                )
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "none",
                },
                "overrides": [],
            },
            "options": {
                "legend": {
                    "displayMode": "table",
                    "placement": "bottom",
                    "calcs": ["lastNotNull"],
                },
                "tooltip": {
                    "mode": "multi",
                },
            },
        },
    ],
}

OUT.parent.mkdir(
    parents=True,
    exist_ok=True,
)

OUT.write_text(
    json.dumps(
        dashboard,
        ensure_ascii=False,
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)

print("Generated:", OUT)
print("Panels:", len(dashboard["panels"]))
print("UID:", dashboard["uid"])
