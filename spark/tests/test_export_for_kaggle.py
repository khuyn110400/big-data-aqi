"""Unit test cho export_for_kaggle.py (chuẩn bị dữ liệu tầng 3 để chạy trên Kaggle). NGƯỜI B · M4."""
from datetime import datetime, timedelta

import pytest

from export_for_kaggle import SEQUENCE_WINDOW_HOURS, build_sequence_windows

AQI_SCHEMA = "station_id string, ts_utc timestamp, aqi double"


def _make_aqi_series(station_id: str, n_hours: int, start=datetime(2026, 6, 1)):
    rows = []
    for h in range(n_hours):
        ts = start + timedelta(hours=h)
        rows.append((station_id, ts, 50.0 + (h % 24) * 2.0))
    return rows


def test_build_sequence_windows_dung_do_dai_va_thu_tu_thoi_gian(spark):
    n_hours = 72
    rows = _make_aqi_series("S1", n_hours=n_hours)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    out = build_sequence_windows(df)

    # h hop le: 23 <= h <= n_hours-25 (du 24h lam chuoi TA du 24h de co target)
    # so dong hop le = n_hours - 47 (voi n_hours >= 48)
    assert out.count() == n_hours - 47

    row = out.orderBy("seq_23").first()  # bat ky dong nao, chi can lay 1 dong hop le
    seq_cols = [f"seq_{i}" for i in range(SEQUENCE_WINDOW_HOURS)]
    values = [row[c] for c in seq_cols]
    assert len(values) == SEQUENCE_WINDOW_HOURS
    assert all(v is not None for v in values)

    # thu tu thoi gian tu nhien: seq_23 (hien tai) phai la gia tri lien ke SAU seq_22 dung 1 gio
    # voi chuoi lap chu ky 24h, gia tri cach nhau 1h se lech dung 2.0 hoac -46.0 (khi qua vong chu ky)
    diff = values[23] - values[22]
    assert diff == pytest.approx(2.0) or diff == pytest.approx(-46.0)


def test_build_sequence_windows_loai_dong_thieu_target(spark):
    n_hours = 47  # duoi nguong toi thieu (48) -> khong co dong nao vua du chuoi vua du target
    rows = _make_aqi_series("S1", n_hours=n_hours)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    out = build_sequence_windows(df)
    assert out.count() == 0
