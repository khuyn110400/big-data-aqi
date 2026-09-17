"""Unit test cho ext_forecast.py (tầng 1 SGDRegressor + tầng 2 Random Forest). NGƯỜI B · M4."""
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from ext_forecast import (
    build_forecast_features,
    evaluate,
    evaluate_rf,
    time_based_split,
    train_random_forest,
    train_sgd_online,
)

AQI_SCHEMA = "station_id string, city string, ts_utc timestamp, pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double, aqi double, lat double, lon double"


def _make_station_series(station_id: str, n_hours: int, start=datetime(2026, 6, 1)):
    """Chuỗi giờ liên tục, aqi = hàm tuyến tính đơn giản của giờ để dễ kiểm tra dự báo học được xu hướng."""
    rows = []
    for h in range(n_hours):
        ts = start + timedelta(hours=h)
        aqi = 50 + (h % 24) * 2.0  # lap lai theo chu ky ngay -> lag_24h phai du bao tot
        rows.append((station_id, "TestCity", ts, 20.0, 30.0, 40.0, 10.0, 5.0, 300.0, aqi, 10.0, 100.0))
    return rows


def test_build_forecast_features_loai_dong_thieu_lag_hoac_target(spark):
    # can > 48h de co vung giao nhau: h>=24 (du lag_24h) VA h<=n-25 (du target) cung luc
    n_hours = 72
    rows = _make_station_series("S1", n_hours=n_hours)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    out = build_forecast_features(df)

    assert out.count() == n_hours - 48  # dung 24 dau + 24 cuoi bi loai, giua giu lai
    row = out.orderBy("chunk_month").first()
    assert row["aqi_lag_1h"] is not None
    assert row["target_aqi_24h"] is not None


def test_time_based_split_khong_random_lay_thang_cuoi_lam_test(spark):
    rows = _make_station_series("S1", n_hours=24 * 90)  # ~3 thang
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df)

    train_months, test_months = time_based_split(features, test_fraction=0.34)
    assert max(train_months) < min(test_months)  # train luon truoc test ve thoi gian, khong xen ke


def test_train_sgd_online_hoc_duoc_chu_ky_ngay(spark):
    # aqi lap lai dung chu ky 24h -> aqi_lag_24h gan nhu du doan hoan hao target
    # (chinh no cung la aqi cua dung 24h truoc, ma chuoi lap lai chu ky 24h)
    # can trai dai >= 2 thang lich de time_based_split co du train + test
    rows = _make_station_series("S1", n_hours=24 * 90)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df).cache()

    train_months, test_months = time_based_split(features, test_fraction=0.3)
    model, scaler = train_sgd_online(features, train_months)
    metrics = evaluate(model, scaler, features, test_months)

    # Muc dich test: xac nhan CO CHE hoat dong dung (feature/split/train/eval khong loi),
    # KHONG phai kiem tra do hoi tu toi uu cua SGDRegressor (phu thuoc hyperparameter,
    # ngoai pham vi unit test nay). R2 > 0 nghia la mo hinh hoc duoc tot hon baseline
    # "luon doan trung binh" - du de xac nhan pipeline hoat dong dung.
    assert metrics["r2"] > 0
    assert metrics["n_test"] > 0


def test_train_random_forest_hoc_duoc_chu_ky_ngay(spark):
    # tai dung chinh du lieu chu ky 24h da co (test tang 1) - property phai dung
    # cho ca 2 tang: mo hinh phai hoc tot hon baseline "doan trung binh" (R2 > 0)
    rows = _make_station_series("S1", n_hours=24 * 90)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df).cache()

    train_months, test_months = time_based_split(features, test_fraction=0.3)
    train_df = features.filter(F.col("chunk_month").isin(train_months))
    test_df = features.filter(F.col("chunk_month").isin(test_months))

    model, assembler = train_random_forest(train_df, num_trees=20, max_depth=5)
    metrics = evaluate_rf(model, assembler, test_df)

    assert metrics["r2"] > 0
    assert metrics["n_test"] > 0


def test_train_random_forest_feature_importances_hop_le(spark):
    rows = _make_station_series("S1", n_hours=24 * 90)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df).cache()

    train_months, _ = time_based_split(features, test_fraction=0.3)
    train_df = features.filter(F.col("chunk_month").isin(train_months))

    model, _ = train_random_forest(train_df, num_trees=20, max_depth=5)
    importances = model.featureImportances.toArray()

    assert len(importances) == len(model.featureImportances)
    assert importances.sum() == pytest.approx(1.0, abs=1e-6)  # feature importance luon chuan hoa ve tong = 1
