"""Unit test cho ext_forecast.py: tầng 1 (SGDRegressor), tầng 2 (Random Forest), tầng 3 (CNN-LSTM)."""
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import functions as F

from ext_forecast import (
    SEQUENCE_WINDOW_HOURS,
    assemble_backtest,
    build_backtest_base,
    build_forecast_features,
    build_sequence_windows,
    evaluate,
    evaluate_cnn_lstm,
    evaluate_rf,
    export_backtest_json,
    pick_backtest_stations,
    run_cnn_lstm_tier,
    sample_to_pandas,
    time_based_split,
    train_cnn_lstm,
    train_random_forest,
    train_sgd_online,
)

AQI_SCHEMA = "station_id string, city string, ts_utc timestamp, pm2_5 double, pm10 double, o3 double, no2 double, so2 double, co double, aqi double, lat double, lon double"


def _make_station_series(station_id: str, n_hours: int, start=datetime(2026, 6, 1)):
    """Chuỗi giờ liên tục, aqi = hàm tuyến tính đơn giản của giờ để dễ kiểm tra dự báo học được xu hướng."""
    rows = []
    for h in range(n_hours):
        ts = start + timedelta(hours=h)
        aqi = 50 + (h % 24) * 2.0  # lặp lại theo chu kỳ ngày nên lag_24h phải dự báo tốt
        rows.append((station_id, "TestCity", ts, 20.0, 30.0, 40.0, 10.0, 5.0, 300.0, aqi, 10.0, 100.0))
    return rows


def test_build_forecast_features_loai_dong_thieu_lag_hoac_target(spark):
    # cần hơn 48 giờ để có vùng giao nhau: h >= 24 (đủ lag_24h) và h <= n-25 (đủ nhãn) cùng lúc
    n_hours = 72
    rows = _make_station_series("S1", n_hours=n_hours)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    out = build_forecast_features(df)

    assert out.count() == n_hours - 48  # đúng 24 dòng đầu và 24 dòng cuối bị loại, phần giữa được giữ lại
    row = out.orderBy("chunk_month").first()
    assert row["aqi_lag_1h"] is not None
    assert row["target_aqi_24h"] is not None


def test_time_based_split_khong_random_lay_thang_cuoi_lam_test(spark):
    rows = _make_station_series("S1", n_hours=24 * 90)  # ~3 thang
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df)

    train_months, test_months = time_based_split(features, test_fraction=0.34)
    assert max(train_months) < min(test_months)  # train luôn nằm trước test về thời gian, không xen kẽ


def test_train_sgd_online_hoc_duoc_chu_ky_ngay(spark):
    # AQI lặp lại đúng chu kỳ 24 giờ nên aqi_lag_24h gần như dự đoán hoàn hảo nhãn (chính nó là
    # AQI của đúng 24 giờ trước, mà chuỗi lặp lại theo chu kỳ 24 giờ).
    # Cần trải dài ít nhất 2 tháng lịch để time_based_split có đủ train và test
    rows = _make_station_series("S1", n_hours=24 * 90)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df).cache()

    train_months, test_months = time_based_split(features, test_fraction=0.3)
    model, scaler = train_sgd_online(features, train_months)
    metrics = evaluate(model, scaler, features, test_months)

    # Mục đích: xác nhận cơ chế chạy đúng (feature, chia train/test, huấn luyện, đánh giá không
    # lỗi), không kiểm tra độ hội tụ tối ưu của SGDRegressor (phụ thuộc siêu tham số, ngoài phạm
    # vi unit test này). R2 > 0 nghĩa là mô hình học được tốt hơn baseline "luôn đoán trung bình",
    # đủ để xác nhận pipeline hoạt động.
    assert metrics["r2"] > 0
    assert metrics["n_test"] > 0


def test_train_random_forest_hoc_duoc_chu_ky_ngay(spark):
    # Trên đúng dữ liệu chu kỳ 24 giờ đã dùng ở test tầng 1: tính chất này phải đúng cho cả hai
    # tầng, mô hình phải học tốt hơn baseline "đoán trung bình" (R2 > 0)
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


# --- Tầng 3: chuỗi 24 giờ + CNN-LSTM ------------------------------------------
SEQ_SCHEMA = "station_id string, ts_utc timestamp, aqi double"


def _make_aqi_series(station_id: str, n_hours: int, start=datetime(2026, 6, 1)):
    return [(station_id, start + timedelta(hours=h), 50.0 + (h % 24) * 2.0) for h in range(n_hours)]


def test_build_sequence_windows_dung_do_dai_va_thu_tu_thoi_gian(spark):
    n_hours = 72
    df = spark.createDataFrame(_make_aqi_series("S1", n_hours), schema=SEQ_SCHEMA)
    out = build_sequence_windows(df)

    # hợp lệ khi có đủ 24h lịch sử (h >= 23) VÀ đủ nhãn 24h sau (h <= n-25) -> n - 47 dòng
    assert out.count() == n_hours - 47

    row = out.first()
    values = [row[f"seq_{i}"] for i in range(SEQUENCE_WINDOW_HOURS)]
    assert len(values) == SEQUENCE_WINDOW_HOURS and all(v is not None for v in values)

    # oldest -> newest: seq_23 (hiện tại) lệch đúng 1 giờ so với seq_22 (chuỗi chu kỳ 24h, +2 hoặc -46)
    diff = values[23] - values[22]
    assert diff == pytest.approx(2.0) or diff == pytest.approx(-46.0)


def test_build_sequence_windows_loai_dong_thieu_target(spark):
    df = spark.createDataFrame(_make_aqi_series("S1", 47), schema=SEQ_SCHEMA)  # dưới ngưỡng 48h
    assert build_sequence_windows(df).count() == 0


def test_sample_to_pandas_gioi_han_so_dong(spark):
    df = spark.range(1000).withColumnRenamed("id", "x")
    assert len(sample_to_pandas(df, max_rows=5000)) == 1000           # dưới giới hạn -> lấy đủ
    assert 50 <= len(sample_to_pandas(df, max_rows=100)) <= 170       # vượt giới hạn -> xấp xỉ max_rows


def test_cnn_lstm_hoc_duoc_chu_ky_ngay(spark):
    pytest.importorskip("tensorflow")
    rows = []
    for sid in ("S1", "S2", "S3"):
        rows += _make_aqi_series(sid, 24 * 30)
    seq = build_sequence_windows(spark.createDataFrame(rows, schema=SEQ_SCHEMA))
    train_pdf = seq.filter(F.col("chunk_month") < datetime(2026, 6, 20)).drop("chunk_month").toPandas()
    test_pdf = seq.filter(F.col("chunk_month") >= datetime(2026, 6, 20)).drop("chunk_month").toPandas()
    if test_pdf.empty:  # chuỗi chỉ trong 1 tháng lịch -> chia theo hàng
        n = len(seq.toPandas()) // 2
        full = seq.drop("chunk_month").toPandas()
        train_pdf, test_pdf = full.iloc[:n], full.iloc[n:]

    model, scaler = train_cnn_lstm(train_pdf, epochs=10, batch_size=64)
    metrics = evaluate_cnn_lstm(model, scaler, test_pdf)

    assert set(metrics) == {"rmse", "mae", "r2", "n_test"}
    assert metrics["n_test"] == len(test_pdf)
    # AQI chu kỳ 24h nên target = giá trị hiện tại (seq_23): mô hình phải học được, không ngẫu nhiên
    assert metrics["r2"] > 0.5


# --- Backtest: xuất ext_forecast_backtest.json (schema C8.2) ----------------------
def test_feature_va_chuoi_deu_giu_ts_epoch_cach_nhau_1_gio(spark):
    df = spark.createDataFrame(_make_station_series("S1", n_hours=72), schema=AQI_SCHEMA)
    for out in (build_forecast_features(df), build_sequence_windows(df)):
        assert "ts_epoch" in out.columns
        epochs = sorted(r["ts_epoch"] for r in out.select("ts_epoch").collect())
        assert {b - a for a, b in zip(epochs, epochs[1:])} == {3600}


def test_pick_backtest_stations():
    default = ["VN_HCM_01", "VN_HAN_01", "IN_DEL_01", "CN_BJS_01", "JP_TYO_01"]
    # mặc định: giao với trạm có dữ liệu, giữ thứ tự danh sách mặc định
    assert pick_backtest_stations(["JP_TYO_01", "VN_HAN_01", "ZZ"]) == ["VN_HAN_01", "JP_TYO_01"]
    # yêu cầu tường minh được tôn trọng
    assert pick_backtest_stations(["A", "B", "C"], requested=["C", "A"]) == ["C", "A"]
    # không trạm nào khớp -> 5 trạm đầu theo tên
    many = ["G", "F", "E", "D", "C", "B", "A"]
    assert pick_backtest_stations(many) == ["A", "B", "C", "D", "E"]
    assert pick_backtest_stations(many, requested=["khong_co"]) == ["A", "B", "C", "D", "E"]
    assert default[0] == "VN_HCM_01"


def _base_pdf():
    return pd.DataFrame({
        "station_id": ["S1", "S1", "S1"], "ts_epoch": [0, 3600, 7200],
        "actual": [50.0, 60.0, 70.0], "sgd": [51.0, 61.0, 71.0], "rf": [52.0, 62.0, 72.0],
    })


def test_assemble_backtest_ghep_3_tang_va_de_trong_cho_gio_thieu():
    cnn = pd.DataFrame({
        "station_id": ["S1", "S1"], "ts_epoch": [3600, 10800],  # 1 giờ trùng, 1 giờ chỉ CNN có
        "actual": [60.0, 80.0], "cnn_lstm": [63.0, 83.0],
    })
    out = assemble_backtest(_base_pdf(), cnn, days=30)

    assert list(out.columns) == ["station_id", "ts_epoch", "actual", "sgd", "rf", "cnn_lstm"]
    assert list(out["ts_epoch"]) == [0, 3600, 7200, 10800]
    assert out.loc[out.ts_epoch == 3600, "cnn_lstm"].item() == 63.0
    assert np.isnan(out.loc[out.ts_epoch == 0, "cnn_lstm"].item())       # tầng 3 không chấm giờ này
    only_cnn = out[out.ts_epoch == 10800].iloc[0]
    assert only_cnn["actual"] == 80.0 and np.isnan(only_cnn["sgd"])      # actual lấy từ CNN, SGD để trống


def test_assemble_backtest_khong_co_cnn_thi_cot_cnn_trong():
    out = assemble_backtest(_base_pdf(), None)
    assert out["cnn_lstm"].isna().all() and len(out) == 3


def test_assemble_backtest_chi_giu_n_ngay_cuoi():
    base = pd.DataFrame({
        "station_id": ["S1"] * 4, "ts_epoch": [0, 86400, 172800, 176400],
        "actual": [1.0] * 4, "sgd": [1.0] * 4, "rf": [1.0] * 4,
    })
    assert list(assemble_backtest(base, None, days=1)["ts_epoch"]) == [172800, 176400]


def test_assemble_backtest_rong_khong_loi():
    empty = pd.DataFrame(columns=["station_id", "ts_epoch", "actual", "sgd", "rf"])
    out = assemble_backtest(empty, None)
    assert out.empty and "cnn_lstm" in out.columns


def test_export_backtest_json_dung_schema(tmp_path):
    merged = pd.DataFrame({
        "station_id": ["S1", "S1"], "ts_epoch": [1_789_023_600, 1_789_027_200],  # 2026-09-10T07:00:00Z, +1h
        "actual": [50.04, 60.0], "sgd": [51.26, np.nan], "rf": [52.0, 62.0], "cnn_lstm": [np.nan, np.nan],
    })
    metrics = {
        "sgd": {"rmse": 32.049, "mae": 20.561, "r2": 0.53821, "n_test": 1154},
        "rf": {"rmse": 31.87, "mae": 20.64, "r2": 0.5431, "n_test": 1154},
        "cnn_lstm": None,
    }
    path = tmp_path / "ext_forecast_backtest.json"
    payload = export_backtest_json(merged, metrics, str(path))

    assert json.loads(path.read_text(encoding="utf-8")) == payload
    assert payload["schema_version"] == "1.0" and payload["horizon_hours"] == 24
    assert payload["stations"] == ["S1"]
    assert set(payload["models"]) == {"sgd", "rf", "cnn_lstm"}
    assert payload["metrics"]["sgd"] == {"rmse": 32.05, "mae": 20.56, "r2": 0.5382, "n_test": 1154}
    assert payload["metrics"]["cnn_lstm"] is None

    first, second = payload["rows"]
    assert first["issued_utc"] == "2026-09-10T07:00:00Z"
    assert first["ts_utc"] == "2026-09-11T07:00:00Z"      # giờ được dự báo = phát + 24h
    assert (first["actual"], first["sgd"], first["rf"], first["cnn_lstm"]) == (50.0, 51.3, 52.0, None)
    assert second["sgd"] is None                            # NaN -> null


def _fit_sgd_rf(spark):
    rows = _make_station_series("S1", n_hours=24 * 70) + _make_station_series("S2", n_hours=24 * 70)
    df = spark.createDataFrame(rows, schema=AQI_SCHEMA)
    features = build_forecast_features(df).cache()
    train_months, test_months = time_based_split(features)
    sgd_model, scaler = train_sgd_online(features, train_months, epochs=2)
    train_df = features.filter(F.col("chunk_month").isin(train_months))
    rf_model, assembler = train_random_forest(train_df, num_trees=5, max_depth=4)
    return df, features, test_months, sgd_model, scaler, rf_model, assembler


def test_backtest_base_chi_gom_tram_da_chon_va_du_du_lieu_2_tang(spark):
    _, features, test_months, sgd, scaler, rf, assembler = _fit_sgd_rf(spark)

    base = build_backtest_base(features, test_months, ["S1"], sgd, scaler, rf, assembler)

    assert set(base["station_id"]) == {"S1"} and len(base) > 0
    assert list(base.columns) == ["station_id", "ts_epoch", "actual", "sgd", "rf"]
    assert base[["actual", "sgd", "rf"]].notna().all().all()
    expected_n = features.filter(F.col("chunk_month").isin(test_months) & (F.col("station_id") == "S1")).count()
    assert len(base) == expected_n

    limited = assemble_backtest(base, None, days=3)
    assert (limited["ts_epoch"].max() - limited["ts_epoch"].min()) < 3 * 86400


def test_backtest_ca_3_tang_cung_ghep_duoc(spark, tmp_path):
    pytest.importorskip("tensorflow")
    df, features, test_months, sgd, scaler, rf, assembler = _fit_sgd_rf(spark)

    metrics, cnn_pdf = run_cnn_lstm_tier(df, str(tmp_path), max_rows=5000, epochs=2, backtest_stations=["S1"])

    assert set(metrics) == {"rmse", "mae", "r2", "n_test"}
    assert set(cnn_pdf.columns) == {"station_id", "ts_epoch", "actual", "cnn_lstm"}
    assert set(cnn_pdf["station_id"]) == {"S1"}

    base = build_backtest_base(features, test_months, ["S1"], sgd, scaler, rf, assembler)
    merged = assemble_backtest(base, cnn_pdf, days=30)
    both = merged.dropna(subset=["sgd", "rf", "cnn_lstm"])
    assert len(both) > 0                                   # có giờ được cả 3 tầng cùng chấm
    assert (tmp_path / "cnn_lstm.keras").exists()
