"""Dự báo AQI 24 giờ tới cho từng trạm. Đây là nhánh mở rộng, ngoài phần lõi.

Chạy 3 mô hình theo 3 tầng rồi so sánh:
  Tầng 1: SGDRegressor(loss="epsilon_insensitive"), thay cho LaSVM, làm mốc so sánh.
  Tầng 2: Random Forest (Spark MLlib). Huấn luyện phân tán trên toàn bộ Spark DataFrame, không
          chia theo tháng rồi toPandas() như tầng 1; đây là điểm cần cho thấy mô hình mở rộng
          tốt hơn.
  Tầng 3: CNN-LSTM (TensorFlow/Keras, huấn luyện ở driver). Đầu vào là chuỗi 24 giờ AQI liên
          tiếp (build_sequence_windows) thay vì các lag 1h/3h/24h của tầng 1 và 2.
          Cần cài tensorflow (xem docs/huong-dan-chay.md); nếu thiếu thì tầng 3 bị bỏ qua và
          có thông báo rõ, tầng 1 và 2 vẫn chạy. Vì huấn luyện ở driver nên số chuỗi bị giới
          hạn bởi --cnn-max-rows (mặc định 300.000, lấy mẫu ngẫu nhiên trong tập train và
          test): 8,5 triệu chuỗi x 24 giờ không vừa RAM của driver và quá chậm trên CPU.
          Dùng --skip-cnn-lstm để chỉ chạy tầng 1 và 2.

Vì sao không dùng LaSVM hoặc SVR thật:
  Spark MLlib không có SVM hồi quy (chỉ có LinearSVC cho phân loại) và cách cập nhật của
  LaSVM (online, từng điểm) không song song hoá theo kiểu Spark được. scikit-learn có SVR
  nhưng độ phức tạp O(n²) đến O(n³) theo số dòng, với 5-8 triệu dòng của backfill sẽ treo máy.
  SGDRegressor với loss epsilon_insensitive là SVM tuyến tính học online (partial_fit), gần với
  LaSVM nhất trong các công cụ có sẵn và mở rộng được tới hàng triệu dòng vì không cần giữ hết
  dữ liệu trong bộ nhớ.

Cách giữ đúng tính "online": dữ liệu được chia theo tháng lịch (năm-tháng thật, khác với
feature "month" 1-12 dùng cho mùa vụ) và partial_fit() được gọi tuần tự từng tháng, nên driver
không bao giờ giữ quá một tháng dữ liệu. Nhờ vậy có thể huấn luyện với nhiều năm dữ liệu hoặc
dữ liệu mới mà không nạp hết vào RAM.

Feature: aqi_lag_1h, aqi_lag_3h, aqi_lag_24h, pm2_5, pm10, o3, no2, so2, co, hour_of_day,
         month, lat, lon.
Nhãn   : AQI sau đó 24 giờ (dự báo, không phải giá trị hiện tại).

Chia train và test theo thời gian (không chia ngẫu nhiên) để dữ liệu tương lai không rò vào
quá khứ: các tháng cuối làm test, phần còn lại làm train.

Đầu vào : /air-quality/aqi/  (đầu ra Pha 2, schema C3)
Đầu ra  : in RMSE, MAE, R2 ra console và lưu các model vào --output
          --backtest-out <file> ghi thêm ext_forecast_backtest.json cho demo (schema C8.2 trong
          CONTRACTS.md): giá trị thật và dự báo của cả 3 tầng cho vài trạm, để chọn model thắng
          mà không phải chạy lại. Mô hình chỉ dự báo một giá trị: AQI sau đúng 24 giờ.

Chạy local:
  python jobs/ext_forecast.py --input /tmp/aqi --output /tmp/forecast_model --backtest-out /tmp/backtest.json"""
import argparse
import json
import os
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

from aqi_core.iaqi import align_hourly_window

FEATURE_COLS = [
    "aqi_lag_1h", "aqi_lag_3h", "aqi_lag_24h",
    "pm2_5", "pm10", "o3", "no2", "so2", "co",
    "hour_of_day", "month", "lat", "lon",
]
TARGET_COL = "target_aqi_24h"
TEST_FRACTION = 0.2  # 20% khoang thoi gian cuoi lam test, theo dung nguyen tac time-series

SEQUENCE_WINDOW_HOURS = 24
SEQ_COLS = [f"seq_{i}" for i in range(SEQUENCE_WINDOW_HOURS)]
CNN_MAX_ROWS = 300_000

HORIZON_HOURS = 24
BACKTEST_JSON_SCHEMA_VERSION = "1.0"
BACKTEST_DEFAULT_STATIONS = ["VN_HCM_01", "VN_HAN_01", "IN_DEL_01", "CN_BJS_01", "JP_TYO_01"]
BACKTEST_MAX_STATIONS = 5
BACKTEST_DAYS = 30


def build_forecast_features(df):
    """Tạo feature lag và nhãn (AQI 24 giờ sau) theo từng trạm. Bỏ các dòng thiếu lag hoặc thiếu
    nhãn (đầu và cuối chuỗi của mỗi trạm: chưa đủ lịch sử hoặc chưa có tương lai)."""
    w = Window.partitionBy("station_id").orderBy("ts_epoch")

    df = df.withColumn("ts_epoch", F.unix_timestamp("ts_utc"))
    df = df.withColumn("chunk_month", F.date_trunc("month", "ts_utc"))  # nam-thang THAT, de sap xep tuan tu
    df = df.withColumn("hour_of_day", F.hour("ts_utc"))
    df = df.withColumn("month", F.month("ts_utc"))  # 1-12, feature mua vu (khac chunk_month)

    df = df.withColumn("aqi_lag_1h", F.lag("aqi", 1).over(w))
    df = df.withColumn("aqi_lag_3h", F.lag("aqi", 3).over(w))
    df = df.withColumn("aqi_lag_24h", F.lag("aqi", 24).over(w))
    df = df.withColumn(TARGET_COL, F.lead("aqi", 24).over(w))

    # ts_epoch = giờ phát dự báo (t); nhãn là AQI tại t+24h. Giữ lại để xuất backtest theo thời gian.
    cols = ["station_id", "chunk_month", "ts_epoch"] + FEATURE_COLS + [TARGET_COL]
    return df.select(*cols).na.drop()


def time_based_split(df, test_fraction=TEST_FRACTION):
    """Lấy các tháng cuối (theo chunk_month) làm test. Không chia ngẫu nhiên để tránh rò rỉ dữ liệu tương lai."""
    months = sorted(r["chunk_month"] for r in df.select("chunk_month").distinct().collect())
    n_test = max(1, int(len(months) * test_fraction))
    train_months, test_months = months[:-n_test], months[-n_test:]
    print(f"Train: {len(train_months)} tháng ({train_months[0]}..{train_months[-1]}) "
          f"| Test: {len(test_months)} tháng ({test_months[0]}..{test_months[-1]})")
    return train_months, test_months


def train_sgd_online(spark_df, months, seed=42, epochs=30):
    """Huấn luyện SGD bằng partial_fit() tuần tự theo từng tháng, nên driver không giữ quá một tháng
    dữ liệu cùng lúc (giữ đúng tinh thần học online, không nạp hết vào RAM như fit() thường).

    epochs lớn hơn 1 nghĩa là lặp lại nhiều lượt qua cùng các tháng đó, không thêm dữ liệu mới. Cần
    thiết vì SGDRegressor.fit() mặc định cũng chạy nhiều epoch để hội tụ, gọi partial_fit() mỗi tháng
    một lần thường không đủ. Mỗi lượt vẫn chỉ đọc lại một tháng tại một thời điểm (Spark filter rồi
    toPandas())."""
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import StandardScaler

    model = SGDRegressor(loss="epsilon_insensitive", epsilon=5.0, random_state=seed, max_iter=1)
    scaler = StandardScaler()

    # Fit scaler một lần trên tháng đầu tiên, đúng tinh thần online: không nhìn trước toàn bộ
    # dữ liệu để chuẩn hoá, chỉ dùng thống kê của dữ liệu đã thấy.
    first_pdf = spark_df.filter(F.col("chunk_month") == months[0]).select(*FEATURE_COLS, TARGET_COL).toPandas()
    scaler.fit(first_pdf[FEATURE_COLS].values)

    for epoch in range(epochs):
        for m in months:
            pdf = spark_df.filter(F.col("chunk_month") == m).select(*FEATURE_COLS, TARGET_COL).toPandas()
            if pdf.empty:
                continue
            X_scaled = scaler.transform(pdf[FEATURE_COLS].values)
            model.partial_fit(X_scaled, pdf[TARGET_COL].values)
        print(f"  epoch {epoch + 1}/{epochs} xong ({len(months)} tháng)")

    return model, scaler


def predict_sgd(model, scaler, pdf):
    return model.predict(scaler.transform(pdf[FEATURE_COLS].values))


def evaluate(model, scaler, spark_df, months):
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    pdf = spark_df.filter(F.col("chunk_month").isin(months)).select(*FEATURE_COLS, TARGET_COL).toPandas()
    X = scaler.transform(pdf[FEATURE_COLS].values)
    y_true = pdf[TARGET_COL].values
    y_pred = model.predict(X)

    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return {"rmse": rmse, "mae": mae, "r2": r2, "n_test": len(pdf)}


def train_random_forest(train_df, seed=42, num_trees=20, max_depth=5):
    """Tầng 2: huấn luyện phân tán trên toàn bộ train_df cùng lúc (không chia theo tháng, không
    toPandas()). Khác tầng 1 vì SGD hay SVR không phân tán được; RandomForestRegressor tự chia việc
    dựng cây cho các partition của Spark."""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    assembled = assembler.transform(train_df)
    rf = RandomForestRegressor(
        featuresCol="features", labelCol=TARGET_COL, seed=seed,
        numTrees=num_trees, maxDepth=max_depth,
    )
    model = rf.fit(assembled)
    return model, assembler


def evaluate_rf(model, assembler, test_df):
    """Trả kết quả cùng dạng {"rmse", "mae", "r2", "n_test"} như evaluate() của tầng 1 để so sánh trực tiếp."""
    predictions = model.transform(assembler.transform(test_df))
    metrics = {}
    for name in ["rmse", "mae", "r2"]:
        evaluator = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName=name)
        metrics[name] = evaluator.evaluate(predictions)
    metrics["n_test"] = predictions.count()
    return metrics


def print_feature_importances(model):
    print("\n=== Feature importance (Random Forest) ===")
    for col, imp in sorted(zip(FEATURE_COLS, model.featureImportances.toArray()), key=lambda x: -x[1]):
        print(f"  {col:<14}: {imp:.4f}")


# ---------------------------------------------------------------------------
# Tầng 3 — CNN-LSTM trên chuỗi 24 giờ AQI liên tiếp
# ---------------------------------------------------------------------------
def build_sequence_windows(df, window_hours=SEQUENCE_WINDOW_HOURS):
    """Chuỗi window_hours giá trị AQI liên tiếp gần nhất (kết thúc ở giờ hiện tại) cùng nhãn (AQI
    24 giờ sau), dùng cho CNN-LSTM. Khác với các lag rời rạc 1h/3h/24h của
    build_forecast_features() (tầng 1 và 2): CNN-LSTM cần chuỗi liên tục thật để Conv1D bắt mẫu
    hình cục bộ và LSTM bắt phụ thuộc dài hạn.

    Dùng aqi_core.align_hourly_window() để đặt đúng vị trí giờ theo timestamp (không dựa vào vị
    trí trong mảng nên an toàn khi có khoảng thiếu), cùng cơ chế với Nowcast trong phase2_aqi.py.

    seq_0 là giờ cũ nhất (t - window_hours + 1), seq_{window_hours-1} là giờ hiện tại (t), tức thứ
    tự thời gian tự nhiên từ cũ đến mới, chuẩn cho đầu vào LSTM.

    Bỏ các dòng có giờ nào trong chuỗi bị null (mạng nơ-ron không nhận NaN và không tự điền số)
    hoặc thiếu nhãn (cuối chuỗi mỗi trạm, chưa đủ tương lai)."""
    w = Window.partitionBy("station_id").orderBy("ts_epoch")
    w_seq = w.rowsBetween(-(window_hours - 1), 0)

    df = df.withColumn("ts_epoch", F.unix_timestamp("ts_utc"))
    df = df.withColumn("chunk_month", F.date_trunc("month", "ts_utc"))
    df = df.withColumn(TARGET_COL, F.lead("aqi", 24).over(w))

    packed = F.collect_list(F.struct(F.col("ts_epoch").alias("ts_epoch"), F.col("aqi").alias("v"))).over(w_seq)

    @F.udf(returnType=ArrayType(DoubleType()))
    def _seq_udf(cur_ts, pairs):
        aligned = align_hourly_window(cur_ts, ((r["ts_epoch"], r["v"]) for r in pairs), window_hours)
        return list(reversed(aligned))  # align_hourly_window: idx0 = hien tai -> dao lai thanh oldest-first

    df = df.withColumn("sequence", _seq_udf(F.col("ts_epoch"), packed))

    seq_cols = [F.col("sequence")[i].alias(f"seq_{i}") for i in range(window_hours)]
    return df.select("station_id", "chunk_month", "ts_epoch", *seq_cols, TARGET_COL).na.drop()


def sample_to_pandas(df, max_rows, seed=42):
    """Chuyển sang pandas có giới hạn: nhiều hơn max_rows thì lấy mẫu ngẫu nhiên xấp xỉ max_rows dòng
    (driver không chứa nổi hàng triệu chuỗi x 24 giờ)."""
    n = df.count()
    if n > max_rows:
        df = df.sample(withReplacement=False, fraction=max_rows / n, seed=seed)
    return df.toPandas()


def _sequence_arrays(pdf, scaler=None, fit=False):
    """Chuẩn hoá toàn cục: gộp mọi giá trị AQI trong chuỗi rồi fit một scaler chung. Hợp lý vì mọi
    vị trí trong chuỗi đều đo cùng một đại lượng (AQI), khác tầng 1 và 2 gồm nhiều feature khác
    đơn vị. Trả X có shape (n, window, 1), y và scaler."""
    import numpy as np
    from sklearn.preprocessing import StandardScaler

    X = pdf[SEQ_COLS].to_numpy(dtype="float32")
    y = pdf[TARGET_COL].to_numpy(dtype="float32")
    if fit:
        # Chuẩn hoá cả nhãn: AQI cỡ 50-500 nếu để nguyên làm loss ban đầu rất lớn, Adam hội tụ
        # chậm và mô hình bị huấn luyện thiếu so với tầng 1 và 2 (Random Forest không cần chuẩn hoá).
        scaler = {"x": StandardScaler().fit(X.reshape(-1, 1)), "y_mean": float(y.mean()),
                  "y_std": float(y.std()) or 1.0}
    scaled = scaler["x"].transform(X.reshape(-1, 1)).reshape(X.shape).astype("float32")
    return scaled[..., np.newaxis], y, scaler


def build_cnn_lstm(window_hours=SEQUENCE_WINDOW_HOURS):
    from tensorflow.keras import layers, models

    model = models.Sequential([
        layers.Input(shape=(window_hours, 1)),
        layers.Conv1D(filters=32, kernel_size=3, activation="relu", padding="causal"),
        layers.Conv1D(filters=32, kernel_size=3, activation="relu", padding="causal"),
        layers.LSTM(32, return_sequences=False),
        layers.Dense(16, activation="relu"),
        layers.Dense(1),
    ])
    # loss="mse" tối ưu sai số bình phương (khớp với RMSE) nên MAE có thể kém hơn tầng 1 và 2
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    return model


def train_cnn_lstm(train_pdf, seed=42, epochs=30, batch_size=256):
    import tensorflow as tf

    tf.keras.utils.set_random_seed(seed)
    X, y, scaler = _sequence_arrays(train_pdf, fit=True)
    y_scaled = (y - scaler["y_mean"]) / scaler["y_std"]
    model = build_cnn_lstm()
    model.fit(
        X, y_scaled,
        validation_split=0.1,
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[tf.keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True)],
        verbose=2,
    )
    return model, scaler


def predict_cnn_lstm(model, scaler, pdf):
    X, _, _ = _sequence_arrays(pdf, scaler=scaler)
    return model.predict(X, verbose=0).flatten() * scaler["y_std"] + scaler["y_mean"]


def evaluate_cnn_lstm(model, scaler, test_pdf):
    """Cùng format {"rmse","mae","r2","n_test"} như evaluate()/evaluate_rf() của tầng 1/2."""
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    y_true = test_pdf[TARGET_COL].to_numpy(dtype="float32")
    y_pred = predict_cnn_lstm(model, scaler, test_pdf)
    return {
        "rmse": mean_squared_error(y_true, y_pred) ** 0.5,
        "mae": mean_absolute_error(y_true, y_pred),
        "r2": r2_score(y_true, y_pred),
        "n_test": len(y_true),
    }


def run_cnn_lstm_tier(df, output_dir, max_rows, epochs, backtest_stations=None):
    """Chạy cả tầng 3 từ DataFrame Pha 2. Trả (metrics, backtest_pdf); backtest_pdf là dự báo
    của CNN-LSTM cho các trạm backtest trong tháng test (None nếu không yêu cầu).
    Thiếu tensorflow -> (None, None)."""
    try:
        import tensorflow  # noqa: F401
    except ImportError:
        print("\n[Tầng 3] BỎ QUA CNN-LSTM: chưa cài tensorflow (pip install tensorflow-cpu).")
        return None, None

    seq = build_sequence_windows(df).cache()
    print(f"\n[Tầng 3] Số chuỗi {SEQUENCE_WINDOW_HOURS}h đủ lịch sử + nhãn: {seq.count()}")
    train_months, test_months = time_based_split(seq)

    train_pdf = sample_to_pandas(seq.filter(F.col("chunk_month").isin(train_months)).drop("chunk_month"), max_rows)
    test_pdf = sample_to_pandas(seq.filter(F.col("chunk_month").isin(test_months)).drop("chunk_month"), max_rows)
    print(f"[Tầng 3] Dùng {len(train_pdf)} chuỗi train / {len(test_pdf)} chuỗi test (giới hạn {max_rows})")

    model, scaler = train_cnn_lstm(train_pdf, epochs=epochs)
    metrics = evaluate_cnn_lstm(model, scaler, test_pdf)
    model.save(os.path.join(output_dir, "cnn_lstm.keras"))

    backtest_pdf = None
    if backtest_stations:
        sel = seq.filter(F.col("chunk_month").isin(test_months) & F.col("station_id").isin(backtest_stations))
        pdf = sel.select("station_id", "ts_epoch", *SEQ_COLS, TARGET_COL).toPandas()
        if not pdf.empty:
            backtest_pdf = pdf[["station_id", "ts_epoch"]].copy()
            backtest_pdf["actual"] = pdf[TARGET_COL].to_numpy()
            backtest_pdf["cnn_lstm"] = predict_cnn_lstm(model, scaler, pdf)
    return metrics, backtest_pdf


# ---------------------------------------------------------------------------
# Backtest: dự báo vs thực tế của cả 3 tầng, xuất JSON cho demo (schema C8.2)
# ---------------------------------------------------------------------------
def pick_backtest_stations(available, requested=None, n=BACKTEST_MAX_STATIONS):
    """Trạm cho backtest: danh sách yêu cầu (hoặc mặc định) giao với trạm có dữ liệu;
    không trạm nào khớp thì lấy n trạm đầu theo tên để luôn có kết quả."""
    available = sorted(set(available))
    wanted = list(requested) if requested else BACKTEST_DEFAULT_STATIONS
    return [s for s in wanted if s in available] or available[:n]


def build_backtest_base(features, test_months, stations, sgd_model, sgd_scaler, rf_model, assembler):
    """Giá trị thật + dự báo của tầng 1 (SGD) và tầng 2 (RF) cho các trạm đã chọn, trong tháng test."""
    import pandas as pd

    sel = features.filter(F.col("chunk_month").isin(test_months) & F.col("station_id").isin(stations))
    pdf = sel.select("station_id", "ts_epoch", *FEATURE_COLS, TARGET_COL).toPandas()
    if pdf.empty:
        return pd.DataFrame(columns=["station_id", "ts_epoch", "actual", "sgd", "rf"])

    rf_pdf = (
        rf_model.transform(assembler.transform(sel))
        .select("station_id", "ts_epoch", F.col("prediction").alias("rf"))
        .toPandas()
    )
    base = pdf[["station_id", "ts_epoch"]].copy()
    base["actual"] = pdf[TARGET_COL].to_numpy()
    base["sgd"] = predict_sgd(sgd_model, sgd_scaler, pdf)
    return base.merge(rf_pdf, on=["station_id", "ts_epoch"], how="left")


def assemble_backtest(base_pdf, cnn_pdf=None, days=BACKTEST_DAYS):
    """Ghép dự báo 3 tầng theo (station_id, ts_epoch), chỉ giữ `days` ngày cuối. Tầng nào không
    chấm điểm giờ đó thì để trống (NaN) — không bịa số."""
    cols = ["station_id", "ts_epoch", "actual", "sgd", "rf", "cnn_lstm"]
    merged = base_pdf.copy()
    if cnn_pdf is not None and not cnn_pdf.empty:
        cnn = cnn_pdf.rename(columns={"actual": "actual_cnn"})
        merged = merged.merge(cnn, on=["station_id", "ts_epoch"], how="outer")
        merged["actual"] = merged["actual"].fillna(merged["actual_cnn"])
        merged = merged.drop(columns="actual_cnn")
    else:
        merged["cnn_lstm"] = float("nan")

    if merged.empty:
        return merged.reindex(columns=cols)
    cutoff = merged["ts_epoch"].max() - days * 86400
    merged = merged[merged["ts_epoch"] > cutoff]
    return merged.sort_values(["station_id", "ts_epoch"]).reset_index(drop=True)[cols]


def _iso(epoch):
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(value):
    if value is None or value != value:  # None hoặc NaN
        return None
    return round(float(value), 1)


def _clean_metrics(m):
    if m is None:
        return None
    return {"rmse": round(float(m["rmse"]), 2), "mae": round(float(m["mae"]), 2),
            "r2": round(float(m["r2"]), 4), "n_test": int(m["n_test"])}


def export_backtest_json(merged, metrics, path):
    """Ghi ext_forecast_backtest.json (schema C8.2). Mỗi dòng là một giờ được dự báo:
    ts_utc = giờ được dự báo (t+24h), issued_utc = giờ phát dự báo (t)."""
    rows = [
        {
            "station_id": r.station_id,
            "ts_utc": _iso(r.ts_epoch + HORIZON_HOURS * 3600),
            "issued_utc": _iso(r.ts_epoch),
            "actual": _num(r.actual),
            "sgd": _num(r.sgd),
            "rf": _num(r.rf),
            "cnn_lstm": _num(r.cnn_lstm),
        }
        for r in merged.itertuples(index=False)
    ]
    payload = {
        "schema_version": BACKTEST_JSON_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "horizon_hours": HORIZON_HOURS,
        "models": {"sgd": "SGDRegressor (Tầng 1)", "rf": "Random Forest (Tầng 2)", "cnn_lstm": "CNN-LSTM (Tầng 3)"},
        "metrics": {name: _clean_metrics(m) for name, m in metrics.items()},
        "stations": sorted({r["station_id"] for r in rows}),
        "rows": rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--since", default=None,
                    help="chỉ dùng dt >= YYYY-MM-DD (UTC) — nên dùng với dữ liệu thật ~8,5 triệu dòng")
    ap.add_argument("--backtest-out", default=None,
                    help="đường dẫn ext_forecast_backtest.json (trong container) cho demo: giá trị thật và dự báo của cả 3 tầng")
    ap.add_argument("--backtest-stations", default=None,
                    help="station_id cách nhau bằng dấu phẩy; mặc định VN_HCM_01,VN_HAN_01,IN_DEL_01,CN_BJS_01,JP_TYO_01 (trạm nào có dữ liệu)")
    ap.add_argument("--backtest-days", type=int, default=BACKTEST_DAYS, help="chỉ xuất N ngày cuối của tháng test")
    ap.add_argument("--skip-cnn-lstm", action="store_true", help="chỉ chạy tầng 1+2")
    ap.add_argument("--cnn-max-rows", type=int, default=CNN_MAX_ROWS,
                    help="số chuỗi tối đa đưa vào CNN-LSTM (train và test, lấy mẫu ngẫu nhiên)")
    ap.add_argument("--cnn-epochs", type=int, default=30, help="số epoch tối đa (có early stopping)")
    args = ap.parse_args()
    os.makedirs(args.output, exist_ok=True)

    spark = (
        SparkSession.builder.appName("ext_forecast")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        # RandomForestRegressor bị tràn bộ nhớ với driver mặc định -Xmx1g (PySpark tự đặt khi
        # không cấu hình) dù dữ liệu nhỏ, vì findBestSplits gom thống kê về driver, tốn hơn các
        # phép DataFrame thường. 2g vẫn tràn trên dữ liệu mẫu 9 nghìn dòng khi chạy cả quy trình
        # (SGD chạy khoảng 90 job Spark trước khi tới Random Forest), nên mặc định là 4g. Chỉ có
        # tác dụng khi chạy bằng `python ...`; với spark-submit hãy dùng --driver-memory.
        .config("spark.driver.memory", os.environ.get("EXT_DRIVER_MEMORY", "4g"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    if args.since:
        df = df.filter(F.col("dt") >= F.lit(args.since).cast("date"))
    features = build_forecast_features(df).persist(StorageLevel.DISK_ONLY)
    print(f"Tổng số dòng có đủ feature+label: {features.count()}")

    train_months, test_months = time_based_split(features)

    sgd_model, scaler = train_sgd_online(features, train_months)
    sgd_metrics = evaluate(sgd_model, scaler, features, test_months)

    train_df = features.filter(F.col("chunk_month").isin(train_months))
    test_df = features.filter(F.col("chunk_month").isin(test_months))
    rf_model, assembler = train_random_forest(train_df)
    rf_metrics = evaluate_rf(rf_model, assembler, test_df)

    backtest_stations = None
    if args.backtest_out:
        available = [r["station_id"] for r in features.select("station_id").distinct().collect()]
        requested = [s.strip() for s in args.backtest_stations.split(",")] if args.backtest_stations else None
        backtest_stations = pick_backtest_stations(available, requested)
        print(f"\n[Backtest] trạm: {backtest_stations}")

    cnn_metrics, cnn_backtest = None, None
    if not args.skip_cnn_lstm:
        cnn_metrics, cnn_backtest = run_cnn_lstm_tier(
            df, args.output, args.cnn_max_rows, args.cnn_epochs, backtest_stations
        )

    print("\n=== So sánh Tầng 1 (SGD, thay LaSVM) vs Tầng 2 (Random Forest) vs Tầng 3 (CNN-LSTM) ===")
    print(f"{'':<16}{'RMSE':>10}{'MAE':>10}{'R2':>10}{'n_test':>10}")
    rows = [("SGD (T1)", sgd_metrics), ("RF (T2)", rf_metrics), ("CNN-LSTM (T3)", cnn_metrics)]
    for name, m in rows:
        if m is None:
            print(f"{name:<16}{'(bỏ qua)':>10}")
        else:
            print(f"{name:<16}{m['rmse']:>10.2f}{m['mae']:>10.2f}{m['r2']:>10.4f}{m['n_test']:>10}")
    print("Đối chiếu: LaSVM baseline trong paper Ghaemi 2015 có R2 ~ 0.81 (chạy trên dữ liệu khác — không so trực tiếp 1-1)")
    print("Lưu ý: n_test tầng 3 khác tầng 1/2 vì chuỗi cần đủ 24h lịch sử và có thể bị lấy mẫu (--cnn-max-rows).")
    print_feature_importances(rf_model)
    print("=" * 60)

    with open(os.path.join(args.output, "sgd_baseline.pkl"), "wb") as f:
        pickle.dump({"model": sgd_model, "scaler": scaler, "feature_cols": FEATURE_COLS, "metrics": sgd_metrics}, f)
    rf_model.write().overwrite().save(os.path.join(args.output, "rf_model"))
    print(f"Đã ghi model vào {args.output}/ (sgd_baseline.pkl, rf_model/)")

    if args.backtest_out:
        base = build_backtest_base(features, test_months, backtest_stations, sgd_model, scaler, rf_model, assembler)
        merged = assemble_backtest(base, cnn_backtest, args.backtest_days)
        payload = export_backtest_json(
            merged, {"sgd": sgd_metrics, "rf": rf_metrics, "cnn_lstm": cnn_metrics}, args.backtest_out
        )
        print(f"Đã ghi {args.backtest_out}: {len(payload['rows'])} dòng, trạm {payload['stations']}")

    spark.stop()


if __name__ == "__main__":
    main()
