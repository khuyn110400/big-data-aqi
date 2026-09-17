"""
TẦNG 3 — CNN-LSTM cho dự báo AQI 24h. Chạy TRÊN KAGGLE, BẬT GPU
(Notebook Settings > Accelerator > GPU T4 x2 hoặc P100) — KHÔNG cần PySpark.

Input: sequence_train.csv, sequence_test.csv — xuất bằng `python jobs/export_for_kaggle.py`
(chạy trên máy có Spark), upload thư mục kaggle_exports/ lên làm Kaggle Dataset, rồi sửa
TRAIN_PATH/TEST_PATH bên dưới cho khớp tên dataset bạn tạo.

Input khác build_forecast_features() (Tầng 1/2): mỗi dòng là 1 CHUỖI 24 giờ AQI liên
tiếp (seq_0=cũ nhất .. seq_23=hiện tại), không phải lag rời rạc 1h/3h/24h — CNN-LSTM cần
chuỗi thật để Conv1D bắt pattern cục bộ + LSTM bắt phụ thuộc dài hạn.

So sánh với Tầng 1 (SGD) và Tầng 2 (Random Forest) — xem docs/experiments.md mục 5.2.
Cùng format RMSE/MAE/R² để so sánh trực tiếp 3 tầng.

Sau khi chạy: chép RMSE/MAE/R² in ra vào docs/experiments.md mục 5.2 — Kaggle không tự
đồng bộ ngược lại máy dev.
"""
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from tensorflow.keras import layers, models

TRAIN_PATH = "/kaggle/input/<ten-dataset-ban-tao>/sequence_train.csv"  # SUA LAI DUONG DAN
TEST_PATH = "/kaggle/input/<ten-dataset-ban-tao>/sequence_test.csv"  # SUA LAI DUONG DAN

WINDOW_HOURS = 24
SEQ_COLS = [f"seq_{i}" for i in range(WINDOW_HOURS)]
TARGET_COL = "target_aqi_24h"

# Tham chieu ket qua Tang 1/2 da co (docs/experiments.md muc 5.2) de in bang so sanh cuoi cung
TIER_1_2_RESULTS = [
    ("SGD (T1)", 32.05, 20.56, 0.5382),
    ("RandomForest (T2)", 31.87, 20.64, 0.5431),
]


def load_data():
    return pd.read_csv(TRAIN_PATH), pd.read_csv(TEST_PATH)


def to_arrays(df, scaler=None, fit=False):
    """Scale TOÀN CỤC (flatten hết giá trị AQI trong chuỗi rồi fit 1 scaler chung) —
    đúng vì mọi vị trí trong chuỗi đều đo CÙNG 1 đại lượng (AQI), không phải nhiều
    feature khác đơn vị như ở tier 1/2 (không cần scale riêng từng cột seq_i)."""
    X = df[SEQ_COLS].values.astype("float32")
    y = df[TARGET_COL].values.astype("float32")
    if fit:
        scaler = StandardScaler().fit(X.reshape(-1, 1))
    X_scaled = scaler.transform(X.reshape(-1, 1)).reshape(X.shape)
    return X_scaled[..., np.newaxis], y, scaler  # shape (n, WINDOW_HOURS, 1)


def build_model(window_hours=WINDOW_HOURS):
    model = models.Sequential([
        layers.Input(shape=(window_hours, 1)),
        layers.Conv1D(filters=32, kernel_size=3, activation="relu", padding="causal"),
        layers.Conv1D(filters=32, kernel_size=3, activation="relu", padding="causal"),
        layers.LSTM(32, return_sequences=False),
        layers.Dense(16, activation="relu"),
        layers.Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    return model


def main():
    train_df, test_df = load_data()
    print(f"Train: {len(train_df)} dong | Test: {len(test_df)} dong")

    X_train, y_train, scaler = to_arrays(train_df, fit=True)
    X_test, y_test, _ = to_arrays(test_df, scaler=scaler)

    model = build_model()
    model.summary()

    model.fit(
        X_train, y_train,
        validation_split=0.1,
        epochs=50,
        batch_size=64,
        callbacks=[tf.keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True)],
        verbose=2,
    )

    y_pred = model.predict(X_test).flatten()
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("\n=== Bang so sanh day du 3 tang ===")
    print(f"{'':<20}{'RMSE':>10}{'MAE':>10}{'R2':>10}")
    for name, r, m, r2_ref in TIER_1_2_RESULTS:
        print(f"{name:<20}{r:>10.2f}{m:>10.2f}{r2_ref:>10.4f}")
    print(f"{'CNN-LSTM (T3)':<20}{rmse:>10.2f}{mae:>10.2f}{r2:>10.4f}")

    model.save("cnn_lstm_model.keras")
    print("\nDa luu cnn_lstm_model.keras — chep bang so sanh o tren vao docs/experiments.md muc 5.2")


if __name__ == "__main__":
    main()
