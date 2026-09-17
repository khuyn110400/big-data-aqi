"""
NHÁNH MỞ RỘNG (ngoài lõi) — dự báo AQI 24h. NGƯỜI B · M4.

Kế hoạch 3 tầng (đã chốt — làm lần lượt, KHÔNG phải "chọn 1" như WORKPLAN gốc):
  Tầng 1        : thay LaSVM bằng SGDRegressor(loss="epsilon_insensitive") — baseline.
  Tầng 2 (file này): Random Forest (Spark MLlib, native) — train PHÂN TÁN THẬT trên
                      toàn bộ Spark DataFrame, không chunk theo tháng + toPandas()
                      như tầng 1 — đây chính là điểm phải chứng minh: scale tốt hơn.
  Tầng 3 (TẠM HOÃN): CNN-LSTM (ngoài phạm vi Spark MLlib, cần thêm framework riêng —
                      TensorFlow/PyTorch, chưa cài). Tạm dừng ở tầng 2 — máy dev đã
                      gặp OOM/treo nhiều lần khi chạy tầng 1+2 (RAM cạn kiệt do nhiều
                      app khác mở cùng lúc), CNN-LSTM sẽ còn nặng hơn nhiều. Hoãn tới
                      khi có máy/server rảnh hơn (hoặc Kaggle GPU). Tầng 1+2 đã xong,
                      test đầy đủ, không bị ảnh hưởng bởi quyết định hoãn này.

VÌ SAO KHÔNG DÙNG LaSVM/SVR THẬT:
  Spark MLlib không có SVM hồi quy (chỉ có LinearSVC cho phân loại), và cách cập
  nhật của LaSVM (online, tăng dần theo từng điểm) không parallelize được kiểu
  Spark. scikit-learn có SVR nhưng độ phức tạp O(n²)-O(n³) theo số dòng — ở quy
  mô backfill thật (200 trạm x 3-5 năm ~ 5-8 triệu dòng) sẽ treo máy. SGDRegressor
  với epsilon_insensitive loss chính là SVM TUYẾN TÍNH học theo kiểu ONLINE/TĂNG
  DẦN (partial_fit) — gần đúng tinh thần LaSVM nhất trong các công cụ có sẵn, và
  scale được tới hàng triệu dòng vì không bao giờ cần giữ hết dữ liệu trong bộ nhớ
  cùng lúc.

CÁCH "ONLINE" ĐƯỢC GIỮ THẬT (không phải chỉ đặt tên): dữ liệu được chia theo
THÁNG LỊCH (năm-tháng thật, KHÁC với cột feature "month" 1-12 dùng cho seasonality)
và train.py gọi partial_fit() TUẦN TỰ theo từng tháng — driver không bao giờ giữ
quá 1 tháng dữ liệu trong bộ nhớ cùng lúc. Đây là cách "batch đổ xuống" sau này
(nhiều năm dữ liệu backfill, hoặc dữ liệu streaming mới liên tục) vẫn train được
mà không phải load hết vào RAM.

Feature: aqi_lag_1h, aqi_lag_3h, aqi_lag_24h, pm2_5, pm10, o3, no2, so2, co,
         hour_of_day, month, lat, lon.
Target : AQI 24 giờ SAU thời điểm hiện tại (dự báo, không phải nowcast).

Train/test split THEO THỜI GIAN (không random) — chống rò rỉ tương lai vào quá
khứ, đúng nguyên tắc time-series: N tháng cuối làm test, còn lại làm train.

Input : /air-quality/aqi/    (output Pha 2, schema C3)
Output: in RMSE/MAE/R2 ra console, ghi model (pickle) + feature importance-ish ra --output

Chạy local:
  python jobs/ext_forecast.py --input /tmp/aqi --output /tmp/forecast_model
"""
import argparse
import os
import pickle
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

FEATURE_COLS = [
    "aqi_lag_1h", "aqi_lag_3h", "aqi_lag_24h",
    "pm2_5", "pm10", "o3", "no2", "so2", "co",
    "hour_of_day", "month", "lat", "lon",
]
TARGET_COL = "target_aqi_24h"
TEST_FRACTION = 0.2  # 20% khoang thoi gian cuoi lam test, theo dung nguyen tac time-series


def build_forecast_features(df):
    """Lag feature + target (AQI 24h sau) theo từng trạm. Loại dòng thiếu lag/target
    (rìa đầu/cuối chuỗi mỗi trạm — không đủ lịch sử hoặc không đủ tương lai để có nhãn)."""
    w = Window.partitionBy("station_id").orderBy("ts_epoch")

    df = df.withColumn("ts_epoch", F.unix_timestamp("ts_utc"))
    df = df.withColumn("chunk_month", F.date_trunc("month", "ts_utc"))  # nam-thang THAT, de sap xep tuan tu
    df = df.withColumn("hour_of_day", F.hour("ts_utc"))
    df = df.withColumn("month", F.month("ts_utc"))  # 1-12, feature mua vu (khac chunk_month)

    df = df.withColumn("aqi_lag_1h", F.lag("aqi", 1).over(w))
    df = df.withColumn("aqi_lag_3h", F.lag("aqi", 3).over(w))
    df = df.withColumn("aqi_lag_24h", F.lag("aqi", 24).over(w))
    df = df.withColumn(TARGET_COL, F.lead("aqi", 24).over(w))

    cols = ["station_id", "chunk_month"] + FEATURE_COLS + [TARGET_COL]
    return df.select(*cols).na.drop()


def time_based_split(df, test_fraction=TEST_FRACTION):
    """N tháng cuối (theo chunk_month) làm test — KHÔNG random split, tránh rò rỉ tương lai."""
    months = sorted(r["chunk_month"] for r in df.select("chunk_month").distinct().collect())
    n_test = max(1, int(len(months) * test_fraction))
    train_months, test_months = months[:-n_test], months[-n_test:]
    print(f"Train: {len(train_months)} tháng ({train_months[0]}..{train_months[-1]}) "
          f"| Test: {len(test_months)} tháng ({test_months[0]}..{test_months[-1]})")
    return train_months, test_months


def train_sgd_online(spark_df, months, seed=42, epochs=30):
    """partial_fit() TUẦN TỰ theo từng tháng — driver không bao giờ giữ quá 1 tháng
    dữ liệu cùng lúc trong bộ nhớ (vẫn đúng tinh thần 'online/tăng dần', không phải
    load hết vào RAM một lần như .fit() thường).

    epochs>1: lặp lại nhiều lượt qua CÙNG các tháng đã có (không phải xem thêm dữ
    liệu mới) — cần thiết vì bản thân SGDRegressor.fit() mặc định cũng chạy nhiều
    epoch nội bộ để hội tụ; gọi partial_fit() đúng 1 lần/tháng thường không đủ.
    Không vi phạm tinh thần "không giữ hết dữ liệu cùng lúc" vì mỗi lượt vẫn chỉ
    đọc lại 1 tháng tại 1 thời điểm (Spark filter + toPandas() theo từng tháng)."""
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import StandardScaler

    model = SGDRegressor(loss="epsilon_insensitive", epsilon=5.0, random_state=seed, max_iter=1)
    scaler = StandardScaler()

    # fit scaler 1 lan duy nhat tren thang dau (dung tinh than online: khong "nhin truoc"
    # toan bo du lieu de chuan hoa, chi dung thong ke cua du lieu da thay)
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


def train_random_forest(train_df, seed=42, num_trees=100, max_depth=10):
    """Tầng 2 — train PHÂN TÁN THẬT trên toàn bộ train_df cùng lúc (không chunk theo
    tháng, không toPandas() — khác hẳn cách tầng 1 phải làm vì SGD/SVR không phân tán
    được). Đây là điểm tier 2 phải chứng minh: RandomForestRegressor tự distribute
    việc xây cây quyết định qua các partition của Spark."""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    assembled = assembler.transform(train_df)
    rf = RandomForestRegressor(
        featuresCol="features", labelCol=TARGET_COL, seed=seed,
        numTrees=num_trees, maxDepth=max_depth,
    )
    model = rf.fit(assembled)
    return model, assembler


def evaluate_rf(model, assembler, test_df):
    """Cùng format kết quả {"rmse","mae","r2","n_test"} như evaluate() của tầng 1
    để so sánh trực tiếp 2 tầng."""
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("ext_forecast")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        # RandomForestRegressor (Tang 2) OOM voi driver mac dinh -Xmx1g (PySpark tu dat
        # khi khong cau hinh) du data nho - findBestSplits collect thong ke ve driver,
        # ton nhieu bo nho hon DataFrame op thuong. Tang len de tranh OOM.
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    features = build_forecast_features(df).cache()
    print(f"Tổng số dòng có đủ feature+label: {features.count()}")

    train_months, test_months = time_based_split(features)

    sgd_model, scaler = train_sgd_online(features, train_months)
    sgd_metrics = evaluate(sgd_model, scaler, features, test_months)

    train_df = features.filter(F.col("chunk_month").isin(train_months))
    test_df = features.filter(F.col("chunk_month").isin(test_months))
    rf_model, assembler = train_random_forest(train_df)
    rf_metrics = evaluate_rf(rf_model, assembler, test_df)

    print("\n=== So sánh Tầng 1 (SGD, thay LaSVM) vs Tầng 2 (Random Forest) ===")
    print(f"{'':<12}{'RMSE':>10}{'MAE':>10}{'R2':>10}{'n_test':>10}")
    print(f"{'SGD (T1)':<12}{sgd_metrics['rmse']:>10.2f}{sgd_metrics['mae']:>10.2f}{sgd_metrics['r2']:>10.4f}{sgd_metrics['n_test']:>10}")
    print(f"{'RF (T2)':<12}{rf_metrics['rmse']:>10.2f}{rf_metrics['mae']:>10.2f}{rf_metrics['r2']:>10.4f}{rf_metrics['n_test']:>10}")
    print("Đối chiếu: LaSVM baseline trong paper Ghaemi 2015 có R2 ~ 0.81 (chạy trên dữ liệu khác — không so trực tiếp 1-1)")
    print_feature_importances(rf_model)
    print("=" * 60)

    os.makedirs(args.output, exist_ok=True)
    with open(os.path.join(args.output, "sgd_baseline.pkl"), "wb") as f:
        pickle.dump({"model": sgd_model, "scaler": scaler, "feature_cols": FEATURE_COLS, "metrics": sgd_metrics}, f)
    rf_model.write().overwrite().save(os.path.join(args.output, "rf_model"))
    print(f"Đã ghi model vào {args.output}/ (sgd_baseline.pkl, rf_model/)")

    spark.stop()


if __name__ == "__main__":
    main()
