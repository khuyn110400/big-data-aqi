"""
NHÁNH MỞ RỘNG (ngoài lõi) — phân cụm vùng. NGƯỜI B · M4.

Kế hoạch 3 tầng (đã chốt với người dùng — làm lần lượt, KHÔNG phải "chọn 1" như
WORKPLAN gốc ghi, vì đã xác nhận chấp nhận tốn thêm thời gian):
  Tầng 1        : K-means (Spark MLlib, native) — baseline. (fit_kmeans)
  Tầng 2 (file này): GMM và Bisecting K-means (Spark MLlib, native) — chạy CẢ HAI
                      rồi so silhouette với K-means để chọn thuật toán tốt nhất
                      cho đúng dữ liệu này, không chọn bừa 1 trong 2.
  Tầng 3 (TẠM HOÃN): DBSCAN/HDBSCAN — Spark MLlib KHÔNG có, sẽ chạy qua scikit-learn
                      ở driver (dữ liệu (city, tháng) rất nhỏ, không cần phân tán).
                      Tạm dừng ở tầng 2 — máy dev đã gặp OOM/treo nhiều lần khi chạy
                      tầng 1+2 (RAM cạn kiệt do nhiều app khác mở cùng lúc), nên hoãn
                      thêm tầng 3 cho tới khi có máy/server rảnh hơn. Tầng 1+2 đã xong,
                      test đầy đủ, không bị ảnh hưởng bởi quyết định hoãn này.

Feature: avg_pm2_5, avg_pm10, avg_o3, avg_no2 (trung bình theo (city, tháng))
         + lat, lon, tháng (1-12).

QUYẾT ĐỊNH THIẾT KẾ:
  - Nhóm theo (city, THÁNG) chứ không phải (city, năm-tháng) — cố tình gộp cùng
    tháng qua nhiều năm để bắt đúng pattern MÙA VỤ (climate/seasonal), khớp gợi ý
    "thêm chiều mùa" đã ghi trong TODO cũ của phase3_aggregate.py. Nếu dữ liệu
    chỉ có 1 năm (như sample hiện tại), kết quả tương đương group theo tháng lịch.
  - k (số cụm) KHÔNG hardcode tuỳ tiện — chọn bằng silhouette score trên 1 dải k,
    in ra để người đọc thấy rõ căn cứ chọn (không bịa số).
  - StandardScaler bắt buộc trước KMeans: avg_pm2_5 (đơn vị µg/m³, hàng chục) và
    lat/lon (hàng chục, nhưng ý nghĩa khác hẳn) lệch scale nhau rất nhiều, không
    chuẩn hoá thì lat/lon sẽ áp đảo khoảng cách Euclidean một cách giả tạo.
  - Output KHÔNG có trong CONTRACTS.md (nhánh mở rộng, không phải C2) — dùng
    namespace riêng /air-quality/ext/clusters/ để tách bạch, không đụng contract.

Input : /air-quality/aqi/        (output Pha 2, schema C3)
Output: /air-quality/ext/clusters/  — city, month, avg_*, cluster label

Chạy local:
  python jobs/ext_clustering.py --input /tmp/aqi --output /tmp/clusters
"""
import argparse
import os
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.ml.clustering import BisectingKMeans, GaussianMixture, KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURE_COLS = ["avg_pm2_5", "avg_pm10", "avg_o3", "avg_no2", "lat", "lon", "month"]
K_CANDIDATES = [2, 3, 4, 5, 6]


def build_city_month_features(df):
    """(city, tháng) -> trung bình nồng độ các chất chính + toạ độ."""
    df = df.withColumn("month", F.month("ts_utc"))
    return df.groupBy("city", "country", "month").agg(
        F.avg("pm2_5").alias("avg_pm2_5"),
        F.avg("pm10").alias("avg_pm10"),
        F.avg("o3").alias("avg_o3"),
        F.avg("no2").alias("avg_no2"),
        F.avg("lat").alias("lat"),
        F.avg("lon").alias("lon"),
        F.count("*").alias("n_hours"),
    )


def _assemble_and_scale(df):
    # withMean=False (KHONG tru trung binh): da phat hien Spark BisectingKMeans suy bien
    # ve DUY NHAT 1 cum khi feature vector bi mean-center (co gia tri am) - test truc
    # tiep xac nhan withMean=True lam BisectingKMeans luon tra ve 1 cum bat ke k/seed, con
    # withMean=False tach dung. Doi voi KMeans/GMM, ket qua GIONG HET nhau du co centering
    # hay khong (khoang cach Euclidean bat bien khi dich chuyen deu tat ca diem cung 1 vector)
    # -> day la fix an toan tuyet doi, khong danh doi gi cho 2 thuat toan kia.
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)
    assembled = assembler.transform(df)
    return scaler.fit(assembled).transform(assembled)


def _pick_best_k(scaled_df, algo_cls, k_candidates=K_CANDIDATES, seed=42):
    """Chọn k bằng silhouette score cho MỘT thuật toán bất kỳ (KMeans/GaussianMixture/
    BisectingKMeans — cả 3 đều nhận k/seed/featuresCol/predictionCol giống nhau trong
    Spark MLlib nên tổng quát hoá được, không viết lại cho từng thuật toán).
    KHÔNG hardcode k tuỳ tiện. In bảng để có căn cứ."""
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
    scores = {}
    for k in k_candidates:
        if k >= scaled_df.count():
            continue  # khong du diem du lieu cho k cum
        try:
            model = algo_cls(k=k, seed=seed, featuresCol="features", predictionCol="cluster").fit(scaled_df)
            result = model.transform(scaled_df)
            scores[k] = evaluator.evaluate(result)
        except Exception as e:
            # BisectingKMeans (va ly thuyet ca GMM) co the sinh cum suy bien (thuc te
            # chi con 1 cum) tren du lieu nho/gan trung nhau -> ClusteringEvaluator
            # crash thay vi tra diem thap. Bo qua k nay, thu k khac, dung crash ca lenh so sanh.
            print(f"  k={k}: bo qua (loi khi fit/evaluate - {type(e).__name__})")

    if not scores:
        raise ValueError(f"{algo_cls.__name__}: khong co k nao trong {k_candidates} chay duoc tren du lieu nay")

    print(f"\n=== Silhouette score theo k — {algo_cls.__name__} (càng gần 1 càng tốt) ===")
    for k, s in scores.items():
        print(f"  k={k}: {s:.4f}")
    best_k = max(scores, key=scores.get)
    print(f"-> Chọn k={best_k} (silhouette cao nhất)\n")
    return best_k


def pick_best_k(scaled_df, k_candidates=K_CANDIDATES, seed=42):
    """Giữ lại cho tương thích ngược (test tầng 1 dùng gián tiếp qua fit_kmeans)."""
    return _pick_best_k(scaled_df, KMeans, k_candidates=k_candidates, seed=seed)


def _fit_generic(df, algo_cls, k=None, seed=42):
    scaled = _assemble_and_scale(df)
    if k is None:
        k = _pick_best_k(scaled, algo_cls, seed=seed)

    model = algo_cls(k=k, seed=seed, featuresCol="features", predictionCol="cluster").fit(scaled)
    result = model.transform(scaled)

    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
    silhouette = evaluator.evaluate(result)
    return result, model, k, silhouette


def fit_kmeans(df, k=None, seed=42):
    """Tầng 1 — baseline."""
    return _fit_generic(df, KMeans, k=k, seed=seed)


def fit_gmm(df, k=None, seed=42):
    """Tầng 2 — Gaussian Mixture Model."""
    return _fit_generic(df, GaussianMixture, k=k, seed=seed)


def fit_bisecting_kmeans(df, k=None, seed=42):
    """Tầng 2 — Bisecting K-means."""
    return _fit_generic(df, BisectingKMeans, k=k, seed=seed)


def compare_clustering_algorithms(df, seed=42):
    """Chạy K-means (tầng 1) + GMM + Bisecting K-means (tầng 2), so silhouette,
    trả về (tên thuật toán thắng, result, model, k, silhouette) — không chọn bừa
    GMM hay Bisecting, để dữ liệu tự quyết định thuật toán nào hợp hơn."""
    candidates = {
        "KMeans": fit_kmeans,
        "GaussianMixture": fit_gmm,
        "BisectingKMeans": fit_bisecting_kmeans,
    }
    runs = {}
    print("\n########## So sánh thuật toán phân cụm (Tầng 1 vs Tầng 2) ##########")
    for name, fn in candidates.items():
        result, model, k, silhouette = fn(df, seed=seed)
        runs[name] = (result, model, k, silhouette)

    print("=== Tổng kết ===")
    for name, (_, _, k, silhouette) in runs.items():
        print(f"  {name:<16}: k={k}, silhouette={silhouette:.4f}")

    best_name = max(runs, key=lambda n: runs[n][3])
    best_result, best_model, best_k, best_silhouette = runs[best_name]
    print(f"-> Thắng: {best_name} (k={best_k}, silhouette={best_silhouette:.4f})")
    print("#" * 70 + "\n")

    return best_name, best_result, best_model, best_k, best_silhouette


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--k", type=int, default=None, help="Số cụm — bỏ trống để tự chọn bằng silhouette score")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("ext_clustering")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    features = build_city_month_features(df)
    print(f"So dong (city x thang): {features.count()}")

    if args.k is not None:
        # k co dinh, nguoi dung tu chon -> giu tang 1 (KMeans) don gian, khong so sanh
        result, model, k, silhouette = fit_kmeans(features, k=args.k)
        print(f"K-means k={k}, silhouette={silhouette:.4f}")
    else:
        best_name, result, model, k, silhouette = compare_clustering_algorithms(features)
        print(f"Dùng kết quả: {best_name} (k={k}, silhouette={silhouette:.4f})")

    print("\n=== Phân cụm theo thành phố/tháng ===")
    result.select("city", "month", "avg_pm2_5", "avg_pm10", "cluster").orderBy("cluster", "city", "month").show(50, truncate=False)

    (
        result.select("city", "country", "month", *FEATURE_COLS[:-3], "lat", "lon", "cluster")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(args.output)
    )
    print(f"Da ghi ket qua vao {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
