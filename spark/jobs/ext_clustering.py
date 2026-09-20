"""
NHÁNH MỞ RỘNG (ngoài lõi) — phân cụm vùng. NGƯỜI B · M4.

Kế hoạch 3 tầng (đã chốt với người dùng — làm CẢ BA rồi so, KHÔNG phải "chọn 1" như
WORKPLAN gốc ghi, vì đã xác nhận chấp nhận tốn thêm thời gian). Cả 3 tầng chạy trong file này:
  Tầng 1: K-means (Spark MLlib) — baseline. (fit_kmeans)
  Tầng 2: GMM và Bisecting K-means (Spark MLlib) — chạy CẢ HAI rồi so với K-means để
          chọn thuật toán hợp dữ liệu, không chọn bừa. (fit_gmm, fit_bisecting_kmeans)
  Tầng 3: DBSCAN và HDBSCAN (scikit-learn ở driver — Spark MLlib không có; dữ liệu
          (city, tháng) rất nhỏ nên không cần phân tán). (fit_dbscan, fit_hdbscan)
  compare_clustering_algorithms() chạy cả 5 và chọn thuật toán thắng theo silhouette.

SO SÁNH CÔNG BẰNG GIỮA 5 THUẬT TOÁN:
  - Silhouette của Spark (ClusteringEvaluator) mặc định dùng khoảng cách squaredEuclidean,
    sklearn dùng Euclidean -> hai con số KHÔNG so được với nhau. Nên bảng so sánh cuối tính
    lại silhouette cho cả 5 bằng common_silhouette() (Euclidean, cùng ma trận feature đã
    chuẩn hoá). Silhouette nội bộ của Spark chỉ còn dùng để chọn k trong TỪNG thuật toán.
  - DBSCAN/HDBSCAN gán nhãn -1 cho điểm nhiễu; silhouette bỏ các điểm này nên thuật toán
    vứt nhiều điểm sẽ được điểm cao giả tạo. Vì vậy chỉ nhận cấu hình có tỉ lệ nhiễu
    <= MAX_NOISE_FRACTION, và bảng tổng kết in kèm tỉ lệ nhiễu của mỗi thuật toán.
  - Silhouette còn thưởng cho việc băm thành nhiều cụm siêu nhỏ, nên tầng 3 bị giới hạn
    <= MAX_CLUSTERS cụm, cùng ngân sách số cụm với dải k của tầng 1/2.

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
        --json-out <file>  — thêm file ext_clusters.json cho demo (schema C8.1 trong CONTRACTS.md)

Chạy local:
  python jobs/ext_clustering.py --input /tmp/aqi --output /tmp/clusters --json-out /tmp/ext_clusters.json
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
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

# Tầng 3: lưới tham số quét (rẻ vì chỉ ~vài nghìn điểm city x tháng); lưới rộng để dùng được
# cả với dữ liệu mẫu 20 điểm lẫn dữ liệu thật ~2.400 điểm.
DBSCAN_EPS = (0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0)
DBSCAN_MIN_SAMPLES = (2, 3, 5, 8)
HDBSCAN_MIN_CLUSTER_SIZE = (2, 3, 5, 8, 12)
MAX_NOISE_FRACTION = 0.2
# Tầng 1/2 chỉ thử k <= max(K_CANDIDATES); tầng 3 cũng bị giới hạn số cụm như vậy. Không có
# giới hạn này silhouette thưởng cho việc băm thành nhiều cụm siêu nhỏ (đã gặp: HDBSCAN
# min_cluster_size=2 cho 7 cụm, silhouette 0.87, thắng cách chia đúng 2 cụm ở 0.74).
MAX_CLUSTERS = max(K_CANDIDATES)


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


def common_silhouette(X, labels):
    """Silhouette Euclidean bỏ điểm nhiễu (-1) — MỘT thước đo chung cho cả 5 thuật toán.
    Trả None nếu không có >= 2 cụm thật (sklearn không tính được)."""
    from sklearn.metrics import silhouette_score

    mask = labels != -1
    n_real_clusters = len(set(labels[mask]))
    if n_real_clusters < 2 or int(mask.sum()) <= n_real_clusters:
        return None
    return float(silhouette_score(X[mask], labels[mask]))


def _matrix_and_labels(result_df):
    """Ma trận feature đã chuẩn hoá + nhãn cụm, lấy trong CÙNG một lần collect nên thứ tự khớp."""
    import numpy as np

    pdf = result_df.select("features", "cluster").toPandas()
    return np.vstack([v.toArray() for v in pdf["features"]]), pdf["cluster"].to_numpy()


def _sweep_density(X, name, candidates, fit_labels):
    """Quét lưới tham số, chọn silhouette cao nhất trong số cấu hình có tỉ lệ nhiễu hợp lệ."""
    best = None
    print(f"\n=== {name}: quét tham số ===")
    for params in candidates:
        try:
            labels = fit_labels(X, **params)
        except ValueError:
            continue  # tham số không hợp lệ với số điểm hiện có (vd min_cluster_size > n)
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        noise = float((labels == -1).mean())
        score = common_silhouette(X, labels)
        shown = f"{score:.4f}" if score is not None else "N/A"
        print(f"  {params}: n_cum={n_clusters}, nhieu={noise:.0%}, silhouette={shown}")
        valid = noise <= MAX_NOISE_FRACTION and n_clusters <= MAX_CLUSTERS
        if score is not None and valid and (best is None or score > best[0]):
            best = (score, params, labels)
    if best is None:
        raise ValueError(
            f"{name}: không cấu hình nào cho 2..{MAX_CLUSTERS} cụm thật với nhiễu <= {MAX_NOISE_FRACTION:.0%}"
        )
    print(f"-> Chọn {best[1]} (silhouette={best[0]:.4f})\n")
    return best


def _fit_density(df, name, candidates, fit_labels):
    import numpy as np

    scaled = _assemble_and_scale(df)
    pdf = scaled.drop("features_raw").toPandas()
    X = np.vstack([v.toArray() for v in pdf["features"]])
    score, params, labels = _sweep_density(X, name, candidates, fit_labels)
    pdf["cluster"] = labels
    result = df.sparkSession.createDataFrame(pdf.drop(columns=["features"]))
    return result, None, params, score


def fit_dbscan(df, seed=42):
    """Tầng 3 — DBSCAN. Trả (result, None, tham_số, silhouette); nhãn -1 = điểm nhiễu."""
    from sklearn.cluster import DBSCAN

    candidates = [{"eps": e, "min_samples": m} for e in DBSCAN_EPS for m in DBSCAN_MIN_SAMPLES]
    return _fit_density(df, "DBSCAN", candidates, lambda X, **p: DBSCAN(**p).fit_predict(X))


def fit_hdbscan(df, seed=42):
    """Tầng 3 — HDBSCAN (sklearn >= 1.3). Trả (result, None, tham_số, silhouette)."""
    from sklearn.cluster import HDBSCAN

    candidates = [{"min_cluster_size": m} for m in HDBSCAN_MIN_CLUSTER_SIZE]
    return _fit_density(df, "HDBSCAN", candidates, lambda X, **p: HDBSCAN(**p).fit_predict(X))


def compare_clustering_algorithms(df, seed=42):
    """Chạy cả 5 thuật toán (K-means; GMM, Bisecting; DBSCAN, HDBSCAN), so bằng cùng một
    silhouette (common_silhouette), trả về (tên thắng, result, model, tham số, silhouette).
    model là None nếu thắng là DBSCAN/HDBSCAN. Thuật toán không chạy được trên dữ liệu này
    bị bỏ qua thay vì làm hỏng cả lệnh so sánh."""
    spark_algos = {"KMeans": fit_kmeans, "GaussianMixture": fit_gmm, "BisectingKMeans": fit_bisecting_kmeans}
    density_algos = {"DBSCAN": fit_dbscan, "HDBSCAN": fit_hdbscan}

    runs = {}
    print("\n########## So sánh thuật toán phân cụm (Tầng 1 + 2 + 3) ##########")
    for name, fn in spark_algos.items():
        result, model, k, _ = fn(df, seed=seed)
        X, labels = _matrix_and_labels(result)
        runs[name] = (result, model, k, common_silhouette(X, labels), len(set(labels)), 0.0)

    for name, fn in density_algos.items():
        try:
            result, model, params, score = fn(df, seed=seed)
        except ValueError as exc:
            print(f"  {name}: bỏ qua ({exc})")
            continue
        labels = result.select("cluster").toPandas()["cluster"].to_numpy()
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        runs[name] = (result, model, params, score, n_clusters, float((labels == -1).mean()))

    scored = {n: r for n, r in runs.items() if r[3] is not None}
    if not scored:
        raise ValueError("Không thuật toán nào cho >= 2 cụm thật trên dữ liệu này")

    print("=== Tổng kết (cùng thước đo: silhouette Euclidean, bỏ điểm nhiễu) ===")
    print(f"  {'Thuật toán':<17}{'Tham số':<38}{'Số cụm':>7}{'Nhiễu':>8}{'Silhouette':>12}")
    for name, (_, _, params, score, n_clusters, noise) in runs.items():
        shown = f"{score:.4f}" if score is not None else "N/A"
        print(f"  {name:<17}{str(params):<38}{n_clusters:>7}{noise:>8.0%}{shown:>12}")

    best_name = max(scored, key=lambda n: scored[n][3])
    best_result, best_model, best_params, best_silhouette, _, _ = scored[best_name]
    print(f"-> Thắng: {best_name} ({best_params}, silhouette={best_silhouette:.4f})")
    print("#" * 70 + "\n")

    return best_name, best_result, best_model, best_params, best_silhouette


CLUSTER_JSON_SCHEMA_VERSION = "1.0"
CLUSTER_JSON_COLS = ["city", "country", "month", "lat", "lon", "avg_pm2_5", "avg_pm10", "avg_o3", "avg_no2", "cluster"]


def export_clusters_json(result, algorithm, params, silhouette, path):
    """Ghi ext_clusters.json (schema C8.1). params: dict, hoặc số k (thuật toán Spark) -> {"k": k}.
    cluster = -1 là điểm nhiễu của DBSCAN/HDBSCAN; n_clusters không tính nhóm nhiễu."""
    pdf = result.select(*CLUSTER_JSON_COLS).orderBy("cluster", "city", "month").toPandas()
    rows = json.loads(pdf.round(4).to_json(orient="records"))  # NaN -> null, kiểu số chuẩn của JSON
    payload = {
        "schema_version": CLUSTER_JSON_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "algorithm": algorithm,
        "params": params if isinstance(params, dict) else {"k": params},
        "silhouette": None if silhouette is None else round(float(silhouette), 4),
        "n_clusters": len({r["cluster"] for r in rows} - {-1}),
        "rows": rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--k", type=int, default=None, help="Số cụm — bỏ trống để tự chọn bằng silhouette score")
    ap.add_argument("--json-out", default=None,
                    help="đường dẫn file ext_clusters.json (trong container, không phải HDFS) cho demo")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("ext_clustering")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    features = build_city_month_features(df).cache()
    n_features = features.count()
    print(f"So dong (city x thang): {n_features}")

    if args.k is not None:
        # k co dinh, nguoi dung tu chon -> giu tang 1 (KMeans) don gian, khong so sanh
        result, model, k, silhouette = fit_kmeans(features, k=args.k)
        best_name = "KMeans"
        print(f"K-means k={k}, silhouette={silhouette:.4f}")
    else:
        best_name, result, model, k, silhouette = compare_clustering_algorithms(features)
        print(f"Dùng kết quả: {best_name} (tham số={k}, silhouette={silhouette:.4f})")

    print("\n=== Phân cụm theo thành phố/tháng ===")
    result.select("city", "month", "avg_pm2_5", "avg_pm10", "cluster").orderBy("cluster", "city", "month").show(50, truncate=False)

    (
        result.select("city", "country", "month", *FEATURE_COLS[:-3], "lat", "lon", "cluster")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(args.output)
    )
    print(f"Da ghi ket qua vao {args.output}")

    if args.json_out:
        payload = export_clusters_json(result, best_name, k, silhouette, args.json_out)
        print(f"Da ghi {args.json_out}: {payload['algorithm']}, {payload['n_clusters']} cum, {len(payload['rows'])} dong")

    features.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
