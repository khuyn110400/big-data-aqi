"""
TẦNG 3 — DBSCAN + HDBSCAN cho phân cụm vùng. Chạy TRÊN KAGGLE — KHÔNG cần PySpark
(dữ liệu (city, tháng) đã được gộp sẵn, rất nhỏ, không cần phân tán).

Input: city_month_features.csv — xuất bằng `python jobs/export_for_kaggle.py` (chạy trên
máy có Spark), upload thư mục kaggle_exports/ lên làm Kaggle Dataset, rồi sửa INPUT_PATH
bên dưới cho khớp tên dataset bạn tạo.

So sánh với Tầng 1/2 đã chạy (xem docs/experiments.md mục 5.1, trên data mẫu):
  KMeans (T1)          k=2  silhouette=0.7127  <- thắng
  GMM (T2)             k=2  silhouette=0.3287
  BisectingKMeans (T2) k=4  silhouette=0.4520

Cài trên Kaggle (sklearn có sẵn, cần thêm gói hdbscan — hoặc dùng sklearn.cluster.HDBSCAN
nếu sklearn >= 1.3, script tự dò):
  !pip install hdbscan -q

Sau khi chạy: chép RMSE/MAE/R2 (à, silhouette) in ra vào docs/experiments.md mục 5.1 —
Kaggle không tự đồng bộ ngược lại máy dev.
"""
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

try:
    import hdbscan as hdbscan_lib

    def _fit_hdbscan(X, min_cluster_size):
        return hdbscan_lib.HDBSCAN(min_cluster_size=min_cluster_size).fit_predict(X)
except ImportError:
    from sklearn.cluster import HDBSCAN as _SKHDBSCAN  # co san tu sklearn >= 1.3

    def _fit_hdbscan(X, min_cluster_size):
        return _SKHDBSCAN(min_cluster_size=min_cluster_size).fit_predict(X)


INPUT_PATH = "/kaggle/input/<ten-dataset-ban-tao>/city_month_features.csv"  # SUA LAI DUONG DAN

FEATURE_COLS = ["avg_pm2_5", "avg_pm10", "avg_o3", "avg_no2", "lat", "lon", "month"]

# Tham chieu ket qua Tang 1/2 da co (docs/experiments.md muc 5.1) de in bang so sanh cuoi cung
TIER_1_2_RESULTS = [
    ("KMeans (T1)", "k=2", 0.7127),
    ("GMM (T2)", "k=2", 0.3287),
    ("BisectingKMeans (T2)", "k=4", 0.4520),
]


def load_and_scale(path=INPUT_PATH):
    df = pd.read_csv(path)
    # with_mean=False: giu dung tinh than nhu ben Spark (ext_clustering.py) — tranh cach
    # ly do tuong tu bug da phat hien o BisectingKMeans ben Spark (xem docstring o do).
    X = StandardScaler(with_mean=False).fit_transform(df[FEATURE_COLS].values)
    return df, X


def _silhouette_excluding_noise(X, labels):
    """DBSCAN/HDBSCAN gan nhan -1 cho diem nhieu — loai khoi tinh silhouette (khong co
    y nghia cum). Can >=2 cum THAT (khong tinh nhieu) moi tinh duoc."""
    mask = labels != -1
    if len(set(labels[mask])) < 2:
        return None
    return silhouette_score(X[mask], labels[mask])


def sweep_dbscan(X, eps_candidates=(0.5, 1.0, 1.5, 2.0, 2.5, 3.0), min_samples_candidates=(2, 3, 4)):
    """Quet eps x min_samples, chon bang silhouette (bo diem nhieu) — KHONG hardcode tuy
    tien, giu dung nguyen tac da ap dung cho k o tang 1/2."""
    best = None
    print("=== DBSCAN: quet eps x min_samples ===")
    for eps in eps_candidates:
        for min_samples in min_samples_candidates:
            labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(X)
            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            n_noise = int((labels == -1).sum())
            score = _silhouette_excluding_noise(X, labels)
            score_str = f"{score:.4f}" if score is not None else "N/A"
            print(f"  eps={eps}, min_samples={min_samples}: n_cluster={n_clusters}, n_noise={n_noise}, silhouette={score_str}")
            if score is not None and (best is None or score > best[0]):
                best = (score, eps, min_samples, labels)
    if best is None:
        raise ValueError("Khong co to hop eps/min_samples nao cho >=2 cum that — thu mo rong eps_candidates")
    score, eps, min_samples, labels = best
    print(f"-> Chon eps={eps}, min_samples={min_samples} (silhouette={score:.4f})\n")
    return labels, eps, min_samples, score


def sweep_hdbscan(X, min_cluster_size_candidates=(2, 3, 4, 5)):
    print("=== HDBSCAN: quet min_cluster_size ===")
    best = None
    for mcs in min_cluster_size_candidates:
        labels = _fit_hdbscan(X, mcs)
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = int((labels == -1).sum())
        score = _silhouette_excluding_noise(X, labels)
        score_str = f"{score:.4f}" if score is not None else "N/A"
        print(f"  min_cluster_size={mcs}: n_cluster={n_clusters}, n_noise={n_noise}, silhouette={score_str}")
        if score is not None and (best is None or score > best[0]):
            best = (score, mcs, labels)
    if best is None:
        raise ValueError("Khong co min_cluster_size nao cho >=2 cum that — thu mo rong candidates")
    score, mcs, labels = best
    print(f"-> Chon min_cluster_size={mcs} (silhouette={score:.4f})\n")
    return labels, mcs, score


def main():
    df, X = load_and_scale()
    print(f"So dong: {len(df)}\n")

    dbscan_labels, eps, min_samples, dbscan_score = sweep_dbscan(X)
    hdbscan_labels, mcs, hdbscan_score = sweep_hdbscan(X)

    print("=== Bang so sanh day du 5 thuat toan (Tang 1+2+3) ===")
    print(f"{'Thuat toan':<22}{'Tham so':<18}{'Silhouette':>12}")
    for name, params, score in TIER_1_2_RESULTS:
        print(f"{name:<22}{params:<18}{score:>12.4f}")
    print(f"{'DBSCAN (T3)':<22}{f'eps={eps},ms={min_samples}':<18}{dbscan_score:>12.4f}")
    print(f"{'HDBSCAN (T3)':<22}{f'min_cl={mcs}':<18}{hdbscan_score:>12.4f}")

    df["dbscan_cluster"] = dbscan_labels
    df["hdbscan_cluster"] = hdbscan_labels
    df.to_csv("clustering_tier3_result.csv", index=False)
    print("\nDa ghi clustering_tier3_result.csv — tai ve va chep bang so sanh vao docs/experiments.md muc 5.1")


if __name__ == "__main__":
    main()
