"""Unit test cho ext_clustering.py (tầng 1 K-means, tầng 2 GMM/Bisecting, tầng 3 DBSCAN/HDBSCAN). NGƯỜI B · M4."""
import json
from datetime import datetime

import numpy as np
import pytest

from ext_clustering import (
    MAX_CLUSTERS,
    build_city_month_features,
    common_silhouette,
    compare_clustering_algorithms,
    export_clusters_json,
    fit_bisecting_kmeans,
    fit_dbscan,
    fit_gmm,
    fit_hdbscan,
    fit_kmeans,
)

AQI_SCHEMA = "city string, country string, ts_utc timestamp, pm2_5 double, pm10 double, o3 double, no2 double, lat double, lon double"


def test_build_city_month_features_gom_dung_theo_thang(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", "VN", datetime(2026, 6, 15), 10.0, 20.0, 30.0, 5.0, 21.0, 105.8),
            ("Hanoi", "VN", datetime(2026, 6, 16), 20.0, 30.0, 40.0, 7.0, 21.0, 105.8),
            ("Hanoi", "VN", datetime(2026, 7, 15), 100.0, 100.0, 100.0, 50.0, 21.0, 105.8),
        ],
        schema=AQI_SCHEMA,
    )
    out = {r["month"]: r for r in build_city_month_features(df).collect()}
    assert out[6]["avg_pm2_5"] == pytest.approx(15.0)  # trung binh (10+20)/2 cua thang 6
    assert out[7]["avg_pm2_5"] == pytest.approx(100.0)  # thang 7 tach rieng, khong lan voi thang 6
    assert out[6]["n_hours"] == 2


def _make_clear_cluster_df(spark):
    # 1 thanh pho o nhiem nang (gap 10x) phai bi tach cum khac voi nhung thanh pho con lai.
    # Dung 6 thang + nhieu diem/thang de GMM uoc luong covariance on dinh. QUAN TRONG:
    # 4 cot chat o nhiem phai BIEN THIEN DOC LAP (khong phai boi so tuyen tinh cua 1 bien
    # duy nhat) -- da tung thu voi pm10=pm2_5*1.3, o3=pm2_5*2,... va phat hien BisectingKMeans
    # khong tach duoc vi sau StandardScaler 4 cot do TRUNG HET NHAU (khong gian suy bien,
    # thieu thuc te). Du lieu that khong bao gio co 4 chat tuong quan tuyen tinh hoan hao.
    rows = []
    for city, base in [("Clean_A", 10.0), ("Clean_B", 12.0), ("Clean_C", 11.0), ("Dirty", 150.0)]:
        for month in range(1, 7):
            t = month - 3.5
            rows.append((
                city, "XX",
                base + t * 0.3,
                base * 1.3 + t * 0.5,
                base * 2.0 - t * 0.4,
                base * 0.5 + t * 0.2,
                10.0, 10.0, month, 24,
            ))
    schema = "city string, country string, avg_pm2_5 double, avg_pm10 double, avg_o3 double, avg_no2 double, lat double, lon double, month int, n_hours long"
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.parametrize("fit_fn", [fit_kmeans, fit_gmm, fit_bisecting_kmeans])
def test_fit_tach_dung_thanh_pho_o_nhiem_khac_biet(spark, fit_fn):
    # property nay phai dung bat ke thuat toan (tang 1 K-means hay tang 2 GMM/Bisecting)
    df = _make_clear_cluster_df(spark)

    result, model, k, silhouette = fit_fn(df, k=2)
    clusters = {r["city"]: r["cluster"] for r in result.select("city", "cluster").distinct().collect()}

    assert clusters["Dirty"] != clusters["Clean_A"]
    assert clusters["Clean_A"] == clusters["Clean_B"] == clusters["Clean_C"]
    assert silhouette > 0.5  # phan cum ro rang, khong mo ho


def test_compare_clustering_algorithms_chon_dung_thuat_toan_tot_nhat(spark):
    df = _make_clear_cluster_df(spark)
    best_name, result, model, k, silhouette = compare_clustering_algorithms(df)

    assert best_name in {"KMeans", "GaussianMixture", "BisectingKMeans", "DBSCAN", "HDBSCAN"}
    assert silhouette > 0.5
    clusters = {r["city"]: r["cluster"] for r in result.select("city", "cluster").distinct().collect()}
    assert clusters["Dirty"] != clusters["Clean_A"]


@pytest.mark.parametrize("fit_fn", [fit_dbscan, fit_hdbscan])
def test_tang3_tach_dung_thanh_pho_o_nhiem_khac_biet(spark, fit_fn):
    df = _make_clear_cluster_df(spark)

    result, model, params, silhouette = fit_fn(df)
    clusters = {r["city"]: r["cluster"] for r in result.select("city", "cluster").distinct().collect()}

    assert model is None  # sklearn, không có model Spark
    assert clusters["Dirty"] != clusters["Clean_A"]
    assert clusters["Clean_A"] == clusters["Clean_B"] == clusters["Clean_C"]
    assert silhouette > 0.5
    assert {"city", "country", "month", "avg_pm2_5", "lat", "lon", "cluster"} <= set(result.columns)


@pytest.mark.parametrize("fit_fn", [fit_dbscan, fit_hdbscan])
def test_tang3_khong_bam_thanh_qua_nhieu_cum_sieu_nho(spark, fit_fn):
    # 24 điểm, mỗi (thành phố) là một chuỗi 6 tháng sát nhau: nếu không giới hạn số cụm thì
    # HDBSCAN(min_cluster_size=2) chia ra 7 cụm và thắng silhouette. Phải nằm trong ngân sách k.
    result, _, _, _ = fit_fn(_make_clear_cluster_df(spark))
    n_clusters = len({r["cluster"] for r in result.select("cluster").collect()} - {-1})
    assert 2 <= n_clusters <= MAX_CLUSTERS


def test_common_silhouette_hai_cum_ro_rang_va_bo_diem_nhieu():
    a = np.array([[0.0, 0.0], [0.1, 0.0], [0.0, 0.1]])
    b = np.array([[10.0, 10.0], [10.1, 10.0], [10.0, 10.1]])
    X = np.vstack([a, b, [[50.0, -50.0]]])
    labels = np.array([0, 0, 0, 1, 1, 1, -1])  # điểm cuối là nhiễu -> không được tính

    score = common_silhouette(X, labels)
    assert score is not None and score > 0.9


def test_common_silhouette_mot_cum_hoac_toan_nhieu_tra_none():
    X = np.array([[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]])
    assert common_silhouette(X, np.array([0, 0, 0])) is None
    assert common_silhouette(X, np.array([-1, -1, -1])) is None


# --- xuất ext_clusters.json (schema C8.1) ---------------------------------------
def test_export_clusters_json_dung_schema(spark, tmp_path):
    result, _, k, silhouette = fit_kmeans(_make_clear_cluster_df(spark), k=2)
    path = tmp_path / "ext_clusters.json"

    payload = export_clusters_json(result, "KMeans", k, silhouette, str(path))

    assert json.loads(path.read_text(encoding="utf-8")) == payload  # file trên đĩa khớp giá trị trả về
    assert payload["schema_version"] == "1.0"
    assert payload["algorithm"] == "KMeans" and payload["params"] == {"k": 2}
    assert payload["n_clusters"] == 2 and len(payload["rows"]) == 24
    assert 0 < payload["silhouette"] <= 1
    assert set(payload["rows"][0]) == {
        "city", "country", "month", "lat", "lon", "avg_pm2_5", "avg_pm10", "avg_o3", "avg_no2", "cluster",
    }
    clusters = {r["city"]: r["cluster"] for r in payload["rows"]}
    assert clusters["Dirty"] != clusters["Clean_A"]


def test_export_clusters_json_diem_nhieu_khong_tinh_vao_so_cum(spark, tmp_path):
    schema = ("city string, country string, month int, lat double, lon double, avg_pm2_5 double, "
              "avg_pm10 double, avg_o3 double, avg_no2 double, cluster int")
    rows = [
        ("A", "XX", 1, 1.0, 2.0, 10.0, 20.0, 30.0, 5.0, 0),
        ("B", "XX", 1, 1.0, 2.0, 11.0, 20.0, 30.0, 5.0, 0),
        ("C", "XX", 1, 1.0, 2.0, 90.0, 20.0, 30.0, 5.0, 1),
        ("D", "XX", 1, 1.0, 2.0, 500.0, 20.0, 30.0, 5.0, -1),  # nhiễu của DBSCAN/HDBSCAN
    ]
    payload = export_clusters_json(
        spark.createDataFrame(rows, schema), "DBSCAN", {"eps": 1.5, "min_samples": 2}, None, str(tmp_path / "c.json")
    )

    assert payload["n_clusters"] == 2                      # cụm 0 và 1, không đếm -1
    assert payload["params"] == {"eps": 1.5, "min_samples": 2}
    assert payload["silhouette"] is None
    assert [r["cluster"] for r in payload["rows"]] == [-1, 0, 0, 1]
