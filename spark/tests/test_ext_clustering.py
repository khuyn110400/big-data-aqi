"""Unit test cho ext_clustering.py (tầng 1 K-means + tầng 2 GMM/Bisecting K-means). NGƯỜI B · M4."""
from datetime import datetime

import pytest

from ext_clustering import (
    build_city_month_features,
    compare_clustering_algorithms,
    fit_bisecting_kmeans,
    fit_gmm,
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

    assert best_name in {"KMeans", "GaussianMixture", "BisectingKMeans"}
    assert silhouette > 0.5
    clusters = {r["city"]: r["cluster"] for r in result.select("city", "cluster").distinct().collect()}
    assert clusters["Dirty"] != clusters["Clean_A"]
