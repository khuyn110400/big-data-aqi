"""
NHÁNH MỞ RỘNG (ngoài lõi) — dự báo AQI 24h. NGƯỜI B · M4 · CHỌN 1 TRONG 2

Theo mục 7.2: Random Forest / XGBoost thay LaSVM (baseline của Ghaemi 2015).
Spark MLlib có RandomForestRegressor native -> ít rủi ro nhất.

Feature: lag AQI 1h/3h/24h, chất ô nhiễm, giờ trong ngày, tháng, lat/lon
Metric : RMSE, MAE, R2 — đối chiếu với baseline LaSVM (R2 ~ 0.81) trong bảng mục 7.2

TODO(B): chỉ làm khi còn thời gian.
"""
