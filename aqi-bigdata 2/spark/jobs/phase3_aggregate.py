"""
PHA 3 — TỔNG HỢP & XẾP HẠNG  (mẫu 3 job của Iris-pot/AQI_analysis). NGƯỜI B · M3

Ba output, đúng như 3 job trong repo tham chiếu:
  1. AQI          -> trung bình/max AQI theo (city, date)
  2. AQIClassify  -> đếm phân bố 6 mức chất lượng theo city
  3. AQIIndex     -> điểm tổng hợp + xếp hạng thành phố

Mapper : key = (city, date) hoặc (city, aqi_level)
Reducer: avg/max/count -> ranking

Input: /air-quality/aqi/   Output: /air-quality/agg/ + ghi HBase cho endpoint /aqi/ranking

TODO(B):
  [ ] chốt công thức "điểm tổng hợp" và ghi rõ trong báo cáo (không bịa)
  [ ] thêm chiều mùa (tháng) — dữ liệu 5 năm cho phép phân tích tính mùa vụ, rất đáng đưa vào báo cáo
"""
