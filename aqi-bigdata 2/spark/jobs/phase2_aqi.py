"""
PHA 2 — TÍNH AQI  (El Fazziki 2015, pha 2). NGƯỜI B · M2
*** ĐÂY LÀ TRÁI TIM CỦA ĐỒ ÁN ***

Mapper : với mỗi bản ghi, phát key = (station_id, ts_utc),
         value = IAQI của từng chất theo bảng breakpoint QĐ 1459
Reducer: AQI = max(IAQI); gán aqi_level, aqi_label, dominant_pollutant

Dùng aqi_core.iaqi — KHÔNG viết lại công thức ở đây.

Input : /air-quality/clean/   Output: /air-quality/aqi/  (schema C3)

TODO(B):
  [ ] gọi aqi_core.register_udfs(spark), áp lên DataFrame
  [ ] cột standard = giá trị env AQI_STANDARD
  [ ] SINH BẢNG ĐỐI CHIẾU aqi (tự tính) vs owm_aqi (1-5) -> lưu docs/, cho vào báo cáo.
      Đây là cách chứng minh phần tính toán của nhóm hoạt động đúng.
  [ ] đo thời gian job và ghi vào docs/experiments.md
"""
