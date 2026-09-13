"""
LÀN STREAMING — Kafka -> AQI -> HBase + HDFS. NGƯỜI B · M3

readStream(kafka) -> parse schema C1 -> aqi_core (CÙNG module với Pha 2) -> 2 sink:
  - HBase  : bảng air_quality, row key theo C4
  - HDFS   : /air-quality/raw/ingest_mode=live/  (để sau này batch xử lý lại được)

TODO(B):
  [ ] checkpointLocation trên HDFS, nếu không restart là mất offset
  [ ] foreachBatch để ghi HBase (không có sink HBase native)
  [ ] xử lý bản ghi sai schema -> topic air-quality-dlq, đừng để job chết
  [ ] watermark cho dữ liệu đến muộn
  [ ] KIỂM CHỨNG: cho cùng một bản ghi chạy qua cả streaming và batch,
      hai bên PHẢI ra cùng AQI. Test này chứng minh "một lõi dùng chung" hoạt động.
"""
