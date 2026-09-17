"""
Gọi API hiện tại theo chu kỳ -> Kafka topic air-quality-raw. NGƯỜI A · M3

Chu kỳ 30-60 phút (OpenWeather cập nhật theo giờ, gọi dày hơn là phí quota).
Chạy như service trong docker-compose, hoặc cron trong WSL.

TODO(A):
  [ ] kafka key = station_id (theo C6, để giữ thứ tự thời gian mỗi trạm)
  [ ] ghi song song một bản vào HDFS raw để không mất dữ liệu nếu Kafka retention hết
  [ ] graceful shutdown: flush producer trước khi thoát
  [ ] health log mỗi vòng: số điểm thành công / lỗi
"""
