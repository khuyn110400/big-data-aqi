"""
NHÁNH MỞ RỘNG (ngoài lõi) — phân cụm. NGƯỜI B · M4 · CHỌN 1 TRONG 2 NHÁNH MỞ RỘNG

Theo mục 7.3 của Phân tích từ paper: thay K-means bằng Bisecting K-means hoặc GMM
(cả hai có native trên Spark MLlib, ít công nhất mà vẫn tốt hơn K-means).

Feature: [avg_pm2_5, avg_pm10, avg_o3, avg_no2, lat, lon, tháng]
Output : nhãn cụm cho mỗi thành phố -> vẽ lên bản đồ Grafana

TODO(B): chỉ làm khi Pha 1/2/3 đã xong và ổn định. Đừng đụng vào sớm.
"""
