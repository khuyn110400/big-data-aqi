# Kết quả thực nghiệm

Tài liệu này ghi các số liệu đo được và giới hạn của từng phép đo. Phần scalability (§2) là bằng chứng cho câu hỏi
"vì sao cần xử lý phân tán"; các phần khác ghi rõ số nào đo trên dữ liệu thật, số nào đo trên dữ liệu mẫu sinh giả.

## 1. Quy mô dữ liệu thu thập

Nguồn: hai báo cáo của phần thu thập và xử lý dữ liệu trong `docs/report/`:
`Big_Data_AQI_Task_A_Data_Platform_FINAL.docx` §3.3 (audit tầng raw sau khi backfill hoàn tất) và
`Bao_cao_ket_qua_BigData_AQI_Nhom4_FINAL_20-09-2026.docx` (kết quả lần chạy cuối, phiên bản mã có tag git
`final-run-stable`, Spark 3.5.6). Các con số dưới đây chép từ hai báo cáo đó, chưa được tái tạo độc lập
từ dữ liệu.

| Chỉ tiêu | Giá trị |
|---|---|
| Số điểm quan trắc | 200 (khớp `collector/config/cities.json`) |
| Khoảng thời gian | 2021-09-01 → 2026-09-01 (21 cửa sổ ~90 ngày, 21/21 hoàn tất) |
| Tổng bản ghi thô | 8.576.904 (97,80% của kỳ vọng 8.769.408; source gap 192.504 = 2,20%, xác nhận là thiếu dữ liệu nguồn OpenWeather, không phải lỗi pipeline) |
| Dung lượng raw (.jsonl.gz) | 279,4 MB (21.805 file), theo báo cáo kết quả gồm cả dữ liệu live |
| Sau Pha 1 (`/clean`, parquet) | 8.769.600 dòng giờ (lưới giờ đầy đủ của 200 trạm); 367,5 MB logic (~1,1 GB tính bản sao HDFS) |
| Sau Pha 2 (`/aqi`, parquet) | 593,0 MB logic (~1,7 GB tính bản sao HDFS) |
| Sau Pha 3 (`/agg`, parquet) | 99,1 MB logic (297,3 MB tính bản sao HDFS); Pha 3 chạy khoảng 8,26 phút |
| Nạp lịch sử vào HBase | 428.866 dòng (01/06/2026 → 01/09/2026) trong khoảng 1 phút 14,7 giây |
| Thời gian chạy Pha 1 và Pha 2 | *(chưa có: báo cáo không ghi thời gian của hai pha này)* |
| Thời gian backfill | *(không đo được: quá trình backfill bị ngắt và chạy lại nhiều lượt `--resume` cách nhau không rõ khoảng thời gian, nên "chạy liên tục mất bao lâu" không còn ý nghĩa rõ ràng)* |
| Số API call đã dùng | ≥ 4.200 (ước tính lý thuyết: 200 trạm × 21 cửa sổ, mỗi cửa sổ đúng 1 call nếu không lỗi) — **không phải số đo thực**, vì `call_count` tăng ở cả lần gọi bị lỗi phải retry (`owm_client.py`), và mỗi lượt `--resume` khởi tạo `OwmClient` mới nên bộ đếm reset về 0 mỗi lần chạy lại. Báo cáo chỉ ghi được `api_calls_this_run=4.000` của đúng lượt `--resume` cuối, không phải tổng cộng dồn. |
| Cấu hình máy chạy (CPU, RAM vật lý, ổ đĩa) | *(chưa ghi lại; chỉ biết RAM cấp cho WSL2 là 10 GB, xem `docs/cai-dat.md`)* |

Kiểm tra chéo giữa các con số: 8.769.600 − 8.576.904 = 192.696, đúng bằng số dòng thiếu O₃, NO₂ và CO
ở Pha 1 (§4), tức các giờ mà lưới giờ có nhưng raw không có bản ghi nào.

## 2. Scalability của Pha 2

Đo thời gian chạy Pha 2 (`compute_iaqi_hour()`: Nowcast UDF và Reducer AQI) trên dữ liệu tổng hợp sinh
trực tiếp bằng Spark, không lẫn chi phí đọc file. Có hai lần đo, cách đo khác nhau; số chính thức là lần 2.

### 2.1. Lần đo chính thức: Spark Standalone, executor thật

Chạy bằng `scripts/run_scalability_distributed.sh`: 4 worker tạm (container Docker), mỗi worker 1 core, cùng
một máy Windows chạy WSL2. `spark.cores.max` giới hạn ứng dụng ở 1, 2 hoặc 4 executor; mỗi cấu hình chạy trong
một tiến trình Spark riêng, driver 2 GB, executor 768 MB. Sau mỗi lần chạy, script đếm số executor thực tế
trong log và xác nhận đúng bằng số yêu cầu (9/9 cấu hình đạt, xem `executor_verification.txt`). Kết quả thô ở
`final_results/scalability/` (`scalability_results.csv`, `scalability_speedup.csv`, `summary.md`).

| Kích thước | 1 executor | 2 executors | 4 executors | Speedup 2 | Speedup 4 |
|---|---|---|---|---|---|
| 100K | 16,39 s | 11,84 s | 15,97 s | 1,38× | 1,03× |
| 1M | 99,00 s | 51,24 s | 48,43 s | 1,93× | 2,04× |
| 8M | 749,31 s | 385,22 s | 308,79 s | 1,95× | **2,43×** |

Speedup tính theo T1 / TN.

**Nhận xét:**

- **Lợi ích phân tán tăng theo kích thước dữ liệu.** Ở 100K dòng, 4 executor thậm chí chậm hơn 2
  executor (15,97 s so với 11,84 s): thời gian khởi tạo và điều phối chiếm phần lớn tổng thời gian.
  Ở 1M và 8M dòng, 4 executor nhanh hơn 1 executor lần lượt 2,04× và 2,43×.
- **Speedup dưới tuyến tính.** Ở 8M dòng, 2 executor gần đạt tuyến tính (1,95×) nhưng 4 executor chỉ 2,43×
  chứ không phải 4×. Nguyên nhân nhiều khả năng: Python UDF thường (không phải `pandas_udf`) nên mỗi dòng
  phải serialize qua lại giữa JVM và tiến trình Python; cộng thêm chi phí điều phối và shuffle. Đây là suy
  đoán, chưa được kiểm chứng bằng đo riêng từng thành phần.
- **Hướng tối ưu:** chuyển sang `pandas_udf` để xử lý theo batch.

**Hạn chế khi trích số này:**

- Cả 4 worker chạy trên cùng một máy (container Docker), không phải cụm nhiều máy. Đúng cách gọi là
  "phân tán trên nhiều tiến trình executor trong một máy", không phải nhiều node.
- Mỗi cấu hình chỉ chạy một lần, không có lặp lại nên không có độ lệch chuẩn; chênh lệch nhỏ (như 100K dòng)
  có thể là nhiễu.
- Số nhân CPU của máy chạy chưa được ghi lại, nên chưa biết 4 executor cộng driver và master có tranh
  tài nguyên hay không.
- Dữ liệu là tổng hợp (200 trạm giả lập), chỉ đo chi phí tính toán, không phải benchmark I/O trên HDFS.

### 2.2. Lần đo ban đầu (chế độ phát triển): `local[N]`

Đo trên máy dev 8 core vật lý, dùng `local[N]` (N luồng trong một tiến trình) làm đại diện cho N executor.
Script: `spark/experiments/scalability_bench.py` chạy qua `spark/experiments/run_scalability.sh`, kết quả thô
ở `docs/experiments_data/scalability_results.csv`. Giữ lại để đối chiếu; máy và cách đo khác lần 2.1 nên hai
bảng không cộng gộp hay so trực tiếp thời gian tuyệt đối.

| Kích thước | 1 executor | 2 executors | 4 executors | Speedup (4 vs 1) |
|---|---|---|---|---|
| 100K | 11.20s | 6.37s | 5.24s | 2.14× |
| 1M | 72.63s | 40.22s | 33.16s | 2.19× |
| 8M | 567.71s | 325.37s | 204.47s | 2.78× |

Biểu đồ: `docs/images/scalability_runtime.png` (thời gian chạy) · `docs/images/scalability_speedup.png` (speedup).
Cả hai lần đo cho cùng xu hướng: speedup tăng theo kích thước dữ liệu và giảm dần khi thêm executor.

## 3. Đối chiếu AQI tự tính với owm_aqi

Đo trên dữ liệu thật sau Pha 2 (nguồn: báo cáo kết quả, Bảng 2; chưa tái tạo độc lập). `owm_aqi` là thang
1–5 riêng của OpenWeather, không phải AQI của đồ án (xem README §1), chỉ dùng để đối chiếu. Bảng nhóm các
bản ghi theo giá trị `owm_aqi` rồi thống kê AQI tự tính (VN_1459) trong từng nhóm:

| owm_aqi | Số dòng | AQI tự tính, trung bình | Nhỏ nhất | Lớn nhất |
|---|---|---|---|---|
| 1 | 2.534.428 | 13,3 | 1 | 163 |
| 2 | 2.405.822 | 29,8 | 1 | 205 |
| 3 | 1.294.595 | 64,0 | 2 | 302 |
| 4 | 961.044 | 95,8 | 2 | 301 |
| 5 | 1.380.785 | 181,2 | 3 | 499 |
| 6 | 12 | 103,8 | 60 | 171 |

**Nhận xét:**

- AQI tự tính trung bình tăng đều theo `owm_aqi` (13,3 → 29,8 → 64,0 → 95,8 → 181,2), tức hai thang có tương quan
  cùng chiều.
- Nhưng khoảng giá trị của các nhóm chồng lấn rất rộng (nhóm 1 có AQI đến 163, nhóm 5 có AQI từ 3 đến 499),
  vì hai thang dùng breakpoint khác nhau và `owm_aqi` chỉ có 5 mức. Vì vậy `owm_aqi` không thay được AQI tính theo
  QĐ 1459, đúng luận điểm ở README §1.
- **Chưa giải thích được:** thang OpenWeather chỉ có 1–5 nhưng bảng có một nhóm `owm_aqi = 6` gồm 12 dòng.
  Cần hỏi người chạy Pha 2 đây là dòng gì (có thể là giá trị lạ trong dữ liệu nguồn hoặc dòng do lưới giờ
  sinh ra) trước khi trích bảng này vào báo cáo.

## 4. Chất lượng dữ liệu sau Pha 1

Đo trên dữ liệu thật: 8.769.600 dòng giờ của 200 trạm (nguồn: báo cáo kết quả, Bảng 1; chưa tái tạo độc lập).

| Chất | Số dòng thiếu | Tỉ lệ thiếu | Số dòng đã nội suy |
|---|---|---|---|
| PM2.5 | 206.549 | 2,4% | 1.116 |
| PM10 | 207.787 | 2,4% | 1.474 |
| O3 | 192.696 | 2,2% | 200 |
| NO2 | 192.696 | 2,2% | 400 |
| SO2 | 192.827 | 2,2% | 42 |
| CO | 192.696 | 2,2% | 0 |

Số ngày đủ điều kiện tính AQI ngày (đạt tối thiểu 75% dữ liệu): 356.646 / 365.600 = 97,6%.

**Nhận xét:**

- Số dòng thiếu O3, NO2, CO đều là 192.696, bằng đúng số giờ có trong lưới nhưng không có bản ghi thô
  (8.769.600 − 8.576.904). Nghĩa là các chất này chỉ thiếu ở những giờ mất cả bản ghi; PM2.5 và PM10 thiếu
  thêm khoảng 13.900 và 15.100 giá trị lẻ.
- Tỉ lệ nội suy rất thấp (dưới 0,02% số dòng) vì đúng thiết kế: chỉ nội suy khoảng thiếu tối đa 3 giờ, còn khối
  thiếu dài giữ nguyên `null` và đánh dấu `missing`, không bịa số liệu.
- Chuẩn QĐ 1459 không ghi rõ ngưỡng đầy đủ tối thiểu trong các mục đã đối chiếu; ngưỡng 75% (18/24 giờ) lấy theo
  chuẩn US EPA (xem `spark/jobs/phase1_clean.py`).

## 5. Nhánh mở rộng M4 — Phân cụm & Dự báo

**Số liệu chính thức**, đo trên dữ liệu thật (200 trạm, xem §1) sau khi chạy Pha 1→2→3
và chạy `ext_clustering.py` / `ext_forecast.py` trên `/air-quality/aqi/` (phân cụm dùng toàn bộ, dự báo dùng dữ liệu từ 01/09/2025; phiên bản mã có tag git
`final-run-stable`, theo đúng lệnh trong `docs/huong-dan-chay.md` bước 4). Kết quả xuất ra
`final_results/json/ext_clusters.json` (sinh 2026-09-20T00:49:20Z) và
`ext_forecast_backtest.json` (sinh 2026-09-20T01:17:08Z), phục vụ qua `/ext/clusters` và
`/ext/forecast` (CONTRACTS.md §C8). Thay cho bản đo trên `data/samples/` (5 trạm, ~4 tháng, dữ
liệu sinh giả) ở các lần đo trước.

### 5.1. Phân cụm vùng — K-means (k=3) thắng

| Thuật toán thắng | Tham số | Số cụm | Silhouette |
|---|---|---|---|
| **K-means** | k=3 | 3 | **0.3547** |

`compare_clustering_algorithms()` chạy cả 5 thuật toán (K-means, GMM, Bisecting K-means,
DBSCAN, HDBSCAN) và chọn thuật toán có silhouette Euclidean cao nhất (cách đo xem ở phần "Đính
chính" bên dưới). `export_clusters_json()` chỉ ghi kết quả của thuật toán thắng vào file JSON, còn bảng so
sánh đầy đủ nằm trong log của lần chạy cuối, được chép vào báo cáo kết quả (Bảng 3, Hình 9):

| Thuật toán | Tham số được chọn | Số cụm | Nhiễu | Silhouette |
|---|---|---|---|---|
| **K-means** | k=3 | 3 | 0% | **0.3547** |
| Bisecting K-means | k=2 | 2 | 0% | 0.3398 |
| DBSCAN | eps=1.5, min_samples=5 | 6 | 1% | 0.3140 |
| GMM | k=2 | 2 | 0% | 0.2354 |
| HDBSCAN | không có cấu hình hợp lệ | — | — | — |

**Đã tái chạy để kiểm tra.** 2.400 dòng `rows` của `final_results/json/ext_clusters.json` (200 thành phố ×
12 tháng) chính là bảng feature đầu vào của bước phân cụm. Nạp chúng vào một DataFrame Spark (cột `city`,
`country`, `month`, `lat`, `lon`, `avg_pm2_5`, `avg_pm10`, `avg_o3`, `avg_no2`) rồi gọi lại
`compare_clustering_algorithms()` (seed 42) cho kết quả trùng bảng trên đến 4 chữ số ở K-means, Bisecting
K-means và DBSCAN. Riêng GMM tái tạo được 0.2214 thay vì 0.2354; GMM khởi tạo ngẫu nhiên nên có thể khác nhau
giữa các phiên bản Spark (lần chạy cuối dùng Spark 3.5.6, lần tái chạy dùng 3.5.9), và thứ hạng không đổi.

Đọc bảng này cần lưu ý:
- K-means hơn Bisecting K-means chỉ 0.015 (0.3547 so với 0.3398), tức thắng sát chứ không cách
  biệt. Không nên viết K-means "vượt trội".
- HDBSCAN bị loại vì không cấu hình nào thoả ràng buộc ≤ 6 cụm và nhiễu ≤ 20% (xem ràng buộc ở
  cuối mục này). Nếu bỏ ràng buộc thì `min_cluster_size=2` cho silhouette 0.3632 nhưng với 598
  cụm và 17% nhiễu, tức chia thành hàng trăm cụm nhỏ, không có ý nghĩa thực tế; đây chính là hiện
  tượng ràng buộc được đặt ra để chặn.
- Bảng có đủ 5 thuật toán, tức lần chạy cuối là lần tự so sánh chứ không dùng tham số `--k`
  (khi có `--k` chương trình chỉ chạy K-means).

**Đánh giá theo thang chuẩn (Kaufman & Rousseeuw):** 0.3547 rơi vào khoảng 0.26–0.50, tức
**"cấu trúc yếu"** — cụm có tồn tại nhưng không tách biệt rõ ràng, không đạt mức "tốt" (>0.5)
theo tiêu chí thống kê chặt. Không nên báo cáo con số này như một phân cụm rõ nét.

**Nhưng các cụm có ý nghĩa thực tế, giải thích được** — kiểm tra bằng đặc trưng trung bình
từng cụm (không chỉ tin con số silhouette):

| Cụm | Số dòng (tỉ lệ) | PM2.5 TB | PM10 TB | O3 TB | NO2 TB | Đặc điểm |
|---|---|---|---|---|---|---|
| 0 | 1.711 (71.3%) | 23.2 | 29.3 | 47.3 | 7.1 | Nền chung: đa số VN (1.053 dòng), IN (418), lẫn các nước khác vào tháng sạch |
| 1 | 449 (18.7%) | **91.8** | **122.4** | 66.8 | 25.4 | Ô nhiễm nặng: đỉnh điểm IN (182), VN (147), AE (60), CN (55) |
| 2 | 240 (10.0%) | **7.9** | **10.1** | 55.4 | 15.5 | Sạch: DE, GB, CA, US (60 dòng mỗi nước) |

Không cụm nào suy biến (không lệch 99%/0.5%/0.5%). Thành phố tiêu biểu cụm 1 (PM2.5 cao nhất
theo trung bình các tháng): Nabagrām (163.7), Mumbai (149.3), Bairāgnia (143.8), New Delhi
(142.1), Beijing (141.6). Thành phố tiêu biểu cụm 2 (PM2.5 thấp nhất): Calgary (3.5), Glasgow
(3.6), Liverpool (4.3), Manchester (4.6), New York (4.6) — đúng khớp trực giác "đô thị châu
Á/Trung Đông ô nhiễm nặng theo mùa" tách khỏi "các nước phát triển, nền ô nhiễm thấp".

**77/200 thành phố đổi cụm giữa các tháng** — đúng tín hiệu mùa vụ mà thiết kế nhắm tới (nhóm
theo thành phố+tháng, không phải thành phố+năm-tháng): các đô thị như Bắc Kinh/Delhi rơi vào
cụm 1 (ô nhiễm nặng) đúng mùa cao điểm, các tháng còn lại rơi về cụm 0.

**Kết luận:** kết quả không "đạt chuẩn" theo nghĩa silhouette cao, nhưng **có ý nghĩa và giải
thích được** — nên trình bày kèm bảng đặc trưng cụm ở trên, không chỉ trích một con số
silhouette, để không gây hiểu lầm là phân cụm tách biệt hoàn hảo.

**Đính chính so với bản đo trên `data/samples/`:** bản đó ghi K-means 0.7127 nhưng dùng thước đo
squaredEuclidean của Spark `ClusteringEvaluator`, không so được với DBSCAN/HDBSCAN đo bằng
Euclidean của sklearn (cùng cách chia cụm K-means k=2, đo lại theo Euclidean chỉ ra 0.4909).
`compare_clustering_algorithms()` đã sửa để dùng chung một thước đo (Euclidean, bỏ điểm nhiễu)
cho cả 5 thuật toán kể từ đó — bảng 0.3547 ở trên đã theo đúng cách đo mới.

Hai ràng buộc khi chọn cấu hình tầng 3 (docstring `ext_clustering.py`): tỉ lệ nhiễu <= 20%
(silhouette bỏ điểm nhiễu nên thuật toán vứt nhiều điểm sẽ được điểm cao giả tạo) và số cụm
<= 6, cùng ngân sách với dải k tầng 1/2 (không giới hạn thì silhouette thưởng cho việc băm
thành nhiều cụm siêu nhỏ — đã gặp HDBSCAN `min_cluster_size=2` cho 7 cụm, silhouette 0.87,
thắng cách chia đúng 2 cụm trên dữ liệu mẫu).

**Phát hiện kỹ thuật đã sửa** (còn nguyên giá trị): `StandardScaler(withMean=True)` làm
`BisectingKMeans` của Spark MLlib suy biến về đúng 1 cụm bất kể k/seed — đã đổi sang
`withMean=False` (không ảnh hưởng K-means/GMM vì 2 thuật toán này bất biến với phép dịch
chuyển đều, chỉ có lợi cho BisectingKMeans).

### 5.2. Dự báo AQI 24h — Random Forest thắng nhẹ, chênh lệch không lớn

| Model | RMSE | MAE | R² | n_test |
|---|---|---|---|---|
| SGDRegressor online (Tầng 1) | 20.29 | **9.99** | 0.7243 | 255.614 |
| **Random Forest (Tầng 2)** | **20.10** | 10.65 | **0.7297** | 255.614 |
| CNN-LSTM (Tầng 3) | 20.37 | 11.18 | 0.7251 | 119.767 |
| *Đối chiếu*: LaSVM, paper Ghaemi 2015 | — | — | *~0.81* | *(data khác, không so trực tiếp)* |

Dự báo chạy trên dữ liệu từ 01/09/2025 (`--since`), không phải toàn bộ 5 năm: 1.590.752 dòng có đủ feature và
nhãn cho tầng SGD/Random Forest, 1.595.355 chuỗi 24 giờ đủ lịch sử cho CNN-LSTM. Train/test split theo thời
gian (không random, tránh rò rỉ tương lai vào quá khứ): 10 tháng huấn luyện (09/2025 → 06/2026) và 2 tháng
kiểm thử (07 → 08/2026). Nguồn: báo cáo kết quả, mục 5. R² thật
(0.72–0.73) tiến gần hơn nhiều tới baseline paper (0.81) so với lần đo trên sample (0.54) —
đúng như dự đoán trước đó: dữ liệu thật nhiều năm giúp mô hình học tốt hơn nhiều.

**Random Forest thắng theo RMSE và R², nhưng KHÔNG thắng theo MAE** — SGD có MAE thấp nhất
(9.99 so với 10.65 của RF). Không phải nghịch lý: hai chỉ số phạt sai số khác nhau (RMSE/R²
phạt nặng sai số lớn hơn MAE); RF khá hơn ở việc giảm sai số lớn nhưng không nhất thiết khá
hơn ở sai số trung bình. **Chọn RF làm tầng thắng vì RMSE và R² là hai tiêu chí chính**, nhưng
nên nêu rõ đánh đổi MAE khi trình bày, không chỉ trích mỗi RMSE.

**Hai điều cần nêu khi trích số này vào báo cáo:**
- **Random Forest chạy với tham số bị giảm so với thiết kế gốc** (`num_trees=20, max_depth=5`
  thay vì `100, 10`) — do giới hạn tài nguyên khi train trên driver (lý do chính xác chưa
  được ghi lại). Kết luận "RF thắng" vẫn đứng, nhưng đây không phải cấu hình RF đầy đủ; RF với
  tham số gốc có thể còn thắng cách biệt hơn.
- **`n_test` của CNN-LSTM (119.767) nhỏ hơn nhiều** so với SGD/RF (255.614): tầng 3 chỉ dùng một
  mẫu ngẫu nhiên các chuỗi 24 giờ (giới hạn `--cnn-max-rows`, áp riêng cho tập train và tập
  test). Báo cáo kết quả ghi "lấy khoảng 120 nghìn chuỗi", nhiều khả năng do đặt tham số này ở
  mức ~120.000 thay vì mặc định 300.000, nhưng lệnh đã chạy không được ghi lại. Ba tầng không đo
  trên đúng cùng một tập test, nên so sánh RMSE/MAE/R² giữa CNN-LSTM và 2 tầng kia chỉ mang tính
  tham khảo. CNN-LSTM chạy đủ 15 epoch và không cải thiện hơn Random Forest.

**Backtest** (`ext_forecast_backtest.json`, dùng cho panel Grafana "AQI Forecast +24h"): 5 trạm
mặc định (CN_BJS_01, IN_DEL_01, JP_TYO_01, VN_HAN_01, VN_HCM_01), 2.898 dòng, khoảng
2026-08-03 → 2026-09-01 (giờ được dự báo). 12/2.898 dòng thiếu SGD/RF (rìa dữ liệu — không đủ
lag để tính), 0 dòng thiếu `actual`/CNN-LSTM.

**Feature importance của Random Forest** (lần chạy cuối, dữ liệu thật; nguồn: báo cáo kết quả, Bảng 5):

| Hạng | Feature | Importance |
|---|---|---|
| 1 | `pm10` | 0.3214 |
| 2 | `pm2_5` | 0.3094 |
| 3 | `aqi_lag_1h` | 0.1894 |
| 4 | `aqi_lag_24h` | 0.0800 |
| 5 | `aqi_lag_3h` | 0.0796 |
| 6 | `lat` | 0.0112 |
| 7 | `co` | 0.0058 |
| 8 | `lon` | 0.0018 |
| — | so2, o3, month, no2, hour_of_day | < 0.001 mỗi feature |

Hai nhóm quan trọng nhất là nồng độ bụi hiện tại (PM10, PM2.5, cộng lại ~0.63) và AQI các giờ trước (lag 1h,
3h, 24h, cộng lại ~0.35). Khác với lần đo trên dữ liệu mẫu (kinh độ đứng đầu, 0.27): kết quả đó là hệ quả của
việc dữ liệu mẫu sinh giả chỉ có 5 trạm, không phản ánh dữ liệu thật.

**Chưa làm được, nên bổ sung nếu còn thời gian:** thêm baseline đơn giản "AQI cùng giờ hôm qua" để có mốc so
sánh cho RF/CNN-LSTM (chưa làm). Nếu không có baseline này thì không biết R² 0.72–0.73 hơn bao nhiêu so với
cách dự báo đơn giản nhất.
