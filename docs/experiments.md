# Kết quả thực nghiệm

Tài liệu này ghi các số liệu đo được và giới hạn của từng phép đo. Phần scalability (§2) là bằng chứng cho câu hỏi
"vì sao cần xử lý phân tán"; các phần khác ghi rõ số nào đo trên dữ liệu thật, số nào đo trên dữ liệu mẫu sinh giả.

## 1. Quy mô dữ liệu thu thập

Nguồn: `docs/report/Big_Data_AQI_Task_A_Data_Platform_FINAL.docx` §3.3 (audit tầng raw sau khi backfill
hoàn tất, trước lần chạy Pha 1→2→3 ở phiên bản mã có tag git `final-run-stable`). Báo cáo đó kết luận không cần
chạy lại toàn bộ backfill, nên số liệu này áp dụng cho cả dữ liệu raw dùng trong lần chạy cuối.

| Chỉ tiêu | Giá trị |
|---|---|
| Số điểm quan trắc | 200 (khớp `collector/config/cities.json`) |
| Khoảng thời gian | 2021-09-01 → 2026-09-01 (21 cửa sổ ~90 ngày, 21/21 hoàn tất) |
| Tổng bản ghi thô | 8.576.904 (97,80% của kỳ vọng 8.769.408; source gap 192.504 = 2,20%, xác nhận là thiếu dữ liệu nguồn OpenWeather, không phải lỗi pipeline) |
| Dung lượng raw (.jsonl.gz) | 279,4 MB (21.805 file) |
| Dung lượng sau Pha 2 (parquet) | *(chưa có — cần chạy `hdfs dfs -du -s -h /air-quality/aqi`, xem `docs/huong-dan-chay.md` bước 1)* |
| Thời gian backfill | *(không đo được — docx không ghi elapsed time; quá trình backfill bị ngắt và chạy lại nhiều lượt `--resume` cách nhau không rõ khoảng thời gian, nên "chạy liên tục mất bao lâu" không còn ý nghĩa rõ ràng)* |
| Số API call đã dùng | ≥ 4.200 (ước tính lý thuyết: 200 trạm × 21 cửa sổ, mỗi cửa sổ đúng 1 call nếu không lỗi) — **không phải số đo thực**, vì `call_count` tăng ở CẢ lần gọi bị lỗi phải retry (`owm_client.py`), và mỗi lượt `--resume` khởi tạo `OwmClient` mới nên bộ đếm reset về 0 mỗi lần chạy lại. Docx chỉ ghi được `api_calls_this_run=4.000` của đúng lượt `--resume` cuối, không phải tổng cộng dồn. |

## 2. Scalability của Pha 2

Đo trên máy dev (8 core vật lý), `local[N]` làm proxy cho N executor — không có cluster
nhiều máy thật. Dữ liệu tổng hợp sinh trực tiếp bằng Spark (200 trạm giả lập), đo đúng
chi phí TÍNH TOÁN của `compute_iaqi_hour()` (Nowcast UDF + Reducer AQI), không lẫn I/O
đọc file. Script: `spark/experiments/scalability_bench.py` (chạy qua `run_scalability.sh`,
kết quả thô ở `docs/experiments_data/scalability_results.csv`).

| Kích thước | 1 executor | 2 executors | 4 executors | Speedup (4 vs 1) |
|---|---|---|---|---|
| 100K | 11.20s | 6.37s | 5.24s | 2.14× |
| 1M | 72.63s | 40.22s | 33.16s | 2.19× |
| 8M | 567.71s | 325.37s | 204.47s | **2.78×** |

Biểu đồ: `docs/images/scalability_runtime.png` (thời gian chạy) · `docs/images/scalability_speedup.png` (speedup).

**Nhận xét:**

- **Speedup tăng theo kích thước dữ liệu** (2.14× ở 100K → 2.78× ở 8M). Ở quy mô nhỏ,
  overhead khởi động Spark (~4-5s cố định, không song song hoá được) chiếm tỉ trọng lớn
  trong tổng thời gian, làm speedup đo được thấp hơn khả năng thật của việc phân tán —
  đây chính là lý do "cần dữ liệu lớn mới thấy rõ lợi ích của Spark", không phải chạy
  vài nghìn bản ghi cho vui.
- **Speedup giảm dần khi tăng thêm executor** (1→2 executor ở 8M: 1.74×; 2→4 executor:
  chỉ thêm 1.59×, không phải 2×). Nguyên nhân nhiều khả năng: `compute_iaqi_hour()` dùng
  **Python UDF thường (không phải `pandas_udf`)** cho cả Nowcast lẫn `iaqi_hour()` — mỗi
  dòng phải serialize qua lại giữa JVM và tiến trình Python worker (không dùng Arrow
  vectorization), nên chi phí serialize/deserialize không giảm tuyến tính khi thêm
  executor, ngược lại còn cạnh tranh băng thông bộ nhớ trên cùng 1 máy 8 core.
  → Hướng tối ưu tiếp theo: chuyển sang `pandas_udf` để vector hoá theo batch, ước tính
  nhanh hơn UDF thường ~10x trên khối triệu dòng — nên là việc làm tiếp nếu cần tối ưu
  thêm cho pipeline thật (Kafka streaming + backfill hàng triệu bản ghi).

## 3. Đối chiếu AQI tự tính vs owm_aqi

Đo trên `data/samples/` (10.128 giờ có cả 2 giá trị). `owm_aqi` là thang 1-5 riêng của
OpenWeather — KHÔNG phải AQI của đồ án (xem README §1), chỉ dùng để đối chiếu tương quan.

| Mức AQI tự tính (VN_1459) | Số bản ghi | owm_aqi tương ứng (mode) | Khớp? |
|---|---|---|---|
| Tốt (1) | 7.522 | 1 | Khớp |
| Trung bình (2) | 1.490 | 3 | Lệch — owm_aqi cao hơn 1 bậc |
| Kém (3) | 585 | 4 | Lệch — owm_aqi cao hơn 1 bậc |
| Xấu (4) | 423 | 5 | Lệch — owm_aqi cao hơn 1 bậc |
| Rất xấu (5) | 101 | 5 | owm_aqi đã chạm trần (max=5), không phân biệt được mức 5 và 6 |
| Nguy hại (6) | 7 | 5 | owm_aqi đã chạm trần (max=5) |

**Nhận xét:** `owm_aqi` chỉ khớp tốt ở mức "Tốt" — từ mức "Trung bình" trở lên, OpenWeather có
xu hướng đánh giá nghiêm trọng hơn 1 bậc so với thang VN_1459 (2 thang đo dùng breakpoint
khác nhau, đây là hệ quả tất yếu chứ không phải lỗi tính toán). Từ mức "Rất xấu" trở lên,
`owm_aqi` bị bão hoà ở trần 5, không còn phân biệt được — càng khẳng định lý do đồ án phải
tự tính AQI theo VN_1459 thay vì dùng thẳng `owm_aqi` (đúng luận điểm ở README §1).

## 4. Chất lượng dữ liệu sau Pha 1

Đo trên `data/samples/` (10.800 giờ, 5 trạm × 90 ngày). Không có ca ngoại lai nào bị loại
trong sample này (README ghi nhận nhúng tỉ lệ ngoại lai rất thấp, ~5 ca âm/10142 bản ghi thô).

| Chất | % thiếu (không nội suy được) | % nội suy (gap ≤3h) |
|---|---|---|
| PM2.5 | 6.2% | 0.0% |
| PM10 | 6.2% | 0.06% |
| O3 | 6.2% | 0.06% |
| NO2 | 6.2% | 0.04% |
| SO2 | 6.2% | 0.06% |
| CO | 6.2% | 0.0% |

**Nhận xét:** tỉ lệ thiếu đồng đều 6.2% ở mọi chất — khớp đúng đặc điểm dữ liệu mẫu (gap là
**khối nguyên giờ mất cả bản ghi**, không phải thiếu riêng lẻ từng chất — xem
`data/samples/README.md`), nên mọi chất cùng thiếu ở đúng những giờ đó. Tỉ lệ nội suy rất
thấp (<0.1%) vì đúng thiết kế: chỉ nội suy gap ≤3h, còn lại (gap 24-96h) giữ nguyên `null`
và đánh dấu `missing` — không bịa số liệu qua khối gap dài.

## 5. Nhánh mở rộng M4 — Phân cụm & Dự báo

**Số liệu chính thức**, đo trên dữ liệu thật (200 trạm, xem §1) sau khi chạy Pha 1→2→3
và chạy `ext_clustering.py` / `ext_forecast.py` trên toàn bộ `/air-quality/aqi/` (phiên bản mã có tag git
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
DBSCAN, HDBSCAN) và chọn theo silhouette Euclidean chung (xem cách đo ở lần chạy trên dữ liệu
mẫu bên dưới). **Hạn chế cần nêu rõ:** `export_clusters_json()` chỉ ghi lại kết quả của thuật
toán thắng, không ghi bảng so sánh đầy đủ; log console (nơi in bảng `=== Tổng kết ===` của cả 5
thuật toán) không được lưu lại. Vì vậy không biết K-means thắng sát nút hay cách biệt các thuật
toán còn lại bao nhiêu trên dữ liệu thật — **cần sửa `export_clusters_json` để ghi thêm bảng so
sánh, rồi chạy lại** (rẻ: chỉ 2.400 dòng, không cần chạy lại Pha 1-3).

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

Train/test split theo thời gian (không random, tránh rò rỉ tương lai vào quá khứ). R² thật
(0.72–0.73) tiến gần hơn nhiều tới baseline paper (0.81) so với lần đo trên sample (0.54) —
đúng như dự đoán trước đó: dữ liệu thật nhiều năm giúp mô hình học tốt hơn nhiều.

**Random Forest thắng theo RMSE và R², nhưng KHÔNG thắng theo MAE** — SGD có MAE thấp nhất
(9.99 so với 10.65 của RF). Không phải nghịch lý: hai chỉ số phạt sai số khác nhau (RMSE/R²
phạt nặng sai số lớn hơn MAE); RF khá hơn ở việc giảm sai số lớn nhưng không nhất thiết khá
hơn ở sai số trung bình. **Chọn RF làm tầng thắng vì RMSE và R² là hai tiêu chí chính**, nhưng
nên nêu rõ đánh đổi MAE khi trình bày, không chỉ trích mỗi RMSE.

**Hai điều cần nêu khi trích số này vào báo cáo:**
- **Random Forest chạy với tham số bị giảm so với thiết kế gốc** (`num_trees=20, max_depth=5`
  thay vì `100, 10`) — do giới hạn tài nguyên khi train trên driver với ~8,5 triệu dòng. Kết
  luận "RF thắng" vẫn đứng, nhưng đây không phải cấu hình RF đầy đủ; RF với tham số gốc có
  thể còn thắng cách biệt hơn.
- **`n_test` của CNN-LSTM (119.767) nhỏ hơn nhiều** so với SGD/RF (255.614) do giới hạn
  `--cnn-max-rows` (lấy mẫu tối đa 300.000 chuỗi) — ba tầng không đo trên đúng cùng một tập
  test, nên so sánh RMSE/MAE/R² giữa CNN-LSTM và 2 tầng kia chỉ mang tính tham khảo.

**Backtest** (`ext_forecast_backtest.json`, dùng cho panel Grafana "AQI Forecast +24h"): 5 trạm
mặc định (CN_BJS_01, IN_DEL_01, JP_TYO_01, VN_HAN_01, VN_HCM_01), 2.898 dòng, khoảng
2026-08-03 → 2026-09-01 (giờ được dự báo). 12/2.898 dòng thiếu SGD/RF (rìa dữ liệu — không đủ
lag để tính), 0 dòng thiếu `actual`/CNN-LSTM.

**Chưa làm được, nên bổ sung nếu còn thời gian:** thêm baseline đơn giản "AQI cùng giờ hôm
qua" để có mốc so sánh cho RF/CNN-LSTM (đã nêu ở lần đo trước, vẫn chưa làm); lấy lại
feature importance của Random Forest trên dữ liệu thật (bảng dưới đây vẫn là số đo trên
`data/samples/`, **không phải số liệu chính thức**, giữ lại chỉ để tham khảo hướng feature
nào quan trọng — cần in lại `print_feature_importances()` từ lần chạy thật và thay bảng này).

**Feature importance (Random Forest, đo trên `data/samples/` — CHƯA phải số liệu chính thức):**

| Hạng | Feature | Importance |
|---|---|---|
| 1 | `lon` (kinh độ) | 0.2707 |
| 2 | `pm10` | 0.1579 |
| 3 | `pm2_5` | 0.1507 |
| 4 | `aqi_lag_1h` | 0.1126 |
| 5 | `aqi_lag_24h` | 0.0706 |
| 6 | `aqi_lag_3h` | 0.0513 |
| — | so2, o3, hour_of_day, no2, co, month, lat | 0.012 – 0.036 |
