# Kết quả thực nghiệm

> NGƯỜI B điền ở M4. Phần này quyết định điểm "vì sao cần big data" của đồ án.

## 1. Quy mô dữ liệu thu thập

| Chỉ tiêu | Giá trị |
|---|---|
| Số điểm quan trắc | |
| Khoảng thời gian | |
| Tổng bản ghi thô | |
| Dung lượng raw (.jsonl.gz) | |
| Dung lượng sau Pha 2 (parquet) | |
| Thời gian backfill | |
| Số API call đã dùng | |

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
  → Hướng tối ưu tiếp theo (đã ghi sẵn trong docstring `register_udfs()` của
  `aqi_core/iaqi.py`): chuyển sang `pandas_udf` để vector hoá theo batch, ước tính
  nhanh hơn UDF thường ~10x trên khối triệu dòng — nên là việc làm tiếp nếu cần tối ưu
  thêm cho pipeline thật (Kafka streaming + backfill hàng triệu bản ghi).

## 3. Đối chiếu AQI tự tính vs owm_aqi

Đo trên `data/samples/` (10.128 giờ có cả 2 giá trị). `owm_aqi` là thang 1-5 riêng của
OpenWeather — KHÔNG phải AQI của đồ án (xem README §1), chỉ dùng để đối chiếu tương quan.

| Mức AQI tự tính (VN_1459) | Số bản ghi | owm_aqi tương ứng (mode) | Khớp? |
|---|---|---|---|
| Tốt (1) | 7.522 | 1 | ✅ Khớp |
| Trung bình (2) | 1.490 | 3 | ❌ Lệch — owm_aqi cao hơn 1 bậc |
| Kém (3) | 585 | 4 | ❌ Lệch — owm_aqi cao hơn 1 bậc |
| Xấu (4) | 423 | 5 | ❌ Lệch — owm_aqi cao hơn 1 bậc |
| Rất xấu (5) | 101 | 5 | ⚠️ owm_aqi đã kịch trần (max=5), không phân biệt được 5 vs 6 |
| Nguy hại (6) | 7 | 5 | ⚠️ owm_aqi đã kịch trần (max=5) |

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

Đo trên `data/samples/` (5 trạm, ~4 tháng dữ liệu). **CHƯA phải số liệu chính thức cuối
cùng** — khi Người A có backfill thật (200 trạm × 3-5 năm), phải chạy lại toàn bộ để lấy
số liệu chính thức cho báo cáo. Chạy lại bằng: `python jobs/ext_clustering.py --input
<aqi_parquet> --output <dir>` và `python jobs/ext_forecast.py --input <aqi_parquet>
--output <dir>` (xem `spark/jobs/ext_clustering.ipynb`, `ext_forecast.ipynb` để chạy
từng bước có giải thích).

### 5.1. Phân cụm vùng — so sánh 5 thuật toán (Tầng 1+2+3)

Tầng 3 (DBSCAN/HDBSCAN) chạy trên Kaggle qua `spark/jobs/kaggle_dbscan_hdbscan.py` (dữ
liệu xuất bằng `export_for_kaggle.py`) — xem hướng dẫn trong docstring 2 file đó.

| Thuật toán | Tham số | Silhouette | Ghi chú |
|---|---|---|---|
| **K-means** (Tầng 1) | k=2 | **0.7127** | 🏆 Thắng |
| DBSCAN (Tầng 3) | eps=1.5, min_samples=3 | 0.5017 | |
| Bisecting K-means (Tầng 2) | k=4 | 0.4520 | |
| HDBSCAN (Tầng 3) | min_cluster_size=2 | 0.4366 | |
| GMM (Tầng 2) | k=2 | 0.3287 | |

k/eps/min_samples/min_cluster_size đều được chọn tự động bằng silhouette score trên 1 dải
giá trị cho từng thuật toán (không hardcode). Kết quả: New Delhi tách thành 1 cụm riêng
(ô nhiễm vượt trội, hệ số ×7 so với nền TP.HCM trong dữ liệu mẫu), các thành phố còn lại
gộp 1 cụm.

**Nhận xét:** K-means vượt trội hẳn (0.71 so với nhóm còn lại 0.33-0.50). DBSCAN/HDBSCAN
vốn mạnh ở việc tìm cụm hình dạng bất kỳ (không lồi) và tự phát hiện nhiễu, nhưng với chỉ
**20 điểm dữ liệu** (5 thành phố × 4 tháng) — cấu trúc đơn giản kiểu "1 outlier tách biệt +
phần còn lại gộp cụm" — không có đủ "đất dụng võ" để 2 thuật toán này thể hiện ưu thế; đây
đúng là kiểu bài toán mà K-means (dựa trên centroid, khoảng cách Euclidean) xử lý tối ưu.
Silhouette trên mẫu quá nhỏ này cũng không đủ tin cậy để kết luận chắc chắn — khi có data
thật (200 trạm, nhiều năm, nhiều nhóm khí hậu phức tạp hơn), thứ hạng này có thể đổi khác.

**Phát hiện kỹ thuật đáng chú ý** (đã sửa, ghi trong code): `StandardScaler(withMean=True)`
làm `BisectingKMeans` của Spark MLlib suy biến về đúng 1 cụm bất kể k/seed — đã đổi sang
`withMean=False` (không ảnh hưởng K-means/GMM vì 2 thuật toán này bất biến với phép dịch
chuyển đều, chỉ có lợi cho BisectingKMeans).

### 5.2. Dự báo AQI 24h — so sánh 3 tầng (SGD → Random Forest → CNN-LSTM)

Tầng 3 (CNN-LSTM) chạy trên Kaggle (GPU) qua `spark/jobs/kaggle_cnn_lstm.py` (dữ liệu
xuất bằng `export_for_kaggle.py` — chuỗi 24h AQI liên tiếp, khác lag rời rạc của tầng 1/2).

| Model | RMSE | MAE | R² | n_test |
|---|---|---|---|---|
| SGDRegressor online (Tầng 1) | 32.05 | **20.56** | 0.5382 | 1.154 |
| Random Forest (Tầng 2) | 31.87 | 20.64 | 0.5431 | 1.154 |
| CNN-LSTM (Tầng 3) | **31.69** | 21.53 | **0.5480** | 1.157 |
| *Đối chiếu*: LaSVM, paper Ghaemi 2015 | — | — | *~0.81* | *(data khác, không so trực tiếp)* |

Train/test split theo thời gian (3 tháng đầu train, 1 tháng cuối test) — không random, tránh
rò rỉ tương lai vào quá khứ.

**Nhận xét về xu hướng 3 tầng:** RMSE và R² cải thiện **đơn điệu** qua 3 tầng (đúng kỳ
vọng — mô hình phức tạp hơn → khá hơn), nhưng **MAE lại tệ dần** (20.56 → 20.64 → 21.53).
Đây không phải nghịch lý: `kaggle_cnn_lstm.py` compile với `loss="mse"` — mô hình tối ưu
theo sai số bình phương (khớp đúng mục tiêu RMSE), không phải MAE. RMSE phạt nặng sai số
lớn hơn MAE, nên CNN-LSTM "hy sinh" độ chính xác trung bình trên phần lớn dự báo để giảm
các sai số lớn/bất thường — đặc điểm cố hữu của việc train bằng MSE loss, không phải lỗi
mô hình. Cải thiện tổng thể qua 3 tầng khá khiêm tốn (RMSE giảm ~1.1%, R² tăng ~1.8%) —
sample 4 tháng/5 trạm quá nhỏ để độ phức tạp thêm của RF/CNN-LSTM phát huy hết lợi thế;
cần data thật (200 trạm × 3-5 năm) để thấy chênh lệch rõ hơn.

**Feature importance (Random Forest):**

| Hạng | Feature | Importance |
|---|---|---|
| 1 | `lon` (kinh độ) | 0.2707 |
| 2 | `pm10` | 0.1579 |
| 3 | `pm2_5` | 0.1507 |
| 4 | `aqi_lag_1h` | 0.1126 |
| 5 | `aqi_lag_24h` | 0.0706 |
| 6 | `aqi_lag_3h` | 0.0513 |
| — | so2, o3, hour_of_day, no2, co, month, lat | 0.012 – 0.036 |

**Nhận xét:**
- RF chỉ nhỉnh hơn SGD một chút trên sample nhỏ (4 tháng) — chênh lệch dự kiến rõ hơn khi
  có dữ liệu thật nhiều năm, vì RF train phân tán trên toàn bộ dữ liệu cùng lúc còn SGD giới
  hạn bởi cách học online từng tháng.
- `lon` (kinh độ) quan trọng nhất — hợp lý vì kinh độ tương ứng trực tiếp với trạm/quốc gia
  (Ấn Độ vs Việt Nam vs Nhật có nền ô nhiễm rất khác biệt), mô hình dùng nó để "nhận diện"
  trạm nào đang dự báo.
- R² thấp hơn baseline paper (0.81) — hợp lý vì sample chỉ có 4 tháng dữ liệu tổng hợp, ít
  hơn nhiều so với dữ liệu thật nhiều năm trong paper gốc.
