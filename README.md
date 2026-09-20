# AQI Analytics: phân tích chất lượng không khí trên nền tảng Big Data

Đồ án môn Big Data. Hệ thống thu thập số liệu ô nhiễm không khí của 200 điểm trên thế giới,
tính chỉ số AQI theo chuẩn Việt Nam bằng pipeline Spark nhiều pha (kế thừa mô hình
multi-phase MapReduce của El Fazziki et al., 2015), lưu vào HBase và hiển thị trên Grafana.

Nguồn dữ liệu: OpenWeather Air Pollution API (số liệu hiện tại và lịch sử từ 27/11/2020).

## Mục lục

1. [Hệ thống làm gì](#1-hệ-thống-làm-gì)
2. [Kiến trúc](#2-kiến-trúc)
3. [Ánh xạ Mapper / Reducer](#3-ánh-xạ-mapper--reducer)
4. [Cấu trúc thư mục](#4-cấu-trúc-thư-mục)
5. [Các thành phần chính](#5-các-thành-phần-chính)
6. [Kết quả chính](#6-kết-quả-chính)
7. [Chạy thử](#7-chạy-thử)
8. [Máy nào chạy được phần nào](#8-máy-nào-chạy-được-phần-nào)
9. [Tài liệu khác](#9-tài-liệu-khác)

---

## 1. Hệ thống làm gì

Thu thập nồng độ sáu chất ô nhiễm (PM2.5, PM10, O₃, NO₂, SO₂, CO) tại nhiều thành phố, tính chỉ số
AQI theo Quyết định 1459/QĐ-TCMT của Việt Nam bằng Spark, lưu trữ phân tán trên HDFS và HBase,
rồi trực quan hoá trên Grafana.

OpenWeather có trả sẵn trường `main.aqi`, nhưng đó là thang 1–5 riêng của họ, không phải AQI 0–500
của Việt Nam. Việc tính AQI từ nồng độ theo bảng breakpoint của QĐ 1459 (Pha 2) là phần tính toán
của đồ án, vì vậy `owm_aqi` chỉ được giữ lại để đối chiếu, không dùng làm kết quả.

Ngoài ba pha chính còn có hai nhánh mở rộng: phân cụm vùng theo mức ô nhiễm (5 thuật toán) và dự báo
AQI sau 24 giờ (3 mô hình theo độ phức tạp tăng dần).

## 2. Kiến trúc

Hệ thống theo Lambda Architecture: một làn batch xử lý lịch sử, một làn streaming xử lý dữ liệu mới,
và một lớp serving phục vụ dashboard. Hai làn dùng chung một module tính AQI (`spark/aqi_core`).

```
                  ┌────────────────────────────────────────┐
                  │     OpenWeather Air Pollution API      │
                  └───────────────────┬────────────────────┘
                                      │
        ┌─────────────────────────────┴─────────────────────────────┐
        │ LÀN BATCH (lịch sử)                LÀN STREAMING (hiện tại)│
        ▼                                                            ▼
 backfill_history.py                                         live_poller.py
 (200 điểm × 5 năm,                                          (mỗi giờ một lần)
  ~8,58 triệu bản ghi)                                               │
        │                                                            ▼
        ▼                                                     ┌─────────────┐
 HDFS /air-quality/raw  ◀──────────────────────────────────── │    Kafka    │
        │                                                     │ air-quality-│
        ▼                                                     │     raw     │
 ┌────────────────────┐                                       └──────┬──────┘
 │ Pha 1: Làm sạch    │                                              ▼
 │ (Spark batch)      │                                   ┌────────────────────┐
 └─────────┬──────────┘                                   │ Spark Structured   │
           ▼                                              │ Streaming          │
 ┌────────────────────┐      ┌─────────────────────┐      └─────────┬──────────┘
 │ Pha 2: Tính AQI    │◀────▶│  aqi_core (dùng     │◀───────────────┘
 │ Mapper: IAQI/chất  │      │  chung): Nowcast và │
 │ Reducer: max → AQI │      │  bảng breakpoint    │
 └─────────┬──────────┘      └─────────────────────┘
           ▼
 ┌────────────────────┐
 │ Pha 3: Tổng hợp    │
 └─────────┬──────────┘
           │
    ┌──────┴───────┐
    ▼              ▼
 HDFS /agg    HBase air_quality  ◀── streaming cũng ghi vào đây
                   │
                   ▼
            FastAPI (Query/API Bridge)
                   │
                   ▼
                Grafana
```

Làn batch tồn tại vì làn streaming chỉ sinh ra vài nghìn bản ghi trong lúc demo, không đủ để gọi
là big data và không chạy được Pha 1/2/3 ở quy mô có ý nghĩa. Làn batch cung cấp khối dữ liệu hàng
triệu bản ghi để đo hiệu năng phân tán.

### Hai làn chạy độc lập

| | Làn batch | Làn streaming |
|---|---|---|
| Chạy khi nào | một lần, thỉnh thoảng chạy lại | liên tục |
| Mỗi lần xử lý | cả khối ~8,58 triệu bản ghi | vài chục bản ghi |
| Thời gian | vài phút đến vài giờ | vài giây |

Nếu gộp làm một thì mỗi giờ có dữ liệu mới lại phải tính lại toàn bộ lịch sử. Hai làn chỉ gặp nhau ở
đầu ra: cùng ghi vào HBase và cùng dùng `aqi_core`, nên một bản ghi đi qua batch hay streaming đều cho
cùng một AQI (có test kiểm tra điều này trong `spark/tests/test_streaming_aqi.py`).

### Một số điểm dễ hiểu nhầm

- Kafka không thu thập dữ liệu. Collector bằng Python mới là bên gọi API; Kafka là ống dẫn và kho đệm,
  nếu Spark dừng vài giờ thì dữ liệu vẫn nằm trong Kafka chờ xử lý.
- Đồ án không dùng thư viện Kafka Streams. Kafka chỉ đóng vai message broker, phần xử lý luồng là
  Spark Structured Streaming.
- Grafana là bên đi lấy dữ liệu: định kỳ nó gọi các endpoint của FastAPI, không có bước nào "đẩy" lên Grafana.
- HDFS vừa là đầu vào vừa là đầu ra của Spark, gồm 4 tầng: `raw` → `clean` → `aqi` → `agg`.

## 3. Ánh xạ Mapper / Reducer

| Pha | Mapper phát ra | Reducer làm gì | Đầu ra |
|---|---|---|---|
| 1. Làm sạch | key `(station_id, ts)`, value là bản ghi thô | Khử trùng lặp, loại giá trị âm hoặc ngoài ngưỡng, nội suy các điểm thiếu ngắn | `/clean` |
| 2. Tính AQI | key `(station_id, ts)`, value là IAQI từng chất theo bảng breakpoint | `AQI = max(IAQI)`, gán mức và chất trội | `/aqi` |
| 3. Tổng hợp | key `(city, date)` hoặc `(city, level)` | Trung bình, max, đếm phân bố mức, xếp hạng | `/agg` |

Trên Spark DataFrame, "Mapper" tương ứng với `withColumn` / `map` và "Reducer" tương ứng với
`groupBy().agg()`. Báo cáo vẫn trình bày theo ngôn ngữ Mapper/Reducer để bám bài báo gốc.

## 4. Cấu trúc thư mục

```
.
├── README.md               file này
├── CONTRACTS.md            định nghĩa schema và giao diện giữa các thành phần
├── .env.example            mẫu biến môi trường (OWM_API_KEY, Kafka, HDFS, HBase)
├── collector/              gọi OpenWeather, ghi Kafka và HDFS
│   ├── src/                owm_client, normalize, backfill_history, live_poller, kafka_producer
│   ├── config/cities.json  200 điểm quan trắc
│   └── tests/              kiểm tra schema của response thật
├── spark/
│   ├── aqi_core/           công thức AQI dùng chung (iaqi.py, bảng breakpoint VN và EPA)
│   ├── jobs/               Pha 1/2/3, streaming, nạp lịch sử vào HBase, phân cụm, dự báo
│   ├── sinks/              ghi HBase (row key, cột)
│   ├── experiments/        đo scalability của Pha 2
│   └── tests/              unit test (pytest)
├── serving/                FastAPI đọc HBase cho Grafana
├── grafana/                dashboard và cấu hình datasource
├── docker/                 docker-compose toàn bộ hệ thống
├── scripts/                khởi tạo topic Kafka, thư mục HDFS, bảng HBase; sinh dashboard
├── data/
│   ├── fixtures/           response nguyên bản của API
│   └── samples/            dữ liệu mẫu để chạy Spark cục bộ không cần hạ tầng
├── final_results/
│   ├── json/               kết quả phân cụm và backtest dự báo của lần chạy cuối
│   └── scalability/        kết quả đo scalability Pha 2 (CSV thời gian, speedup, xác nhận executor)
└── docs/
    ├── cai-dat.md          cài đặt, cấu hình máy, kết nối HBase–HDFS, tham số đã chỉnh
    ├── huong-dan-chay.md   các lệnh chạy Pha 1→3, nạp HBase, streaming, phân cụm, dự báo
    ├── experiments.md      số liệu thực nghiệm
    ├── images/, experiments_data/   biểu đồ và số đo thô của thực nghiệm scalability
    └── report/             báo cáo Word: thu thập dữ liệu, kết quả lần chạy cuối
```

## 5. Các thành phần chính

### 5.1. `CONTRACTS.md`

Định nghĩa những thứ mà các thành phần phải dùng giống hệt nhau: schema message (C1), cây thư mục HDFS
(C2), schema parquet sau Pha 2 (C3), row key HBase (C4), endpoint API (C5), topic Kafka (C6), biến môi
trường (C7) và định dạng hai file kết quả của nhánh mở rộng (C8). Muốn đổi mục nào thì sửa file này
trước rồi mới sửa code.

### 5.2. `docker/docker-compose.yml`

Khai báo các service: Kafka (KRaft), HDFS (namenode, datanode), Spark (master, worker), ZooKeeper,
HBase (master, regionserver), HBase Thrift, FastAPI và Grafana. Nên dựng từng service một và kiểm tra
xong mới thêm service tiếp theo, thứ tự `kafka → hdfs → spark → zookeeper → hbase → grafana`; dựng
cả file một lần thì nhiều lỗi xuất hiện cùng lúc và khó biết lỗi nào gây ra lỗi nào. Chi tiết cấu hình
máy xem [docs/cai-dat.md](docs/cai-dat.md).

### 5.3. `collector/`

Phần tạo ra dữ liệu cho toàn bộ hệ thống.

| File | Vai trò |
|---|---|
| `src/owm_client.py` | Bọc hai endpoint OpenWeather (hiện tại, lịch sử); xử lý retry, throttle ≤ 1 call/giây, lỗi 401 và 429 |
| `src/normalize.py` | Đổi response thô của API thành schema C1. API không có trường nào định danh thành phố, nên `station_id`, `city`, `country` được gắn vào từ `config/cities.json` |
| `src/backfill_history.py` | Quét lịch sử 200 điểm × 5 năm, ghi HDFS `/air-quality/raw/`, có checkpoint để chạy lại được |
| `src/live_poller.py` | Gọi API hiện tại mỗi giờ và gửi vào Kafka (OpenWeather cập nhật theo giờ nên gọi dày hơn không có thêm dữ liệu) |
| `src/kafka_producer.py` | Producer, key là `station_id` để cùng một trạm luôn vào cùng partition và giữ đúng thứ tự thời gian |
| `config/cities.json` | 200 điểm quan trắc; vừa là đầu vào của collector, vừa là bảng tra thông tin trạm |

Backfill chia theo cửa sổ 90 ngày vì một lần gọi lấy được đủ 90 ngày (thử thực tế: 2.041 bản ghi, không
bị cắt). So với chia 30 ngày, cách này giảm số call từ 12.200 xuống 4.200 (200 trạm × 21 cửa sổ), khoảng
1,2 giờ ở tốc độ 1 call/giây thay vì 3,4 giờ. Đã kiểm tra rằng tách một cửa sổ 90 ngày thành hai cửa sổ 45 ngày trả về đúng các mốc thời gian
cũ, nên các giờ bị thiếu là thiếu ở nguồn OpenWeather chứ không do cách chia cửa sổ.

### 5.4. Kiểm chứng schema với API thật

Ngày 13/09/2026 đã gọi API thật (TP.HCM: 1 bản ghi hiện tại và 145 bản ghi lịch sử 7 ngày) để đối chiếu
các giả định. Response lưu ở `data/fixtures/owm_air_pollution_raw.json`.

| Giả định | Kết quả |
|---|---|
| Top-level `{coord, list}`, `list[i] = {main, components, dt}` | đúng |
| `components` có đúng 8 trường, đơn vị µg/m³ | đúng, history và current giống nhau |
| Bảng breakpoint 1–5 của OpenWeather | khớp 146/146 bản ghi |
| `coord` trả về bằng toạ độ đã gửi | sai, lệch khoảng 78 m |
| `dt` luôn tròn giờ | sai với endpoint hiện tại |
| History trả đủ mọi mốc giờ | sai, chỉ có 85,8% |

Hai giả định sai đã trở thành quy tắc trong `CONTRACTS.md`:

- R1, timestamp: `history` tròn giờ nhưng `current` thì không (ví dụ `11:37:10Z`). `normalize()` làm
  tròn xuống giờ và giữ số gốc ở `dt_raw`. Nếu không làm vậy, bản ghi live 11:37 và bản ghi history
  11:00 là hai bản ghi khác nhau, `dropDuplicates` không bắt được và cùng một giờ bị đếm hai lần.
- R2, toạ độ: `lat`/`lon` luôn lấy từ `cities.json`, không lấy từ `coord` của response.

Dữ liệu thiếu có quy luật: trong 90 ngày thật có 4 khoảng đứt (24h, 24h, 48h, 24h), đều là khối nguyên
ngày và đều bắt đầu đúng 01:00 UTC. Vì vậy Pha 1 không nội suy qua các khoảng dài mà đánh dấu ngày đó là
thiếu dữ liệu. Tỉ lệ đầy đủ trên 90 ngày là 94,4%; mẫu 7 ngày cho 85,8% vì rơi trúng một khoảng đứt, nên
không nên kết luận từ cửa sổ quá ngắn. Cửa sổ 7 ngày cũng không đủ để hiệu chỉnh phân phối: PM2.5 lớn
nhất là 10,1 trong 7 ngày (mùa mưa) nhưng 112,9 trong 90 ngày.

### 5.5. `data/`

| File | Nội dung |
|---|---|
| `fixtures/owm_air_pollution_raw.json` | Response nguyên bản của API |
| `samples/air_quality_sample.jsonl` | 10.142 bản ghi đã chuẩn hoá theo C1 (5 trạm × 90 ngày), sinh bằng mô hình thống kê hiệu chỉnh theo số liệu thật |
| `samples/generate_sample.py` | Script sinh lại sample (seed cố định) |
| `samples/ext_*_sample.json` | Mẫu hai file kết quả của nhánh mở rộng, dùng khi chưa có kết quả thật |

Sample là dữ liệu sinh giả, dùng để chạy Spark cục bộ (`local[*]`, không cần Kafka, HDFS hay Docker) và
để test, không dùng để rút ra kết luận phân tích. Mọi số liệu trong báo cáo phải lấy từ dữ liệu thật trên HDFS.

### 5.6. `spark/aqi_core/`

`iaqi.py` là phần cốt lõi của đồ án:

```
        I_high - I_low
IAQI = ----------------- × (C - BP_low) + I_low        nội suy tuyến tính, mỗi chất một giá trị
        BP_high - BP_low

AQI  = max(IAQI của các chất)                          chất nào tệ nhất quyết định AQI
```

PM2.5 và PM10 dùng Nowcast trên cửa sổ 12 giờ, các chất còn lại dùng giá trị theo giờ. Làn batch và làn
streaming cùng import module này; không sao chép công thức sang nơi khác, vì như vậy hai làn có thể cho
hai kết quả khác nhau. Bảng breakpoint nằm trong `breakpoints_vn.json` và `breakpoints_epa.json` để đổi
chuẩn mà không sửa code.

Các test trong `spark/tests/test_iaqi.py` đối chiếu với các ví dụ tính tay theo QĐ 1459.

### 5.7. `spark/jobs/`

| Job | Vào → ra | Nội dung |
|---|---|---|
| `phase1_clean.py` | `/raw` → `/clean` | Khử trùng lặp, loại giá trị âm và ngoài ngưỡng vật lý, dựng lưới giờ đầy đủ để lộ ra các giờ thiếu, nội suy các khoảng thiếu ≤ 3 giờ, gắn cờ chất lượng (`ok`, `interpolated`, `missing`) và cờ ngày không đủ dữ liệu |
| `phase1_clean_resumable_v4.py` | `/raw` → `/clean` | Bản Pha 1 chia thành ba giai đoạn (grid, interpolate, final) có thể chạy tiếp khi bị ngắt; dùng khi chạy trên toàn bộ dữ liệu 5 năm. Chạy qua `scripts/run_phase1_v4.sh` |
| `phase2_aqi.py` | `/clean` → `/aqi` | Tính IAQI từng chất và AQI bằng `aqi_core`; có thể xuất bảng đối chiếu `aqi` tự tính với `owm_aqi` |
| `phase3_aggregate.py` | `/aqi` → `/agg` | Trung bình AQI theo (thành phố, ngày), phân bố 6 mức, xếp hạng thành phố |
| `load_history_to_hbase.py` | `/aqi` → HBase | Nạp lịch sử vào HBase (ghi theo row key nên chạy lại chỉ ghi đè cùng dòng) |
| `streaming_aqi.py` | Kafka → HBase, HDFS | Đọc Kafka, tính AQI bằng `aqi_core`, ghi HBase và lưu bản ghi thô vào HDFS. Lấy 11 giờ trước đó của từng trạm từ HBase để tính Nowcast, nên phải nạp lịch sử vào HBase trước khi bật streaming |
| `ext_clustering.py` | `/aqi` → JSON | Phân cụm (thành phố, tháng) bằng K-means, GMM, Bisecting K-means, DBSCAN, HDBSCAN; chọn theo silhouette Euclidean chung |
| `ext_forecast.py` | `/aqi` → JSON | Dự báo AQI sau 24 giờ ở ba tầng: SGD hồi quy trực tuyến, Random Forest, CNN-LSTM |

### 5.8. `serving/app/main.py`

Grafana không nối thẳng vào HBase mà đi qua FastAPI, để đổi schema HBase chỉ phải sửa một chỗ.

| Endpoint | Nội dung |
|---|---|
| `GET /health` | Kiểm tra dịch vụ |
| `GET /stations` | Danh sách trạm (lọc theo `country`) |
| `GET /aqi/latest` | AQI mới nhất của từng trạm |
| `GET /aqi/timeseries` | Chuỗi AQI của một trạm theo khoảng thời gian |
| `GET /aqi/ranking` | Xếp hạng thành phố theo ngày |
| `GET /ext/clusters` | Kết quả phân cụm, lọc theo `month`, `country` |
| `GET /ext/forecast` | Backtest dự báo, lọc theo `station_id` |

Hai endpoint `/ext/*` chỉ đọc file JSON trong `final_results/json/` (nếu chưa có thì dùng file mẫu trong
`data/samples/`); chúng không chạy mô hình nào. Schema xem CONTRACTS.md, mục C5 và C8.

### 5.9. `scripts/`, `grafana/`

| File | Vai trò |
|---|---|
| `scripts/01_create_topics.sh` | Tạo hai topic Kafka theo C6 |
| `scripts/02_init_hdfs.sh` | Tạo cây thư mục HDFS theo C2 |
| `scripts/03_init_hbase.sh` | Tạo bảng `air_quality` theo C4 |
| `scripts/run_phase1_v4.sh` | Chạy Pha 1 bản chịu lỗi trong container `spark-master` |
| `scripts/run_scalability_distributed.sh` | Đo scalability Pha 2 với 4 worker Spark Standalone (1 core mỗi worker) |
| `scripts/generate_grafana_dashboard.py` | Sinh `grafana/dashboards/aqi-overview.json` |
| `scripts/generate_cities_200.py`, `curate_global_50.py` | Dựng danh sách 200 điểm trong `cities.json` |
| `grafana/` | Dashboard (UID `aqi-big-data-overview`, file `dashboards/aqi-overview.json`) và provisioning; datasource Infinity trỏ vào `http://serving-api:8000` |

## 6. Kết quả chính

Chi tiết, kèm các hạn chế của từng phép đo, ở [docs/experiments.md](docs/experiments.md).

| Nội dung | Kết quả |
|---|---|
| Dữ liệu thu thập | 200 điểm, 2021-09-01 → 2026-09-01, 8.576.904 bản ghi thô (97,80% so với kỳ vọng; phần thiếu là thiếu ở nguồn OpenWeather), 279,4 MB nén |
| Scalability Pha 2 (Spark Standalone, executor thật) | 8M bản ghi: 749,3 s với 1 executor, 308,8 s với 4 executor, tăng tốc 2,43 lần; speedup tăng theo kích thước dữ liệu (ở 100K dòng thì 4 executor không nhanh hơn 2) |
| Phân cụm | K-means k=3 chọn theo silhouette, giá trị 0,3547 (cấu trúc yếu theo thang Kaufman & Rousseeuw) nhưng ba cụm giải thích được: nền chung, ô nhiễm nặng, sạch |
| Dự báo AQI 24 giờ | Random Forest thắng nhẹ: RMSE 20,10, R² 0,7297; SGD và CNN-LSTM sát sau (R² 0,7243 và 0,7251) |

Một số hạn chế cần biết khi đọc các con số trên (giải thích đầy đủ trong `docs/experiments.md`):
scalability đo bằng các executor chạy trên cùng một máy (container Docker) chứ không phải cụm nhiều máy, và mỗi
cấu hình chỉ đo một lần; Random Forest chạy với tham số nhỏ hơn thiết kế (`num_trees=20`, `max_depth=5`) do giới
hạn tài nguyên trên driver; tập test của CNN-LSTM nhỏ hơn hai tầng còn lại nên không so sánh trực tiếp được;
K-means chỉ hơn Bisecting K-means 0,015 silhouette.

## 7. Chạy thử

### 7.1. Không cần hạ tầng: test và chạy Spark cục bộ trên dữ liệu mẫu

Cần Python 3.10 trở lên và Java 17 (PySpark 3.5 cần JVM).

```bash
cd spark
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest                                   # 97 test

python jobs/phase1_clean.py --input ../data/samples/air_quality_sample.jsonl --output /tmp/aqi_clean
python jobs/phase2_aqi.py   --input /tmp/aqi_clean --output /tmp/aqi_aqi
python jobs/phase3_aggregate.py --input /tmp/aqi_aqi --output /tmp/aqi_agg
```

### 7.2. Toàn bộ hệ thống (Docker, khuyến nghị Windows + WSL2)

```bash
cp .env.example .env                     # điền OWM_API_KEY
docker compose --env-file .env -f docker/docker-compose.yml up -d
bash scripts/01_create_topics.sh
bash scripts/02_init_hdfs.sh
bash scripts/03_init_hbase.sh
```

Sau đó chạy các job theo [docs/huong-dan-chay.md](docs/huong-dan-chay.md). Các địa chỉ để kiểm tra:
FastAPI `http://localhost:8000/docs`, Grafana `http://localhost:3000`, HDFS `http://localhost:9870`,
HBase `http://localhost:16010`, Spark master `http://localhost:8080`.

## 8. Máy nào chạy được phần nào

| Phần | macOS (Apple Silicon) | Windows 11 + WSL2 + Docker Desktop |
|---|---|---|
| Unit test (`pytest`), Spark `local[*]` trên dữ liệu mẫu | chạy được | chạy được |
| Đo scalability (`spark/experiments/`) | chạy được | chạy được |
| Phân cụm, dự báo trên dữ liệu mẫu | chạy được | chạy được |
| Kafka, HDFS, HBase, FastAPI, Grafana (`docker compose`) | không chạy | chạy được |
| Backfill 5 năm, Pha 1→3 trên dữ liệu thật, streaming, phân cụm và dự báo trên dữ liệu thật | không chạy | chạy được |

Lý do: image Hadoop và HBase dùng trong compose (`bde2020/*`) chỉ có bản amd64, nên trên chip Apple Silicon
phải chạy giả lập, chậm và HBase hay chết vặt; phần code Spark thì không phụ thuộc kiến trúc CPU. Cấu hình
cần thiết cho máy chạy Docker (RAM cấp cho WSL2, các tham số đã chỉnh, cách kiểm tra kết nối HBase–HDFS)
nằm ở [docs/cai-dat.md](docs/cai-dat.md).

## 9. Tài liệu khác

- [CONTRACTS.md](CONTRACTS.md): schema và giao diện giữa các thành phần
- [docs/cai-dat.md](docs/cai-dat.md): cài đặt, cấu hình, kết nối và tham số
- [docs/huong-dan-chay.md](docs/huong-dan-chay.md): lệnh chạy từng bước
- [docs/experiments.md](docs/experiments.md): số liệu thực nghiệm
- [docs/report/](docs/report/): hai báo cáo Word, một của phần thu thập dữ liệu và một của kết quả lần chạy cuối
- [data/samples/README.md](data/samples/README.md): cách dữ liệu mẫu được hiệu chỉnh
