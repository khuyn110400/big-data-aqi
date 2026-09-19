# Bàn giao B → A: chạy Pha 1→2→3, nạp HBase, bật streaming, phân cụm và dự báo

Người B viết code trên Mac (không có Docker), nên **các lệnh dưới đây chưa được chạy thử trên stack
thật**. Logic đã được test bằng bảng HBase giả và Spark local (97 test xanh), nhưng phần nối với
Kafka/HDFS/HBase thật chỉ chạy được ở máy của Người A. Lệnh nào lỗi thì gửi log cho B, đừng sửa `spark/`.

**Cách đọc file này:** mục *Tổng quan* và *A1–A7* là phần việc của A để chuẩn bị; các bước 0–4 là lệnh chạy final;
mục 5 là việc sau khi chạy; *Nghiệm thu* là checklist.

**Thứ tự bắt buộc khi chạy:** Pha 1→2→3 → nạp lịch sử vào HBase → (tuỳ chọn) bật streaming.
Streaming lấy 11 giờ trước đó của từng trạm từ HBase để tính Nowcast, nên HBase phải có lịch sử trước.

---

## Tổng quan: ai làm gì, theo thứ tự

| # | Việc | Ai | Ở đâu |
|---|---|---|---|
| 1 | Viết code, test, xuất JSON mẫu | B | Mac (đã xong) |
| 2 | Chuẩn bị phía A: image Spark, endpoint `/ext/*`, panel Grafana, duyệt C8 (mục A1–A7 bên dưới) | A | WSL2; làm ngay được bằng file JSON mẫu |
| 3 | Gộp nhánh, đánh tag `final-run` | A + B | git |
| 4 | Chạy final: các bước 0 → 4 | A | WSL2 |
| 5 | Đưa JSON thật vào demo, gửi kết quả cho B (mục 5) | A | WSL2 |
| 6 | Đọc số liệu, **chọn tay** thuật toán và tầng dự báo thắng, điền `docs/experiments.md`, báo A đường cần nổi bật | B | Mac |

Bước 1 và 2 độc lập nhau: A không phải chờ số thật, vì endpoint và panel dựng được bằng file mẫu ở `data/samples/`.

---

## A. Việc của A trước khi chạy final

### A1. Lấy code và xử lý nhánh

- Lấy nhánh `feat/b-hbase-streaming` (chứa cả code stack của A lẫn code của B). B đẩy nhánh lên trước.
- `origin/main` đã có PR #3 (bản backfill ghi thư mục thường + notebook Colab), xung đột với nhánh của A ở
  `collector/src/backfill_history.py` và `collector/config/cities.json`. Khi gộp phải **giữ `cities.json` và
  `backfill_history.py` của A**: 8,58 triệu bản ghi trên HDFS mang `station_id` theo đúng bản `cities.json` đó.
  Cần thống nhất đích của PR (nên gộp `feat/a-collector-backfill` vào `main` trước, rồi mới PR nhánh này).
- Khi code chạy final đã chốt, đánh tag (`git tag final-run`) để báo cáo ghi rõ số liệu đến từ phiên bản nào.

### A2. Image Spark có sẵn thư viện (thay cho `pip install` tay)

Bước 0 và bước 4 đang bắt `pip install` tay trong container đang chạy, và mất khi container bị tạo lại.
Đề xuất `docker/spark/Dockerfile` và `build:` cho `spark-master` trong compose. Chỉ `spark-master` cần, vì các job
chạy `local[*]`.

```dockerfile
# docker/spark/Dockerfile — CHƯA kiểm chứng trên image bitnamilegacy/spark:3.5, A chỉnh nếu lỗi
FROM bitnamilegacy/spark:3.5
USER root
RUN pip install --no-cache-dir happybase pandas numpy pyarrow scikit-learn tensorflow-cpu
USER 1001
```

`tensorflow-cpu` nặng (~500 MB); bỏ đi nếu không chạy tầng 3 CNN-LSTM. Có thể mount `../spark:/tmp/aqi/spark:ro`
thay cho `docker cp` để không phải copy lại mỗi lần B sửa code. Làm xong A2 thì bỏ qua các lệnh `pip install` ở bước 0 và 4.

### A3. Tài nguyên máy

WSL2 nên có RAM tối thiểu 8 GB (`.wslconfig`, theo WORKPLAN). Hai job nặng nhất là Pha 1 trên dữ liệu 5 năm và
`ext_forecast.py` (Random Forest chạy ở driver, heap 4 GB).

### A4. FastAPI: hai endpoint `/ext/clusters` và `/ext/forecast`

Viết trong `serving/app/main.py`. Hai endpoint **chỉ đọc file JSON rồi trả lại**, giống `load_stations()` đọc
`cities.json`. Chúng không chạy model nào và code **không phụ thuộc model nào thắng**: viết một lần bằng file mẫu,
sau này chỉ thay nội dung file.

| Method | Path | Query | Trả về |
|---|---|---|---|
| GET | `/ext/clusters` | `month?` (1–12) | `{algorithm, params, silhouette, n_clusters, rows}`; `rows` lọc theo tháng nếu có |
| GET | `/ext/forecast` | `station_id` (bắt buộc), `from?`, `to?` | `{horizon_hours, models, metrics, rows}`; `rows` của trạm đó, lọc `ts_utc` theo `from`/`to` |

Schema hai file nằm ở `CONTRACTS.md` mục C8. Gợi ý cài đặt:

- Thư mục dữ liệu lấy từ biến môi trường (ví dụ `EXT_DATA_DIR`, mặc định `serving/demo_data`) và mount bằng volume
  trong compose, để thay file mà không phải build lại image. Đọc lại file khi thời gian sửa đổi (mtime) đổi.
- File chưa có: trả 404 kèm thông báo rõ, để Grafana hiện "No data" thay vì lỗi.
- `station_id` không nằm trong `stations` của file: trả 404 (backtest chỉ có vài trạm).
- Dựng và thử bằng `data/samples/ext_clusters_sample.json` và `data/samples/ext_forecast_backtest_sample.json`
  (dữ liệu giả, không dùng làm số liệu báo cáo).

### A5. Grafana: hai panel mới

Thêm vào `scripts/generate_grafana_dashboard.py` rồi sinh lại `grafana/dashboards/aqi-overview.json`. Datasource
Infinity, root selector `rows`.

- **Bản đồ cụm** (`geomap`): tô màu điểm theo `cluster` (`-1` là nhiễu, tô xám), có biến chọn tháng (`month`),
  tooltip gồm thành phố, `avg_pm2_5`, `avg_pm10`. Tiêu đề nên hiện tên thuật toán lấy từ trường `algorithm`.
- **Thật vs dự báo** (`timeseries`): trục thời gian `ts_utc`, 4 đường `actual`, `sgd`, `rf`, `cnn_lstm` (bỏ qua
  giá trị `null`), theo biến `station_id`; thêm bảng nhỏ `metrics` (RMSE, MAE, R²). Backtest chỉ có vài trạm nên
  biến trạm nên giới hạn theo `stations` trong file.
- Tầng nào thắng do **B chọn tay** sau khi có số thật, rồi báo A đường cần nổi bật (đậm hơn hoặc đổi màu). Đó chỉ là
  cấu hình panel, không đụng endpoint hay file JSON.

### A6. `CONTRACTS.md`

Duyệt hoặc sửa mục C8 do B đề xuất (schema hai file và tên endpoint). Nếu đồng ý, thêm hai endpoint vào bảng C5 và ghi
vào nhật ký thay đổi. Muốn đổi tên trường thì báo B trước, vì B phải sửa code xuất.

### A7. (Tuỳ chọn) `scripts/run_final.sh`

Gộp các bước 0–4 thành một script chạy tuần tự, mỗi bước ghi log vào `final_run_logs/` cùng `time` và kích thước từng
tầng HDFS (`hdfs dfs -du`). Giảm nguy cơ sót bước và gom sẵn số cho báo cáo. Các lệnh đã có sẵn trong runbook này;
B soạn hộ nếu A muốn.

---

## 0. Chuẩn bị (làm 1 lần, lặp lại nếu recreate container)

```bash
# ở thư mục gốc repo, nhánh feat/b-hbase-streaming
docker compose --env-file .env -f docker/docker-compose.yml ps        # 11 service phải Up
bash scripts/03_init_hbase.sh                                          # bảng air_quality nếu chưa có

# copy code Spark vào container spark-master
docker exec spark-master mkdir -p /tmp/aqi
docker cp spark spark-master:/tmp/aqi/

# happybase để ghi HBase qua Thrift (chỉ cần trên spark-master)
docker exec -u root spark-master pip install happybase
docker exec spark-master python3 -c "import happybase; print('happybase', happybase.__version__)"
```

Nếu `-u root` không được: `docker exec spark-master pip install --user happybase`
(compose đã đặt `HOME=/tmp` nên cài `--user` vào được).

Đặt sẵn một hàm cho gọn (bash/zsh):

```bash
sp() { docker exec spark-master spark-submit --master 'local[*]' --driver-memory 2g "$@"; }
HDFS=hdfs://namenode:9000/air-quality
```

---

## 1. Pha 1 → 2 → 3 trên raw thật

```bash
time sp /tmp/aqi/spark/jobs/phase1_clean.py     --input $HDFS/raw   --output $HDFS/clean
time sp /tmp/aqi/spark/jobs/phase2_aqi.py       --input $HDFS/clean --output $HDFS/aqi --comparison-output /tmp/owm_comparison
time sp /tmp/aqi/spark/jobs/phase3_aggregate.py --input $HDFS/aqi   --output $HDFS/agg
```

Pha 1 đã được kiểm chứng trên layout phân vùng `ingest_mode=/country=/dt=/part-*.jsonl.gz` (dựng từ
`data/samples/`, 267 thư mục): kết quả giống hệt bản đọc file phẳng, 0 dòng khác biệt. Vì vậy lỗi ở bước
này nhiều khả năng do môi trường (RAM, đường dẫn HDFS) chứ không phải logic; nếu gặp lỗi lạ, gửi nguyên
stack trace cho B.

Nếu hết RAM: chạy từng nước, ví dụ `--input $HDFS/raw/ingest_mode=history/country=VN --output $HDFS/clean_VN`.

Ghi lại để B điền `docs/experiments.md`:

```bash
docker exec namenode hdfs dfs -du -s -h /air-quality/raw /air-quality/clean /air-quality/aqi /air-quality/agg
docker cp spark-master:/tmp/owm_comparison ./owm_comparison      # bảng đối chiếu aqi vs owm_aqi
```
cùng với thời gian `time` của từng pha và phần báo cáo chất lượng / top xếp hạng mà Pha 1, Pha 3 in ra.

---

## 2. Nạp lịch sử vào HBase

```bash
# thử trước: chỉ đếm + in mẫu, không ghi
sp /tmp/aqi/spark/jobs/load_history_to_hbase.py --input $HDFS/aqi --since 2026-06-01 --dry-run

# ghi thật
time sp /tmp/aqi/spark/jobs/load_history_to_hbase.py --input $HDFS/aqi --since 2026-06-01 --hbase-host hbase-thrift
```

- `--since 2026-06-01` (~90 ngày, khoảng 440 nghìn dòng) là đủ cho dashboard. Nạp cả 5 năm (~8,5
  triệu dòng) qua Thrift sẽ lâu, chỉ làm nếu thật sự cần.
- Chạy lại an toàn: ghi theo row key nên chỉ ghi đè cùng dòng.

Kiểm tra:

```bash
echo "scan 'air_quality', {LIMIT => 2}" | docker exec -i hbase-master hbase shell
curl -s "http://localhost:8000/aqi/latest?limit=3"
curl -s "http://localhost:8000/aqi/ranking?dt=2026-08-31" | head -c 600
curl -s "http://localhost:8000/aqi/timeseries?station_id=VN_HCM_01&from=2026-08-30T00:00:00Z&to=2026-08-31T00:00:00Z" | head -c 600
```
Sau bước này các panel Grafana ("AQI Ranking", "AQI Time Series", "Latest AQI") phải có dữ liệu.

---

## 3. Streaming (Kafka → AQI → HBase + HDFS)

```bash
docker exec spark-master spark-submit --version     # xem đúng bản Spark, ví dụ 3.5.x
```

Chạy thử một lượt (xử lý hết dữ liệu đang có trong Kafka rồi thoát), thay `3.5.X` bằng đúng bản trên:

```bash
# terminal 1 (máy host): đẩy dữ liệu live vào Kafka
python collector/src/live_poller.py --once

# terminal 2: xử lý
docker exec spark-master spark-submit --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.X \
  /tmp/aqi/spark/jobs/streaming_aqi.py --starting-offsets earliest --once
```
Container cần ra Internet để tải connector Kafka khi dùng `--packages`.

Kiểm tra: log in `batch 0: {'received': 200, ...}`; `curl /aqi/latest` có giờ hiện tại;
`docker exec namenode hdfs dfs -ls -R /air-quality/raw/ingest_mode=live | head`.

Chạy liên tục (bỏ `--once`, `--starting-offsets latest`), log ra file:

```bash
docker exec -d spark-master sh -c "spark-submit --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.X \
  /tmp/aqi/spark/jobs/streaming_aqi.py > /tmp/streaming.log 2>&1"
docker exec spark-master tail -f /tmp/streaming.log
```

**Thử đường DLQ** (B không test được vì cần Kafka thật): gửi một message sai schema rồi đọc topic DLQ.

```bash
echo '{"hong": 1}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic air-quality-raw
# chạy streaming một lượt như trên, rồi:
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic air-quality-dlq --from-beginning --max-messages 1
```

Lưu ý: `--starting-offsets` chỉ có tác dụng ở lần chạy đầu. Muốn đọc lại từ đầu, xoá checkpoint:
`docker exec namenode hdfs dfs -rm -r /air-quality/_checkpoints/streaming_aqi`.

---

## 4. Nhánh mở rộng: phân cụm và dự báo (cả 3 tầng)

Chạy **sau khi có `$HDFS/aqi`** (bước 1). Cả 3 tầng của mỗi nhánh nằm trong 2 file `ext_*.py`, không
còn bước xuất CSV lên Kaggle.

```bash
# thư viện Python cho driver (trong spark-master)
docker exec -u root spark-master pip install pandas numpy pyarrow scikit-learn
docker exec -u root spark-master pip install tensorflow-cpu     # CHỈ cho CNN-LSTM, nặng ~500 MB; bỏ qua nếu không cần

# phân cụm: K-means, GMM, Bisecting, DBSCAN, HDBSCAN (dữ liệu nhỏ, chạy nhanh)
time sp /tmp/aqi/spark/jobs/ext_clustering.py --input $HDFS/aqi --output $HDFS/ext/clusters \
    --json-out /tmp/ext_clusters.json

# dự báo AQI 24h: SGD, Random Forest, CNN-LSTM. --output là thư mục TRONG container (không phải HDFS)
time sp --driver-memory 4g /tmp/aqi/spark/jobs/ext_forecast.py --input $HDFS/aqi --output /tmp/forecast_model \
    --since 2025-09-01 --backtest-out /tmp/ext_forecast_backtest.json

# lấy hai file JSON cho demo ra khỏi container
docker cp spark-master:/tmp/ext_clusters.json ./ext_clusters.json
docker cp spark-master:/tmp/ext_forecast_backtest.json ./ext_forecast_backtest.json
```

- **Thứ tự quan trọng:** với dữ liệu thật (~8,5 triệu dòng), `ext_forecast.py` tính lag và huấn luyện Random
  Forest ngay trên driver, rất nặng. Nên bắt đầu bằng `--since` (ví dụ 12 tháng gần nhất) và thêm
  `--skip-cnn-lstm` để chạy tầng 1+2 trước. Hết RAM thì tăng `--driver-memory` hoặc rút ngắn `--since`.
- **CNN-LSTM chạy trên CPU của driver**, chỉ dùng tối đa `--cnn-max-rows` chuỗi (mặc định 300.000, lấy mẫu
  ngẫu nhiên) và `--cnn-epochs` epoch (mặc định 30, có early stopping). Thiếu `tensorflow` thì tầng 3 tự
  bỏ qua và in rõ, tầng 1+2 vẫn chạy.
- **Ghi lại để B điền `docs/experiments.md` §5:** bảng tổng kết mà mỗi script in ra cuối (phân cụm in số
  cụm và tỉ lệ nhiễu của từng thuật toán; dự báo in RMSE, MAE, R² của 3 tầng), cùng thời gian `time`.
  Chép nguyên văn bảng, kèm câu lệnh và giá trị `--since` đã dùng.
- **Hai file JSON cho demo** (schema đề xuất ở `CONTRACTS.md` mục C8, file mẫu ở `data/samples/ext_*_sample.json`):
  - `--json-out` ghi `ext_clusters.json`: nhãn cụm của thuật toán thắng, theo (thành phố, tháng).
  - `--backtest-out` ghi `ext_forecast_backtest.json`: giá trị thật và dự báo của **cả 3 tầng** cho vài trạm,
    N ngày cuối của tháng test (`--backtest-days`, mặc định 30; `--backtest-stations` để chọn trạm, mặc định
    `VN_HCM_01,VN_HAN_01,IN_DEL_01,CN_BJS_01,JP_TYO_01` nếu có dữ liệu). Ghi đủ 3 tầng để chọn model thắng
    sau khi chạy mà không phải chạy lại. Thiếu `tensorflow` thì cột `cnn_lstm` là `null`.
  - Đưa hai file vào FastAPI và gửi cho B theo mục 5. Tầng/thuật toán thắng do B chọn tay khi đọc số liệu.
- `ext_forecast.py` chạy bằng `python` thì mặc định heap driver 4g (đổi bằng biến `EXT_DRIVER_MEMORY`); chạy bằng
  `spark-submit` thì dùng `--driver-memory` như trên. Heap 2g từng bị tràn ở bước Random Forest trên dữ liệu mẫu.
- Lệnh `docker cp` để lấy model ra ngoài (nếu cần): `docker cp spark-master:/tmp/forecast_model ./forecast_model`.

---

## 5. Sau khi chạy final: đưa kết quả vào demo và gửi lại cho B

1. Copy hai file JSON thật vào thư mục dữ liệu của FastAPI (thay file mẫu), rồi kiểm tra:

   ```bash
   curl -s "http://localhost:8000/ext/clusters?month=8" | head -c 400
   curl -s "http://localhost:8000/ext/forecast?station_id=VN_HCM_01" | head -c 400
   ```
   Mở Grafana: hai panel mới phải có dữ liệu.

2. Gửi cho B (thư mục `final_run_logs/` nếu dùng `run_final.sh`, hoặc chép tay):
   - Thời gian `time` từng pha (Pha 1, 2, 3) và kích thước các tầng HDFS (raw, clean, aqi, agg).
   - Báo cáo chất lượng Pha 1, bảng đối chiếu `owm_comparison` (CSV), top xếp hạng Pha 3.
   - Số dòng HBase đã nạp, giá trị `--since` đã dùng, một dòng log batch streaming (`received: 200`), kết quả thử DLQ.
   - Bảng tổng kết phân cụm (5 thuật toán) và bảng dự báo (3 tầng) mà hai script in ra cuối, kèm câu lệnh và `--since`.
   - Hai file JSON, và ảnh chụp dashboard (các panel chính cùng hai panel mới).
   - Mã commit của code đã chạy (`git rev-parse HEAD`) và tag `final-run`.

3. B đọc số liệu, **chọn tay** thuật toán phân cụm và tầng dự báo thắng, điền `docs/experiments.md` (§1, §3, §5), rồi
   báo A đường nào cần nổi bật trên panel dự báo. File JSON không có trường "model thắng"; đổi cách hiển thị chỉ cần
   sửa panel, không đổi endpoint hay file.

---

## Nghiệm thu: xong khi

- [ ] `$HDFS/clean`, `$HDFS/aqi`, `$HDFS/agg` có dữ liệu; log Pha 1→3 không lỗi.
- [ ] HBase có dữ liệu (`scan` ra dòng); `/aqi/latest`, `/aqi/ranking`, `/aqi/timeseries` trả số thật; các panel Grafana lõi có dữ liệu.
- [ ] Streaming: một lượt `--once` in `received: 200`; `curl /aqi/latest` có giờ hiện tại; thử DLQ đọc được 1 message lỗi.
- [ ] `ext_clustering.py` và `ext_forecast.py` chạy xong, in đủ bảng, tạo hai file JSON.
- [ ] `/ext/clusters` và `/ext/forecast` trả dữ liệu thật; hai panel mới có dữ liệu.
- [ ] Đã gửi B đủ danh sách ở mục 5.

---

## Sự cố thường gặp

| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| `ModuleNotFoundError: happybase` | Container bị recreate, mất pip install | Chạy lại bước 0 |
| `TTransportException` / timeout khi ghi HBase | hbase-thrift hoặc RegionServer chưa lên | `docker compose ps`; thử kết nối cổng 9090 như trong `docs/TASK_A_DATA_PLATFORM.md` |
| `Failed to find data source: kafka` | Thiếu `--packages` hoặc container không có Internet | Thêm `--packages`, kiểm tra mạng |
| `local[*]` bị shell mở rộng | Quên nháy đơn | Luôn viết `'local[*]'` |
| Pha 1 lỗi khi đọc raw trên HDFS | Thường là RAM hoặc đường dẫn (logic layout đã kiểm chứng) | Kiểm tra `$HDFS/raw` có file, tăng `--driver-memory`, gửi stack trace cho B |
| `/aqi/latest` trả `aqi: null` hoặc rỗng | HBase chưa có dữ liệu cho trạm đó | Kiểm tra `scan`, chạy lại bước 2 |
| `ModuleNotFoundError: sklearn` / `pandas` khi chạy `ext_*.py` | Chưa cài thư viện cho driver | Chạy lại `pip install` ở mục 4 |
| Dự báo báo `BỎ QUA CNN-LSTM` | Chưa cài `tensorflow-cpu` | Cài như mục 4, hoặc bỏ qua nếu chỉ cần tầng 1+2 |
| `ext_forecast.py` hết bộ nhớ / treo | Dữ liệu 5 năm quá lớn cho driver | Thêm `--since`, tăng `--driver-memory`, hoặc `--skip-cnn-lstm` |
| Streaming không ghi gì | Checkpoint cũ đã qua hết offset | Xoá checkpoint (xem trên) hoặc đẩy thêm dữ liệu |
