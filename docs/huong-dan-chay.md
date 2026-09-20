# Hướng dẫn chạy

Các lệnh dưới đây chạy trên máy đã dựng xong Docker stack (xem [cai-dat.md](cai-dat.md)), từ thư mục gốc của repo.
Toàn bộ code Spark được test bằng bảng HBase giả và Spark cục bộ (97 test, chạy được trên macOS); phần nối với
Kafka, HDFS và HBase thật chỉ chạy được trên máy có Docker stack.

**Thứ tự bắt buộc:** Pha 1 → 2 → 3, sau đó nạp lịch sử vào HBase, sau cùng mới (tuỳ chọn) bật streaming.
Streaming lấy 11 giờ trước đó của từng trạm từ HBase để tính Nowcast, nên HBase phải có lịch sử trước.
Phân cụm và dự báo chỉ cần `/air-quality/aqi` nên chạy được ngay sau Pha 2.

| Bước | Việc | Đầu ra |
|---|---|---|
| 0 | Chuẩn bị container `spark-master` | |
| 1 | Pha 1 → 2 → 3 trên dữ liệu thô | HDFS `/clean`, `/aqi`, `/agg` |
| 2 | Nạp lịch sử vào HBase | bảng `air_quality` |
| 3 | Streaming Kafka → AQI → HBase và HDFS | (tuỳ chọn) |
| 4 | Phân cụm và dự báo | `ext_clusters.json`, `ext_forecast_backtest.json` |
| 5 | Đưa kết quả vào FastAPI/Grafana và ghi lại số liệu | |
| 6 | (Tuỳ chọn) Đo scalability Pha 2 | `final_results/scalability/` |

Dữ liệu thô (`/air-quality/raw`) do collector tạo ra từ trước bằng `collector/src/backfill_history.py`
(lịch sử) và `collector/src/live_poller.py` (hiện tại).

---

## 0. Chuẩn bị (làm một lần, làm lại nếu container bị tạo lại)

```bash
docker compose --env-file .env -f docker/docker-compose.yml ps        # các service phải ở trạng thái Up
bash scripts/03_init_hbase.sh                                          # tạo bảng air_quality nếu chưa có

# copy code Spark vào container spark-master
docker exec spark-master mkdir -p /tmp/aqi
docker cp spark spark-master:/tmp/aqi/

# happybase để ghi HBase qua Thrift (chỉ cần trong spark-master)
docker exec -u root spark-master pip install happybase
docker exec spark-master python3 -c "import happybase; print('happybase', happybase.__version__)"
```

Nếu `-u root` không được: `docker exec spark-master pip install --user happybase` (compose đã đặt
`HOME=/tmp` nên cài `--user` vào được).

Đặt sẵn hai biến cho gọn (bash/zsh):

```bash
sp() { docker exec spark-master spark-submit --master 'local[*]' --driver-memory 2g "$@"; }
HDFS=hdfs://namenode:9000/air-quality
```

---

## 1. Pha 1 → 2 → 3 trên dữ liệu thô

```bash
time sp /tmp/aqi/spark/jobs/phase1_clean.py     --input $HDFS/raw   --output $HDFS/clean
time sp /tmp/aqi/spark/jobs/phase2_aqi.py       --input $HDFS/clean --output $HDFS/aqi --comparison-output /tmp/owm_comparison
time sp /tmp/aqi/spark/jobs/phase3_aggregate.py --input $HDFS/aqi   --output $HDFS/agg
```

Pha 1 đọc được layout phân vùng `ingest_mode=/country=/dt=/part-*.jsonl.gz`: đã kiểm chứng trên layout dựng từ
`data/samples/` (267 thư mục), kết quả giống hệt bản đọc file phẳng, 0 dòng khác biệt. Nếu Pha 1 lỗi khi đọc
trên HDFS thì nhiều khả năng do môi trường (RAM, đường dẫn) chứ không phải logic.

Nếu hết RAM, chạy từng nước:
`--input $HDFS/raw/ingest_mode=history/country=VN --output $HDFS/clean_VN`.

**Chạy Pha 1 trên toàn bộ dữ liệu 5 năm bằng bản chịu lỗi.** `spark/jobs/phase1_clean_resumable_v4.py` chia Pha 1
thành ba giai đoạn (`grid`, `interpolate`, `final`), lưu trung gian ở `$HDFS/_phase1_work_v4` và có `--resume`
nên bị ngắt thì chạy lại tiếp tục từ giai đoạn đang dở. Script `scripts/run_phase1_v4.sh` chạy lần lượt cả ba
giai đoạn bên trong container:

```bash
docker cp scripts/run_phase1_v4.sh spark-master:/tmp/aqi/run_phase1_v4.sh
docker exec -d spark-master sh /tmp/aqi/run_phase1_v4.sh
docker exec spark-master cat /tmp/aqi/phase1_v4_status                # RUNNING:<giai đoạn> / DONE / FAILED:...
docker exec spark-master tail -f /tmp/aqi/phase1_v4_logs/final.log
```

Ngữ nghĩa làm sạch giống `phase1_clean.py` (cùng cách loại ngoại lai, cùng luật nội suy tối đa 3 giờ) và kết quả
vẫn ghi vào `$HDFS/clean`, nên bước Pha 2 giữ nguyên.

Số liệu cần ghi lại cho báo cáo (điền vào [experiments.md](experiments.md)):

```bash
docker exec namenode hdfs dfs -du -s -h /air-quality/raw /air-quality/clean /air-quality/aqi /air-quality/agg
docker cp spark-master:/tmp/owm_comparison ./owm_comparison      # bảng đối chiếu aqi tự tính và owm_aqi
```

cùng với thời gian `time` của từng pha và phần báo cáo chất lượng (Pha 1) và bảng xếp hạng (Pha 3) mà các job
in ra màn hình.

---

## 2. Nạp lịch sử vào HBase

```bash
# thử trước: chỉ đếm và in mẫu, không ghi
sp /tmp/aqi/spark/jobs/load_history_to_hbase.py --input $HDFS/aqi --since 2026-06-01 --dry-run

# ghi thật
time sp /tmp/aqi/spark/jobs/load_history_to_hbase.py --input $HDFS/aqi --since 2026-06-01 --hbase-host hbase-thrift
```

- `--since 2026-06-01` (khoảng 90 ngày, ~440 nghìn dòng) đủ cho dashboard. Nạp cả 5 năm (~8,5 triệu dòng) qua
  Thrift rất lâu, chỉ làm khi thật sự cần.
- Chạy lại an toàn: ghi theo row key nên chỉ ghi đè cùng dòng.

Kiểm tra:

```bash
echo "scan 'air_quality', {LIMIT => 2}" | docker exec -i hbase-master hbase shell
curl -s "http://localhost:8000/aqi/latest?limit=3"
curl -s "http://localhost:8000/aqi/ranking?dt=2026-08-31" | head -c 600
curl -s "http://localhost:8000/aqi/timeseries?station_id=VN_HCM_01&from=2026-08-30T00:00:00Z&to=2026-08-31T00:00:00Z" | head -c 600
```

Sau bước này các panel Grafana "AQI Ranking", "AQI Time Series", "Latest AQI" phải có dữ liệu.

---

## 3. Streaming (Kafka → AQI → HBase và HDFS)

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

Chạy liên tục (bỏ `--once`, dùng `--starting-offsets latest`), log ra file:

```bash
docker exec -d spark-master sh -c "spark-submit --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.X \
  /tmp/aqi/spark/jobs/streaming_aqi.py > /tmp/streaming.log 2>&1"
docker exec spark-master tail -f /tmp/streaming.log
```

**Thử đường DLQ** (cần Kafka thật nên không có unit test): gửi một message sai schema rồi đọc topic DLQ.

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

## 4. Phân cụm và dự báo

Chạy sau khi có `$HDFS/aqi` (bước 1). Cả ba tầng của mỗi nhánh nằm trong hai file `ext_clustering.py` và
`ext_forecast.py`.

```bash
# thư viện Python cho driver (trong spark-master)
docker exec -u root spark-master pip install pandas numpy pyarrow scikit-learn
docker exec -u root spark-master pip install tensorflow-cpu     # chỉ cho CNN-LSTM, nặng ~500 MB; bỏ qua nếu không cần

# phân cụm: K-means, GMM, Bisecting K-means, DBSCAN, HDBSCAN (dữ liệu nhỏ, chạy nhanh)
time sp /tmp/aqi/spark/jobs/ext_clustering.py --input $HDFS/aqi --output $HDFS/ext/clusters \
    --json-out /tmp/ext_clusters.json

# dự báo AQI 24 giờ: SGD, Random Forest, CNN-LSTM. --output là thư mục TRONG container (không phải HDFS)
time sp --driver-memory 4g /tmp/aqi/spark/jobs/ext_forecast.py --input $HDFS/aqi --output /tmp/forecast_model \
    --since 2025-09-01 --backtest-out /tmp/ext_forecast_backtest.json

# lấy hai file JSON ra khỏi container
docker cp spark-master:/tmp/ext_clusters.json ./ext_clusters.json
docker cp spark-master:/tmp/ext_forecast_backtest.json ./ext_forecast_backtest.json
```

- Với dữ liệu thật (~8,5 triệu dòng), `ext_forecast.py` tính lag và huấn luyện Random Forest ngay trên driver
  nên rất nặng. Nên bắt đầu bằng `--since` (ví dụ 12 tháng gần nhất) và thêm `--skip-cnn-lstm` để chạy hai tầng
  đầu trước. Hết RAM thì tăng `--driver-memory` hoặc rút ngắn `--since`.
- CNN-LSTM chạy trên CPU của driver, chỉ dùng tối đa `--cnn-max-rows` chuỗi (mặc định 300.000, lấy mẫu ngẫu
  nhiên) và `--cnn-epochs` epoch (mặc định 30, có early stopping). Thiếu `tensorflow` thì tầng 3 tự bỏ qua
  và in rõ, hai tầng đầu vẫn chạy.
- `ext_forecast.py` chạy bằng `python` thì mặc định heap driver 4g (đổi bằng biến `EXT_DRIVER_MEMORY`); chạy bằng
  `spark-submit` thì dùng `--driver-memory`. Heap 2g từng bị tràn ở bước Random Forest.
- Mỗi script in bảng tổng kết ở cuối (phân cụm: số cụm và tỉ lệ nhiễu của từng thuật toán; dự báo: RMSE, MAE, R²
  của ba tầng). Chép nguyên văn bảng này cùng câu lệnh và giá trị `--since` đã dùng, vì file JSON chỉ giữ kết quả
  của thuật toán được chọn chứ không có bảng so sánh.
- Hai file JSON (schema ở CONTRACTS.md, mục C8; file mẫu ở `data/samples/ext_*_sample.json`):
  - `--json-out` ghi `ext_clusters.json`: nhãn cụm của thuật toán có silhouette cao nhất, theo (thành phố, tháng).
  - `--backtest-out` ghi `ext_forecast_backtest.json`: giá trị thật và dự báo của cả ba tầng cho vài trạm,
    N ngày cuối của tháng test (`--backtest-days`, mặc định 30; `--backtest-stations` để chọn trạm, mặc định
    `VN_HCM_01,VN_HAN_01,IN_DEL_01,CN_BJS_01,JP_TYO_01` nếu có dữ liệu). Ghi đủ ba tầng để chọn mô hình sau
    khi chạy mà không phải chạy lại. Thiếu `tensorflow` thì cột `cnn_lstm` là `null`.
  - File JSON không có trường "mô hình thắng": việc chọn tầng thắng được làm thủ công khi đọc số liệu.
- Lấy mô hình đã huấn luyện ra ngoài (nếu cần): `docker cp spark-master:/tmp/forecast_model ./forecast_model`.

---

## 5. Đưa kết quả vào demo và ghi lại số liệu

1. Copy hai file JSON thật vào `final_results/json/` (thay cho file mẫu; thư mục này được mount vào container
   `serving-api`), rồi kiểm tra:

   ```bash
   cp ext_clusters.json ext_forecast_backtest.json final_results/json/
   curl -s "http://localhost:8000/ext/clusters?month=8" | head -c 400
   curl -s "http://localhost:8000/ext/forecast?station_id=VN_HCM_01" | head -c 400
   ```

   Mở Grafana: hai panel phân cụm và dự báo phải có dữ liệu.

2. Ghi lại để điền [experiments.md](experiments.md):
   - Thời gian `time` từng pha (Pha 1, 2, 3) và kích thước các tầng HDFS (raw, clean, aqi, agg).
   - Báo cáo chất lượng Pha 1, bảng đối chiếu `owm_comparison` (CSV), bảng xếp hạng Pha 3.
   - Số dòng HBase đã nạp, giá trị `--since` đã dùng, một dòng log batch streaming (`received: 200`), kết quả thử DLQ.
   - Bảng tổng kết phân cụm (5 thuật toán) và bảng dự báo (3 tầng), kèm câu lệnh và `--since`.
   - Ảnh chụp dashboard.
   - Mã commit của code đã chạy (`git rev-parse HEAD`).

---

## 6. Đo scalability Pha 2 (tuỳ chọn)

Đo thời gian Pha 2 trên dữ liệu tổng hợp 100K, 1M, 8M dòng với 1, 2, 4 executor thật (Spark Standalone,
mỗi worker 1 core). Cần stack Docker đang chạy (`spark-master`, `spark-worker`):

```bash
bash scripts/run_scalability_distributed.sh
```

Script dựng 4 worker tạm cạnh `spark-master`, chạy 9 cấu hình (mỗi cấu hình một tiến trình Spark riêng), đếm số
executor thực tế trong log và dừng nếu sai, rồi ghi `scalability_results.csv`, `scalability_speedup.csv`,
`executor_verification.txt` và `summary.md` vào `final_results/scalability/`. Lúc kết thúc (kể cả khi lỗi), script
xoá các worker tạm và bật lại `spark-worker`. Tổng thời gian đo ghi trong CSV là khoảng 28 phút (cấu hình 8M dòng với 1 executor
mất khoảng 12,5 phút), chưa tính thời gian khởi động và dựng worker.

Để đo nhanh trên máy không có Docker: `bash spark/experiments/run_scalability.sh` (chế độ `local[N]`, chỉ để
tham khảo, xem [experiments.md](experiments.md) mục 2.2).

---

## Kiểm tra hoàn thành

- [ ] `$HDFS/clean`, `$HDFS/aqi`, `$HDFS/agg` có dữ liệu; log Pha 1→3 không lỗi.
- [ ] HBase có dữ liệu (`scan` ra dòng); `/aqi/latest`, `/aqi/ranking`, `/aqi/timeseries` trả số thật; các panel Grafana lõi có dữ liệu.
- [ ] Streaming: một lượt `--once` in `received: 200`; `curl /aqi/latest` có giờ hiện tại; thử DLQ đọc được một message lỗi.
- [ ] `ext_clustering.py` và `ext_forecast.py` chạy xong, in đủ bảng, tạo hai file JSON.
- [ ] `/ext/clusters` và `/ext/forecast` trả dữ liệu thật; hai panel tương ứng có dữ liệu.
- [ ] Đã ghi lại đủ danh sách ở mục 5.

---

## Sự cố thường gặp

| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| `ModuleNotFoundError: happybase` | Container bị tạo lại, mất gói đã cài | Chạy lại bước 0 |
| `TTransportException` / timeout khi ghi HBase | `hbase-thrift` hoặc RegionServer chưa lên | `docker compose ps`; thử kết nối cổng 9090 như trong [cai-dat.md](cai-dat.md) |
| `Failed to find data source: kafka` | Thiếu `--packages` hoặc container không có Internet | Thêm `--packages`, kiểm tra mạng |
| `local[*]` bị shell mở rộng | Quên nháy đơn | Luôn viết `'local[*]'` |
| Pha 1 lỗi khi đọc raw trên HDFS | Thường là RAM hoặc đường dẫn (logic layout đã kiểm chứng) | Kiểm tra `$HDFS/raw` có file, tăng `--driver-memory`, hoặc dùng bản chịu lỗi ở bước 1 |
| `/aqi/latest` trả `aqi: null` hoặc rỗng | HBase chưa có dữ liệu cho trạm đó | Kiểm tra bằng `scan`, chạy lại bước 2 |
| `ModuleNotFoundError: sklearn` / `pandas` khi chạy `ext_*.py` | Chưa cài thư viện cho driver | Chạy lại `pip install` ở bước 4 |
| Dự báo báo `BỎ QUA CNN-LSTM` | Chưa cài `tensorflow-cpu` | Cài như bước 4, hoặc bỏ qua nếu chỉ cần hai tầng đầu |
| `ext_forecast.py` hết bộ nhớ hoặc treo | Dữ liệu 5 năm quá lớn cho driver | Thêm `--since`, tăng `--driver-memory`, hoặc `--skip-cnn-lstm` |
| Streaming không ghi gì | Checkpoint cũ đã qua hết offset | Xoá checkpoint (xem bước 3) hoặc đẩy thêm dữ liệu |
