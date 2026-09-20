# Cài đặt và cấu hình

Tài liệu này mô tả cách dựng môi trường chạy dự án, cách các thành phần Hadoop/HBase nối với nhau và
những tham số đã phải chỉnh. Các bước chạy pipeline nằm ở [huong-dan-chay.md](huong-dan-chay.md).

## 1. Hai môi trường của dự án

| | Máy phát triển (macOS, Apple Silicon) | Máy chạy hệ thống (Windows 11 + WSL2) |
|---|---|---|
| Dùng để | Viết code, unit test, chạy Spark `local[*]` trên dữ liệu mẫu, đo scalability | Chạy toàn bộ stack Docker, backfill, Pha 1→3 trên dữ liệu thật, streaming, phân cụm và dự báo trên dữ liệu thật |
| Docker stack | Không chạy | Chạy |
| Lý do | Image `bde2020/hadoop-*` và `bde2020/hbase-*` chỉ có bản amd64; trên chip ARM phải giả lập, chậm và HBase RegionServer hay chết | Kiến trúc x86-64, chạy image gốc |
| Cần cài | Python ≥ 3.10, Java 17 | Docker Desktop (bật WSL2 backend), Ubuntu 24.04 trong WSL2, Python ≥ 3.10 |

Phần code Spark và Python không phụ thuộc kiến trúc CPU nên chạy giống nhau ở cả hai môi trường; khác biệt
chỉ nằm ở chỗ có dựng được Docker stack hay không.

### Cấu hình máy Windows đã dùng

| Thông số | Giá trị |
|---|---|
| CPU | *(chưa ghi lại: điền model và số nhân)* |
| RAM vật lý | *(chưa ghi lại)* |
| RAM cấp cho WSL2 | 10 GB (`memory=10GB` trong `.wslconfig`, xem mục 2) |
| Ổ đĩa | *(chưa ghi lại)* |
| Phiên bản Docker Desktop | *(chưa ghi lại)* |

Các dòng đánh dấu "chưa ghi lại" cần điền từ máy đã chạy lần cuối; số liệu thời gian trong
[experiments.md](experiments.md) chỉ có ý nghĩa khi đi kèm cấu hình này.

## 2. Cài đặt trên Windows + WSL2

1. Bật WSL2 và cài Ubuntu 24.04. Cài Docker Desktop, trong Settings → General chọn "Use the WSL 2 based
   engine", và trong Resources → WSL Integration bật cho bản Ubuntu vừa cài.
2. Cấp RAM cho WSL2 trước khi dựng stack. Tạo file `C:\Users\<tên user>\.wslconfig`:

   ```ini
   [wsl2]
   memory=10GB
   ```

   Sau đó chạy `wsl --shutdown` trong PowerShell rồi mở lại WSL. Mặc định WSL2 chỉ lấy khoảng một nửa RAM
   máy; HBase, HDFS, Spark và Kafka chạy cùng lúc dễ hết bộ nhớ, và khi đó HBase RegionServer chết mà không
   báo lỗi rõ ràng. Nếu máy có ít hơn 10 GB RAM trống thì cần giảm tải (xem mục 6).
3. Clone repo vào bên trong hệ tệp của WSL (ví dụ `~/big-data-aqi`), không đặt trong `/mnt/c/...`, vì
   I/O qua `/mnt/c` chậm hơn nhiều.
4. Tạo file môi trường:

   ```bash
   cp .env.example .env      # điền OWM_API_KEY; các biến còn lại giữ mặc định nếu chạy chung một máy
   ```

   `.env` không được commit. Chỉ cần `OWM_API_KEY` khi chạy collector (backfill, live poller).
5. Dựng stack theo đúng thứ tự (từng service một, kiểm tra xong mới sang service tiếp theo):

   ```bash
   docker compose --env-file .env -f docker/docker-compose.yml up -d kafka
   docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --version
   bash scripts/01_create_topics.sh

   docker compose --env-file .env -f docker/docker-compose.yml up -d namenode datanode
   bash scripts/02_init_hdfs.sh

   docker compose --env-file .env -f docker/docker-compose.yml up -d spark-master spark-worker
   docker compose --env-file .env -f docker/docker-compose.yml up -d zookeeper
   docker compose --env-file .env -f docker/docker-compose.yml up -d hbase-master hbase-regionserver hbase-thrift
   bash scripts/03_init_hbase.sh

   docker compose --env-file .env -f docker/docker-compose.yml up -d serving-api grafana
   ```

   Dựng cả file một lần thì nhiều lỗi xuất hiện cùng lúc và khó biết lỗi nào gây ra lỗi nào.
6. Kiểm tra:

   ```bash
   docker compose --env-file .env -f docker/docker-compose.yml ps          # các service phải ở trạng thái Up
   curl -s http://localhost:8000/health                                   # {"status":"ok"}
   python3 -c "import socket; s=socket.create_connection(('127.0.0.1',9090),3); print('OK'); s.close()"   # cổng Thrift
   ```

   Giao diện web: HDFS `http://localhost:9870`, HBase `http://localhost:16010`, Spark `http://localhost:8080`,
   FastAPI `http://localhost:8000/docs`, Grafana `http://localhost:3000` (đăng nhập mặc định `admin`/`admin`,
   đổi mật khẩu ở lần đầu).

Sau khi `serving-api` đã chạy, đặt hai file kết quả nhánh mở rộng vào `final_results/json/` để endpoint
`/ext/*` trả dữ liệu thật; thư mục này được mount vào container (chỉ đọc).

## 3. Cài đặt trên macOS (máy phát triển)

Không cần Docker. Cần Java 17 (`brew install openjdk@17`) và Python từ 3.10 trở lên.

```bash
cd spark
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest                      # 97 test
```

- `pyspark` được ghim ở 3.5.9 trong `spark/requirements.txt`; PySpark 3.5 cần JVM 8, 11 hoặc 17.
- `tensorflow-cpu` là tuỳ chọn, chỉ cần cho tầng CNN-LSTM của `ext_forecast.py`; thiếu thì tầng 3 tự bỏ qua.
  Bỏ dấu `#` ở dòng cuối `spark/requirements.txt` hoặc `pip install tensorflow-cpu` nếu muốn chạy.
- Collector chạy bằng môi trường riêng: `pip install -r collector/requirements.txt`.
- Chạy Pha 1/2/3 trên dữ liệu mẫu và đo scalability: xem [README](../README.md#7-chạy-thử) và
  `spark/experiments/run_scalability.sh`.

## 4. Phiên bản các thành phần

| Thành phần | Image / phiên bản |
|---|---|
| Kafka | `apache/kafka:3.8.0` (KRaft, một node, không cần ZooKeeper cho Kafka) |
| HDFS | `bde2020/hadoop-namenode` và `hadoop-datanode` `2.0.0-hadoop3.2.1-java8` (Hadoop 3.2.1) |
| Spark (master, worker) | `bitnamilegacy/spark:3.5` |
| ZooKeeper (cho HBase) | `zookeeper:3.8` |
| HBase | `bde2020/hbase-master` và `hbase-regionserver` `1.0.0-hbase1.2.6` (HBase 1.2.6) |
| FastAPI | Python 3.12, `fastapi`, `uvicorn`, `happybase` (xem `serving/requirements.txt`) |
| Grafana | `grafana/grafana-oss:11.1.0`, plugin `yesoreyeram-infinity-datasource` 3.7.1 |
| PySpark trên máy dev | 3.5.9 |

Ghi chú: các image `bitnami/spark` cũ đã chuyển sang tổ chức `bitnamilegacy`, nên compose dùng
`bitnamilegacy/spark:3.5`.

## 5. Kết nối HBase với HDFS và các thành phần khác

```
 serving-api (FastAPI)      Spark jobs (load_history_to_hbase, streaming_aqi)
          │                              │
          └───────────┬──────────────────┘
                      │  happybase, Thrift cổng 9090
                      ▼
               ┌──────────────┐
               │ hbase-thrift │
               └──────┬───────┘
                      │ HBase client
                      ▼
 ┌────────────┐   ┌──────────────┐   ┌────────────────────┐
 │ zookeeper  │◀──│ hbase-master │   │ hbase-regionserver │──▶ zookeeper
 │   :2181    │   └──────┬───────┘   └─────────┬──────────┘
 └────────────┘          │  hbase.rootdir      │
                         ▼  = hdfs://namenode:9000/hbase
                  ┌──────────────┐      ┌──────────────┐
                  │   namenode   │◀────▶│   datanode   │
                  │    :9000     │      │    :9864     │
                  └──────────────┘      └──────────────┘
```

- HBase lưu dữ liệu trên HDFS: HMaster và RegionServer đều đặt `hbase.rootdir=hdfs://namenode:9000/hbase`,
  `hbase.cluster.distributed=true` và `hbase.zookeeper.quorum=zookeeper` (biến `HBASE_CONF_*` trong compose).
- Các container Hadoop dùng chung khối `x-hadoop-env`: `fs.defaultFS=hdfs://namenode:9000`, số bản sao
  `dfs.replication=1` (một datanode), tắt kiểm tra quyền (`dfs.permissions.enabled=false`) và tắt kiểm tra
  hostname khi datanode đăng ký (`...registration.ip-hostname-check=false`).
- Thứ tự khởi động được bảo đảm bằng `depends_on` (chờ healthcheck của namenode) và biến `SERVICE_PRECONDITION`
  (chờ các cổng `namenode:9870`, `datanode:9864`, `zookeeper:2181`, `hbase-master:16010` mở).
- Ứng dụng Python không nói chuyện trực tiếp với HBase mà đi qua dịch vụ Thrift (`hbase-thrift`, cổng 9090),
  bằng thư viện `happybase`. Dịch vụ này dùng lại image `hbase-master` nhưng chạy tiến trình Thrift riêng,
  không ảnh hưởng tiến trình HMaster. FastAPI kết nối `hbase-thrift:9090`; các job Spark ghi HBase
  (`load_history_to_hbase.py`, `streaming_aqi.py`) cũng dùng `--hbase-host hbase-thrift`.
- Spark đọc và ghi HDFS qua `hdfs://namenode:9000/air-quality/...`. Trong container `spark-master` chạy các job
  bằng `spark-submit --master 'local[*]'`; `spark-worker` chỉ dùng cho giao diện cụm, không chạy các job của dự án.
- Bảng HBase `air_quality` có một column family `d`, `VERSIONS => 1` (xem `scripts/03_init_hbase.sh` và
  CONTRACTS.md C4).

## 6. Tham số đã chỉnh và lý do

| Chỗ | Tham số | Lý do |
|---|---|---|
| WSL2 | `memory=10GB` | Mặc định quá thấp cho toàn bộ stack; thiếu RAM thì HBase RegionServer tự chết |
| HBase | Tạo bảng không dùng `COMPRESSION => 'SNAPPY'` | Image HBase không có thư viện native của Snappy, RegionServer không mở được bảng; dùng GZ hoặc không nén |
| Kafka | `KAFKA_LOG_DIRS=/var/lib/kafka/data` cùng volume `kafka-data` | Mặc định log nằm ở `/tmp`, tạo lại container là mất topic |
| Kafka | Healthcheck bằng `nc -z localhost 9092` | Gọi `kafka-broker-api-versions.sh` khởi động một JVM mới mỗi lần, lúc máy nghẽn I/O có thể quá 20 giây và báo unhealthy giả |
| Namenode | Healthcheck `timeout: 25s`, `retries: 10` | Khi cả stack cùng khởi động, một lệnh `curl` nội bộ từng mất hơn 15 giây dù NameNode đã lên |
| Spark | `HOME=/tmp` và `JAVA_TOOL_OPTIONS=-Duser.home=/tmp` | Container chạy bằng user không có thư mục home ghi được; đặt về `/tmp` để ghi được cache và để `pip install --user` chạy được |
| Spark worker | `SPARK_WORKER_MEMORY=2G`, `SPARK_WORKER_CORES=2` | Giới hạn theo RAM của WSL2 |
| Job Spark | `--driver-memory 2g` (Pha 1→3, nạp HBase), `4g` (`ext_forecast.py`), `6g` (Pha 1 bản v4) | Pha 1 trên 5 năm dữ liệu và Random Forest ở driver là hai bước nặng nhất; `ext_forecast.py` từng tràn heap ở 2g |
| Pha 1 v4 | `spark.sql.shuffle.partitions=96`, `spark.default.parallelism=96`, `spark.network.timeout=600s`, `spark.executor.heartbeatInterval=30s` | Bản chạy trên toàn bộ 8,58 triệu bản ghi, chia thành 3 giai đoạn có thể chạy tiếp khi bị ngắt (`scripts/run_phase1_v4.sh`) |
| Backfill | Cửa sổ 90 ngày, throttle ≤ 1 call/giây, checkpoint trong `.state/` | Một call lấy được đủ 90 ngày; 4.200 call cho 200 trạm × 5 năm |
| Nạp HBase | `--since 2026-06-01` | Khoảng 90 ngày (≈ 440 nghìn dòng) đủ cho dashboard; nạp cả 5 năm qua Thrift rất lâu |
| Dự báo | Random Forest `num_trees=20`, `max_depth=5` | Giảm so với thiết kế gốc (100 và 10) do giới hạn tài nguyên khi huấn luyện trên driver với ~8,5 triệu dòng |
| Dự báo | `--cnn-max-rows 300000`, `--cnn-epochs 30` (có early stopping) | CNN-LSTM chạy trên CPU của driver nên phải lấy mẫu chuỗi |

Nếu máy có ít RAM hơn: chạy Pha 1 theo từng nước (`--input .../raw/ingest_mode=history/country=VN`), rút ngắn
`--since` của `ext_forecast.py`, thêm `--skip-cnn-lstm`, hoặc tắt `spark-worker` vì các job không dùng đến.

## 7. Sự cố thường gặp khi cài đặt

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| HBase RegionServer tự thoát, log không rõ lỗi | WSL2 thiếu RAM | Kiểm tra `.wslconfig`, chạy `wsl --shutdown` rồi dựng lại |
| Bảng `air_quality` không mở được, lỗi về native library | Tạo bảng có nén Snappy | Tạo lại bảng không nén (`scripts/03_init_hbase.sh`) |
| `docker compose ps` báo kafka unhealthy dù chạy được | JVM khởi động chậm dưới tải I/O | Chờ thêm hoặc dựng từng service như mục 2 |
| `TTransportException` / timeout khi ghi HBase | `hbase-thrift` hoặc RegionServer chưa lên | `docker compose ps`; thử kết nối cổng 9090 như mục 2 |
| `ModuleNotFoundError: happybase` trong `spark-master` | Container bị tạo lại nên mất gói pip đã cài | Cài lại theo `huong-dan-chay.md` mục 0 |
| Image `bitnami/spark` không kéo được | Tag cũ đã chuyển sang `bitnamilegacy` | Dùng `bitnamilegacy/spark:3.5` như trong compose |
| Trên Mac: Docker báo lỗi kiến trúc hoặc HBase chạy chập chờn | Image chỉ có bản amd64 | Không dựng stack trên Mac, dùng máy Windows |
