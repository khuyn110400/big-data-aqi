# Chạy hạ tầng trên Apple Silicon (arm64)

> Chỉ đọc nếu máy dựng Docker là MacBook M-series (`uname -m` → `arm64`).
> Máy Windows/Intel thì bỏ qua file này.

## Vấn đề

`Thiết kế hệ thống v1` giả định môi trường Windows 11 + WSL2. Trên Apple Silicon, phần lớn
Docker image có sẵn của **Hadoop và HBase chỉ build cho amd64**. Docker Desktop vẫn chạy được
nhờ giả lập (Rosetta), nhưng:

- chậm hơn đáng kể, nhất là các job Spark/HDFS nặng I/O
- HBase RegionServer hay chết vặt trong môi trường giả lập, khó debug vì lỗi không rõ ràng
- thư viện native của Hadoop (nén Snappy, tính CRC) không có bản arm64 → báo warning hoặc lỗi

Kafka, Spark và ZooKeeper thì dễ thở hơn nhiều vì image đa kiến trúc phổ biến hơn.

## Kiểm tra image có arm64 hay không — làm TRƯỚC khi viết compose

Đừng đoán, hỏi thẳng Docker Hub:

```bash
docker manifest inspect <image>:<tag> | grep -A1 '"platform"' | grep architecture
```

Thấy `"architecture": "arm64"` → chạy native, dùng được.
Chỉ thấy `"amd64"` → phải giả lập hoặc tự build.

Làm việc này cho cả 6 image **trước** khi viết `docker-compose.yml`, ghi kết quả vào bảng dưới:

| Service | Image định dùng | Có arm64? | Cách xử lý |
|---|---|---|---|
| Kafka | | | |
| HDFS (namenode/datanode) | | | |
| Spark master/worker | | | |
| ZooKeeper | | | |
| HBase | | | |
| Grafana | | | |

## Ba cách xử lý, theo thứ tự nên thử

### Cách 1 ⭐ — Tự build image từ JDK arm64 + tarball chính thức

**Đây là cách tốt nhất và ít ai nghĩ tới.** Hadoop, HBase, Spark, Kafka, ZooKeeper đều là
**phần mềm Java** — bản phân phối chính thức (`.tar.gz`) là **độc lập kiến trúc**.
Chỉ có base image (JDK) là phụ thuộc CPU, mà `eclipse-temurin` thì có sẵn arm64.

```dockerfile
FROM eclipse-temurin:17-jre        # có arm64 native
ARG HBASE_VERSION=2.6.6
RUN curl -fsSL https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    | tar -xz -C /opt && ln -s /opt/hbase-${HBASE_VERSION} /opt/hbase
ENV HBASE_HOME=/opt/hbase PATH=$PATH:/opt/hbase/bin
```

Kết quả chạy **native trên arm64**, nhanh bằng máy Intel.
Thiết kế v1 (Bảng 5, bước 6) vốn đã ghi "build HBase" — nên cách này khớp với kế hoạch sẵn có.

**Đánh đổi:** mất thư viện native của Hadoop (`libhadoop.so`). Hadoop tự động rơi về bản thuần Java,
chỉ in warning, vẫn chạy đúng. Với quy mô đồ án thì không ảnh hưởng gì.

### Cách 2 — Dùng image cộng đồng đã build sẵn cho arm64

Có sẵn vài repo làm việc này. Kiểm tra ngày cập nhật cuối và phiên bản trước khi dùng —
phiên bản cũ hơn Bảng 1 của Thiết kế v1 thì phải sửa bảng đó cho khớp thực tế.

- `wxw-matt/docker-hadoop` — Hadoop chạy native trên arm64 và Intel
- `Gradiant/dockerized-hadoop` — có `Dockerfile.multiarch`
- `alecuba16/dockerized-hbase-apple-m1` — HBase cho Apple Silicon

### Cách 3 — Ép giả lập (phương án cuối)

```yaml
services:
  hbase-master:
    platform: linux/amd64
    image: <image chỉ có amd64>
```

Bật **Rosetta for x86/amd64 emulation** trong Docker Desktop → Settings → General.
Chạy được nhưng chậm; để dành cho service nào thật sự không tìm được bản arm64.

## Hai chỗ phải sửa trong repo

### 1. Nén HBase: bỏ SNAPPY

`scripts/03_init_hbase.sh` đang tạo bảng với `COMPRESSION => 'SNAPPY'`.
Snappy cần thư viện native — trên arm64 thường không có, và RegionServer sẽ **không mở được bảng**
với lỗi khó hiểu. Prototype thì dùng `NONE`:

```
create 'air_quality', {NAME => 'd', VERSIONS => 1}
```

Muốn có nén thì `GZ` (thuần Java, luôn chạy được, chậm hơn Snappy nhưng không sao ở quy mô này).

### 2. Cấp RAM cho Docker

Phần `.wslconfig` trong README **không áp dụng cho Mac**. Trên macOS:

**Docker Desktop → Settings → Resources** → Memory ≥ **10 GB**, CPU ≥ 4, Swap ≥ 2 GB.

Mặc định Docker Desktop trên Mac chỉ cấp ~8 GB; chạy đồng thời Kafka + HDFS + Spark + ZooKeeper +
HBase + Grafana là chạm trần và các container sẽ bị kill âm thầm (exit code 137).

## Nếu mất quá nhiều thời gian

Đừng để việc dựng hạ tầng nuốt mất thời gian làm phần phân tích. Nếu sau ~2 ngày mà HBase
vẫn không lên được trên arm64, có hai đường lùi hợp lệ:

1. **Đổi máy dựng hạ tầng** sang máy Windows/Intel của người kia, Mac chỉ dùng để code Spark.
2. **Bỏ HBase, thay bằng Parquet trên HDFS + đọc trực tiếp bằng FastAPI.**
   Mất phần "lưu trữ NoSQL" trong báo cáo, nhưng lõi Pha 1/2/3 và phần thực nghiệm scalability
   — thứ quyết định điểm — vẫn nguyên vẹn. Ghi rõ lý do trong báo cáo là một quyết định kỹ thuật
   có căn cứ, không phải thiếu sót.
