# WORKPLAN — Chia việc cho 2 người

## Nguyên tắc chia

Đường cắt là **biên giới Kafka/HDFS**:

```
        NGƯỜI A  —  Data Platform                 NGƯỜI B  —  Data Processing
 ┌────────────────────────────────────┐   ┌────────────────────────────────────┐
 │ OpenWeather API                    │   │ aqi_core (IAQI/AQI theo breakpoint)│
 │ Collector (backfill + live)        │   │ Pha 1  làm sạch                    │
 │ Kafka                              │──▶│ Pha 2  tính AQI                    │
 │ HDFS (tạo layout, ghi raw)         │   │ Pha 3  tổng hợp / xếp hạng         │
 │ HBase (schema, tạo bảng)           │◀──│ Spark Streaming job                │
 │ Query/API Bridge (FastAPI)         │   │ Nhánh mở rộng: clustering, dự báo  │
 │ Grafana dashboard                  │   │ Thực nghiệm scalability            │
 │ docker-compose toàn hệ thống       │   │                                    │
 └────────────────────────────────────┘   └────────────────────────────────────┘
```

**A cầm hạ tầng và dữ liệu. B cầm thuật toán và phân tích.**

Vì sao chia thế này:
- B là phần "đồ án" thật sự — Pha 1/2/3 chính là cái kế thừa từ paper El Fazziki, là cái đem đi bảo vệ.
- A là phần "hệ thống" — đúng theo Thiết kế v1, là cái đem đi demo.
- Hai bên **không block nhau** nhờ file `data/samples/air_quality_sample.jsonl` (xem M0).

---

## Mẹo quan trọng: không ai phải chờ ai

Ngày đầu tiên, **cùng nhau** tạo `data/samples/air_quality_sample.jsonl` — khoảng 5.000 bản ghi thật, đúng schema C1, commit thẳng vào repo.

- B chạy Spark **local[*]** đọc file này → làm được Pha 1/2/3 hoàn chỉnh mà **không cần Kafka, không cần HDFS, không cần Docker**.
- A dựng hạ tầng và test Kafka/HBase bằng chính file này làm dữ liệu giả.

Đến M2 ghép lại: B chỉ cần đổi đường dẫn input từ `data/samples/` sang `hdfs://.../raw/`. Không sửa logic.

---

## Milestone (mốc tương đối, không gắn ngày cứng)

### M0 — Chốt hợp đồng · CẢ HAI · ~2 ngày
- [ ] Cùng đọc và chốt `CONTRACTS.md`. Ai không đồng ý chỗ nào thì cãi **bây giờ**, không phải lúc ghép code.
- [ ] A lấy API key, gọi thử `/air_pollution/history` 1 thành phố 1 tháng.
- [ ] A sinh `data/samples/air_quality_sample.jsonl` (5.000 dòng thật) → commit.
- [ ] B dựng `spark/aqi_core/breakpoints_vn.json`, **đối chiếu số với QĐ 1459/QĐ-TCMT bản gốc**.
- [ ] Thống nhất: branch `feat/a-*` và `feat/b-*`, PR vào `main`, không push thẳng `main`.

**Xong khi:** cả hai chạy được `python -c "import json; print(sum(1 for _ in open('data/samples/air_quality_sample.jsonl')))"` ra 5000.

---

### M1 — Nền móng · song song

**NGƯỜI A**
- [ ] `docker/docker-compose.yml`: Kafka (KRaft), HDFS (namenode + datanode), Spark master/worker, ZooKeeper, HBase, Grafana
- [ ] `scripts/01_create_topics.sh`, `02_init_hdfs.sh`, `03_init_hbase.sh`
- [ ] Test từng service độc lập theo Bảng 5 của Thiết kế v1
- [ ] `collector/src/owm_client.py` — wrapper gọi API, có retry + throttle 60 calls/phút

**NGƯỜI B**
- [ ] `spark/aqi_core/iaqi.py` — hàm nội suy tuyến tính IAQI + `aqi()` + `level()`
- [ ] `spark/tests/test_iaqi.py` — **unit test đối chiếu ví dụ tính tay trong QĐ 1459**
- [ ] `spark/jobs/phase1_clean.py` chạy được trên `data/samples/` bằng `local[*]`
- [ ] **Xử lý gap theo khối nguyên ngày** (không nội suy 24h) — xem `data/samples/README.md`

**Xong khi:** A `docker compose up` lên đủ service; B `pytest` xanh và Pha 1 ra parquet.

---

### M2 — Có dữ liệu thật · song song

**NGƯỜI A**
- [ ] `collector/src/backfill_history.py` — chạy backfill 200 điểm × 3–5 năm vào HDFS `/air-quality/raw/`
  - **Chunk theo QUÝ (90 ngày)** — đã kiểm chứng 1 call lấy đủ 90 ngày → 4.200 call cho 200 trạm (~1,2 giờ), thay vì 12.200 call nếu chunk theo tháng
  - Checkpoint để chạy lại không mất công
  - Ghi `.jsonl.gz`, gộp file đủ lớn (≥64 MB)
- [ ] `collector/config/cities.json` — danh sách điểm (VN + IN + global), lọc từ `city.list.json.gz`
- [ ] Báo cáo số liệu: tổng bản ghi, dung lượng, thời gian chạy

**NGƯỜI B**
- [ ] `spark/jobs/phase2_aqi.py` — Mapper: IAQI từng chất theo `(station_id, ts)`; Reducer: `max(IAQI)` → AQI + level
- [ ] Chạy Pha 1 → Pha 2 trên HDFS raw thật
- [ ] So sánh `aqi` tự tính với `owm_aqi` (1–5) → **một bảng đối chiếu cho báo cáo**

**Xong khi:** `/air-quality/aqi/` có parquet của toàn bộ dữ liệu lịch sử.

---

### M3 — Lưu trữ & xử lý liên tục · song song

**NGƯỜI A**
- [ ] `collector/src/live_poller.py` — gọi `/air_pollution` hiện tại mỗi 30–60 phút → Kafka
- [ ] Tạo bảng HBase theo C4
- [ ] `serving/app/main.py` — FastAPI đủ 5 endpoint theo C5

**NGƯỜI B**
- [ ] `spark/jobs/phase3_aggregate.py` — trung bình ngày, phân bố mức, xếp hạng thành phố (mẫu 3 job của AQI_analysis)
- [ ] `spark/jobs/streaming_aqi.py` — Kafka → `aqi_core` (**cùng module, không copy code**) → HBase + HDFS
- [ ] Ghi kết quả Pha 3 vào HBase cho endpoint `/aqi/ranking`

**Xong khi:** `curl localhost:8000/aqi/latest` trả dữ liệu thật.

---

### M4 — Trực quan & mở rộng · song song

**NGƯỜI A**
- [ ] Grafana + datasource Infinity (JSON API) trỏ vào FastAPI
- [ ] Dashboard: bản đồ AQI theo trạm · timeseries PM2.5/PM10 · bảng xếp hạng · phân bố mức
- [ ] Export JSON dashboard vào `grafana/dashboards/`

**NGƯỜI B**
- [ ] **Thực nghiệm scalability** (bắt buộc cho đồ án big data — xem §Thực nghiệm bên dưới)
- [ ] Nhánh mở rộng (chọn 1, không tham): Bisecting K-means/GMM phân cụm vùng, **hoặc** Random Forest dự báo AQI 24h

---

### M5 — Hoàn thiện · cả hai
- [ ] Test lỗi: API die, Kafka restart, HBase restart → hệ thống có tự hồi phục không
- [ ] `README.md` hướng dẫn chạy lại từ đầu trên máy trắng
- [ ] Cập nhật `Thiết kế hệ thống_v2.docx` cho khớp code thật
- [ ] Slide + demo

---

## §Thực nghiệm scalability — đừng bỏ phần này

Đồ án big data bị chấm nặng ở chỗ **chứng minh được vì sao cần phân tán**. Chạy Pha 2 và đo:

| Kích thước dữ liệu | 1 executor | 2 executors | 4 executors |
|---|---|---|---|
| 100K bản ghi | | | |
| 1M bản ghi | | | |
| 8M bản ghi | | | |

Vẽ 2 biểu đồ: **thời gian chạy** và **speedup**. Kèm nhận xét chỗ nào bị nghẽn (shuffle? đọc file nhỏ?).

Ghi số vào `docs/experiments.md`.

---

## Quy ước làm việc chung

| Hạng mục | Quy ước |
|---|---|
| Branch | `feat/a-collector-backfill`, `feat/b-phase2-aqi` |
| Commit | Tiếng Việt hoặc Anh đều được, nhưng **1 commit = 1 việc** |
| Merge | PR + người kia review, không push thẳng `main` |
| Đổi schema | Sửa `CONTRACTS.md` trước, báo người kia, rồi mới sửa code |
| Vùng cấm | A không sửa `spark/`, B không sửa `collector/` `serving/` `docker/` — trừ khi báo trước |
| Vùng chung | `CONTRACTS.md`, `data/samples/`, `README.md` — sửa thì báo |
| Secrets | Không commit `.env`. Lỡ commit key → **revoke key ngay trên OpenWeather**, không chỉ xoá commit |

## Rủi ro cần canh

| Rủi ro | Dấu hiệu | Xử lý |
|---|---|---|
| Key mới chưa active | `401 Invalid API key` | Chờ 10 phút – 2 tiếng, đừng debug code |
| Backfill quá chậm | ~4.200 call (chunk 90 ngày) | Checkpoint, 1 call/giây, ~1,2 giờ. Đừng chunk theo tháng — tốn gấp 3 |
| B bị block chờ hạ tầng A | B ngồi chơi | Luôn phát triển trên `data/samples/` bằng `local[*]` |
| HBase trên WSL2 hay chết | RegionServer dead | Cấp thêm RAM cho WSL2 (`.wslconfig`), tối thiểu 8 GB |
| **Dựng Docker trên Apple Silicon** | image Hadoop/HBase chỉ có amd64; container exit 137 hoặc RegionServer chết vặt | Đọc `docs/ARM64-APPLE-SILICON.md` **trước khi viết compose**. Kiểm `docker manifest inspect` từng image. Quá 2 ngày không xong → đổi máy dựng hạ tầng, hoặc bỏ HBase dùng Parquet+FastAPI |
| Nhiều file nhỏ trên HDFS | Job chậm bất thường | `coalesce()` trước khi ghi |
| 2 người sửa cùng file | Conflict liên miên | Tôn trọng "vùng cấm" ở bảng trên |

---

# HƯỚNG DẪN: Code nhẹ ở local (Mac) → chạy collect trên Google Colab

> Bối cảnh: máy Mac (Apple Silicon) **không chạy được cụm Docker Hadoop/HBase** (image chỉ có amd64),
> và cấu hình không đủ để chạy dữ liệu lớn. Cách làm: **viết + kiểm code ở Mac** (phần nhẹ), rồi
> **đẩy lên Colab chạy phần nặng** (backfill + Pha 1/2/3 trên big data), lưu dữ liệu vào **Google Drive**.
>
> Nguyên tắc vàng: *viết* code collect thì nhẹ, chỉ *chạy* nó mới nặng. Nên toàn bộ code hoàn thành và
> test ở Mac bằng vài call thử, chỉ đẩy lên Colab khi đã chắc đúng.

## 0. Bản đồ việc — cái gì làm ở đâu

| Việc | Ở đâu | Vì sao |
|---|---|---|
| Viết `backfill_history.py`, mở rộng `cities.json`, viết notebook Colab | 🟢 Mac | Chỉ là code, không đụng dữ liệu lớn |
| Test logic: `--dry-run`, `--limit 2` (vài call thật) | 🟢 Mac | Nhẹ, xác minh luồng đúng |
| Pha 1/2/3 trên `data/samples/` (10K dòng) | 🟢 Mac | `local[*]`, không cần HDFS/cụm |
| **Backfill thật** ~200 trạm × 3–5 năm | 🔵 Colab | ~1,2h chạy, cần chạy liên tục |
| Pha 1→2→3 trên **toàn bộ** dữ liệu + scalability 8M | 🔵 Colab | Cần RAM/CPU nhiều |
| Cụm Kafka/HDFS/HBase/Grafana đầy đủ | ⚪ Để sau | 1 VM x86 hoặc bỏ HBase→Parquet (xem bảng Rủi ro) |

## 1. Chuẩn bị máy Mac (làm 1 lần)

```bash
brew install openjdk@17            # Spark 3.5 hợp Java 17 — ĐỪNG dùng Java 21 (lỗi reflection)
cd big-data-aqi
python3 -m venv .venv && source .venv/bin/activate
pip install -r collector/requirements.txt   # requests, tenacity, ...
pip install -r spark/requirements.txt        # pyspark, pytest, pandas, ...
```

Verify phần nhẹ đã có sẵn chạy đúng trước khi viết thêm:
```bash
PYTHONPATH=spark pytest spark/tests/test_iaqi.py -q        # 14 test lõi AQI — phải xanh
PYTHONPATH=spark pytest spark/tests -q                     # cả Pha 1/2/3 (local[*])
```

## 2. Viết `collector/src/backfill_history.py` (hiện là stub)

Các mảnh có sẵn — **tái sử dụng, đừng viết lại**:
- `owm_client.OwmClient().history(lat, lon, start, end)` → list item thô (đã có retry + throttle 1 call/giây).
- `normalize.normalize(owm_item, station, ingest_mode="history")` → 1 bản ghi đúng schema C1.
- `station` là dict `{station_id, city, country, lat, lon}` lấy từ `cities.json["stations"]`.

### 2.1 CLI cần có
```
--cities   collector/config/cities.json     # danh sách trạm
--from     2021-01-01                        # OWM history có từ 2020-11-27, đừng lấy sớm hơn
--to       2026-09-01
--out      <thư mục raw>                      # local hoặc /content/drive/.../air-quality/raw
--resume                                      # đọc checkpoint, bỏ qua phần đã xong
--dry-run                                     # CHỈ in số call sẽ dùng, không gọi API
--limit    N                                  # chỉ chạy N trạm đầu (để test ở Mac)
```

### 2.2 Thuật toán (chunk theo QUÝ — đã kiểm chứng 1 call lấy đủ 90 ngày)
```
đọc stations; nếu --limit thì cắt bớt
tạo danh sách cửa sổ quý [(start_epoch, end_epoch), ...] từ --from..--to  (mỗi cửa sổ ~90 ngày)
tổng_call = len(stations) * len(windows)
nếu --dry-run: in tổng_call, ước lượng thời gian (~1 call/giây) rồi THOÁT
load checkpoint (set các cặp (station_id, window) đã xong) nếu --resume
for station in stations:
    for (start, end) in windows:
        nếu (station_id, window) đã trong checkpoint: continue        # --resume
        items = client.history(station.lat, station.lon, start, end)
        for it in items:
            try: rec = normalize(it, station, "history")
            except NormalizeError: ghi vào file .dlq.jsonl để debug; continue
            gom rec vào buffer theo (country, dt_ngày_UTC)             # partition C2
        ghi checkpoint (station_id, window) đã xong                    # ghi NGAY để --resume an toàn
    flush buffer → ghi file .jsonl.gz theo layout C2
in báo cáo cuối: tổng bản ghi, tổng call đã dùng (client.call_count), dung lượng, thời gian
```

### 2.3 Ghi ra đĩa — theo C2 trong CONTRACTS.md
```
<out>/ingest_mode=history/country=VN/dt=2026-09-13/part-0001.jsonl.gz
```
- Dùng `gzip.open(path, "wt")` ghi mỗi bản ghi 1 dòng JSON (`json.dumps(rec, ensure_ascii=False)`).
- File lịch sử theo ngày sẽ nhỏ → **không ép ≥64 MB ở lớp raw**; luật ≥64 MB áp dụng ở output parquet
  (Pha 1 đã `coalesce()` khi ghi). Nếu muốn ít file hơn, gom theo **quý** thành 1 `part-YYYYQn.jsonl.gz`
  — nhưng đó là **đổi C2 → phải sửa CONTRACTS.md + báo Người B trước**.

### 2.4 Checkpoint (bắt buộc — để Colab ngắt giữa chừng chạy lại không mất công)
- File JSON, ví dụ `<out>/_checkpoint.json`, chứa list `["VN_HCM_01|2021Q1", ...]`.
- Ghi checkpoint **ngay sau mỗi cửa sổ** hoàn tất, không đợi đến cuối.
- Vì đặt trên Google Drive nên phiên Colab mới `--resume` đọc lại được.

### 2.5 Test ở Mac (KHÔNG tốn nhiều call)
```bash
export OWM_API_KEY=xxxx
python collector/src/backfill_history.py --cities collector/config/cities.json \
    --from 2026-08-01 --to 2026-09-01 --out /tmp/raw --dry-run          # chỉ in số call
python collector/src/backfill_history.py --cities collector/config/cities.json \
    --from 2026-08-01 --to 2026-09-01 --out /tmp/raw --limit 2          # 2 trạm × 1 quý = 2 call thật
# kiểm output đúng schema:
zcat /tmp/raw/ingest_mode=history/country=*/dt=*/part-*.jsonl.gz | head -1 | python -m json.tool
```

## 3. Mở rộng `collector/config/cities.json` (hiện 8 trạm → mục tiêu ~200)

- Tải danh sách thành phố: `http://bulk.openweathermap.org/sample/city.list.json.gz`.
- Lọc ~200 điểm: 100 VN + 50 IN + 50 global (đa dạng khí hậu/mức ô nhiễm cho phần phân cụm).
- Sinh `station_id` theo format C1: `{COUNTRY}_{CITYCODE}_{NN}` (ổn định giữa các lần chạy).
- Chỉ giữ đúng 5 trường: `station_id, city, country, lat, lon` (làm tròn lat/lon 4 chữ số).
- **Có thể viết 1 script nhỏ** `collector/config/build_cities.py` để lọc — chạy 1 lần ở Mac, commit kết quả.

## 4. Notebook Colab để chạy collect (`docs/colab_collect.ipynb` — bạn tạo)

```python
# Cell 1 — lấy code + cài deps
!git clone -b claude/exciting-cannon-artc3j https://github.com/khuyn110400/big-data-aqi.git
%cd big-data-aqi
!pip install -q -r collector/requirements.txt

# Cell 2 — mount Google Drive (nơi lưu dữ liệu, không mất khi Colab ngắt)
from google.colab import drive; drive.mount('/content/drive')
DATA = "/content/drive/MyDrive/big-data-aqi/air-quality"

# Cell 3 — API key qua Colab Secrets (KHÔNG hard-code, KHÔNG commit key)
import os
from google.colab import userdata
os.environ["OWM_API_KEY"] = userdata.get("OWM_API_KEY")   # thêm secret tên OWM_API_KEY ở panel bên trái

# Cell 4 — chạy backfill vào Drive (có --resume: chạy lại phiên sau tiếp tục chỗ dở)
!python collector/src/backfill_history.py \
    --cities collector/config/cities.json \
    --from 2021-01-01 --to 2026-09-01 \
    --out {DATA}/raw --resume

# Cell 5 — chạy Pha 1 → 2 → 3 ngay trên dữ liệu Drive (chỉ đổi đường dẫn, logic không đổi)
!PYTHONPATH=spark python spark/jobs/phase1_clean.py     --input {DATA}/raw   --output {DATA}/clean
!PYTHONPATH=spark python spark/jobs/phase2_aqi.py       --input {DATA}/clean --output {DATA}/aqi --comparison-output docs/
!PYTHONPATH=spark python spark/jobs/phase3_aggregate.py --input {DATA}/aqi   --output {DATA}/agg
```

**Lưu ý Colab (free):**
- Hay **ngắt kết nối sau ~90 phút idle**; backfill ~1,2h → dựa vào `--resume` để chạy lại tiếp. Giữ tab
  hoạt động, hoặc chia nhỏ theo quốc gia/năm chạy nhiều lần.
- Đĩa `/content` là **tạm** (mất khi hết phiên) → **luôn ghi vào `/content/drive/...`**, không ghi `/content`.
- Backfill nén lại khá nhỏ (~vài trăm MB cho 8–9 triệu bản ghi) → Drive thừa sức chứa.
- Không dùng HDFS trên Colab: layout C2 chỉ là **thư mục thường** trên Drive, Pha 1/2/3 đọc y hệt.

## 5. Bàn giao & quy ước đường dẫn

- Dữ liệu chuẩn trên Drive: `MyDrive/big-data-aqi/air-quality/{raw,clean,aqi,agg}/` — chia sẻ thư mục này
  cho người kia để cùng dùng, khỏi backfill lại.
- Sau khi có `aqi/` và `agg/`, điền số thật vào **`docs/experiments.md §1`** (tổng bản ghi, dung lượng,
  thời gian, số call) — phần này đang trống và là điểm chấm "vì sao cần big data".

## 6. Checklist làm theo thứ tự

- [ ] Mac: `brew install openjdk@17`, tạo venv, `pytest spark/tests/test_iaqi.py` xanh
- [ ] Mac: mở rộng `cities.json` lên ~200 trạm
- [ ] Mac: viết `backfill_history.py`, test `--dry-run` và `--limit 2` ra file đúng schema C1
- [ ] Mac: chạy Pha 1→2→3 trên `data/samples/` cho chắc luồng
- [ ] Colab: tạo `colab_collect.ipynb`, mount Drive, thêm secret `OWM_API_KEY`
- [ ] Colab: chạy backfill vào `Drive/.../raw` (dùng `--resume` nếu bị ngắt)
- [ ] Colab: chạy Pha 1→2→3 trên dữ liệu Drive
- [ ] Điền `docs/experiments.md §1` bằng số liệu thật
