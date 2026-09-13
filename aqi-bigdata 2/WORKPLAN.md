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
