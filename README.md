# AQI Analytics — Phân tích chất lượng không khí trên nền tảng Big Data

Đồ án môn Big Data · Kế thừa kiến trúc multi-phase MapReduce từ El Fazziki et al. (2015)
Nguồn dữ liệu: **OpenWeather Air Pollution API** (hiện tại + lịch sử từ 27/11/2020)

## Mục lục

1. [Hệ thống làm gì](#1-hệ-thống-làm-gì)
2. [Kiến trúc — hai làn, một lõi](#2-kiến-trúc--hai-làn-một-lõi)
3. [Ánh xạ Mapper / Reducer](#3-ánh-xạ-mapper--reducer-kế-thừa-el-fazziki-2015)
4. [Hai thứ tự đừng nhầm lẫn](#4-hai-thứ-tự-đừng-nhầm-lẫn)
5. [Bản đồ repo](#5-bản-đồ-repo)
6. [Giải thích từng phần](#6-giải-thích-từng-phần)
7. [Ai chờ ai](#7-ai-chờ-ai)
8. [Nếu chuyển Pha 1 sang Người A](#8-nếu-chuyển-pha-1-làm-sạch-sang-người-a)
9. [Bắt đầu](#9-bắt-đầu)
10. [Môi trường](#10-môi-trường)

---

## 1. Hệ thống làm gì

Thu thập nồng độ các chất ô nhiễm (PM2.5, PM10, O₃, NO₂, SO₂, CO) tại nhiều thành phố,
tính **chỉ số AQI theo chuẩn Việt Nam (QĐ 1459/QĐ-TCMT)** bằng pipeline Spark nhiều pha,
lưu trữ phân tán và trực quan hóa trên dashboard.

> **Điểm cần hiểu rõ:** OpenWeather trả sẵn một trường `main.aqi` nhưng đó là thang **1–5 riêng của họ**,
> không phải AQI 0–500 của Việt Nam. Phần tính AQI từ nồng độ theo bảng breakpoint chính là
> **đóng góp thuật toán của đồ án** — nằm ở Pha 2, tuyệt đối không được thay bằng `owm_aqi`.

## 2. Kiến trúc — hai làn, một lõi

```
                     ┌──────────────────────────────────────────┐
                     │        OpenWeather Air Pollution API     │
                     └───────────────┬──────────────────────────┘
                                     │
         ┌───────────────────────────┴────────────────────────────┐
         │ LÀN BATCH (lịch sử)                LÀN STREAM (hiện tại)│
         ▼                                                        ▼
  backfill_history.py                                      live_poller.py
  (200 điểm × 3–5 năm                                      (mỗi 30–60 phút)
   ≈ 8.7 triệu bản ghi)                                           │
         │                                                        ▼
         ▼                                                 ┌─────────────┐
  HDFS /air-quality/raw/ ◀──────────────────────────────── │    Kafka    │
         │                                                 │air-quality- │
         │                                                 │    raw      │
         ▼                                                 └──────┬──────┘
  ┌──────────────────┐                                            │
  │ PHA 1  Làm sạch  │                                            ▼
  │ Spark batch      │                                  ┌────────────────────┐
  └────────┬─────────┘                                  │ Spark Structured   │
           ▼                                            │ Streaming          │
  ┌──────────────────┐        ╔═══════════════╗         └─────────┬──────────┘
  │ PHA 2  Tính AQI  │◀──────▶║   aqi_core    ║◀──────────────────┘
  │ M: IAQI mỗi chất │        ║  LÕI DÙNG CHUNG║
  │ R: max → AQI     │        ║ breakpoint VN  ║   ← chỉ một implementation,
  └────────┬─────────┘        ╚═══════════════╝      cả 2 làn gọi cùng module
           ▼
  ┌──────────────────┐
  │ PHA 3  Tổng hợp  │
  │ TB ngày, xếp hạng│
  └────────┬─────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
 HDFS /agg     HBase air_quality
                   │
                   ▼
           Query/API Bridge (FastAPI)
                   │
                   ▼
               Grafana
```

**Vì sao phải có làn batch?** Vì làn streaming chỉ sinh ra vài nghìn bản ghi trong lúc demo —
không đủ để gọi là big data, và không chạy được Pha 1/2/3 đúng tinh thần paper.
Làn batch cho khối dữ liệu triệu bản ghi để đo hiệu năng phân tán.

### Hai làn KHÔNG gộp lại làm một

Đây là hiểu nhầm hay gặp nhất. Hai làn chạy độc lập, khác nhịp hoàn toàn:

| | Làn batch | Làn stream |
|---|---|---|
| Chạy khi nào | 1 lần (hoặc thỉnh thoảng chạy lại) | liên tục 24/7 |
| Mỗi lần xử lý | cả khối 8.7 triệu bản ghi | vài chục bản ghi |
| Mất bao lâu | vài phút đến vài giờ | vài giây |

Nếu gộp chung thì mỗi giờ có dữ liệu mới lại phải tính lại toàn bộ 8.7 triệu bản ghi — vô lý.
**Hai làn gặp nhau ở ĐẦU RA** (cùng ghi vào `/aqi` và HBase), không phải ở đầu vào.

Kiến trúc này có tên: **Lambda Architecture** (batch layer + speed layer + serving layer).
Nên gọi đúng tên trong báo cáo.

### Vài điểm dễ nói sai trong báo cáo

- **Kafka không thu thập dữ liệu.** Python Collector mới là thứ gọi API. Kafka là **ống dẫn + kho đệm**:
  nếu Spark chết 2 tiếng, dữ liệu vẫn nằm trong Kafka chờ, không mất.
- **"Kafka Streams" là thư viện khác, đồ án không dùng.** Ở đây Kafka chỉ đóng vai **message broker**;
  phần *xử lý* stream là **Spark Structured Streaming**.
- **Không ai "đẩy lên Grafana".** Grafana **tự đi lấy (pull)**: cứ 30 giây nó gọi `GET /aqi/latest`
  của FastAPI. Chiều mũi tên ngược với các bước trước.
- **HDFS không phải chỗ lưu một bản sao cuối cùng.** Nó vừa là **đầu vào** vừa là **đầu ra** của Spark,
  và có 4 tầng: `/raw` → `/clean` → `/aqi` → `/agg`.

## 3. Ánh xạ Mapper / Reducer (kế thừa El Fazziki 2015)

| Pha | Mapper phát ra | Reducer làm gì | Output |
|---|---|---|---|
| **1. Làm sạch** | `key=(station_id, ts)`, `value=bản ghi thô` | Khử trùng lặp, loại giá trị âm/ngoài ngưỡng, nội suy điểm thiếu | `/clean` |
| **2. Tính AQI** | `key=(station_id, ts)`, `value=IAQI từng chất` theo bảng breakpoint | `AQI = max(IAQI)`, gán mức + chất trội | `/aqi` |
| **3. Tổng hợp** | `key=(city, date)` hoặc `(city, level)` | Trung bình/max, đếm phân bố mức, xếp hạng | `/agg`, HBase |

> Trên Spark DataFrame, **"Mapper" = `withColumn`/`map`**, **"Reducer" = `groupBy().agg()`**.
> Trong báo cáo vẫn trình bày theo ngôn ngữ Mapper/Reducer để bám paper.

---

## 4. Hai thứ tự đừng nhầm lẫn

**Thứ tự DỮ LIỆU CHẠY** (lúc hệ thống đã xong):

```
API → Collector → Kafka → Spark → HDFS/HBase → FastAPI → Grafana
```

**Thứ tự MÌNH CODE** (lúc đang làm):

```
Công thức AQI → Pha 1,2 chạy file local → HDFS → HBase/API/Grafana → Kafka/Streaming
```

Hai thứ tự này **khác nhau**, và code theo thứ tự thứ nhất là sai lầm tốn thời gian nhất.

Vì sao: nếu dựng Collector → Kafka → Spark theo đúng dòng dữ liệu, thì phải xong gần hết
hệ thống mới biết công thức AQI đúng hay sai. Sai ở bước cuối = đập đi làm lại.
Còn công thức AQI thì **chạy được ngay bằng `python`, không cần Docker, Kafka hay HDFS gì cả**.

| | Làm gì | Vì sao trước |
|---|---|---|
| 1 | **Công thức AQI + unit test** | Không cần hạ tầng gì. Phần dễ sai nhất mà lại dễ test nhất. |
| 2 | **Pha 1 + Pha 2 trên file local** | `spark local[*]` đọc jsonl. Đến đây đã có "đồ án" rồi. |
| 3 | **Backfill dữ liệu thật + HDFS** | Giờ mới cần Hadoop. Đổi đường dẫn input, logic không sửa. |
| 4 | **HBase + FastAPI + Grafana** | Có kết quả rồi mới hiển thị. |
| 5 | **Kafka + streaming** | **Cuối cùng.** Khó debug nhất, và không tạo ra kết quả phân tích nào mới. |

Kafka + Streaming xếp cuối vì giá trị của nó là chứng minh hệ thống chạy liên tục.
Hết thời gian thì bỏ được làn streaming mà đồ án vẫn đứng vững; bỏ Pha 2 thì không còn gì để bảo vệ.

---

## 5. Bản đồ repo

```
aqi-bigdata/
├── README.md              ★ file này — kiến trúc + giải thích từng phần
├── CONTRACTS.md           ★ hợp đồng schema giữa 2 người — đọc trước tiên
├── WORKPLAN.md            ★ chia việc + milestone M0–M5
├── docker/                [A] docker-compose toàn hệ thống
├── collector/             [A] gọi OpenWeather → Kafka + HDFS
├── spark/                 [B] aqi_core + 4 job (pha 1,2,3 + streaming)
├── serving/               [A] FastAPI đọc HBase cho Grafana
├── grafana/               [A] dashboard JSON
├── data/fixtures/         [A] response nguyên bản của API
├── data/samples/          ★ dữ liệu mẫu để 2 người làm song song không chờ nhau
├── scripts/               [A] script khởi tạo topic / HDFS / HBase
└── docs/                  [B] thực nghiệm · [A] hướng dẫn arm64
```

| Thư mục | Là gì | Ai giữ | Cần có trước |
|---|---|---|---|
| `CONTRACTS.md` | Hợp đồng schema giữa 2 người | **chung** | — |
| `WORKPLAN.md` | Chia việc, milestone | **chung** | — |
| `docker/` | Dựng 6 service hạ tầng | A | — |
| `collector/` | Gọi API → Kafka + HDFS | A | Docker |
| `serving/` | FastAPI đọc HBase | A | HBase có dữ liệu |
| `grafana/` | Dashboard | A | FastAPI chạy |
| `scripts/` | Khởi tạo topic/HDFS/HBase | A | Docker |
| `data/fixtures/` | Response **nguyên bản** của API | A | — |
| `data/samples/` | Dữ liệu **đã chuẩn hoá** để dev offline | chung | — |
| `spark/aqi_core/` | Công thức AQI — **lõi đồ án** | B | — |
| `spark/jobs/` | Pha 1, 2, 3 + streaming | B | aqi_core |
| `docs/` | Kết quả thực nghiệm | B | có dữ liệu |

---

## 6. Giải thích từng phần

> Mỗi phần trả lời 4 câu: **là gì · vì sao cần · vào gì ra gì · ai giữ**.

### 6.1. `CONTRACTS.md` — quan trọng nhất repo

**Là gì:** định nghĩa đúng 7 thứ mà cả hai người phải dùng giống hệt nhau:
schema message, đường dẫn HDFS, schema parquet, row key HBase, endpoint API, topic Kafka, biến môi trường.

**Vì sao cần:** khi 2 người code song song, thứ duy nhất khiến code ghép được vào nhau là các
định nghĩa này. Nếu A đặt tên trường `pm25` mà B viết code đọc `pm2_5` thì đến lúc ghép mới phát hiện —
và lúc đó đã có vài nghìn dòng code viết sai.

**Quy tắc:** muốn đổi bất kỳ mục nào → báo người kia, sửa `CONTRACTS.md` trước, rồi mới sửa code.
Không bao giờ sửa code trước.

### 6.2. `docker/docker-compose.yml` · NGƯỜI A

**Là gì:** khai báo 6 service: Kafka, HDFS (namenode + datanode), Spark (master + worker),
ZooKeeper, HBase (master + regionserver), Grafana.

**Vì sao cần:** để cả hệ thống dựng lại được bằng 1 lệnh trên máy bất kỳ. Đồ án bị chấm ở chỗ
"chạy lại được không" — cài tay từng thứ thì đến lúc demo trên máy khác là chết.

**Cách làm đúng:** **dựng từng service một, test xong mới thêm service tiếp theo.**
Viết cả file rồi `docker compose up` một lần là cách nhanh nhất để có 6 lỗi cùng lúc
không biết lỗi nào gây ra lỗi nào. Thứ tự: `kafka` → `hdfs` → `spark` → `zookeeper` → `hbase` → `grafana`

> **Bẫy WSL2:** phải cấp ≥10GB RAM trong `C:\Users\<user>\.wslconfig` trước.
> Mặc định WSL2 lấy ~50% RAM máy, HBase RegionServer sẽ chết ngẫu nhiên mà không báo lỗi rõ ràng.

### 6.3. `collector/` · NGƯỜI A

Phần **tạo ra dữ liệu cho toàn bộ đồ án**. Không có nó thì B không có gì để xử lý.

#### `src/owm_client.py`
**Là gì:** lớp bọc quanh 2 endpoint OpenWeather (hiện tại + lịch sử).
**Vì sao tách riêng:** để chỗ retry / throttle / đếm quota nằm một chỗ, không rải khắp code.
**Vào → ra:** `(lat, lon, start, end)` → list JSON thô.
**Phải xử lý:** `401` (key chưa active — chờ 10 phút–2 tiếng, đừng retry vô hạn),
`429` (vượt rate limit — backoff), throttle ≤1 call/giây.

#### `src/normalize.py` ← **file quan trọng nhất của Người A**
**Là gì:** biến response thô của API thành schema C1.
**Vì sao quan trọng:** mọi thứ phía sau phụ thuộc vào nó đúng schema. Sai một tên trường là Spark job của B chết.

```
API trả:  {"coord":{...}, "list":[{"dt":..., "main":{"aqi":2}, "components":{...}}]}
                                     ↓  normalize()
C1:       {"station_id":"VN_HCM_01", "city":"Ho Chi Minh City", "country":"VN",
           "ts_utc":"...", "owm_aqi":2, "components":{...}}
```

API **không có trường nào định danh thành phố** — `station_id`, `city`, `country` phải do Collector
gắn vào từ `config/cities.json`. Đó chính là lý do file này tồn tại.

**Input mẫu để viết:** `data/fixtures/owm_air_pollution_raw.json`

#### `src/backfill_history.py` ← **thứ tạo ra "big data" cho đồ án**
**Là gì:** quét 200 điểm × 3–5 năm lịch sử → HDFS `/air-quality/raw/`.
**Vì sao cần:** làn streaming chạy demo 1 tuần chỉ được ~33.000 bản ghi. Backfill cho ~8.7 triệu.
Không có nó thì Pha 1/2/3 không có gì để chạy và không đo được scalability.

**Cách chạy — đã kiểm chứng:** một call lấy được **đủ 90 ngày** (thử 90 ngày → nhận 2041 bản ghi,
không bị cắt). Nên chunk theo **quý**, không phải theo tháng:

| Chunk | Call/trạm (5 năm) | 200 trạm | Thời gian ở 1 call/giây |
|---|---|---|---|
| 30 ngày | 61 | 12.200 | ~3,4 giờ |
| **90 ngày** ⭐ | **21** | **4.200** | **~1,2 giờ** |

Vẫn phải có checkpoint để chạy lại không mất công. Ước tính thu được **~8,3 triệu bản ghi**
(200 trạm × 5 năm × 8760h × 94,4% tỉ lệ đầy đủ).

#### `src/live_poller.py`
**Là gì:** gọi API hiện tại mỗi 30–60 phút → Kafka.
**Vì sao chỉ 30–60 phút:** OpenWeather cập nhật theo giờ. Gọi dày hơn chỉ phí quota, không có dữ liệu mới.

#### `src/kafka_producer.py`
Wrapper producer. Key = `station_id` → cùng trạm luôn vào cùng partition → giữ đúng thứ tự thời gian.

#### `config/cities.json`
Danh sách ~200 điểm quan trắc, lọc từ `city.list.json.gz` của OpenWeather (file này tải miễn phí).
Vừa là input của Collector, vừa là **bảng dimension** để join trong Spark sau này.

### 6.4. `data/` — dữ liệu mẫu · CHUNG

Thứ khiến **2 người làm song song mà không phải chờ nhau**.

| File | Dạng | Cho ai |
|---|---|---|
| `fixtures/owm_air_pollution_raw.json` | Response **nguyên bản** của API + 8 cái bẫy đã biết | **A** — viết `normalize()` ăn đúng dạng này |
| `samples/air_quality_sample.jsonl` | ~4.900 bản ghi **đã chuẩn hoá theo C1** | **B** — Spark `local[*]` đọc thẳng |
| `samples/generate_sample.py` | Script sinh lại sample (seed cố định) | chung |

B chạy `local[*]` trên sample → làm được toàn bộ Pha 1/2/3 mà **không cần Kafka, HDFS hay Docker**.

Sample cố tình chứa 5 loại lỗi của dữ liệu thật (gap, trùng, null, âm, spike) và mô phỏng đúng
động lực học (tự tương quan lag-1 ≈ 0.83, chu kỳ ngày đêm đỉnh 7h/19h). Nếu sample quá sạch thì
Pha 1 viết ra sẽ vỡ khi gặp dữ liệu thật ở M2.

> ⚠️ Sample hiện tại là **dữ liệu sinh giả**. Ở M0 phải thay bằng dữ liệu thật từ API.

#### Mức căn cứ của schema — ĐÃ KIỂM CHỨNG

Ngày **13/09/2026** đã gọi API thật (TP.HCM, 1 bản ghi hiện tại + 145 bản ghi lịch sử 7 ngày)
và đối chiếu toàn bộ giả định. Response thật lưu ở `data/fixtures/owm_air_pollution_raw.json`.

| Giả định | Kết quả |
|---|---|
| Top-level `{coord, list}` | ✅ đúng |
| `coord` là object `{lon, lat}` | ✅ đúng |
| `list[i]` = `{main, components, dt}` | ✅ đúng |
| `components` đúng 8 trường, µg/m³ | ✅ đúng, history và current giống hệt nhau |
| Bảng breakpoint 1–5 của OpenWeather | ✅ **khớp 146/146 bản ghi (100%)** — bảng em nhớ là đúng |
| `coord` trả về = toạ độ gửi đi | ❌ **SAI** — lệch ~78m → quy tắc **R2** |
| `dt` luôn tròn giờ | ❌ **SAI một nửa** → quy tắc **R1** |
| History trả đủ mọi mốc giờ | ❌ **SAI** — chỉ 85.8% |

**Hai giả định sai đã thành quy tắc bắt buộc trong `CONTRACTS.md`:**

- **R1 — timestamp.** `history` tròn giờ (145/145) nhưng `current` **không** (`11:37:10Z`).
  `normalize()` phải làm tròn **xuống** giờ, giữ số gốc ở trường mới `dt_raw`.
  Không làm thế thì bản ghi live 11:37 và bản ghi history 11:00 thành hai bản ghi khác nhau
  → `dropDuplicates` không bắt được → HBase có 2 row cho cùng một giờ → số liệu đếm hai lần.
- **R2 — toạ độ.** Gửi `10.8231/106.6297`, API trả `10.8238/106.6289`.
  `lat`/`lon` luôn lấy từ `config/cities.json`, không bao giờ lấy từ `coord` của response.

**Phát hiện lớn nhất — dữ liệu thiếu CÓ QUY LUẬT:**

Trên 90 ngày thật có 4 khoảng đứt: `24h`, `24h`, `48h`, `24h`. Tất cả đều là **khối nguyên ngày**
và tất cả đều **bắt đầu đúng 01:00 UTC**. Không một giờ lẻ nào bị thiếu.

Đây không phải nhiễu ngẫu nhiên mà là **ngày dữ liệu bị mất cả khối**, và nó đổi hẳn cách viết Pha 1:

- ❌ **Không** nội suy qua khoảng 24 giờ — quá dài, nội suy sẽ bịa ra số liệu.
- ✅ Đánh dấu ngày đó **thiếu dữ liệu**, loại khỏi AQI ngày, và **báo cáo tỉ lệ ngày hợp lệ**.
- Khớp luôn với yêu cầu của QĐ 1459 về độ đầy đủ tối thiểu trong cửa sổ 24h.

Tỉ lệ đầy đủ thật: **94,4% trên 90 ngày**. (Mẫu 7 ngày cho 85,8% vì rơi trúng đúng một gap —
đây cũng là bài học: đừng kết luận từ cửa sổ quá ngắn.)

**Và 7 ngày không đủ để hiệu chỉnh phân phối:** `pm2_5` max trong 7 ngày là **10,1** nhưng
trong 90 ngày là **112,9** — rộng gấp 11 lần. Cửa sổ 7 ngày rơi vào mùa mưa, không khí sạch.

### 6.5. `spark/aqi_core/` — LÕI ĐỒ ÁN · NGƯỜI B

#### `iaqi.py` ← **trái tim của đồ án**

```
        I_high - I_low
 IAQI = --------------- × (C - BP_low) + I_low        ← nội suy tuyến tính, mỗi chất một số
        BP_high - BP_low

 AQI  = max(IAQI của tất cả các chất)                 ← chất nào tệ nhất quyết định
```

**Vì sao đây là lõi:** OpenWeather có trả sẵn `owm_aqi` nhưng đó là **thang 1–5 riêng của họ**.
Nếu dùng thẳng số đó thì Pha 2 không còn gì để tính và **đồ án mất luôn phần đóng góp khoa học**.

**QUY TẮC VÀNG:** làn batch và làn streaming **cùng import module này**, không ai được copy công thức
sang chỗ khác. Copy = hai làn cho ra số khác nhau và không ai biết số nào đúng.

#### `breakpoints_vn.json` / `breakpoints_epa.json`
Bảng tra nồng độ → khoảng AQI. Tách ra file riêng để đổi chuẩn mà không sửa code.

> ⚠️ Số trong 2 file này **chưa được kiểm chứng** — dựng sẵn để code chạy được.
> Người B phải đối chiếu từng số với văn bản QĐ 1459 gốc ở M0 trước khi dùng cho báo cáo.

#### `tests/test_iaqi.py`
**Phải có ≥3 case tính tay đối chiếu ví dụ trong QĐ 1459** — đây là bằng chứng duy nhất chứng minh
công thức đúng. Không có nó thì không ai tin con số trong báo cáo.

### 6.6. `spark/jobs/` — pipeline · NGƯỜI B

#### `phase1_clean.py` — PHA 1: LÀM SẠCH
| | |
|---|---|
| Vào | `/air-quality/raw/` (jsonl thô) |
| Ra | `/air-quality/clean/` (parquet) |
| Mapper | key = `(station_id, ts_utc)`, value = bản ghi thô |
| Reducer | khử trùng lặp, loại ngoại lai, nội suy điểm thiếu |

Phải xử lý đúng 5 loại lỗi có trong sample. **Và một việc ít ai để ý:** chuẩn VN yêu cầu
PM2.5/PM10 dùng **trung bình trượt 24 giờ**, các chất khác dùng giá trị giờ.
Cửa sổ trung bình này tính ở Pha 1, không phải Pha 2.

#### `phase2_aqi.py` — PHA 2: TÍNH AQI ← trọng tâm
| | |
|---|---|
| Vào | `/air-quality/clean/` |
| Ra | `/air-quality/aqi/` (schema C3) |
| Mapper | mỗi bản ghi → IAQI của từng chất |
| Reducer | `AQI = max(IAQI)` + gán mức + chất trội |

Gọi `aqi_core.iaqi` — **không viết lại công thức ở đây**.
Output phụ cần cho báo cáo: bảng đối chiếu `aqi` tự tính vs `owm_aqi`.

#### `phase3_aggregate.py` — PHA 3: TỔNG HỢP
Ba output đúng như 3 job của repo tham chiếu Iris-pot/AQI_analysis: trung bình AQI theo
(thành phố, ngày) · đếm phân bố 6 mức · xếp hạng thành phố.
Dữ liệu 5 năm cho phép thêm chiều **mùa vụ** — rất đáng đưa vào báo cáo.

#### `streaming_aqi.py` — làn stream
Kafka → parse C1 → `aqi_core` (**cùng module với Pha 2**) → HBase + HDFS.

**Test bắt buộc:** cho cùng một bản ghi chạy qua cả batch và streaming → hai bên phải ra **cùng AQI**.
Đây là bằng chứng "một lõi dùng chung" hoạt động.

#### `ext_clustering.py` / `ext_forecast.py` — NGOÀI LÕI
Chọn **1 trong 2**, chỉ làm khi Pha 1/2/3 đã xong và ổn định. Đây là phần mục 7 của tài liệu khảo cứu
(thay K-means bằng GMM/Bisecting K-means, thay LaSVM bằng Random Forest/XGBoost).

### 6.7. `serving/app/main.py` — cầu nối · NGƯỜI A

**Vì sao cần:** Grafana **không** nối thẳng vào HBase. Lớp này tách dashboard khỏi chi tiết HBase —
đổi schema HBase sau này chỉ phải sửa một chỗ, dashboard không biết gì.

5 endpoint chốt ở `CONTRACTS.md` §C5.

### 6.8. `grafana/`, `scripts/`, `docs/`

| | |
|---|---|
| `grafana/dashboards/` | JSON export của dashboard, để dựng lại được trên máy khác |
| `scripts/01_create_topics.sh` | tạo 2 topic Kafka theo §C6 |
| `scripts/02_init_hdfs.sh` | tạo cây thư mục HDFS theo §C2 |
| `scripts/03_init_hbase.sh` | tạo bảng `air_quality` theo §C4 |
| `docs/experiments.md` | **bảng số liệu thực nghiệm — phần dễ bị bỏ quên nhất** |

Về `experiments.md`: đồ án big data bị chấm nặng ở chỗ **chứng minh vì sao cần phân tán**.
Chạy Pha 2 trên 100K / 1M / 8M bản ghi × 1 / 2 / 4 executor, vẽ biểu đồ thời gian chạy và speedup.
Không có bảng này thì hội đồng sẽ hỏi "sao không dùng pandas cho nhanh".

---

## 7. Ai chờ ai

```
A: docker ──▶ collector ──▶ backfill ──▶ HDFS /raw ──┐
                                                      ├──▶ Pha 1 ──▶ Pha 2 ──▶ Pha 3 ──▶ HBase ──▶ API ──▶ Grafana
B: aqi_core ─────────────────────────────────────────┘                                      (A)      (A)
```

Nhìn sơ đồ thì B phải chờ A xong backfill. **Nhưng không**, nhờ `data/samples/`:

| Giai đoạn | A làm | B làm | Có chờ nhau? |
|---|---|---|---|
| M0 | lấy API key, sinh sample | chốt bảng breakpoint | **không** |
| M1 | dựng Docker | aqi_core + Pha 1 trên sample | **không** |
| M2 | backfill vào HDFS | Pha 2 trên sample → rồi đổi sang HDFS | **không** |
| M3 | HBase + FastAPI | Pha 3 + streaming | B cần bảng HBase của A |
| M4 | Grafana | thực nghiệm + mở rộng | A cần API của B chạy xong |
| M5 | test end-to-end | báo cáo | cùng làm |

Chỗ duy nhất thật sự chờ nhau là **M3** (B cần bảng HBase để ghi vào).
Giải: A tạo bảng HBase **sớm ở M1** — tạo bảng rỗng chỉ mất 1 lệnh, không cần đợi có dữ liệu.

---

## 8. Nếu chuyển Pha 1 (làm sạch) sang Người A

**Không block — nhưng chỉ khi làm thêm 3 việc ở M0.** Và có một cái bẫy về nội dung
quan trọng hơn chuyện block.

### Bẫy: Pha 1 không thuần tuý là "làm sạch"

Nó trộn hai loại việc có bản chất khác hẳn nhau:

| | Việc | Cần biết gì | Ai nên làm |
|---|---|---|---|
| **1a** | khử trùng lặp · loại giá trị âm · loại spike ngoài ngưỡng vật lý · phát hiện gap · ép kiểu · chuẩn hoá timestamp | **hiểu dữ liệu API sinh ra lỗi gì** | **A** — vì A viết Collector, A biết rõ nhất |
| **1b** | trung bình trượt 24h cho PM2.5/PM10 · kiểm tra độ đầy đủ dữ liệu trong cửa sổ · nội suy điểm thiếu | **hiểu chuẩn QĐ 1459 yêu cầu cửa sổ nào** | **B** — vì đây là yêu cầu của công thức AQI |

Nhóm **1b không phải làm sạch, nó là chuẩn bị đầu vào cho công thức AQI**. Chuẩn VN quy định
PM2.5/PM10 tính trên trung bình 24 giờ và cửa sổ phải đủ số giờ hợp lệ tối thiểu mới được coi là có giá trị.
Nếu A viết phần này mà không nắm chuẩn, kết quả sẽ **sai một cách âm thầm** — số vẫn ra, biểu đồ vẫn đẹp,
nhưng AQI sai và không ai phát hiện cho đến lúc bảo vệ.

### Ba phương án

| | Cách chia | Ưu | Nhược |
|---|---|---|---|
| **1** | Pha 1 → A **toàn bộ** | A gọn một mảng | Rủi ro 1b sai âm thầm; A phải học cả PySpark trong khi đang gánh 6 service Docker |
| **2** ⭐ | **1a → A, 1b → B** | Logic nghiệp vụ ở đúng người hiểu nó; A vẫn được tiếp cận Spark | Phải tách file, thêm 1 mục hợp đồng |
| **3** | Giữ Pha 1 cho B, **chuyển `streaming_aqi.py` sang A** | Không đụng vào chuỗi phụ thuộc; A đã sở hữu Kafka + HBase nên job này chủ yếu là "đấu nối" | B vẫn nặng phần phân tích |

**Đề xuất: phương án 2.** Chia theo *bản chất công việc*, không theo *tên pha*.

```
/raw ──▶ phase1a_clean.py (A) ──▶ /clean ──▶ phase1b_window.py (B) ──▶ /ready ──▶ phase2_aqi.py (B)
         kỹ thuật, không cần                  nghiệp vụ, bám chuẩn
         biết chuẩn AQI                       QĐ 1459
```

### Ba việc phải làm thêm ở M0 để không block

**1. Thêm mục `C2b` vào `CONTRACTS.md` — schema parquet của `/clean`.**
Hiện hợp đồng có C1 (message thô) và C3 (sau Pha 2), **chưa có schema của `/clean`** — trước đây
không cần vì Pha 1 và Pha 2 cùng một người. Giờ nó thành biên giới giữa 2 người → bắt buộc phải chốt.
Tối thiểu: tên cột, kiểu, cột nào cho phép null, và **cột `qc_flag`** (`ok` / `interpolated` /
`outlier_removed`) để B biết bản ghi nào tin được, và để báo cáo có số liệu chất lượng dữ liệu.

**2. Commit `data/samples/clean_sample.parquet`.**
Đúng trò cũ đã dùng với `air_quality_sample.jsonl`: B phát triển 1b + Pha 2 trên file mẫu này,
không phải chờ A viết xong 1a.

**3. Sửa quy ước "vùng cấm" trong `WORKPLAN.md`.**
Quy ước hiện tại chia theo **thư mục** (*A không sửa `spark/`*) → gãy khi A phải viết file trong
`spark/jobs/`. Đổi sang chia theo **file**:

| File | Chủ |
|---|---|
| `spark/jobs/phase1a_clean.py` | A |
| `spark/jobs/phase1b_window.py` | B |
| `spark/jobs/phase2_aqi.py`, `phase3_aggregate.py`, `streaming_aqi.py` | B |
| `spark/aqi_core/**` | B — **A không sửa, kể cả một dòng** |
| `spark/requirements.txt` | chung, báo trước khi sửa |

### Chi phí ẩn cần cân nhắc

A sẽ phải dựng môi trường PySpark và học DataFrame API, **trong khi đang gánh phần nặng nhất về vận hành**
(6 service Docker trên WSL2, HBase hay chết, ZooKeeper khó debug). Nếu A chưa quen Spark,
đây có thể là thứ làm chậm cả tiến độ chung.

Nếu lo điều đó: chọn **phương án 3** — giữ nguyên Pha 1 cho B, chuyển `streaming_aqi.py` sang A.
Job đó chủ yếu là đấu nối Kafka → HBase (hai thứ A đã sở hữu), phần tính toán chỉ là một lời gọi
`aqi_core`, nên A tiếp cận Spark ở mức nhẹ hơn nhiều.

---

## 9. Bắt đầu

```bash
cp .env.example .env          # điền OWM_API_KEY

# Người A:
cd docker && docker compose up -d
bash ../scripts/01_create_topics.sh

# Người B (không cần Docker):
cd spark && pip install -r requirements.txt && pytest
python jobs/phase1_clean.py --input ../data/samples/air_quality_sample.jsonl --output /tmp/clean
```

Đọc `WORKPLAN.md` để biết việc của mình ở milestone hiện tại.

## 10. Môi trường

Phiên bản service theo Bảng 1 của *Thiết kế hệ thống v1*.
**Máy nào dựng Docker thì đọc đúng mục của máy đó.**

### Windows 11 + WSL2 + Ubuntu 24.04 + Docker Desktop

> **Cấp RAM cho WSL2** trước khi chạy: tạo `C:\Users\<user>\.wslconfig` với `memory=10GB`.
> HBase + HDFS + Spark + Kafka cùng lúc rất dễ OOM ở mặc định.

### macOS

```bash
uname -m
```

- `x86_64` (Mac Intel) → giống Linux thường, không có gì đặc biệt.
- `arm64` (Apple Silicon M-series) → **đọc `docs/ARM64-APPLE-SILICON.md` TRƯỚC khi viết
  `docker-compose.yml`.** Phần lớn image Hadoop/HBase chỉ có bản amd64; chạy giả lập thì chậm
  và HBase hay chết vặt. File đó hướng dẫn cách tự build image chạy native trên arm64.

Cấp RAM: **Docker Desktop → Settings → Resources** → Memory ≥ 10 GB, CPU ≥ 4.
Mục `.wslconfig` ở trên **không áp dụng cho Mac**.

## Tài liệu gốc

- `Phân tích từ paper.docx` — khảo cứu, chọn El Fazziki et al. (2015)
- `Thiết kế hệ thống_v1.docx` — kiến trúc streaming, phiên bản service, quy ước
