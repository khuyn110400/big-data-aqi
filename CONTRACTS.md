# CONTRACTS — Hợp đồng giữa 2 người

> **Đây là file quan trọng nhất của repo.** Mọi thứ trong file này phải được **chốt trước khi ai đó viết dòng code thật đầu tiên**.
> Khi 2 người làm song song, cái duy nhất khiến code ghép được vào nhau là các schema dưới đây.
>
> **Quy tắc:** muốn đổi bất kỳ mục nào trong file này → nhắn cho người kia trước, sửa file này trước, rồi mới sửa code. Không tự đổi.

---

## C1. Message schema — `air-quality-raw` (Kafka) & JSONL trên HDFS

Một bản ghi = một lần đo tại một điểm, một mốc giờ. **Dùng chung cho cả làn batch và làn streaming.**

```json
{
  "schema_version": "1.0",
  "station_id": "VN_HCM_01",
  "city": "Ho Chi Minh City",
  "country": "VN",
  "lat": 10.8231,
  "lon": 106.6297,
  "ts_utc": "2026-09-13T07:00:00Z",
  "ts_epoch": 1789023600,
  "dt_raw": 1789023600,
  "source": "openweather.air_pollution",
  "ingest_mode": "history",
  "fetched_at_utc": "2026-09-13T10:12:03Z",
  "owm_aqi": 2,
  "components": {
    "co": 233.65,
    "no": 0.04,
    "no2": 7.71,
    "o3": 68.66,
    "so2": 2.62,
    "pm2_5": 18.43,
    "pm10": 24.11,
    "nh3": 1.15
  }
}
```

| Trường | Kiểu | Bắt buộc | Ghi chú |
|---|---|---|---|
| `schema_version` | string | ✅ | Tăng lên khi đổi schema. Spark dùng để bỏ qua bản ghi lạ. |
| `station_id` | string | ✅ | Do Collector sinh, format `{COUNTRY}_{CITYCODE}_{NN}`. **Là key chính**. |
| `city`, `country` | string | ✅ | `country` là ISO-2 (`VN`, `IN`, ...). |
| `lat`, `lon` | double | ✅ | Làm tròn 4 chữ số. |
| `ts_utc` | string | ✅ | ISO-8601 **UTC**, **làm tròn XUỐNG giờ** (`:00:00Z`). Xem quy tắc R1. |
| `ts_epoch` | long | ✅ | Giây, khớp `ts_utc`. Dùng dựng row key HBase. |
| `dt_raw` | long | ✅ | `dt` **nguyên bản** API trả về, chưa làm tròn. Giữ để truy vết. |
| `ingest_mode` | string | ✅ | `history` \| `live`. Để tách 2 làn khi debug. |
| `owm_aqi` | int | ⬜ | Chỉ số 1–5 **của OpenWeather** — KHÔNG phải AQI của đề tài. Giữ để đối chiếu. |
| `components.*` | double | ⬜ | μg/m³. Có thể thiếu → Pha 1 xử lý. |

#### R1 — Quy tắc timestamp (đã kiểm chứng bằng API thật 13/09/2026)

Hai endpoint trả `dt` **khác nhau**:

| Endpoint | `dt` | Ví dụ thật |
|---|---|---|
| `/air_pollution/history` | **tròn giờ** (145/145 bản ghi) | `1788692400` → `2026-09-06 11:00:00Z` |
| `/air_pollution` (hiện tại) | **KHÔNG tròn giờ** | `1789299430` → `2026-09-13 11:37:10Z` (dư 37ph10s) |

**Quy tắc:** `normalize()` luôn **làm tròn XUỐNG giờ** để tạo `ts_utc`/`ts_epoch`,
và giữ nguyên số gốc ở `dt_raw`.

```python
ts_epoch = dt_raw - (dt_raw % 3600)
```

**Vì sao bắt buộc:** cùng một giờ, làn live và làn batch phải sinh ra **cùng một key**.
Nếu không làm tròn, bản ghi live 11:37 và bản ghi history 11:00 trở thành hai bản ghi khác nhau
→ `dropDuplicates` ở Pha 1 không bắt được, HBase có 2 row cho cùng một giờ, và số liệu bị đếm hai lần.

#### R2 — Toạ độ (đã kiểm chứng)

API trả `coord` **lệch** so với toạ độ gửi đi (gửi `10.8231/106.6297` → trả `10.8238/106.6289`, lệch ~78m).
`lat`/`lon` trong bản ghi **luôn lấy từ `config/cities.json`**, không bao giờ lấy từ `coord` của response.

**Không đổi tên trường. Không thêm cấp lồng nhau. Không đổi đơn vị.**

---

## C2. HDFS layout

```
/air-quality/
├── raw/                                  ← Người A ghi, Người B đọc
│   └── ingest_mode=history/country=VN/dt=2026-09-13/part-*.jsonl.gz
├── clean/                                ← Pha 1 (Người B)
│   └── country=VN/dt=2026-09-13/part-*.parquet
├── aqi/                                  ← Pha 2 (Người B)
│   └── country=VN/dt=2026-09-13/part-*.parquet
└── agg/                                  ← Pha 3 (Người B)
    ├── daily/country=VN/dt=.../part-*.parquet
    └── ranking/dt=.../part-*.parquet
```

Quy ước: partition `country` + `dt` (ngày UTC). **Không tạo hàng nghìn file nhỏ** — ép `coalesce()` sao cho mỗi file ≥ 64 MB (bài học vận hành từ paper El Fazziki).

## C3. Schema Parquet sau Pha 2 (`/air-quality/aqi/`)

| Cột | Kiểu | Mô tả |
|---|---|---|
| `station_id` | string | |
| `city`, `country` | string | |
| `lat`, `lon` | double | |
| `ts_utc` | timestamp | |
| `pm2_5`,`pm10`,`o3`,`no2`,`so2`,`co` | double | nồng độ đã làm sạch |
| `iaqi_pm2_5` … `iaqi_co` | double | sub-index từng chất (Pha 2 Mapper) |
| `aqi` | double | = max(iaqi_*) (Pha 2 Reducer) |
| `aqi_level` | int | 1–6 |
| `aqi_label` | string | `Tốt`/`Trung bình`/`Kém`/`Xấu`/`Rất xấu`/`Nguy hại` |
| `dominant_pollutant` | string | chất có IAQI cao nhất |
| `standard` | string | `VN_1459` \| `US_EPA` |

## C4. HBase

```
Table:  air_quality
Column family: d   (viết tắt "data", 1 ký tự để tiết kiệm dung lượng)

Row key:  {station_id}#{reverse_ts}
          reverse_ts = 9999999999 - ts_epoch   (để bản ghi mới nhất nằm đầu, scan nhanh)

Columns:  d:city  d:country  d:lat  d:lon  d:ts_utc
          d:pm25  d:pm10  d:o3  d:no2  d:so2  d:co
          d:aqi   d:level  d:label  d:dom   d:std
```

> Row key bắt đầu bằng `station_id` → tránh hotspot region khi ghi streaming, và cho phép scan theo trạm + khoảng thời gian chỉ bằng prefix.

## C5. Query/API Bridge — endpoint cho Grafana

Base: `http://localhost:8000`

| Method | Path | Query params | Trả về |
|---|---|---|---|
| GET | `/health` | | `{"status":"ok"}` |
| GET | `/stations` | `country?` | `[{station_id, city, country, lat, lon}]` |
| GET | `/aqi/latest` | `country?`, `limit?` | `[{station_id, city, lat, lon, ts_utc, aqi, aqi_level, aqi_label, dominant_pollutant}]` |
| GET | `/aqi/timeseries` | `station_id` (bắt buộc), `from`, `to` | `[{ts_utc, aqi, pm2_5, pm10, o3, no2, so2, co}]` |
| GET | `/aqi/ranking` | `dt`, `country?` | `[{rank, city, avg_aqi, max_aqi, worst_pollutant}]` |

Thời gian trong response **luôn là ISO-8601 UTC**. Grafana đổi sang giờ VN ở tầng hiển thị.

## C6. Kafka

| Topic | Partitions | Nội dung |
|---|---|---|
| `air-quality-raw` | 3 | Bản ghi theo C1, key = `station_id` |
| `air-quality-dlq` | 1 | Bản ghi lỗi schema, để debug |

Key = `station_id` → cùng một trạm luôn vào cùng partition → giữ đúng thứ tự thời gian.

## C7. Biến môi trường (`.env`)

| Biến | Ví dụ | Ai dùng |
|---|---|---|
| `OWM_API_KEY` | `abc123...` | A |
| `KAFKA_BOOTSTRAP` | `localhost:9092` / `broker:19092` | A, B |
| `HDFS_URI` | `hdfs://namenode:9000` | A, B |
| `HBASE_ZK` | `zookeeper:2181` | A, B |
| `AQI_STANDARD` | `VN_1459` | B |

`.env` **không commit**. Chỉ commit `.env.example`.

## C8. File kết quả nhánh mở rộng cho demo — ĐỀ XUẤT, chờ Người A duyệt

> **Trạng thái: đề xuất của B, chưa có hiệu lực cho tới khi A xác nhận.** Hai file dưới đây do
> `spark/jobs/ext_clustering.py` (`--json-out`) và `spark/jobs/ext_forecast.py` (`--backtest-out`) sinh ra.
> Chúng nhỏ (vài KB đến vài trăm KB) nên A phục vụ như file tĩnh, giống cách đọc `cities.json`.
> File mẫu sinh từ `data/samples/` (dữ liệu giả, chỉ để dựng endpoint và panel):
> `data/samples/ext_clusters_sample.json`, `data/samples/ext_forecast_backtest_sample.json`.

Quy tắc chung: giờ luôn là ISO-8601 UTC; số thiếu là `null`; không đổi tên trường. Thêm trường mới thì tăng `schema_version`.

### C8.1 `ext_clusters.json` — phân cụm vùng theo (thành phố, tháng)

```json
{
  "schema_version": "1.0",
  "generated_at_utc": "2026-09-19T07:27:46Z",
  "algorithm": "KMeans",
  "params": {"k": 2},
  "silhouette": 0.4909,
  "n_clusters": 2,
  "rows": [
    {"city": "New Delhi", "country": "IN", "month": 6, "lat": 28.6139, "lon": 77.209,
     "avg_pm2_5": 52.4137, "avg_pm10": 73.8789, "avg_o3": 32.4236, "avg_no2": 35.9836, "cluster": 0}
  ]
}
```

| Trường | Kiểu | Ghi chú |
|---|---|---|
| `algorithm` | string | Thuật toán thắng: `KMeans` \| `GaussianMixture` \| `BisectingKMeans` \| `DBSCAN` \| `HDBSCAN` |
| `params` | object | Tham số đã chọn: `{"k": 2}` hoặc `{"eps": 2.0, "min_samples": 2}` hoặc `{"min_cluster_size": 3}` |
| `silhouette` | number | Silhouette Euclidean bỏ điểm nhiễu, cùng thước đo cho cả 5 thuật toán |
| `n_clusters` | int | Số cụm thật, **không** tính nhóm nhiễu |
| `rows[].month` | int | 1–12. Cùng một thành phố có thể thuộc cụm khác nhau ở các tháng khác nhau (mùa vụ) |
| `rows[].cluster` | int | Nhãn cụm từ 0. **`-1` là điểm nhiễu** (chỉ có khi thuật toán thắng là DBSCAN/HDBSCAN) |
| `rows[].avg_*` | number | Trung bình nồng độ (µg/m³) của (thành phố, tháng) — dùng cho tooltip |

### C8.2 `ext_forecast_backtest.json` — dự báo AQI 24 giờ so với thực tế

Mô hình dự báo **một giá trị**: AQI sau đúng 24 giờ kể từ giờ phát dự báo (không phải đường cong 24 giờ).
Mỗi dòng là một giờ đã được dự báo và đã biết giá trị thật, của cả 3 tầng, để chọn model sau này mà không phải chạy lại.

```json
{
  "schema_version": "1.0",
  "generated_at_utc": "2026-09-19T07:37:01Z",
  "horizon_hours": 24,
  "models": {"sgd": "SGDRegressor (Tầng 1)", "rf": "Random Forest (Tầng 2)", "cnn_lstm": "CNN-LSTM (Tầng 3)"},
  "metrics": {
    "sgd": {"rmse": 32.05, "mae": 20.56, "r2": 0.5382, "n_test": 1154},
    "rf": {"rmse": 31.87, "mae": 20.64, "r2": 0.5431, "n_test": 1154},
    "cnn_lstm": {"rmse": 32.11, "mae": 21.26, "r2": 0.5359, "n_test": 1157}
  },
  "stations": ["IN_DEL_01", "VN_HCM_01"],
  "rows": [
    {"station_id": "IN_DEL_01", "ts_utc": "2026-09-02T00:00:00Z", "issued_utc": "2026-09-01T00:00:00Z",
     "actual": 197.0, "sgd": 246.6, "rf": 181.4, "cnn_lstm": 184.4}
  ]
}
```

| Trường | Kiểu | Ghi chú |
|---|---|---|
| `metrics.*` | object \| null | Đo trên tập test của **mọi trạm**, không chỉ các trạm trong file (riêng `cnn_lstm` đo trên mẫu tối đa `--cnn-max-rows` chuỗi). `cnn_lstm` là `null` nếu tầng 3 bị bỏ qua |
| `stations` | string[] | Vài trạm được chọn (mặc định `VN_HCM_01, VN_HAN_01, IN_DEL_01, CN_BJS_01, JP_TYO_01` nếu có dữ liệu) |
| `rows[].ts_utc` | string | Giờ **được dự báo** (dùng làm trục thời gian khi vẽ) |
| `rows[].issued_utc` | string | Giờ phát dự báo = `ts_utc` − 24 giờ |
| `rows[].actual` | number | AQI thật tại `ts_utc` |
| `rows[].sgd` / `rf` / `cnn_lstm` | number \| null | Dự báo của từng tầng; `null` nếu tầng đó không chấm giờ này |

Chỉ giữ N ngày cuối của tháng test (`--backtest-days`, mặc định 30) để file nhỏ.

### Endpoint gợi ý (do Người A quyết định)

| Method | Path | Query | Trả về |
|---|---|---|---|
| GET | `/ext/clusters` | `month?` | `rows` của C8.1 (lọc theo tháng) kèm `algorithm`, `silhouette`, `n_clusters` |
| GET | `/ext/forecast` | `station_id` (bắt buộc), `from?`, `to?` | `rows` của C8.2 cho trạm đó kèm `metrics` |

---

## Nhật ký thay đổi hợp đồng

| Ngày | Mục | Đổi gì | Ai đề xuất |
|---|---|---|---|
| 2026-09-19 | C8 (mới) | ĐỀ XUẤT schema 2 file JSON của nhánh mở rộng (phân cụm, backtest dự báo); chờ A duyệt và quyết định endpoint | B |
