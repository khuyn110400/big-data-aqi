# Dữ liệu mẫu

Để **2 người làm song song không phải chờ nhau**.

| File | Dạng | Cho ai |
|---|---|---|
| `../fixtures/owm_air_pollution_raw.json` | **Response THẬT** + 9 phát hiện đã kiểm chứng | **A** — viết `normalize()` |
| `air_quality_sample.jsonl` | 10.142 bản ghi đã chuẩn hoá theo C1 (5 trạm × 90 ngày) | **B** — Spark `local[*]` |
| `generate_sample.py` | Script sinh lại (seed cố định) | chung |

## Hiệu chỉnh theo 2041 bản ghi thật, 90 ngày

Bản đầu em sinh bừa (cao gấp 4 lần). Bản thứ hai hiệu chỉnh theo 7 ngày — **vẫn sai**, vì cửa sổ
7 ngày rơi vào mùa mưa: `pm2_5` max chỉ 10,1 trong khi 90 ngày là **112,9**.
Bản hiện tại hiệu chỉnh theo 90 ngày:

| Chất | TB thật / sample | p95 thật / sample | max thật / sample |
|---|---|---|---|
| pm2_5 | 8.95 / **8.90** | 25.90 / **23.97** | 112.93 / **118.67** |
| pm10 | 11.31 / 12.49 | 29.54 / 33.93 | 118.71 / 159.13 |
| o3 | 32.12 / **32.29** | 55.90 / **53.26** | 69.38 / **70.79** |
| no2 | 5.33 / 5.55 | 15.97 / **15.87** | 25.63 / 74.54 |
| so2 | 1.15 / **1.14** | 2.54 / 2.67 | 5.53 / 7.39 |
| co | 293.35 / **290.58** | 711.11 / 751.49 | 2414.71 / 2076.80 |

Mô hình dùng để sinh, tất cả tham số đo từ dữ liệu thật:

- **PM2.5 log-chuẩn** `mean(log)=1.838`, `sd(log)=0.806` → độ lệch phải TB/trung vị = 1.42 (thật 1.44)
- **AR(1) trên thang log**, `φ = 0.964` — chuỗi rất "trơn", giờ này gần bằng giờ trước
- **Profile theo giờ nhúng thẳng từ dữ liệu thật** (mảng 24 số), không fit hàm cos —
  vì profile thật không đối xứng: PM đỉnh 5h nhưng đáy 12h (cách nhau 7h, không phải 12h)
- **O3 đỉnh 14h, đáy 4h** (quang hoá), biên độ ±43%
- **pm10/pm2_5 = 1.41**
- `owm_aqi` tính bằng bảng breakpoint 1–5 **đã kiểm chứng khớp 100% trên 2187 bản ghi thật**

> Sample dài đúng **90 ngày (2160 giờ)** cho mỗi trạm. Ngắn hơn thì chuỗi AR(1) với φ=0.964
> không kịp đi hết đuôi phân phối và sample sẽ "sạch" giả tạo — đúng cái bẫy mà mẫu 7 ngày mắc phải.

## GAP có quy luật — điều quan trọng nhất cho Pha 1

4 khoảng đứt trong 90 ngày thật: **24h, 24h, 48h, 24h**.
Tất cả là **khối nguyên ngày**, tất cả **bắt đầu đúng 01:00 UTC**. Không một giờ lẻ nào thiếu.

Không phải nhiễu ngẫu nhiên — là **ngày dữ liệu bị mất cả khối**. Pha 1 phải:

- ❌ **KHÔNG nội suy** qua khoảng 24 giờ. Nội suy 24 điểm liên tiếp = bịa số liệu.
- ✅ Đánh dấu ngày đó **thiếu dữ liệu** (`qc_flag`), loại khỏi AQI ngày.
- ✅ **Báo cáo tỉ lệ ngày hợp lệ** — khớp luôn yêu cầu độ đầy đủ tối thiểu của QĐ 1459.

Chỉ nội suy khi thiếu **≤ 3 giờ lẻ**.

## Lỗi nhúng trong sample vs quan sát thật

| Loại | Sample | Dữ liệu thật (2187 bản ghi) |
|---|---|---|
| **GAP khối nguyên ngày** | 24/48/72/96h, bắt đầu 01:00 UTC, đầy đủ 93.8% | ✅ **CÓ THẬT** — 24/48h, 01:00 UTC, đầy đủ 94.4% |
| **DUP** trùng mốc giờ | 10 | ❌ 0 ca — nhưng backfill theo chunk dễ trùng ở biên |
| **NULL** thiếu lẻ một chất | ~20 | ❌ 0 ca |
| **NEG** giá trị âm | ~5 | ❌ 0 ca |

GAP là lỗi **chính**. Ba loại còn lại chưa từng quan sát thấy — nhúng tỉ lệ rất thấp để Pha 1
phòng thủ, đừng dồn công sức vào đó.

> Sample có thêm gap 72h/96h do hai khối liền nhau dính lại — dữ liệu thật chưa thấy,
> nhưng Pha 1 xử lý được 96h thì xử lý được 24h.

## ⚠️ Vẫn là dữ liệu sinh giả

Đúng thang giá trị, đúng phân phối, đúng động lực học của **TP.HCM tháng 6–9/2026**.
Không dùng để rút kết luận phân tích.

Hệ số các thành phố khác (Hà Nội 2.6×, Delhi 7×, Tokyo 1.2×) là **ước lượng chưa kiểm chứng** —
mới chỉ gọi API cho TP.HCM.

Từ M2 trở đi mọi số liệu trong báo cáo phải lấy từ dữ liệu thật trên HDFS.
