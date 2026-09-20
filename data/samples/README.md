# Dữ liệu mẫu

Dữ liệu mẫu để phát triển và chạy test mà không cần API key, Kafka hay HDFS.

| File | Nội dung | Dùng để |
|---|---|---|
| `../fixtures/owm_air_pollution_raw.json` | Response thật của API, kèm các phát hiện đã kiểm chứng | Viết và kiểm tra `normalize()` |
| `air_quality_sample.jsonl` | 10.142 bản ghi đã chuẩn hoá theo C1 (5 trạm × 90 ngày) | Chạy Spark `local[*]` và các test |
| `generate_sample.py` | Script sinh lại sample (seed cố định) | Tái tạo `air_quality_sample.jsonl` |
| `ext_clusters_sample.json`, `ext_forecast_backtest_sample.json` | Mẫu hai file kết quả nhánh mở rộng (CONTRACTS.md, C8) | Dựng endpoint `/ext/*` khi chưa có kết quả thật |

## Hiệu chỉnh theo 2041 bản ghi thật, 90 ngày

Bản sinh đầu tiên cho giá trị cao gấp khoảng 4 lần thực tế. Bản thứ hai hiệu chỉnh theo 7 ngày nhưng vẫn sai,
vì cửa sổ 7 ngày rơi vào mùa mưa: `pm2_5` lớn nhất chỉ 10,1 trong khi ở 90 ngày là 112,9.
Bản hiện tại hiệu chỉnh theo 90 ngày:

| Chất | TB thật / sample | p95 thật / sample | max thật / sample |
|---|---|---|---|
| pm2_5 | 8.95 / 8.90 | 25.90 / 23.97 | 112.93 / 118.67 |
| pm10 | 11.31 / 12.49 | 29.54 / 33.93 | 118.71 / 159.13 |
| o3 | 32.12 / 32.29 | 55.90 / 53.26 | 69.38 / 70.79 |
| no2 | 5.33 / 5.55 | 15.97 / 15.87 | 25.63 / 74.54 |
| so2 | 1.15 / 1.14 | 2.54 / 2.67 | 5.53 / 7.39 |
| co | 293.35 / 290.58 | 711.11 / 751.49 | 2414.71 / 2076.80 |

Mô hình dùng để sinh, các tham số đều đo từ dữ liệu thật:

- PM2.5 theo phân phối log-chuẩn, `mean(log)=1.838`, `sd(log)=0.806`, cho độ lệch phải trung bình/trung vị = 1.42 (thật 1.44)
- AR(1) trên thang log, `φ = 0.964`: chuỗi rất trơn, giờ này gần bằng giờ trước
- Profile theo giờ lấy thẳng từ dữ liệu thật (mảng 24 số), không fit hàm cos, vì profile thật không đối xứng:
  PM đỉnh lúc 5h nhưng đáy lúc 12h (cách nhau 7h, không phải 12h)
- O3 đỉnh lúc 14h, đáy lúc 4h (quang hoá), biên độ ±43%
- pm10/pm2_5 = 1.41
- `owm_aqi` tính bằng bảng breakpoint 1–5, đã kiểm chứng khớp 100% trên 2187 bản ghi thật

Sample dài đúng 90 ngày (2160 giờ) cho mỗi trạm. Ngắn hơn thì chuỗi AR(1) với φ=0.964 không kịp đi hết đuôi
phân phối và sample sẽ sạch một cách giả tạo, đúng như mẫu 7 ngày.

## Khoảng thiếu có quy luật (quan trọng nhất cho Pha 1)

Trong 90 ngày thật có 4 khoảng đứt: 24h, 24h, 48h, 24h. Tất cả là khối nguyên ngày và đều bắt đầu đúng 01:00 UTC;
không có giờ lẻ nào bị thiếu. Đây không phải nhiễu ngẫu nhiên mà là ngày dữ liệu bị mất cả khối, nên Pha 1:

- Không nội suy qua khoảng 24 giờ, vì nội suy 24 điểm liên tiếp là bịa số liệu.
- Đánh dấu ngày đó là thiếu dữ liệu (`qc_flag`) và loại khỏi AQI ngày.
- Báo cáo tỉ lệ ngày hợp lệ. Ngưỡng đầy đủ tối thiểu là 75% (18/24 giờ), lấy theo chuẩn US EPA vì các mục QĐ 1459
  đã đối chiếu không ghi rõ số phần trăm.

Chỉ nội suy khi thiếu tối đa 3 giờ lẻ.

## Lỗi nhúng trong sample vs quan sát thật

| Loại | Sample | Dữ liệu thật (2187 bản ghi) |
|---|---|---|
| Khoảng thiếu (khối nguyên ngày) | 24/48/72/96h, bắt đầu 01:00 UTC, đầy đủ 93.8% | Có thật: 24/48h, 01:00 UTC, đầy đủ 94.4% |
| Trùng mốc giờ | 10 | 0 ca (nhưng backfill theo chunk dễ trùng ở biên) |
| Thiếu lẻ một chất | ~20 | 0 ca |
| Giá trị âm | ~5 | 0 ca |

Khoảng thiếu là lỗi chính. Ba loại còn lại chưa từng quan sát thấy trong dữ liệu thật; chúng được nhúng với tỉ lệ
rất thấp để Pha 1 có phòng thủ.

Sample có thêm khoảng thiếu 72h/96h do hai khối liền nhau dính lại. Dữ liệu thật chưa thấy, nhưng Pha 1 xử lý
được 96h thì cũng xử lý được 24h.

## Lưu ý: đây vẫn là dữ liệu sinh giả

Sample có thang giá trị, phân phối và động lực học giống TP.HCM tháng 6–9/2026, nhưng không dùng để rút kết luận
phân tích.

Hệ số của các thành phố khác (Hà Nội 2.6×, Delhi 7×, Tokyo 1.2×) là ước lượng chưa kiểm chứng, vì khi hiệu chỉnh
mới chỉ gọi API cho TP.HCM.

Mọi số liệu chính thức trong báo cáo phải lấy từ dữ liệu thật trên HDFS.
