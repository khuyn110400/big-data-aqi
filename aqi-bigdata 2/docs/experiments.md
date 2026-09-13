# Kết quả thực nghiệm

> NGƯỜI B điền ở M4. Phần này quyết định điểm "vì sao cần big data" của đồ án.

## 1. Quy mô dữ liệu thu thập

| Chỉ tiêu | Giá trị |
|---|---|
| Số điểm quan trắc | |
| Khoảng thời gian | |
| Tổng bản ghi thô | |
| Dung lượng raw (.jsonl.gz) | |
| Dung lượng sau Pha 2 (parquet) | |
| Thời gian backfill | |
| Số API call đã dùng | |

## 2. Scalability của Pha 2

| Kích thước | 1 executor | 2 executors | 4 executors | Speedup (4 vs 1) |
|---|---|---|---|---|
| 100K | | | | |
| 1M | | | | |
| 8M | | | | |

Biểu đồ: thời gian chạy · speedup. Nhận xét điểm nghẽn (shuffle / small files / GC).

## 3. Đối chiếu AQI tự tính vs owm_aqi

| Mức AQI tự tính (VN_1459) | Số bản ghi | owm_aqi tương ứng (mode) | Khớp? |
|---|---|---|---|
| Tốt (1) | | | |
| Trung bình (2) | | | |
| ... | | | |

## 4. Chất lượng dữ liệu sau Pha 1

| Chất | % thiếu | % ngoại lai bị loại | % nội suy |
|---|---|---|---|
| PM2.5 | | | |
| PM10 | | | |
