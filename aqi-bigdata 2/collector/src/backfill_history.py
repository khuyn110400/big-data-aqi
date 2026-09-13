"""
Backfill dữ liệu lịch sử -> HDFS /air-quality/raw/. NGƯỜI A · M2

ĐÂY LÀ THỨ TẠO RA "BIG DATA" CHO ĐỒ ÁN.
Mục tiêu: ~200 điểm x 3-5 năm x 8760 giờ ~= 5-9 triệu bản ghi.

Chiến lược:
  - Chunk theo tháng (1 call = 1 điểm x 1 tháng ~ 720 bản ghi)
  - 200 điểm x 60 tháng = 12.000 call -> ~3.5 giờ ở 1 call/giây. Chạy qua đêm.
  - Checkpoint vào state file: chạy lại không mất công đã làm
  - Gộp output đủ lớn (>= 64 MB/file) rồi mới ghi HDFS -> tránh small-file problem
    (bài học vận hành từ paper El Fazziki)

Chạy:
  python backfill_history.py --cities config/cities.json \
      --from 2021-01-01 --to 2026-09-01 --out hdfs:///air-quality/raw/

TODO(A):
  [ ] --resume đọc state file
  [ ] --dry-run in ra số call sẽ dùng trước khi chạy thật
  [ ] ghi .jsonl.gz, partition ingest_mode/country/dt theo C2
  [ ] in báo cáo cuối: tổng bản ghi, dung lượng, thời gian, số call
"""
