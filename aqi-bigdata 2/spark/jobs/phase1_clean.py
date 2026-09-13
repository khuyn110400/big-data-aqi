"""
PHA 1 — LÀM SẠCH  (El Fazziki 2015, pha 1). NGƯỜI B · M1

Mapper : key = (station_id, ts_utc), value = bản ghi thô
Reducer: khử trùng lặp, loại ngoại lai, nội suy điểm thiếu

Input : hdfs:///air-quality/raw/**/*.jsonl.gz   (hoặc data/samples/ khi dev local)
Output: hdfs:///air-quality/clean/  parquet, partition country/dt

Chạy local không cần cluster:
  python jobs/phase1_clean.py --input ../data/samples/air_quality_sample.jsonl --output /tmp/clean

TODO(B):
  [ ] dropDuplicates trên (station_id, ts_utc) — API lịch sử có thể trả trùng ở biên chunk
  [ ] loại nồng độ < 0 hoặc > ngưỡng vật lý (đặt hằng số MAX_PLAUSIBLE mỗi chất, ghi rõ nguồn)
  [ ] điểm thiếu <= 3 giờ liên tiếp -> nội suy tuyến tính theo trạm; > 3 giờ -> để null
  [ ] TÍNH CỬA SỔ TRUNG BÌNH: PM2.5/PM10 cần trung bình trượt 24h (yêu cầu của QĐ 1459),
      các chất còn lại dùng giá trị giờ. Đây là việc của Pha 1, không phải Pha 2.
  [ ] coalesce() trước khi ghi để file >= 64 MB
  [ ] in ra bảng thống kê chất lượng dữ liệu (tỉ lệ thiếu mỗi chất) -> cho báo cáo
"""
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    raise NotImplementedError("TODO(B) M1")


if __name__ == "__main__":
    main()
