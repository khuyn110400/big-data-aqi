"""
Backfill dữ liệu lịch sử -> raw layout theo C2 (HDFS khi có cụm, thư mục thường khi chạy
trên Google Drive/Colab). NGƯỜI A · M2

ĐÂY LÀ THỨ TẠO RA "BIG DATA" CHO ĐỒ ÁN.
Mục tiêu: ~200 điểm x 3-5 năm x 8760 giờ ~= 5-9 triệu bản ghi.

Chiến lược: chunk theo QUÝ (~90 ngày/cửa sổ) — đã kiểm chứng bằng
`collector/tests/verify_api_schema.py` là 1 call lấy đủ 90 ngày không bị cắt.
200 trạm x ~21 quý (2021-01-01 .. 2026-09-01) ~= 4.200 call -> ~1,2 giờ ở 1 call/giây,
thay vì ~12.600 call nếu chunk theo tháng.

Checkpoint ghi ngay sau mỗi cửa sổ hoàn tất -> Colab bị ngắt (free tier ~90 phút idle)
chạy lại với --resume không mất công đã làm.

Chạy trên Mac (test, không tốn nhiều call):
  python collector/src/backfill_history.py --cities collector/config/cities.json \
      --from 2026-08-01 --to 2026-09-01 --out /tmp/raw --dry-run
  python collector/src/backfill_history.py --cities collector/config/cities.json \
      --from 2026-08-01 --to 2026-09-01 --out /tmp/raw --limit 2

Chạy trên Colab (thật, vào Google Drive):
  python collector/src/backfill_history.py --cities collector/config/cities.json \
      --from 2021-01-01 --to 2026-09-01 \
      --out /content/drive/MyDrive/big-data-aqi/air-quality/raw --resume
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from normalize import NormalizeError, normalize  # noqa: E402
from owm_client import OwmAuthError, OwmClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

QUARTER_SECONDS = 90 * 24 * 3600
CALLS_PER_SECOND = 1.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cities", required=True, help="Đường dẫn collector/config/cities.json")
    p.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (UTC), OWM history có từ 2020-11-27")
    p.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--out", required=True, help="Thư mục gốc để ghi raw (local, hoặc thư mục Drive đã mount)")
    p.add_argument("--resume", action="store_true", help="Đọc checkpoint, bỏ qua (station, window) đã xong")
    p.add_argument("--dry-run", action="store_true", help="Chỉ in số call sẽ dùng, không gọi API")
    p.add_argument("--limit", type=int, default=None, help="Chỉ chạy N trạm đầu (để test ở Mac)")
    return p.parse_args(argv)


def load_stations(cities_path: str, limit: int | None) -> list[dict]:
    with open(cities_path, encoding="utf-8") as f:
        data = json.load(f)
    stations = data["stations"]
    if limit is not None:
        stations = stations[:limit]
    return stations


def build_quarter_windows(date_from: str, date_to: str) -> list[tuple[int, int]]:
    """Cửa sổ ~90 ngày liên tiếp, giờ tròn (UTC), phủ kín [date_from, date_to)."""
    start = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end <= start:
        raise ValueError(f"--to ({date_to}) phải sau --from ({date_from})")

    windows: list[tuple[int, int]] = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(seconds=QUARTER_SECONDS), end)
        windows.append((int(cur.timestamp()), int(nxt.timestamp())))
        cur = nxt
    return windows


def window_label(window: tuple[int, int]) -> str:
    """Nhãn duy nhất cho mỗi cửa sổ 90 ngày -> dùng làm khoá checkpoint.

    Không dùng "YYYYQn" (quý dương lịch): 2 cửa sổ liên tiếp ~90 ngày có thể
    rơi vào cùng 1 quý dương lịch -> trùng nhãn -> checkpoint đè nhau, bỏ sót
    cửa sổ thật khi --resume. Ngày bắt đầu (duy nhất theo xây dựng cửa sổ) an toàn hơn.
    """
    dt = datetime.fromtimestamp(window[0], tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def load_checkpoint(out_dir: Path) -> set[str]:
    ckpt_path = out_dir / "_checkpoint.json"
    if not ckpt_path.exists():
        return set()
    with open(ckpt_path, encoding="utf-8") as f:
        return set(json.load(f))


def save_checkpoint(out_dir: Path, done: set[str]) -> None:
    ckpt_path = out_dir / "_checkpoint.json"
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = ckpt_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(sorted(done), f)
    tmp_path.replace(ckpt_path)


def write_partition(out_dir: Path, country: str, dt_day: str, records: list[dict]) -> Path:
    """Ghi theo layout C2: <out>/ingest_mode=history/country=<C>/dt=<YYYY-MM-DD>/part-0001.jsonl.gz"""
    part_dir = out_dir / "ingest_mode=history" / f"country={country}" / f"dt={dt_day}"
    part_dir.mkdir(parents=True, exist_ok=True)
    existing = list(part_dir.glob("part-*.jsonl.gz"))
    part_path = part_dir / f"part-{len(existing) + 1:04d}.jsonl.gz"
    with gzip.open(part_path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return part_path


def write_dlq(out_dir: Path, station_id: str, window_key: str, raw_item: dict, error: str) -> None:
    dlq_path = out_dir / "_backfill.dlq.jsonl"
    with open(dlq_path, "a", encoding="utf-8") as f:
        entry = {"station_id": station_id, "window": window_key, "error": error, "raw_item": raw_item}
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run(args: argparse.Namespace) -> None:
    stations = load_stations(args.cities, args.limit)
    windows = build_quarter_windows(args.date_from, args.date_to)
    total_calls = len(stations) * len(windows)

    logger.info(
        "%d trạm x %d cửa sổ (~90 ngày) = %d call dự kiến (~%.1f phút ở %.0f call/giây)",
        len(stations), len(windows), total_calls, total_calls / CALLS_PER_SECOND / 60, CALLS_PER_SECOND,
    )

    if args.dry_run:
        print(f"[DRY RUN] {len(stations)} trạm x {len(windows)} cửa sổ = {total_calls} call")
        print(f"[DRY RUN] ước lượng thời gian: ~{total_calls / CALLS_PER_SECOND / 60:.1f} phút (1 call/giây)")
        for w in windows:
            print(f"  - {window_label(w)}: {datetime.fromtimestamp(w[0], tz=timezone.utc):%Y-%m-%d} "
                  f"-> {datetime.fromtimestamp(w[1], tz=timezone.utc):%Y-%m-%d}")
        return

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    done = load_checkpoint(out_dir) if args.resume else set()
    if args.resume:
        logger.info("Đã đọc checkpoint: %d cửa sổ hoàn tất trước đó", len(done))

    client = OwmClient(rate_limit_per_sec=CALLS_PER_SECOND)
    start_ts = time.monotonic()
    total_records = 0
    total_dlq = 0

    for station in stations:
        station_id = station["station_id"]
        for window in windows:
            wkey = f"{station_id}|{window_label(window)}"
            if wkey in done:
                continue

            try:
                items = client.history(station["lat"], station["lon"], window[0], window[1])
            except OwmAuthError as exc:
                logger.error("%s -- DỪNG (đừng debug code, chờ key active rồi --resume)", exc)
                raise

            buffer: dict[tuple[str, str], list[dict]] = defaultdict(list)
            for item in items:
                try:
                    rec = normalize(item, station, ingest_mode="history")
                except NormalizeError as exc:
                    write_dlq(out_dir, station_id, wkey, item, str(exc))
                    total_dlq += 1
                    continue
                dt_day = rec["ts_utc"][:10]
                buffer[(rec["country"], dt_day)].append(rec)

            for (country, dt_day), records in buffer.items():
                write_partition(out_dir, country, dt_day, records)
                total_records += len(records)

            done.add(wkey)
            save_checkpoint(out_dir, done)

        logger.info(
            "%s xong (%d call dùng, %d bản ghi, %d dlq)",
            station_id, client.call_count, total_records, total_dlq,
        )

    elapsed = time.monotonic() - start_ts
    raw_dir = out_dir / "ingest_mode=history"
    total_bytes = sum(f.stat().st_size for f in raw_dir.rglob("*.jsonl.gz")) if raw_dir.exists() else 0

    print("=" * 60)
    print("BACKFILL XONG")
    print(f"  tổng bản ghi     : {total_records}")
    print(f"  tổng bản ghi lỗi : {total_dlq} (xem {out_dir}/_backfill.dlq.jsonl)")
    print(f"  tổng call đã dùng: {client.call_count}")
    print(f"  dung lượng raw   : {total_bytes / 1024 / 1024:.1f} MB")
    print(f"  thời gian chạy   : {elapsed / 60:.1f} phút")
    print("=" * 60)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(args)


if __name__ == "__main__":
    main()
