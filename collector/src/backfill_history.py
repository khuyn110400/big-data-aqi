"""Backfill dữ liệu lịch sử OpenWeather vào HDFS (thư mục raw).

- Chia khoảng thời gian thành các cửa sổ 90 ngày, mỗi cửa sổ một call cho mỗi trạm.
- Chuẩn hoá từng bản ghi theo schema C1.
- Ghi file .jsonl.gz theo layout:
  /air-quality/raw/ingest_mode=history/country=XX/dt=YYYY-MM-DD/part-*.jsonl.gz
- Lưu checkpoint theo cửa sổ; chạy lại với --resume sẽ bỏ qua các cửa sổ đã hoàn tất.
- Tạo thư mục HDFS theo lô và upload song song để giảm thời gian ghi nhiều file nhỏ.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from normalize import normalize
from owm_client import OwmClient

logger = logging.getLogger(__name__)

DEFAULT_CITIES = Path(__file__).resolve().parents[1] / "config" / "cities.json"
DEFAULT_STATE = Path(".state/backfill_state.json")
DEFAULT_UPLOAD_WORKERS = 4
MKDIR_BATCH_SIZE = 200


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Ngày không hợp lệ {value!r}; dùng YYYY-MM-DD"
        ) from exc


def load_stations(path: str | Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stations = data.get("stations")
    if not isinstance(stations, list) or not stations:
        raise ValueError(f"Không có stations hợp lệ trong {path}")

    required = {"station_id", "city", "country", "lat", "lon"}
    seen = set()

    for i, station in enumerate(stations, start=1):
        missing = required - set(station)
        if missing:
            raise ValueError(f"Station #{i} thiếu trường: {sorted(missing)}")

        sid = station["station_id"]
        if sid in seen:
            raise ValueError(f"station_id bị trùng: {sid}")
        seen.add(sid)

    return stations


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def iter_windows(start: date, end: date, days: int = 90):
    cur = start
    while cur <= end:
        window_end = min(cur + timedelta(days=days - 1), end)
        yield cur, window_end
        cur = window_end + timedelta(days=1)


def epoch_bounds(start: date, end: date) -> tuple[int, int]:
    start_dt = datetime.combine(start, dt_time.min, tzinfo=timezone.utc)
    end_exclusive = datetime.combine(
        end + timedelta(days=1), dt_time.min, tzinfo=timezone.utc
    )
    return int(start_dt.timestamp()), int(end_exclusive.timestamp()) - 1


def normalize_hdfs_root(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme and parsed.scheme != "hdfs":
        raise ValueError("Chỉ hỗ trợ đường dẫn HDFS hoặc absolute path")

    root = parsed.path if parsed.scheme == "hdfs" else value
    root = "/" + root.strip("/")
    return root


def window_id(start: date, end: date) -> str:
    return f"{start.isoformat()}__{end.isoformat()}"


def load_state(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def run_checked(cmd: list[str], *, stdin=None) -> None:
    subprocess.run(
        cmd,
        stdin=stdin,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def hdfs_mkdir_many(container: str, paths: list[str]) -> None:
    unique = sorted(set(paths))
    for i in range(0, len(unique), MKDIR_BATCH_SIZE):
        batch = unique[i : i + MKDIR_BATCH_SIZE]
        run_checked(
            ["docker", "exec", container, "hdfs", "dfs", "-mkdir", "-p", *batch]
        )


def hdfs_put_stdin(container: str, local_path: Path, dest: str) -> None:
    with local_path.open("rb") as src:
        run_checked(
            [
                "docker",
                "exec",
                "-i",
                container,
                "hdfs",
                "dfs",
                "-put",
                "-f",
                "-",
                dest,
            ],
            stdin=src,
        )


def stage_station_records(
    stage_dir: Path,
    rows: list[dict],
    station: dict,
) -> int:
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)

    for item in rows:
        record = normalize(item, station, "history")
        dt_day = record["ts_utc"][:10]
        country = record["country"]
        grouped[(country, dt_day)].append(
            json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        )

    for (country, dt_day), lines in grouped.items():
        path = stage_dir / f"{country}__{dt_day}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines))
            f.write("\n")

    return sum(len(lines) for lines in grouped.values())


def prepare_upload_jobs(
    stage_dir: Path,
    hdfs_root: str,
    start: date,
    end: date,
) -> tuple[list[tuple[Path, str]], int]:
    jobs: list[tuple[Path, str]] = []
    compressed_bytes = 0
    suffix = f"{start:%Y%m%d}-{end:%Y%m%d}"

    for src in sorted(stage_dir.glob("*.jsonl")):
        country, dt_day = src.stem.split("__", 1)

        gz_path = src.with_suffix(".jsonl.gz")
        with src.open("rb") as fin, gzip.open(
            gz_path, "wb", compresslevel=6
        ) as fout:
            shutil.copyfileobj(fin, fout)

        dest = (
            f"{hdfs_root}/ingest_mode=history/"
            f"country={country}/dt={dt_day}/"
            f"part-backfill-{suffix}.jsonl.gz"
        )

        jobs.append((gz_path, dest))
        compressed_bytes += gz_path.stat().st_size

    return jobs, compressed_bytes


def upload_window(
    stage_dir: Path,
    hdfs_root: str,
    container: str,
    start: date,
    end: date,
    workers: int,
) -> tuple[int, int]:
    jobs, compressed_bytes = prepare_upload_jobs(
        stage_dir, hdfs_root, start, end
    )

    if not jobs:
        return 0, 0

    parents = [dest.rsplit("/", 1)[0] for _, dest in jobs]

    mkdir_started = time.monotonic()
    hdfs_mkdir_many(container, parents)
    logger.info(
        "HDFS mkdir: %d directories in %.1fs",
        len(set(parents)),
        time.monotonic() - mkdir_started,
    )

    upload_started = time.monotonic()
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(hdfs_put_stdin, container, local_path, dest): dest
            for local_path, dest in jobs
        }

        for future in as_completed(futures):
            dest = futures[future]
            try:
                future.result()
            except Exception:
                logger.error("HDFS upload failed: %s", dest)
                raise

            done += 1
            if done % 100 == 0 or done == len(jobs):
                logger.info(
                    "HDFS upload progress: %d/%d files",
                    done,
                    len(jobs),
                )

    logger.info(
        "HDFS upload completed: %d files in %.1fs (workers=%d)",
        len(jobs),
        time.monotonic() - upload_started,
        workers,
    )

    return len(jobs), compressed_bytes


def make_state(
    cities_path: Path,
    cities_hash: str,
    start: date,
    end: date,
    station_count: int,
) -> dict:
    return {
        "version": 1,
        "cities_path": str(cities_path),
        "cities_sha256": cities_hash,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "station_count": station_count,
        "completed_windows": [],
        "updated_at_utc": None,
    }


def validate_resume_state(
    state: dict,
    cities_hash: str,
    start: date,
    end: date,
    station_count: int,
) -> None:
    expected = {
        "cities_sha256": cities_hash,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "station_count": station_count,
    }

    mismatches = {
        k: (state.get(k), v)
        for k, v in expected.items()
        if state.get(k) != v
    }

    if mismatches:
        details = "; ".join(
            f"{k}: state={old!r}, current={new!r}"
            for k, (old, new) in mismatches.items()
        )
        raise RuntimeError(
            "Checkpoint không khớp cấu hình hiện tại. "
            f"Không resume để tránh bỏ sót dữ liệu. {details}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cities", default=str(DEFAULT_CITIES))
    parser.add_argument("--from", dest="from_date", type=parse_date, required=True)
    parser.add_argument("--to", dest="to_date", type=parse_date, required=True)
    parser.add_argument("--out", default="hdfs:///air-quality/raw/")
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--station-limit",
        type=int,
        default=None,
        help="Chỉ dùng cho smoke test; lấy N station đầu tiên.",
    )
    parser.add_argument(
        "--namenode-container",
        default="namenode",
        help="Tên Docker container có lệnh hdfs.",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        default=DEFAULT_UPLOAD_WORKERS,
        help="Số HDFS upload chạy song song (mặc định 4).",
    )
    args = parser.parse_args()

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.from_date > args.to_date:
        parser.error("--from phải <= --to")

    if args.upload_workers <= 0:
        parser.error("--upload-workers phải > 0")

    cities_path = Path(args.cities)
    stations = load_stations(cities_path)

    if args.station_limit is not None:
        if args.station_limit <= 0:
            parser.error("--station-limit phải > 0")
        stations = stations[: args.station_limit]

    hdfs_root = normalize_hdfs_root(args.out)
    state_path = Path(args.state)
    cities_hash = sha256_file(cities_path)
    windows = list(iter_windows(args.from_date, args.to_date, days=90))

    total_days = (args.to_date - args.from_date).days + 1
    estimated_calls = len(stations) * len(windows)
    estimated_records = len(stations) * total_days * 24

    logger.info(
        "Plan: stations=%d, days=%d, windows=%d, API calls≈%d, records_upper_bound≈%d",
        len(stations),
        total_days,
        len(windows),
        estimated_calls,
        estimated_records,
    )
    logger.info("HDFS root: %s", hdfs_root)
    logger.info("HDFS upload workers: %d", args.upload_workers)

    if args.dry_run:
        for i, (ws, we) in enumerate(windows, start=1):
            logger.info(
                "Window %d/%d: %s -> %s (%d ngày)",
                i,
                len(windows),
                ws,
                we,
                (we - ws).days + 1,
            )
        return

    existing_state = load_state(state_path)

    if args.resume and existing_state is not None:
        validate_resume_state(
            existing_state,
            cities_hash,
            args.from_date,
            args.to_date,
            len(stations),
        )
        state = existing_state
    else:
        state = make_state(
            cities_path,
            cities_hash,
            args.from_date,
            args.to_date,
            len(stations),
        )
        save_state(state_path, state)

    completed = set(state.get("completed_windows", []))
    client = OwmClient()

    total_records = 0
    total_files = 0
    total_compressed_bytes = 0
    total_source_gaps = 0
    started = time.monotonic()

    for window_no, (ws, we) in enumerate(windows, start=1):
        wid = window_id(ws, we)

        if args.resume and wid in completed:
            logger.info(
                "SKIP window %d/%d %s -> %s (checkpoint)",
                window_no,
                len(windows),
                ws,
                we,
            )
            continue

        start_epoch, end_epoch = epoch_bounds(ws, we)
        expected_per_station = ((we - ws).days + 1) * 24

        logger.info(
            "START window %d/%d: %s -> %s",
            window_no,
            len(windows),
            ws,
            we,
        )

        with tempfile.TemporaryDirectory(prefix="aqi-backfill-") as tmp:
            stage_dir = Path(tmp)
            window_records = 0
            window_source_gaps = 0

            for station_no, station in enumerate(stations, start=1):
                rows = client.history(
                    float(station["lat"]),
                    float(station["lon"]),
                    start_epoch,
                    end_epoch,
                )

                count = stage_station_records(stage_dir, rows, station)
                window_records += count

                gap = max(0, expected_per_station - count)
                window_source_gaps += gap

                if gap:
                    logger.warning(
                        "[%d/%d] %-15s rows=%d expected=%d source_gap=%d calls=%d",
                        station_no,
                        len(stations),
                        station["station_id"],
                        count,
                        expected_per_station,
                        gap,
                        client.call_count,
                    )
                else:
                    logger.info(
                        "[%d/%d] %-15s rows=%d calls=%d",
                        station_no,
                        len(stations),
                        station["station_id"],
                        count,
                        client.call_count,
                    )

            files, compressed_bytes = upload_window(
                stage_dir,
                hdfs_root,
                args.namenode_container,
                ws,
                we,
                args.upload_workers,
            )

        total_records += window_records
        total_files += files
        total_compressed_bytes += compressed_bytes
        total_source_gaps += window_source_gaps

        completed.add(wid)
        state["completed_windows"] = sorted(completed)
        state["updated_at_utc"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        save_state(state_path, state)

        logger.info(
            "DONE window %s -> %s: records=%d source_gaps=%d files=%d compressed=%.2f MiB",
            ws,
            we,
            window_records,
            window_source_gaps,
            files,
            compressed_bytes / 1024 / 1024,
        )

    elapsed = time.monotonic() - started

    logger.info("========== BACKFILL REPORT ==========")
    logger.info("records_written_this_run=%d", total_records)
    logger.info("source_gaps_this_run=%d", total_source_gaps)
    logger.info("hdfs_files_written_this_run=%d", total_files)
    logger.info("compressed_written_this_run=%.2f MiB", total_compressed_bytes / 1024 / 1024)
    logger.info("api_calls_this_run=%d", client.call_count)
    logger.info("elapsed_seconds=%.1f", elapsed)
    logger.info("state=%s", state_path)


if __name__ == "__main__":
    main()
