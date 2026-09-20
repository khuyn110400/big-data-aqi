"""
Gọi OpenWeather Air Pollution API theo chu kỳ và đẩy bản ghi vào Kafka topic air-quality-raw.
Bản ghi lỗi schema được đẩy sang topic air-quality-dlq.
"""

import argparse
import json
import logging
import os
import signal
import time
from pathlib import Path

from dotenv import load_dotenv

from kafka_producer import KafkaPublisher
from normalize import NormalizeError, normalize
from owm_client import OwmAuthError, OwmClient

logger = logging.getLogger(__name__)

DEFAULT_CITIES = Path(__file__).resolve().parents[1] / "config" / "cities.json"

_stop_requested = False


def _request_stop(signum, frame):
    global _stop_requested
    _stop_requested = True
    logger.info("Nhận tín hiệu dừng, sẽ flush Kafka trước khi thoát...")


def load_stations(path: str | Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stations = data.get("stations")
    if not isinstance(stations, list) or not stations:
        raise ValueError(f"Không có stations hợp lệ trong {path}")

    return stations


def run_once(
    client: OwmClient,
    publisher: KafkaPublisher,
    stations: list[dict],
) -> tuple[int, int]:
    success = 0
    failed = 0

    for station in stations:
        if _stop_requested:
            break

        response = None
        station_id = station.get("station_id", "UNKNOWN")

        try:
            response = client.current(
                float(station["lat"]),
                float(station["lon"]),
            )

            items = response.get("list", [])
            if not items:
                raise NormalizeError("OpenWeather response không có list[0]")

            record = normalize(items[0], station, "live")
            publisher.send_raw(record)

            success += 1
            logger.info(
                "OK %-15s %s %s",
                station_id,
                record["ts_utc"],
                record["components"].get("pm2_5"),
            )

        except OwmAuthError:
            # 401 là lỗi toàn cục của key, không nên gọi tiếp các trạm.
            raise

        except NormalizeError as exc:
            failed += 1
            logger.error("Normalize lỗi %s: %s", station_id, exc)

            publisher.send_dlq(
                raw_data=response,
                error=exc,
                station_id=station_id,
            )

        except Exception:
            failed += 1
            logger.exception("Collector lỗi tại %s", station_id)

    publisher.flush()

    logger.info(
        "Hoàn tất vòng collector: success=%d failed=%d total=%d",
        success,
        failed,
        len(stations),
    )

    return success, failed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cities",
        default=str(DEFAULT_CITIES),
        help="Đường dẫn cities.json",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Chạy đúng một vòng rồi thoát",
    )
    args = parser.parse_args()

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    interval_min = int(os.getenv("COLLECT_INTERVAL_MIN", "60"))
    stations = load_stations(args.cities)

    logger.info("Đã nạp %d stations", len(stations))
    logger.info("Chu kỳ collector: %d phút", interval_min)

    client = OwmClient()
    publisher = KafkaPublisher()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    try:
        while not _stop_requested:
            run_once(client, publisher, stations)

            if args.once or _stop_requested:
                break

            logger.info("Ngủ %d phút đến vòng tiếp theo", interval_min)

            # Sleep theo từng giây để Ctrl+C có thể dừng nhanh.
            for _ in range(interval_min * 60):
                if _stop_requested:
                    break
                time.sleep(1)

    except OwmAuthError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    finally:
        publisher.flush()
        logger.info("Collector đã dừng an toàn.")


if __name__ == "__main__":
    main()
