"""Kafka producer cho collector. NGƯỜI A · M3."""

import json
import logging
import os

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

RAW_TOPIC = "air-quality-raw"
DLQ_TOPIC = "air-quality-dlq"


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str | None = None):
        bootstrap = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    @staticmethod
    def _json_bytes(data: dict) -> bytes:
        return json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            logger.error("Kafka delivery failed: %s", err)
        else:
            logger.debug(
                "Kafka delivered topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send_raw(self, record: dict) -> None:
        station_id = record.get("station_id")
        if not station_id:
            raise ValueError("record thiếu station_id")

        self._producer.produce(
            RAW_TOPIC,
            key=str(station_id).encode("utf-8"),
            value=self._json_bytes(record),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def send_dlq(
        self,
        raw_data,
        error: Exception | str,
        station_id: str | None = None,
    ) -> None:
        payload = {
            "error": str(error),
            "station_id": station_id,
            "raw_data": raw_data,
        }

        key = station_id.encode("utf-8") if station_id else None

        self._producer.produce(
            DLQ_TOPIC,
            key=key,
            value=self._json_bytes(payload),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining:
            raise RuntimeError(
                f"Kafka flush timeout: còn {remaining} message chưa gửi"
            )
