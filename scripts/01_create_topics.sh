#!/usr/bin/env bash
# Tạo các Kafka topic theo CONTRACTS.md §C6
set -e
KAFKA_BIN=/opt/kafka/bin
docker exec kafka "$KAFKA_BIN/kafka-topics.sh" --create --if-not-exists \
  --topic air-quality-raw --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
docker exec kafka "$KAFKA_BIN/kafka-topics.sh" --create --if-not-exists \
  --topic air-quality-dlq --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
docker exec kafka "$KAFKA_BIN/kafka-topics.sh" --list --bootstrap-server localhost:9092
