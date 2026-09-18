# Task A — Data Platform

## Scope

Task A owns the data-platform layer for the AQI Big Data project:

- OpenWeather Air Pollution API integration
- Historical backfill collector
- Live collector
- Kafka topics and producer
- HDFS raw-zone layout
- HBase schema and Thrift access
- FastAPI serving bridge
- Grafana dashboard
- Docker Compose integration

## Current architecture

OpenWeather -> Collector -> Kafka / HDFS raw

HBase -> HBase Thrift -> FastAPI -> Grafana

## Key endpoints

- FastAPI health: `http://localhost:8000/health`
- FastAPI Swagger: `http://localhost:8000/docs`
- Grafana: `http://localhost:3000`
- HDFS NameNode UI: `http://localhost:9870`
- HBase Master UI: `http://localhost:16010`

## Kafka

Expected topics:

- `air-quality-raw` — 3 partitions, replication factor 1
- `air-quality-dlq` — DLQ

## HDFS raw layout

```text
/air-quality/raw/
  ingest_mode=history/
    country=<CC>/
      dt=<YYYY-MM-DD>/
        part-backfill-*.jsonl.gz
```

## HBase

Table: `air_quality`

Column family: `d`

Row key:

```text
{station_id}#{reverse_ts}
```

where:

```text
reverse_ts = 9999999999 - ts_epoch
```

## FastAPI contract

- `GET /health`
- `GET /stations`
- `GET /aqi/latest`
- `GET /aqi/timeseries`
- `GET /aqi/ranking`

The API container connects to HBase through `hbase-thrift:9090`.

## Grafana

Dashboard UID:

```text
aqi-big-data-overview
```

Provisioned dashboard:

```text
grafana/dashboards/aqi-overview.json
```

Grafana talks to the containerized FastAPI service at:

```text
http://serving-api:8000
```

Panels that depend on processed HBase data may show `No data` until Task B writes processed AQI rows to HBase.

## Historical backfill

Target interval:

```text
2021-09-01 -> 2026-09-01
```

The collector uses 90-day API windows and checkpoint/resume state.

An audit confirmed that observed missing hourly records are source gaps from OpenWeather: splitting a 90-day request into two 45-day requests returned the same timestamps, so the collector keeps the 90-day window size.

Runtime checkpoint files under `.state/` are intentionally not committed.

## Validation checklist

```bash
docker compose --env-file .env -f docker/docker-compose.yml ps
```

```bash
curl -s http://localhost:8000/health
```

```bash
curl -s http://localhost:8000/stations
```

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092 --list
```

```bash
python3 -c "import socket; s=socket.create_connection(('127.0.0.1',9090),3); print('OK'); s.close()"
```

## Task ownership boundary

Task A provides the platform and serving contracts. Spark cleaning, AQI computation, aggregation, streaming writes to HBase/HDFS, clustering, forecasting, and scalability processing belong to Task B.
