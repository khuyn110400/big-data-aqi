#!/usr/bin/env bash
# NGƯỜI A · M1 — tạo cây thư mục HDFS theo CONTRACTS.md §C2
set -e
for d in /air-quality/raw /air-quality/clean /air-quality/aqi /air-quality/agg /air-quality/checkpoints; do
  docker exec namenode hdfs dfs -mkdir -p "$d"
done
docker exec namenode hdfs dfs -ls -R /air-quality
docker exec namenode hdfs dfsadmin -report | head -20
