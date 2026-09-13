#!/usr/bin/env bash
# NGƯỜI A · M3 — tạo bảng HBase theo CONTRACTS.md §C4
docker exec -it hbase-master hbase shell <<'HBASE'
# LƯU Ý: không dùng COMPRESSION => 'SNAPPY'.
# Snappy cần thư viện native; trên Apple Silicon (arm64) thường không có và
# RegionServer sẽ không mở được bảng với lỗi khó hiểu. Cần nén thì dùng 'GZ' (thuần Java).
create 'air_quality', {NAME => 'd', VERSIONS => 1}
describe 'air_quality'
status
HBASE
