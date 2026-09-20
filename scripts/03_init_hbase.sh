#!/usr/bin/env bash
# Tạo bảng HBase theo CONTRACTS.md §C4
docker exec -i hbase-master hbase shell <<'HBASE'
# Không dùng COMPRESSION => 'SNAPPY': Snappy cần thư viện native mà image này thường không có,
# RegionServer sẽ không mở được bảng và báo lỗi khó hiểu. Nếu cần nén thì dùng 'GZ' (thuần Java).
create 'air_quality', {NAME => 'd', VERSIONS => 1}
describe 'air_quality'
status
HBASE
