#!/bin/sh
# Chạy Pha 1 bản chịu lỗi (spark/jobs/phase1_clean_resumable_v4.py) trong container spark-master,
# lần lượt qua 3 giai đoạn grid, interpolate, final. Script gọi với --resume nên nếu bị ngắt thì
# chạy lại sẽ tiếp tục từ giai đoạn đang dở. Đường dẫn /tmp/aqi là thư mục chứa mã Spark
# trong container.
set -eu

export USER=root
export LOGNAME=root
export HADOOP_USER_NAME=root
export SPARK_MASTER='local[*]'

PY=/tmp/aqi/spark/jobs/phase1_clean_resumable_v4.py
HDFS=hdfs://namenode:9000/air-quality
INPUT="$HDFS/raw"
OUTPUT="$HDFS/clean"
WORKDIR="$HDFS/_phase1_work_v4"
LOGDIR=/tmp/aqi/phase1_v4_logs
STATUS=/tmp/aqi/phase1_v4_status

mkdir -p "$LOGDIR"
rm -f "$STATUS"

run_stage() {
    stage="$1"
    log="$LOGDIR/${stage}.log"

    echo "RUNNING:$stage" > "$STATUS"
    echo "===== START $stage $(date -Iseconds) =====" | tee "$log"

    if spark-submit \
        --master 'local[*]' \
        --driver-memory 6g \
        --conf spark.default.parallelism=96 \
        --conf spark.sql.shuffle.partitions=96 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.adaptive.skewJoin.enabled=true \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=30s \
        "$PY" \
        --stage "$stage" \
        --input "$INPUT" \
        --output "$OUTPUT" \
        --workdir "$WORKDIR" \
        --partitions 96 \
        --resume \
        >> "$log" 2>&1
    then
        echo "===== OK $stage $(date -Iseconds) =====" | tee -a "$log"
    else
        rc=$?
        echo "FAILED:$stage:rc=$rc" > "$STATUS"
        echo "===== FAILED $stage rc=$rc $(date -Iseconds) =====" | tee -a "$log"
        exit "$rc"
    fi
}

run_stage grid
run_stage interpolate
run_stage final

echo "DONE" > "$STATUS"
echo "===== PHASE1_V4_DONE $(date -Iseconds) ====="
