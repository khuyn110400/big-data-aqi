"""Fixture dùng chung cho test Spark — 1 SparkSession local[*] cho cả session pytest."""
import os
import sys
from pathlib import Path

_SPARK_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _SPARK_ROOT)              # aqi_core
sys.path.insert(0, os.path.join(_SPARK_ROOT, "jobs"))  # phase1_clean, phase2_aqi
# UDF chạy trong tiến trình worker riêng -> cần PYTHONPATH, không chỉ sys.path của driver
os.environ["PYTHONPATH"] = _SPARK_ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.appName("pytest-local")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        # Đặt múi giờ UTC: to_date() và from_unixtime() đổi theo múi giờ của session (mặc định
        # là múi giờ máy chạy, ví dụ +07), nếu không đặt thì dt và aqi_day bị lệch ngày.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
