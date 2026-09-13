"""
aqi_core.iaqi — LÕI TÍNH AQI DÙNG CHUNG cho cả làn batch (Pha 2) và làn streaming.

QUY TẮC VÀNG: không copy hàm ở đây sang chỗ khác. Streaming job và batch job
phải import cùng module này, nếu không hai làn sẽ cho ra số khác nhau và
không ai biết số nào đúng.

Công thức nội suy tuyến tính (chuẩn dùng chung của VN_AQI và US EPA):

        I_high - I_low
 IAQI = --------------- * (C - BP_low) + I_low
        BP_high - BP_low

 AQI  = max(IAQI của tất cả các chất có dữ liệu)     <- đây là "Reducer" của Pha 2

Người phụ trách: NGƯỜI B
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).parent

POLLUTANT_COLS = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]


@lru_cache(maxsize=4)
def load_standard(standard: str = "VN_1459") -> dict:
    """Nạp bảng breakpoint. standard: 'VN_1459' | 'US_EPA'."""
    fname = {"VN_1459": "breakpoints_vn.json", "US_EPA": "breakpoints_epa.json"}[standard]
    with open(_HERE / fname, encoding="utf-8") as f:
        return json.load(f)


def iaqi(concentration: Optional[float], pollutant: str, standard: str = "VN_1459") -> Optional[float]:
    """
    Tính sub-index cho MỘT chất. Trả None nếu thiếu dữ liệu.

    TODO(B):
      - Nồng độ vượt mốc cao nhất -> kẹp về 500 (hoặc ngoại suy? chốt rồi ghi vào docstring)
      - Nồng độ âm -> None (nhưng Pha 1 lẽ ra đã lọc rồi)
      - Làm tròn: chuẩn VN yêu cầu làm tròn tới số nguyên
    """
    if concentration is None or concentration < 0:
        return None

    std = load_standard(standard)
    bp = std["pollutants"][pollutant]["bp"]
    levels = std["aqi_breakpoints"]

    for i in range(len(bp) - 1):
        if bp[i] <= concentration <= bp[i + 1]:
            bp_low, bp_high = bp[i], bp[i + 1]
            i_low, i_high = levels[i]["i_low"], levels[i]["i_high"]
            return (i_high - i_low) / (bp_high - bp_low) * (concentration - bp_low) + i_low

    return float(levels[-1]["i_high"])  # vượt ngưỡng cao nhất -> kẹp 500


def aqi(components: dict, standard: str = "VN_1459") -> dict:
    """
    Reducer của Pha 2: gom IAQI các chất -> AQI tổng + mức + chất trội.

    components: {"pm2_5": 18.4, "pm10": 24.1, "o3": 68.7, ...}
    return: {"aqi", "aqi_level", "aqi_label", "dominant_pollutant", "iaqi": {...}}
    """
    subs = {p: iaqi(components.get(p), p, standard) for p in POLLUTANT_COLS}
    valid = {p: v for p, v in subs.items() if v is not None}

    if not valid:
        return {"aqi": None, "aqi_level": None, "aqi_label": None,
                "dominant_pollutant": None, "iaqi": subs}

    dom = max(valid, key=valid.get)
    value = valid[dom]
    lvl = level_of(value, standard)

    return {"aqi": round(value), "aqi_level": lvl["level"], "aqi_label": lvl["label"],
            "dominant_pollutant": dom, "iaqi": subs}


def level_of(aqi_value: float, standard: str = "VN_1459") -> dict:
    """AQI -> {'level': 1..6, 'label': 'Tốt'...}"""
    for lv in load_standard(standard)["aqi_breakpoints"]:
        if aqi_value <= lv["i_high"]:
            return {"level": lv["level"], "label": lv["label"]}
    return {"level": 6, "label": "Nguy hại"}


# ---------------------------------------------------------------------------
# TODO(B): bản Spark UDF để Pha 2 dùng trên DataFrame
# ---------------------------------------------------------------------------
def register_udfs(spark):
    """
    Đăng ký `aqi_udf` trả về StructType(aqi, aqi_level, aqi_label, dominant_pollutant).

    Gợi ý: dùng pandas_udf để nhanh hơn python UDF thường ~10x trên khối triệu dòng.
    """
    raise NotImplementedError("TODO(B) M1")
