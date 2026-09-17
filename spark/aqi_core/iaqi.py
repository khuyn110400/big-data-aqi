"""
aqi_core.iaqi — LÕI TÍNH AQI DÙNG CHUNG cho cả làn batch (Pha 2) và làn streaming.

QUY TẮC VÀNG: không copy hàm ở đây sang chỗ khác. Streaming job và batch job
phải import cùng module này, nếu không hai làn sẽ cho ra số khác nhau và
không ai biết số nào đúng.

Công thức nội suy tuyến tính (Công thức 1/2, QĐ 1459 mục 2.2.1b/2.2.2b):

        I_high - I_low
 IAQI = --------------- * (C - BP_low) + I_low
        BP_high - BP_low

 AQI  = max(IAQI của tất cả các chất có dữ liệu)     <- đây là "Reducer" của Pha 2

`iaqi_hour()`/`iaqi_day()` chỉ hỗ trợ standard="VN_1459": VN_AQI phân biệt
AQI giờ (Nowcast cho PM, TB1h cho khí, O3 dùng breakpoint 1h) và AQI ngày
(TB24h cho PM, max TB1h ngày cho khí, O3 dùng max(1h ngày, 8h ngày) trừ khi
TB8h ngày > 400 thì bỏ nhánh 8h) — 2 khái niệm này không tồn tại ở US_EPA.

Người phụ trách: NGƯỜI B
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

_HERE = Path(__file__).parent

POLLUTANT_COLS = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]
_HOUR_DAY_COLS = ["pm2_5", "pm10", "so2", "no2", "co"]  # O3 xử lý riêng (bp o3_1h/o3_8h)


@lru_cache(maxsize=4)
def load_standard(standard: str = "VN_1459") -> dict:
    """Nạp bảng breakpoint. standard: 'VN_1459' | 'US_EPA'."""
    fname = {"VN_1459": "breakpoints_vn.json", "US_EPA": "breakpoints_epa.json"}[standard]
    with open(_HERE / fname, encoding="utf-8") as f:
        return json.load(f)


def iaqi(concentration: Optional[float], pollutant: str, standard: str = "VN_1459") -> Optional[float]:
    """
    Tính sub-index cho MỘT chất. Trả None nếu thiếu dữ liệu.

    Nồng độ âm -> None (Pha 1 lẽ ra đã lọc, đây là lớp bảo vệ thứ 2).
    Nồng độ vượt mốc cao nhất -> kẹp về giá trị AQI lớn nhất (500).
    """
    if concentration is None or concentration < 0:
        return None

    std = load_standard(standard)
    bp = std["pollutants"][pollutant]["bp"]
    bands = std["aqi_breakpoints"]

    for i in range(len(bp) - 1):
        if bp[i] <= concentration <= bp[i + 1]:
            bp_low, bp_high = bp[i], bp[i + 1]
            i_low, i_high = bands[i]["i_low"], bands[i]["i_high"]
            return (i_high - i_low) / (bp_high - bp_low) * (concentration - bp_low) + i_low

    return float(bands[-1]["i_high"])  # vượt ngưỡng cao nhất -> kẹp 500


def level_of(aqi_value: float, standard: str = "VN_1459") -> dict:
    """AQI -> {'level': 1..6, 'label': 'Tốt'...} — tra theo bảng hiển thị (Bảng 1)."""
    for lv in load_standard(standard)["level_labels"]:
        if aqi_value <= lv["i_high"]:
            return {"level": lv["level"], "label": lv["label"]}
    return {"level": 6, "label": "Nguy hại"}


def nowcast(hourly_values: list[Optional[float]]) -> Optional[float]:
    """
    Giá trị Nowcast cho PM2.5/PM10 (QĐ 1459 mục 2.2.1a).

    hourly_values[0] = giờ hiện tại (c1) ... hourly_values[-1] = xa nhất (tối đa c12).
    Trả None nếu không đủ 2/3 giá trị đầu (c1,c2,c3) có dữ liệu.
    """
    if sum(v is not None for v in hourly_values[:3]) < 2:
        return None
    present = [(i, v) for i, v in enumerate(hourly_values) if v is not None]
    if not present:
        return None
    vals = [v for _, v in present]
    w_star = min(vals) / max(vals)
    w = w_star if w_star > 0.5 else 0.5
    num = sum((w ** i) * v for i, v in present)
    den = sum((w ** i) for i, _ in present)
    return num / den


def align_hourly_window(
    current_ts: int,
    ts_value_pairs: Iterable[tuple[int, Optional[float]]],
    window_hours: int = 12,
) -> list[Optional[float]]:
    """
    Ghép các cặp (ts_epoch, value) rời rạc vào đúng vị trí giờ so với `current_ts`,
    ra mảng đúng định dạng input của nowcast(). DÙNG CHUNG cho batch (Pha 2, lấy
    (ts_epoch, value) từ Spark window) và streaming (lấy từ HBase lookback) — đây
    là chỗ duy nhất quy đổi timestamp -> vị trí giờ, không viết lại ở 2 nơi.

    hourly[0] = current_ts, hourly[i] = giá trị cách current_ts đúng i giờ.
    Cặp (ts_epoch, value) phải TỰ bao gồm current_ts nếu muốn hourly[0] có giá trị
    (offset=0) — hàm này không tự thêm bản ghi hiện tại.
    Cặp nào rơi ngoài [0, window_hours) hoặc value=None bị bỏ qua.
    """
    hourly: list[Optional[float]] = [None] * window_hours
    for ts_epoch, value in ts_value_pairs:
        if value is None:
            continue
        offset = int((current_ts - ts_epoch) // 3600)
        if 0 <= offset < window_hours:
            hourly[offset] = value
    return hourly


def _reduce(subs: dict, standard: str) -> dict:
    """Reducer dùng chung cho iaqi_hour/iaqi_day: AQI = max(IAQI có dữ liệu)."""
    valid = {p: v for p, v in subs.items() if v is not None}
    if not valid:
        return {"aqi": None, "aqi_level": None, "aqi_label": None,
                "dominant_pollutant": None, "iaqi": subs}

    dom = max(valid, key=valid.get)
    value = valid[dom]
    lvl = level_of(value, standard)

    return {"aqi": round(value), "aqi_level": lvl["level"], "aqi_label": lvl["label"],
            "dominant_pollutant": dom, "iaqi": subs}


def iaqi_hour(components: dict, standard: str = "VN_1459") -> dict:
    """
    AQI giờ (Công thức 1/2, QĐ 1459 mục 2.2.1). CHỈ hỗ trợ VN_1459.

    components: pm2_5/pm10 ĐÃ LÀ Nowcast (gọi nowcast() trước — việc tính cửa sổ
    12 giờ theo từng trạm thuộc về Pha 1/streaming, không phải hàm này);
    so2/no2/co/o3 là giá trị TB1h hiện tại.
    """
    if standard != "VN_1459":
        raise NotImplementedError("iaqi_hour() chỉ hỗ trợ VN_1459 (Nowcast là khái niệm riêng của VN_AQI)")

    subs = {p: iaqi(components.get(p), p, standard) for p in _HOUR_DAY_COLS}
    subs["o3"] = iaqi(components.get("o3"), "o3_1h", standard)
    return _reduce(subs, standard)


def _iaqi_o3_day(o3_1h_max: Optional[float], o3_8h_max: Optional[float], standard: str) -> Optional[float]:
    """O3 ngày = max(AQI theo TB1h ngày, AQI theo TB8h ngày) — TRỪ KHI TB8h ngày > 400
    thì bỏ nhánh 8h, chỉ dùng O3(1h) (QĐ 1459 mục 2.2.2b, ghi chú)."""
    v_1h = iaqi(o3_1h_max, "o3_1h", standard)
    if o3_8h_max is not None and o3_8h_max <= 400:
        v_8h = iaqi(o3_8h_max, "o3_8h", standard)
        candidates = [v for v in (v_1h, v_8h) if v is not None]
        return max(candidates) if candidates else None
    return v_1h


def iaqi_day(components: dict, standard: str = "VN_1459") -> dict:
    """
    AQI ngày (Công thức 1, QĐ 1459 mục 2.2.2). CHỈ hỗ trợ VN_1459.

    components: pm2_5/pm10 là TB24h; so2/no2/co là max TB1h trong ngày;
    o3_1h_max/o3_8h_max là max TB1h / max TB8h trong ngày.
    """
    if standard != "VN_1459":
        raise NotImplementedError("iaqi_day() chỉ hỗ trợ VN_1459")

    subs = {p: iaqi(components.get(p), p, standard) for p in _HOUR_DAY_COLS}
    subs["o3"] = _iaqi_o3_day(components.get("o3_1h_max"), components.get("o3_8h_max"), standard)
    return _reduce(subs, standard)


# ---------------------------------------------------------------------------
# TODO(B): bản Spark UDF để Pha 2 dùng trên DataFrame
# ---------------------------------------------------------------------------
def register_udfs(spark):
    """
    Đăng ký `aqi_udf` trả về StructType(aqi, aqi_level, aqi_label, dominant_pollutant).

    Gợi ý: dùng pandas_udf để nhanh hơn python UDF thường ~10x trên khối triệu dòng.
    """
    raise NotImplementedError("TODO(B) M1")
