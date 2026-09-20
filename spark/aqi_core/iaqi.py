"""
Lõi tính AQI, dùng chung cho Pha 2 (batch) và streaming.

Hai làn cùng import module này để cho ra cùng một kết quả trên cùng dữ liệu, vì vậy
không copy các hàm ở đây sang nơi khác.

Nội suy tuyến tính (QĐ 1459, mục 2.2.1b và 2.2.2b):

        I_high - I_low
 IAQI = --------------- * (C - BP_low) + I_low
        BP_high - BP_low

AQI = max(IAQI của các chất có dữ liệu).

iaqi_hour() và iaqi_day() chỉ hỗ trợ VN_1459. Theo QĐ 1459, AQI giờ dùng Nowcast cho
bụi PM, trung bình 1 giờ cho các khí và breakpoint 1 giờ cho O3. AQI ngày dùng trung bình
24 giờ cho PM, trị số lớn nhất của trung bình 1 giờ trong ngày cho các khí, còn O3 lấy
max(1 giờ, 8 giờ) và bỏ nhánh 8 giờ khi trung bình 8 giờ lớn nhất vượt 400. US_EPA không
có hai khái niệm này.
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


def max_plausible(standard: str = "VN_1459") -> dict:
    """Nồng độ tối đa hợp lý của từng chất: mốc ứng với I=500 trong bảng breakpoint.
    Pha 1 và streaming cùng dùng để loại giá trị ngoại lai."""
    std = load_standard(standard)["pollutants"]
    return {
        "pm2_5": std["pm2_5"]["bp"][-1],
        "pm10": std["pm10"]["bp"][-1],
        "so2": std["so2"]["bp"][-1],
        "no2": std["no2"]["bp"][-1],
        "co": std["co"]["bp"][-1],
        "o3": std["o3_1h"]["bp"][-1],
    }


def iaqi(concentration: Optional[float], pollutant: str, standard: str = "VN_1459") -> Optional[float]:
    """
    Tính chỉ số con (IAQI) của một chất. Trả None nếu thiếu dữ liệu.

    Nồng độ âm bị coi là không hợp lệ (Pha 1 đã lọc, đây là lớp kiểm tra thứ hai).
    Nồng độ vượt mốc cao nhất được kẹp về giá trị AQI lớn nhất (500).
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
    """Đổi AQI thành {'level': 1..6, 'label': 'Tốt'...} theo bảng mức hiển thị (Bảng 1)."""
    for lv in load_standard(standard)["level_labels"]:
        if aqi_value <= lv["i_high"]:
            return {"level": lv["level"], "label": lv["label"]}
    return {"level": 6, "label": "Nguy hại"}


def nowcast(hourly_values: list[Optional[float]]) -> Optional[float]:
    """
    Giá trị Nowcast cho PM2.5 và PM10 (QĐ 1459, mục 2.2.1a).

    hourly_values[0] là giờ hiện tại (c1), phần tử cuối là giờ xa nhất (tối đa c12).
    Trả None nếu trong 3 giá trị đầu (c1, c2, c3) có ít hơn 2 giá trị.
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
    Xếp các cặp (ts_epoch, value) rời rạc vào đúng vị trí giờ so với current_ts, ra mảng
    đúng định dạng đầu vào của nowcast(). Pha 2 (lấy cặp từ Spark window) và streaming
    (lấy từ HBase) cùng dùng hàm này để đổi timestamp thành vị trí giờ.

    hourly[0] ứng với current_ts, hourly[i] ứng với giờ cách current_ts đúng i giờ. Hàm
    không tự thêm bản ghi hiện tại: muốn hourly[0] có giá trị thì các cặp truyền vào phải
    chứa current_ts. Cặp nằm ngoài [0, window_hours) hoặc có value None bị bỏ qua.
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
    """Gộp các chỉ số con thành AQI = max(IAQI có dữ liệu). iaqi_hour và iaqi_day dùng chung."""
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
    AQI giờ (QĐ 1459, mục 2.2.1). Chỉ hỗ trợ VN_1459.

    pm2_5 và pm10 trong components phải là giá trị Nowcast (gọi nowcast() trước; việc dựng
    cửa sổ 12 giờ của từng trạm do Pha 2 hoặc streaming làm). so2, no2, co, o3 là trung
    bình 1 giờ hiện tại.
    """
    if standard != "VN_1459":
        raise NotImplementedError("iaqi_hour() chỉ hỗ trợ VN_1459 (Nowcast là khái niệm riêng của VN_AQI)")

    subs = {p: iaqi(components.get(p), p, standard) for p in _HOUR_DAY_COLS}
    subs["o3"] = iaqi(components.get("o3"), "o3_1h", standard)
    return _reduce(subs, standard)


def _iaqi_o3_day(o3_1h_max: Optional[float], o3_8h_max: Optional[float], standard: str) -> Optional[float]:
    """O3 ngày = max(AQI theo trung bình 1 giờ, AQI theo trung bình 8 giờ). Khi trung bình
    8 giờ lớn nhất vượt 400 thì chỉ dùng nhánh 1 giờ (QĐ 1459, mục 2.2.2b, phần ghi chú)."""
    v_1h = iaqi(o3_1h_max, "o3_1h", standard)
    if o3_8h_max is not None and o3_8h_max <= 400:
        v_8h = iaqi(o3_8h_max, "o3_8h", standard)
        candidates = [v for v in (v_1h, v_8h) if v is not None]
        return max(candidates) if candidates else None
    return v_1h


def iaqi_day(components: dict, standard: str = "VN_1459") -> dict:
    """
    AQI ngày (QĐ 1459, mục 2.2.2). Chỉ hỗ trợ VN_1459.

    pm2_5 và pm10 là trung bình 24 giờ; so2, no2, co là trị số lớn nhất của trung bình
    1 giờ trong ngày; o3_1h_max và o3_8h_max là trị số lớn nhất của trung bình 1 giờ và
    8 giờ trong ngày.
    """
    if standard != "VN_1459":
        raise NotImplementedError("iaqi_day() chỉ hỗ trợ VN_1459")

    subs = {p: iaqi(components.get(p), p, standard) for p in _HOUR_DAY_COLS}
    subs["o3"] = _iaqi_o3_day(components.get("o3_1h_max"), components.get("o3_8h_max"), standard)
    return _reduce(subs, standard)
