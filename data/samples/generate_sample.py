"""
Sinh dữ liệu mẫu đúng schema C1, HIỆU CHỈNH THEO 90 NGÀY DỮ LIỆU THẬT.

Căn cứ: 2041 bản ghi thật của OpenWeather cho TP.HCM, 2026-06-15 → 2026-09-13.
Xem data/fixtures/owm_air_pollution_raw.json.

Vì sao dùng 90 ngày thay vì 7 ngày: cửa sổ 7 ngày rơi đúng mùa mưa, không khí sạch
(pm2_5 max chỉ 10.1). Trên 90 ngày, pm2_5 max = 112.9 — rộng gấp 11 lần. Nếu hiệu chỉnh
theo 7 ngày thì AQI lúc nào cũng "Tốt" và Pha 2/Pha 3 không có gì để phân tích.

Thông số lấy từ dữ liệu thật 90 ngày:
  pm2_5      : log-chuẩn, mean(log)=1.838  sd(log)=0.806  (TB 8.95, trung vị 6.20, max 112.9)
  tự tương quan lag-1 = 0.964 (tính trên các đoạn liên tục)
  pm10/pm2_5 = 1.41
  PM2.5 theo giờ VN : đỉnh 5h, đáy 12h, biên độ ±23%
  O3    theo giờ VN : đỉnh 14h, đáy 4h,  biên độ ±43%

GAP CÓ QUY LUẬT — phát hiện quan trọng nhất:
  4 khoảng đứt trong 90 ngày, TẤT CẢ đều là khối NGUYÊN NGÀY (24h hoặc 48h)
  và TẤT CẢ đều bắt đầu lúc 01:00 UTC. Không có giờ lẻ nào bị thiếu.
  → Không phải nhiễu ngẫu nhiên mà là ngày dữ liệu bị mất cả khối.
  → Pha 1 KHÔNG được nội suy qua khoảng này (24h quá dài); phải đánh dấu ngày đó
    không đủ dữ liệu và loại khỏi AQI ngày.
  Tỉ lệ đầy đủ thật: 94.4% trên 90 ngày (7 ngày cho 85.8% vì rơi trúng 1 gap).

Chạy: python data/samples/generate_sample.py
"""
import json, math, random, datetime as dt

random.seed(7)

STATIONS = [                                    # hệ số nhân lên nền TP.HCM
    ("VN_HCM_01", "Ho Chi Minh City", "VN", 10.8231, 106.6297, 7, 1.0),
    ("VN_HAN_01", "Hanoi",            "VN", 21.0278, 105.8342, 7, 2.6),
    ("VN_BDU_01", "Thu Dau Mot",      "VN", 10.9804, 106.6519, 7, 0.9),
    ("IN_DEL_01", "New Delhi",        "IN", 28.6139,  77.2090, 5, 7.0),
    ("JP_TYO_01", "Tokyo",            "JP", 35.6762, 139.6503, 9, 1.2),
]

MU_LOG, SD_LOG = 1.838, 0.806     # PM2.5 log-chuẩn, đo trên 90 ngày thật
AR1            = 0.964            # tự tương quan lag-1 thật
PM10_RATIO     = 1.41
O3_MEAN        = 32.12

# log-chuẩn: mu = log(TRUNG VỊ) (không phải log(trung bình)), sigma suy từ tỉ lệ mean/median thật
NO2_MED, NO2_SD = 3.68, 0.862
SO2_MED, SO2_SD = 0.94, 0.635
CO_MED,  CO_SD  = 223.0, 0.741

# Profile theo giờ ĐỊA PHƯƠNG (VN), đo trực tiếp từ 2041 bản ghi thật, chuẩn hoá TB = 1.
# Không fit hàm cos vì profile thật không đối xứng: PM đỉnh 5h nhưng đáy 12h (cách 7h, không phải 12h).
PM_PROFILE = [1.2179, 1.1909, 1.1729, 1.1971, 1.2236, 1.2345, 1.2045, 1.1358,
              0.9359, 0.9029, 0.9145, 0.6738, 0.6441, 0.6879, 0.7391, 0.7773,
              0.7712, 0.7953, 0.8937, 0.9829, 1.0845, 1.1727, 1.2206, 1.2263]
O3_PROFILE = [0.7601, 0.7545, 0.7524, 0.7435, 0.7422, 0.7465, 0.7501, 0.7783,
              0.8878, 0.9824, 1.1257, 1.3244, 1.3802, 1.4168, 1.4283, 1.4011,
              1.3529, 1.2568, 1.1059, 0.9834, 0.9012, 0.8418, 0.8050, 0.7787]

# 2160 giờ = 90 ngày, bằng đúng cửa sổ dữ liệu thật. Ngắn hơn thì chuỗi AR(1) với phi=0.964
# không kịp đi hết đuôi phân phối và sample sẽ "sạch" giả tạo.
START, HOURS   = dt.datetime(2026, 6, 15, tzinfo=dt.timezone.utc), 2160

OWM_BP = {   # ĐÃ KIỂM CHỨNG khớp 100% trên 2187 bản ghi thật
    "pm2_5": [10, 25, 50, 75], "pm10": [20, 50, 100, 200],
    "no2":   [40, 70, 150, 200], "o3":  [60, 100, 140, 180],
    "so2":   [20, 80, 250, 350], "co":  [4400, 9400, 12400, 15400],
}

def owm_aqi(c):
    w = 1
    for p, bps in OWM_BP.items():
        v = c.get(p)
        if v is None or v < 0:
            continue
        i = 5
        for k, b in enumerate(bps):
            if v < b:
                i = k + 1
                break
        w = max(w, i)
    return w

def diurnal_pm(h): return PM_PROFILE[h]
def diurnal_o3(h): return O3_PROFILE[h]

rows = []
for sid, city, country, lat, lon, off, f in STATIONS:
    log_state = MU_LOG + math.log(f)
    skip_until = -1
    for h in range(HOURS):
        ts = START + dt.timedelta(hours=h)

        # GAP: khối NGUYÊN NGÀY, luôn bắt đầu 01:00 UTC — đúng quy luật quan sát được
        if h < skip_until:
            continue
        if ts.hour == 1 and random.random() < 0.055:
            skip_until = h + 24 * random.choice([1, 1, 1, 2])
            continue

        local_h = (ts.hour + off) % 24
        # AR(1) trên thang log -> phân phối biên log-chuẩn, lệch phải như thật
        log_state = AR1 * log_state + (1 - AR1) * (MU_LOG + math.log(f)) \
                    + random.gauss(0, SD_LOG * math.sqrt(1 - AR1 ** 2))
        pm25 = math.exp(log_state) * diurnal_pm(local_h)
        pm10 = pm25 * random.gauss(PM10_RATIO, 0.10)
        o3   = max(0.0, random.gauss(O3_MEAN, 7) * diurnal_o3(local_h))
        no2  = max(0.5, math.exp(random.gauss(math.log(NO2_MED * f), NO2_SD)))
        so2  = max(0.3, math.exp(random.gauss(math.log(SO2_MED * f), SO2_SD)))
        co   = max(80.0, math.exp(random.gauss(math.log(CO_MED * (0.6 + 0.4 * f)), CO_SD))
                         * diurnal_pm(local_h))

        comp = {"co": round(co, 2),
                "no": round(abs(random.gauss(0.2, 0.3)), 2) if random.random() > 0.24 else 0,
                "no2": round(no2, 2), "o3": round(o3, 2), "so2": round(so2, 2),
                "pm2_5": round(pm25, 2), "pm10": round(pm10, 2),
                "nh3": round(abs(random.gauss(2.0, 0.7)), 2)}

        # NULL / NEG: chưa quan sát thấy trong 2187 bản ghi thật -> tỉ lệ rất thấp, chỉ để Pha 1 phòng thủ
        if random.random() < 0.002:
            comp[random.choice(["pm10", "so2", "o3"])] = None
        if random.random() < 0.0005:
            comp["no2"] = round(-abs(random.gauss(3, 2)), 2)

        rec = {"schema_version": "1.1", "station_id": sid, "city": city, "country": country,
               "lat": lat, "lon": lon,
               "ts_utc": ts.strftime("%Y-%m-%dT%H:00:00Z"), "ts_epoch": int(ts.timestamp()),
               "dt_raw": int(ts.timestamp()),
               "source": "SYNTHETIC_calibrated_on_real_90d_2026-09-13",
               "ingest_mode": "history", "fetched_at_utc": "2026-09-13T00:00:00Z",
               "owm_aqi": owm_aqi(comp), "components": comp}
        rows.append(rec)

        if ts.day == 1 and ts.hour == 0:     # DUP ở biên chunk khi backfill
            rows.append(json.loads(json.dumps(rec)))

with open("data/samples/air_quality_sample.jsonl", "w", encoding="utf-8") as fo:
    for r in rows:
        fo.write(json.dumps(r, ensure_ascii=False) + "\n")
print(f"rows: {len(rows)}")
