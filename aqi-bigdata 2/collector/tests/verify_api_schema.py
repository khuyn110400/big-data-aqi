#!/usr/bin/env python3
"""
KIỂM CHỨNG GIẢ ĐỊNH SCHEMA BẰNG DỮ LIỆU THẬT — NGƯỜI A chạy ở M0, ngay khi có API key.

Vì sao có file này:
  Toàn bộ schema C1, file fixtures/ và data/samples/ hiện đang dựa trên TÀI LIỆU
  của OpenWeather, KHÔNG phải trên một response thật đã gọi được. Chừng nào chưa
  chạy file này thì mọi giả định bên dưới vẫn chỉ là giả định.

Script làm 3 việc:
  1. Gọi API thật (current + history), in ra response nguyên bản
  2. Đối chiếu từng giả định -> báo PASS / FAIL cụ thể
  3. Ghi response thật vào data/fixtures/ để thay file dựng sẵn

Chạy:
  export OWM_API_KEY=...
  python collector/tests/verify_api_schema.py
"""
import json, os, sys, time, urllib.request, urllib.parse, datetime as dt

KEY = os.environ.get("OWM_API_KEY")
LAT, LON = 10.8231, 106.6297          # TP.HCM
BASE = "https://api.openweathermap.org/data/2.5/air_pollution"

# ---- CÁC GIẢ ĐỊNH ĐANG DÙNG TRONG REPO (cần kiểm chứng) --------------------
ASSUMED_TOP_KEYS   = {"coord", "list"}
ASSUMED_ITEM_KEYS  = {"main", "components", "dt"}
ASSUMED_COMPONENTS = {"co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"}
ASSUMED_AQI_RANGE  = (1, 5)
ASSUMED_COORD_IS_OBJECT = True        # {lon, lat}, không phải mảng
ASSUMED_DT_CURRENT_ALIGNED  = False   # ĐÃ KIỂM CHỨNG 13/09/2026: current KHÔNG tròn giờ
ASSUMED_DT_HISTORY_ALIGNED  = True    # ĐÃ KIỂM CHỨNG: history tròn giờ 145/145
ASSUMED_HISTORY_COMPLETENESS = 0.85   # ĐÃ KIỂM CHỨNG: 7 ngày thật chỉ đạt 85.8%
ASSUMED_HISTORY_FROM    = "2020-11-27"

ok, fail = [], []
def check(cond, label, detail=""):
    (ok if cond else fail).append(f"{label}{' — ' + detail if detail else ''}")
    print(("  [PASS] " if cond else "  [FAIL] ") + label + (f"  {detail}" if detail else ""))

def get(path, **params):
    params["appid"] = KEY
    url = f"{BASE}{path}?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())

if not KEY:
    sys.exit("Chưa có OWM_API_KEY. Chạy: export OWM_API_KEY=...")

print("=" * 70)
print("1. CURRENT  /air_pollution")
print("=" * 70)
try:
    cur = get("", lat=LAT, lon=LON)
except Exception as e:
    sys.exit(f"Gọi API lỗi: {e}\n  401 = key chưa active, chờ 10 phút - 2 tiếng rồi chạy lại.")

print(json.dumps(cur, indent=2, ensure_ascii=False)[:900])
print()

check(set(cur) == ASSUMED_TOP_KEYS, "Top-level keys = {coord, list}", f"thực tế: {sorted(cur)}")
check(isinstance(cur.get("coord"), dict) == ASSUMED_COORD_IS_OBJECT,
      "coord là object {lon,lat}", f"thực tế: {type(cur.get('coord')).__name__}")

item = cur["list"][0]
check(set(item) == ASSUMED_ITEM_KEYS, "list[0] keys = {main, components, dt}", f"thực tế: {sorted(item)}")

comp = item["components"]
check(set(comp) == ASSUMED_COMPONENTS, "components đúng 8 trường",
      f"thừa: {set(comp)-ASSUMED_COMPONENTS or '-'} | thiếu: {ASSUMED_COMPONENTS-set(comp) or '-'}")
check(all(isinstance(v, (int, float)) for v in comp.values()), "components toàn số")

a = item["main"]["aqi"]
check(ASSUMED_AQI_RANGE[0] <= a <= ASSUMED_AQI_RANGE[1], f"main.aqi trong 1..5", f"thực tế: {a}")
check(isinstance(item["dt"], int), "dt là int (unix giây)")
check((item["dt"] % 3600 == 0) == ASSUMED_DT_CURRENT_ALIGNED,
      "current dt KHÔNG tròn giờ (đúng như đã kiểm chứng)",
      f"dt={item['dt']} dư {item['dt']%3600}s -> normalize() PHẢI làm tròn xuống giờ (quy tắc R1)")

# toạ độ trả về có lệch so với toạ độ gửi đi không -> quyết định dùng coord hay config
c = cur["coord"]
d_lat, d_lon = abs(c["lat"] - LAT), abs(c["lon"] - LON)
check(d_lat < 1e-4 and d_lon < 1e-4, "coord trả về == toạ độ gửi đi",
      f"lệch {d_lat:.4f}/{d_lon:.4f} -> nếu lệch thì PHẢI dùng lat/lon trong cities.json")

print()
print("=" * 70)
print("2. HISTORY  /air_pollution/history  — 7 ngày gần nhất")
print("=" * 70)
end = int(time.time()) // 3600 * 3600
start = end - 7 * 24 * 3600
hist = get("/history", lat=LAT, lon=LON, start=start, end=end)
lst = hist["list"]

expected_hours = (end - start) // 3600
ts = sorted(x["dt"] for x in lst)
uniq = sorted(set(ts))
gaps = [(a, b) for a, b in zip(uniq, uniq[1:]) if b - a > 3600]

print(f"  Yêu cầu {expected_hours} giờ, API trả {len(lst)} bản ghi ({len(uniq)} mốc giờ khác nhau)")
check(set(lst[0]) == ASSUMED_ITEM_KEYS, "history item cùng cấu trúc với current")
check(set(lst[0]["components"]) == ASSUMED_COMPONENTS, "history components cùng 8 trường")
check(len(ts) == len(uniq), "history KHÔNG trả trùng mốc giờ",
      f"{len(ts)-len(uniq)} bản ghi trùng" if len(ts) != len(uniq) else "")
print(f"  [INFO]  history thiếu {len(gaps)} khoảng giờ" + (f", dài nhất {max((b-a)//3600 for a,b in gaps)}h" if gaps else "")
        + "  <- BÌNH THƯỜNG, đã xác nhận có thật. Pha 1 phải xử lý.")
check(len(uniq) >= expected_hours * ASSUMED_HISTORY_COMPLETENESS,
      f"history đạt >= {ASSUMED_HISTORY_COMPLETENESS:.0%} số giờ yêu cầu",
      f"{len(uniq)}/{expected_hours} = {100*len(uniq)/expected_hours:.1f}%")

# giới hạn 1 call: thử xin 90 ngày xem có bị cắt không
h90 = get("/history", lat=LAT, lon=LON, start=end - 90*24*3600, end=end)
print(f"\n  Xin 90 ngày ({90*24} giờ) -> API trả {len(h90['list'])} bản ghi")
check(len(h90["list"]) >= 90*24*0.9, "1 call lấy được 90 ngày (không bị cắt)",
      f"chỉ nhận {len(h90['list'])} -> PHẢI chunk nhỏ hơn khi backfill")

print()
print("=" * 70)
print(f"KẾT QUẢ: {len(ok)} PASS · {len(fail)} FAIL")
print("=" * 70)
if fail:
    print("\nCÁC GIẢ ĐỊNH SAI — phải sửa CONTRACTS.md C1 và normalize() theo thực tế:")
    for f in fail:
        print("  -", f)
else:
    print("\nMọi giả định trong repo khớp với API thật.")

# ghi response thật ra fixtures
out = "data/fixtures/owm_real_response.json"
os.makedirs("data/fixtures", exist_ok=True)
with open(out, "w", encoding="utf-8") as fo:
    json.dump({"_captured_at": dt.datetime.now(dt.timezone.utc).isoformat(),
               "_lat": LAT, "_lon": LON,
               "current": cur, "history_sample": {"coord": hist["coord"], "list": lst[:5]}},
              fo, indent=2, ensure_ascii=False)
print(f"\nĐã ghi response THẬT vào {out}")
print("-> Dùng file này thay cho owm_air_pollution_raw.json (bản dựng sẵn) khi viết normalize().")
sys.exit(1 if fail else 0)
