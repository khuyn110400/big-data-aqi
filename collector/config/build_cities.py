"""
Sinh collector/config/cities.json (~200 trạm) từ city.list.json.gz của OpenWeather.
NGƯỜI A · M2. Chạy 1 LẦN ở Mac, commit kết quả -- không chạy lại tự động trong pipeline.

Nguồn: http://bulk.openweathermap.org/sample/city.list.json.gz
  (danh sách ~210.000 điểm toàn cầu: {id, name, state, country, coord:{lat,lon}} --
   KHÔNG có dân số, nên không lọc theo "thành phố lớn" được mà lọc theo trải đều địa lý)

Chiến lược mỗi nhóm:
  VN (mục tiêu 100): dữ liệu gốc chỉ có 168 điểm, lẫn nhiều mục rác (tên thôn/xóm,
    "Socialist Republic of Vietnam"). Lọc rác -> khử trùng lặp theo khoảng cách
    -> chọn tối đa bằng farthest-point sampling (FPS) để trải đều Bắc/Trung/Nam.
  IN (mục tiêu 50): ~3.634 điểm có sẵn, đủ nhiều -> chọn thẳng bằng FPS để trải đều
    Himalaya / sa mạc Rajasthan / bờ biển nhiệt đới -- vừa hình dạng địa lý vừa khí hậu.
  Global (mục tiêu 50): FPS không có ý nghĩa nếu áp cho toàn cầu (sẽ ra ngẫu nhiên,
    không đại diện) -> chọn TAY danh sách thành phố lớn/nổi tiếng trải khắp châu lục,
    cố tình lẫn cả nơi ô nhiễm nặng (Cairo, Lagos, Ulaanbaatar) và nơi rất sạch
    (Reykjavik, Wellington) để có tương phản cho phần phân cụm (Nhánh mở rộng M4).

8 trạm đã có sẵn trong cities.json (VN_HCM_01, VN_HAN_01, VN_DAD_01, VN_BDU_01,
IN_DEL_01, IN_BOM_01, CN_BJS_01, JP_TYO_01) được GIỮ NGUYÊN làm seed -- đã được
data/samples/generate_sample.py, README.md, CONTRACTS.md dùng làm ví dụ.

Chạy:
  python collector/config/build_cities.py
  # tải city.list.json.gz vào /tmp nếu chưa có, ghi đè collector/config/cities.json
"""
from __future__ import annotations

import gzip
import json
import math
import re
import unicodedata
import urllib.request
from pathlib import Path

BULK_URL = "http://bulk.openweathermap.org/sample/city.list.json.gz"
CACHE_PATH = Path("/tmp/owm_city_list.json.gz")
OUT_PATH = Path(__file__).resolve().parent / "cities.json"

TARGET_VN = 100
TARGET_IN = 50
TARGET_GLOBAL = 50

SEED_STATIONS = [
    {"station_id": "VN_HCM_01", "city": "Ho Chi Minh City", "country": "VN", "lat": 10.8231, "lon": 106.6297},
    {"station_id": "VN_HAN_01", "city": "Hanoi", "country": "VN", "lat": 21.0278, "lon": 105.8342},
    {"station_id": "VN_DAD_01", "city": "Da Nang", "country": "VN", "lat": 16.0544, "lon": 108.2022},
    {"station_id": "VN_BDU_01", "city": "Thu Dau Mot", "country": "VN", "lat": 10.9804, "lon": 106.6519},
    {"station_id": "IN_DEL_01", "city": "New Delhi", "country": "IN", "lat": 28.6139, "lon": 77.2090},
    {"station_id": "IN_BOM_01", "city": "Mumbai", "country": "IN", "lat": 19.0760, "lon": 72.8777},
    {"station_id": "CN_BJS_01", "city": "Beijing", "country": "CN", "lat": 39.9042, "lon": 116.4074},
    {"station_id": "JP_TYO_01", "city": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
]

# name tra trong bulk data (khong dau, khong phan biet hoa/thuong) -> hien thi dep hon
GLOBAL_CITIES = [
    # Dong A / Dong Nam A
    ("Seoul", "KR"), ("Bangkok", "TH"), ("Jakarta", "ID"), ("Manila", "PH"),
    ("Singapore", "SG"), ("Ulaanbaatar", "MN"), ("Taipei", "TW"), ("Hong Kong", "HK"),
    # Nam A (ngoai IN)
    ("Dhaka", "BD"), ("Karachi", "PK"), ("Kathmandu", "NP"), ("Colombo", "LK"),
    # Trung Dong
    ("Dubai", "AE"), ("Riyadh", "SA"), ("Tehran", "IR"), ("Doha", "QA"), ("Istanbul", "TR"),
    # Chau Phi
    ("Cairo", "EG"), ("Lagos", "NG"), ("Nairobi", "KE"), ("Johannesburg", "ZA"),
    ("Addis Ababa", "ET"), ("Kinshasa", "CD"), ("Casablanca", "MA"),
    # Chau Au
    ("London", "GB"), ("Paris", "FR"), ("Berlin", "DE"), ("Madrid", "ES"), ("Rome", "IT"),
    ("Warsaw", "PL"), ("Moscow", "RU"), ("Stockholm", "SE"), ("Reykjavik", "IS"),
    ("Athens", "GR"), ("Amsterdam", "NL"),
    # Bac My
    ("New York City", "US"), ("Los Angeles", "US"), ("Chicago", "US"),
    ("Mexico City", "MX"), ("Toronto", "CA"),
    # Nam My
    ("Sao Paulo", "BR"), ("Bogota", "CO"), ("Lima", "PE"), ("Santiago", "CL"), ("Buenos Aires", "AR"),
    # Chau Dai Duong
    ("Sydney", "AU"), ("Auckland", "NZ"), ("Melbourne", "AU"),
]

# rac / qua nho de xet la "diem quan trac" hop ly
_VN_JUNK_PATTERNS = re.compile(
    r"^(Socialist Republic|Xóm |Ấp |Thôn |Huyện |Huyen |Thị xã |Thi xa )", re.IGNORECASE
)

_VIET_MAP = str.maketrans({"đ": "d", "Đ": "D"})


def ascii_fold(text: str) -> str:
    text = text.translate(_VIET_MAP)
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def load_bulk() -> list[dict]:
    if not CACHE_PATH.exists():
        print(f"Tải {BULK_URL} -> {CACHE_PATH} ...")
        urllib.request.urlretrieve(BULK_URL, CACHE_PATH)
    with gzip.open(CACHE_PATH, "rt", encoding="utf-8") as f:
        return json.load(f)


def dedup_by_distance(entries: list[dict], min_km: float) -> list[dict]:
    """Loại điểm quá gần điểm đã giữ (ưu tiên giữ điểm KHÔNG có tiền tố 'Tỉnh')."""
    entries = sorted(entries, key=lambda e: e["name"].startswith("Tỉnh "))
    kept: list[dict] = []
    for e in entries:
        coord = (e["coord"]["lat"], e["coord"]["lon"])
        if all(haversine_km(coord, (k["coord"]["lat"], k["coord"]["lon"])) >= min_km for k in kept):
            kept.append(e)
    return kept


def farthest_point_sample(entries: list[dict], n: int, exclude_near: list[tuple[float, float]]) -> list[dict]:
    """Chọn n điểm trải đều nhất bằng farthest-point sampling (deterministic)."""
    candidates = [e for e in entries if e["coord"] is not None]
    coords = [(e["coord"]["lat"], e["coord"]["lon"]) for e in candidates]

    min_dist = [min((haversine_km(c, ex) for ex in exclude_near), default=math.inf) for c in coords]
    for i, d in enumerate(min_dist):
        if d < 5.0:  # loại điểm trùng seed
            min_dist[i] = -1

    selected_idx: list[int] = []
    start = max(range(len(coords)), key=lambda i: min_dist[i])
    selected_idx.append(start)
    min_dist = [
        d if d < 0 else min(d, haversine_km(coords[i], coords[start])) for i, d in enumerate(min_dist)
    ]

    while len(selected_idx) < n and len(selected_idx) < len(candidates):
        for i in selected_idx:
            min_dist[i] = -1
        nxt = max(range(len(coords)), key=lambda i: min_dist[i])
        if min_dist[nxt] < 0:
            break
        selected_idx.append(nxt)
        min_dist = [
            d if d < 0 else min(d, haversine_km(coords[i], coords[nxt])) for i, d in enumerate(min_dist)
        ]

    return [candidates[i] for i in selected_idx]


def select_vn(bulk: list[dict], seeds: list[dict]) -> list[dict]:
    vn = [e for e in bulk if e["country"] == "VN" and not _VN_JUNK_PATTERNS.match(e["name"])]
    vn = dedup_by_distance(vn, min_km=8.0)
    seed_coords = [(s["lat"], s["lon"]) for s in seeds if s["country"] == "VN"]
    n_needed = TARGET_VN - len(seed_coords)
    picked = farthest_point_sample(vn, n_needed, seed_coords)
    return [
        {"city": e["name"].replace("Tỉnh ", "").strip(), "country": "VN",
         "lat": round(e["coord"]["lat"], 4), "lon": round(e["coord"]["lon"], 4)}
        for e in picked
    ]


def select_in(bulk: list[dict], seeds: list[dict]) -> list[dict]:
    inn = [e for e in bulk if e["country"] == "IN" and e["coord"] is not None]
    seed_coords = [(s["lat"], s["lon"]) for s in seeds if s["country"] == "IN"]
    n_needed = TARGET_IN - len(seed_coords)
    picked = farthest_point_sample(inn, n_needed, seed_coords)
    return [
        {"city": e["name"], "country": "IN",
         "lat": round(e["coord"]["lat"], 4), "lon": round(e["coord"]["lon"], 4)}
        for e in picked
    ]


def select_global(bulk: list[dict], seeds: list[dict]) -> list[dict]:
    by_country: dict[str, list[dict]] = {}
    for e in bulk:
        by_country.setdefault(e["country"], []).append(e)

    seed_names = {(s["city"].lower(), s["country"]) for s in seeds}
    result = []
    misses = []
    for name, country in GLOBAL_CITIES:
        if (name.lower(), country) in seed_names:
            continue
        candidates = by_country.get(country, [])
        target = ascii_fold(name).lower()
        match = next((c for c in candidates if ascii_fold(c["name"]).lower() == target), None)
        if match is None:
            match = next((c for c in candidates if target in ascii_fold(c["name"]).lower()), None)
        if match is None:
            misses.append(f"{name}, {country}")
            continue
        result.append({
            "city": name, "country": country,
            "lat": round(match["coord"]["lat"], 4), "lon": round(match["coord"]["lon"], 4),
        })

    if misses:
        print(f"[CẢNH BÁO] không tìm thấy trong bulk data (bỏ qua): {misses}")
    return result[:TARGET_GLOBAL - sum(1 for s in seeds if s["country"] in ("CN", "JP"))]


def gen_station_id(city: str, country: str, used_codes: dict[str, int]) -> str:
    ascii_name = re.sub(r"[^A-Za-z]", "", ascii_fold(city)).upper()
    code = (ascii_name[:3] or "XXX").ljust(3, "X")
    key = f"{country}_{code}"
    used_codes[key] = used_codes.get(key, 0) + 1
    return f"{country}_{code}_{used_codes[key]:02d}"


def main() -> None:
    bulk = load_bulk()
    print(f"Bulk: {len(bulk)} điểm toàn cầu")

    vn = select_vn(bulk, SEED_STATIONS)
    inn = select_in(bulk, SEED_STATIONS)
    glb = select_global(bulk, SEED_STATIONS)

    print(f"VN: {len(SEED_STATIONS)} seed (VN) + {len(vn)} mới")
    print(f"IN: chọn thêm {len(inn)}")
    print(f"Global: chọn thêm {len(glb)}")

    used_codes: dict[str, int] = {}
    # seed station_id giữ nguyên, nhưng vẫn đăng ký vào used_codes để tránh đụng NN
    for s in SEED_STATIONS:
        code = s["station_id"].rsplit("_", 1)[0]
        n = int(s["station_id"].rsplit("_", 1)[1])
        used_codes[code] = max(used_codes.get(code, 0), n)

    stations = list(SEED_STATIONS)
    for e in vn + inn + glb:
        sid = gen_station_id(e["city"], e["country"], used_codes)
        stations.append({"station_id": sid, **e})

    print(f"TỔNG: {len(stations)} trạm "
          f"(VN={sum(1 for s in stations if s['country']=='VN')}, "
          f"IN={sum(1 for s in stations if s['country']=='IN')}, "
          f"khác={sum(1 for s in stations if s['country'] not in ('VN','IN'))})")

    out = {
        "_note": "Sinh bởi collector/config/build_cities.py từ city.list.json.gz (OpenWeather bulk).",
        "_target": "~200 điểm: 100 VN + 50 IN + 50 global (đa dạng khí hậu/mức ô nhiễm cho phần phân cụm)",
        "stations": stations,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Đã ghi {OUT_PATH}")


if __name__ == "__main__":
    main()
