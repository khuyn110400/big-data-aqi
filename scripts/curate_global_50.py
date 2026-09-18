from __future__ import annotations

import gzip
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

SOURCE = Path("data/reference/city.list.json.gz")
CURRENT = Path("collector/config/cities.json")
BASE8 = Path("collector/config/cities.before_200.json")
OUTPUT = Path("collector/config/cities.json")

TARGETS = {
    "CN": ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu"],
    "JP": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"],
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "CA": ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa"],
    "GB": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool"],
    "DE": ["Berlin", "Hamburg", "Munich", "Köln", "Frankfurt am Main"],
    "AU": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "BR": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"],
    "ZA": ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth"],
    "AE": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman City", "Al Ain City"],
}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", s.casefold())


def city_code(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]", "", s).upper()
    return (s[:3] + "XXX")[:3] if s else "CTY"


with CURRENT.open(encoding="utf-8") as f:
    current = json.load(f)["stations"]

with BASE8.open(encoding="utf-8") as f:
    base8 = json.load(f)["stations"]

# Giữ nguyên 150 VN + IN đã sinh
vn_in = [x for x in current if x["country"] in {"VN", "IN"}]

assert len(vn_in) == 150, (
    f"Cần đúng 150 VN+IN, hiện có {len(vn_in)}"
)

# Giữ chính xác Beijing/Tokyo ban đầu
base_global = {
    (x["country"], norm(x["city"])): x
    for x in base8
    if x["country"] not in {"VN", "IN"}
}

with gzip.open(SOURCE, "rt", encoding="utf-8") as f:
    raw = json.load(f)

index = defaultdict(list)

for item in raw:
    try:
        country = str(item["country"]).upper().strip()
        name = str(item["name"]).strip()
        lat = round(float(item["coord"]["lat"]), 4)
        lon = round(float(item["coord"]["lon"]), 4)
        source_id = int(item["id"])
    except (KeyError, TypeError, ValueError):
        continue

    index[(country, norm(name))].append({
        "city": name,
        "country": country,
        "lat": lat,
        "lon": lon,
        "source_id": source_id,
    })

selected = []
missing = []

for country, names in TARGETS.items():
    for wanted in names:
        key = (country, norm(wanted))

        # Beijing/Tokyo: giữ bản gốc của repo
        if key in base_global:
            x = dict(base_global[key])
            x["source_id"] = -1
            selected.append(x)
            continue

        matches = index.get(key, [])

        if not matches:
            missing.append(f"{country}:{wanted}")
            continue

        x = sorted(matches, key=lambda z: z["source_id"])[0]
        selected.append(x)

if missing:
    raise SystemExit(
        "Không tìm thấy city trong OpenWeather:\n  "
        + "\n  ".join(missing)
    )

assert len(selected) == 50

used_ids = {x["station_id"] for x in vn_in}
global_stations = []

for x in selected:
    # entry gốc đã có station_id
    if "station_id" in x:
        sid = x["station_id"]
    else:
        base = f'{x["country"]}_{city_code(x["city"])}'
        n = 1
        sid = f"{base}_{n:02d}"

        while sid in used_ids:
            n += 1
            sid = f"{base}_{n:02d}"

    if sid in used_ids:
        raise RuntimeError(f"station_id trùng: {sid}")

    used_ids.add(sid)

    global_stations.append({
        "station_id": sid,
        "city": x["city"],
        "country": x["country"],
        "lat": round(float(x["lat"]), 4),
        "lon": round(float(x["lon"]), 4),
    })

stations = vn_in + global_stations

assert len(stations) == 200
assert len({x["station_id"] for x in stations}) == 200
assert len({(x["lat"], x["lon"]) for x in stations}) == 200

doc = {
    "_note": (
        "200 điểm AQI: 100 VN + 50 IN + 50 global. "
        "Global gồm 10 quốc gia x 5 thành phố lớn."
    ),
    "_target": "100 VN + 50 IN + 50 global",
    "stations": stations,
}

OUTPUT.write_text(
    json.dumps(doc, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

print("Updated:", OUTPUT)
print("Total:", len(stations))
print("VN:", sum(x["country"] == "VN" for x in stations))
print("IN:", sum(x["country"] == "IN" for x in stations))
print(
    "Global:",
    sum(x["country"] not in {"VN", "IN"} for x in stations),
)
print("Countries:", len({x["country"] for x in stations}))

for country in TARGETS:
    print(
        country,
        sum(x["country"] == country for x in stations),
    )
