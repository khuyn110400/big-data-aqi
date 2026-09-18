from __future__ import annotations

import gzip
import json
import math
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

SOURCE = Path("data/reference/city.list.json.gz")
CURRENT = Path("collector/config/cities.json")
OUTPUT = Path("collector/config/cities.json")

TARGET_VN = 100
TARGET_IN = 50
TARGET_GLOBAL = 50


def valid_city(x):
    try:
        country = str(x["country"]).upper().strip()
        name = str(x["name"]).strip()
        lat = float(x["coord"]["lat"])
        lon = float(x["coord"]["lon"])
        cid = int(x["id"])
    except (KeyError, TypeError, ValueError):
        return None

    if not country or not name:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None

    return {
        "source_id": cid,
        "city": name,
        "country": country,
        "lat": round(lat, 4),
        "lon": round(lon, 4),
    }


def haversine(a, b):
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )

    return 6371.0 * 2 * math.asin(math.sqrt(h))


def coord(x):
    return (float(x["lat"]), float(x["lon"]))


def dedupe(candidates, existing):
    used_coords = {
        (round(float(x["lat"]), 4), round(float(x["lon"]), 4))
        for x in existing
    }

    used_names = {
        (x["country"].upper(), x["city"].casefold())
        for x in existing
    }

    out = []
    seen_coords = set()
    seen_names = set()

    for x in candidates:
        c = (x["lat"], x["lon"])
        n = (x["country"], x["city"].casefold())

        if c in used_coords or n in used_names:
            continue
        if c in seen_coords or n in seen_names:
            continue

        seen_coords.add(c)
        seen_names.add(n)
        out.append(x)

    return out


def farthest_sample(candidates, seeds, n):
    if n <= 0:
        return []

    candidates = list(candidates)
    selected = []
    selected_coords = [coord(x) for x in seeds]

    if not selected_coords and candidates:
        first = min(
            candidates,
            key=lambda x: (
                abs(x["lat"]),
                abs(x["lon"]),
                x["source_id"],
            ),
        )
        selected.append(first)
        selected_coords.append(coord(first))
        candidates.remove(first)

    while len(selected) < n:
        if not candidates:
            raise RuntimeError(
                f"Không đủ candidate: cần thêm {n}, "
                f"chỉ chọn được {len(selected)}"
            )

        best = None
        best_dist = -1

        for x in candidates:
            p = coord(x)
            d = min(haversine(p, q) for q in selected_coords)

            if d > best_dist:
                best = x
                best_dist = d

        selected.append(best)
        selected_coords.append(coord(best))
        candidates.remove(best)

    return selected


def country_centroid(items):
    lat = sum(x["lat"] for x in items) / len(items)

    # Dùng vector để tránh vấn đề kinh tuyến ±180.
    xs = ys = 0.0
    for x in items:
        lon = math.radians(x["lon"])
        xs += math.cos(lon)
        ys += math.sin(lon)

    lon = math.degrees(math.atan2(ys, xs))

    return lat, lon


def representative(items, center):
    return min(
        items,
        key=lambda x: (
            haversine(coord(x), center),
            x["city"].casefold(),
            x["source_id"],
        ),
    )


def select_global(candidates, existing_global, n):
    groups = defaultdict(list)

    existing_countries = {
        x["country"].upper()
        for x in existing_global
    }

    for x in candidates:
        if x["country"] in {"VN", "IN"}:
            continue
        if x["country"] in existing_countries:
            continue
        groups[x["country"]].append(x)

    reps = []

    for country, items in sorted(groups.items()):
        center = country_centroid(items)
        rep = representative(items, center)

        rep = dict(rep)
        rep["_center_lat"] = center[0]
        rep["_center_lon"] = center[1]
        reps.append(rep)

    seeds = existing_global

    selected = []
    seed_coords = [coord(x) for x in seeds]

    while len(selected) < n:
        if not reps:
            raise RuntimeError("Không đủ quốc gia cho global sample")

        best = None
        best_dist = -1

        for x in reps:
            p = (x["_center_lat"], x["_center_lon"])

            compare = seed_coords + [
                (y["_center_lat"], y["_center_lon"])
                for y in selected
            ]

            d = min(haversine(p, q) for q in compare)

            if d > best_dist:
                best = x
                best_dist = d

        selected.append(best)
        reps.remove(best)

    for x in selected:
        x.pop("_center_lat", None)
        x.pop("_center_lon", None)

    return selected


def city_code(name):
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]", "", s).upper()

    if not s:
        return "CTY"

    return (s[:3] + "XXX")[:3]


def assign_ids(existing, additions):
    used = {x["station_id"] for x in existing}
    counters = defaultdict(int)

    result = []

    for x in sorted(
        additions,
        key=lambda z: (
            z["country"],
            z["city"].casefold(),
            z["source_id"],
        ),
    ):
        code = city_code(x["city"])
        base = f'{x["country"]}_{code}'

        while True:
            counters[base] += 1
            sid = f"{base}_{counters[base]:02d}"
            if sid not in used:
                break

        used.add(sid)

        result.append(
            {
                "station_id": sid,
                "city": x["city"],
                "country": x["country"],
                "lat": x["lat"],
                "lon": x["lon"],
            }
        )

    return result


with CURRENT.open("r", encoding="utf-8") as f:
    current_doc = json.load(f)

existing = current_doc["stations"]

with gzip.open(SOURCE, "rt", encoding="utf-8") as f:
    raw = json.load(f)

cities = []

for item in raw:
    x = valid_city(item)
    if x is not None:
        cities.append(x)

existing_vn = [x for x in existing if x["country"] == "VN"]
existing_in = [x for x in existing if x["country"] == "IN"]
existing_global = [
    x for x in existing
    if x["country"] not in {"VN", "IN"}
]

vn_pool = dedupe(
    [x for x in cities if x["country"] == "VN"],
    existing,
)

in_pool = dedupe(
    [x for x in cities if x["country"] == "IN"],
    existing,
)

global_pool = dedupe(
    [x for x in cities if x["country"] not in {"VN", "IN"}],
    existing,
)

vn_add = farthest_sample(
    vn_pool,
    existing_vn,
    TARGET_VN - len(existing_vn),
)

in_add = farthest_sample(
    in_pool,
    existing_in,
    TARGET_IN - len(existing_in),
)

global_add = select_global(
    global_pool,
    existing_global,
    TARGET_GLOBAL - len(existing_global),
)

new_items = assign_ids(
    existing,
    vn_add + in_add + global_add,
)

stations = existing + new_items

counts = defaultdict(int)
for x in stations:
    counts[x["country"]] += 1

vn_count = counts["VN"]
in_count = counts["IN"]
global_count = len(stations) - vn_count - in_count

assert len(stations) == 200, len(stations)
assert vn_count == 100, vn_count
assert in_count == 50, in_count
assert global_count == 50, global_count
assert len({x["station_id"] for x in stations}) == 200

coords = {
    (round(float(x["lat"]), 4), round(float(x["lon"]), 4))
    for x in stations
}
assert len(coords) == 200, "Có tọa độ station bị trùng"

doc = {
    "_note": (
        "200 điểm AQI: giữ nguyên 8 station ban đầu; "
        "phần còn lại chọn deterministic từ OpenWeather city.list.json.gz"
    ),
    "_target": "100 VN + 50 IN + 50 global",
    "stations": stations,
}

OUTPUT.write_text(
    json.dumps(
        doc,
        ensure_ascii=False,
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)

print("Generated:", OUTPUT)
print("Total:", len(stations))
print("VN:", vn_count)
print("IN:", in_count)
print("Global:", global_count)
print("Countries:", len(counts))
