#!/usr/bin/env python3
"""
Fetch NDBC station list and generate data/stations.json.
Only includes stations with buoy-type hulls (likely to have wave data).
"""

import json
import re
import urllib.request
from pathlib import Path

URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
OUT = Path(__file__).resolve().parent.parent / "data" / "stations.json"

BUOY_TYPES = {"3-meter discus buoy", "buoy", "atlas buoy", "dart ii", "dart 4g",
              "6-meter nomad", "discus buoy", "6-meter foam buoy",
              "self-contained ocean observing payload",
              "ocean racing buoy", "wave rider", "waverider", "datawell",
              "3-meter foam buoy", "ocean buoy"}

def parse_location(loc_str):
    """Parse '12.000 N 23.000 W' into (lat, lon)."""
    m = re.match(r'([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])', loc_str)
    if not m:
        return None, None
    lat = float(m.group(1)) * (1 if m.group(2) == 'N' else -1)
    lon = float(m.group(3)) * (1 if m.group(4) == 'E' else -1)
    return lat, lon

def main():
    print("Fetching NDBC station table...")
    req = urllib.request.Request(URL, headers={"User-Agent": "SurfCheck/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8", errors="replace")

    stations = []
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 7:
            continue
        sid = parts[0].strip()
        ttype = parts[2].strip()
        name = parts[4].strip()
        loc_raw = parts[6].strip()

        # Only buoy types
        if not ttype or ttype.lower() not in BUOY_TYPES:
            # Also accept if "buoy" appears in the type string
            if "buoy" not in ttype.lower() and "dart" not in ttype.lower() and "rider" not in ttype.lower():
                continue

        # Parse lat/lon from the first part before the parenthetical
        loc_simple = loc_raw.split("(")[0].strip()
        lat, lon = parse_location(loc_simple)
        if lat is None or (lat == 0 and lon == 0):
            continue

        stations.append({
            "id": sid,
            "name": name or sid,
            "type": ttype,
            "lat": round(lat, 3),
            "lon": round(lon, 3),
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(stations, indent=2))
    print(f"Wrote {len(stations)} buoy stations to {OUT}")

if __name__ == "__main__":
    main()
