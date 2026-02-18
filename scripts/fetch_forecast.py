#!/usr/bin/env python3
"""
SurfCheck data pipeline — fetches NDBC buoy data and GFS-Wave forecast.

Usage:
  python fetch_forecast.py              # full run (buoy + forecast)
  python fetch_forecast.py --buoy-only  # buoy data only
"""

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MISSING_VALS = {99.0, 999.0, 9999.0, 99.00, 999.00}

def safe_float(val, missing_extra=None):
    """Convert to float, returning None for NDBC missing sentinels."""
    try:
        f = float(val)
    except (ValueError, TypeError):
        return None
    # NDBC uses 99, 999, 9999, and sometimes 9.9 for missing
    if f in MISSING_VALS:
        return None
    if missing_extra and f in missing_extra:
        return None
    return f


def safe_float_wave(val):
    """Float conversion for wave heights — also treat 9.9 as missing, reject > 8m."""
    f = safe_float(val, missing_extra={9.9})
    if f is not None and f > 8.0:
        return None  # unrealistic wave height for this region
    return f


def fetch_text(url):
    """Download text from URL."""
    req = urllib.request.Request(url, headers={"User-Agent": "SurfCheck/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Part 1 — NDBC Buoy 44097
# ---------------------------------------------------------------------------

BUOY = "44097"
NDBC_BASE = "https://www.ndbc.noaa.gov/data/realtime2"

def parse_stdmet(text):
    """Parse standard meteorological data (latest observation)."""
    lines = text.strip().splitlines()
    if len(lines) < 3:
        return None
    # line 0 = header, line 1 = units, line 2+ = data (newest first)
    header = lines[0].replace("#", "").split()
    row = lines[2].split()
    if len(row) < 13:
        return None
    # cols: YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP ...
    return {
        "time": f"{row[0]}-{row[1]}-{row[2]}T{row[3]}:{row[4]}Z",
        "waveHeight_m": safe_float_wave(row[8]),
        "dominantPeriod_s": safe_float(row[9]),
        "avgPeriod_s": safe_float(row[10]),
        "meanDirection_deg": safe_float(row[11]),
        "waterTemp_C": safe_float(row[14]) if len(row) > 14 else None,
        "windSpeed_mps": safe_float(row[6]),
        "windDir_deg": safe_float(row[5]),
        "pressure_hPa": safe_float(row[12]),
        "gust_mps": safe_float(row[7]),
        "airTemp_C": safe_float(row[13]) if len(row) > 13 else None,
    }


def parse_spectral(text):
    """Parse spectral data (alpha1 directional file)."""
    lines = text.strip().splitlines()
    if len(lines) < 3:
        return []
    # Latest record only (line 2)
    row = lines[2].split()
    if len(row) < 7:
        return []
    time_str = f"{row[0]}-{row[1]}-{row[2]}T{row[3]}:{row[4]}Z"
    # remaining columns are (freq, alpha1) pairs packed or just alpha1 values
    # Actually the spectral alpha1 file format:
    # YY MM DD hh mm  freq1(dir1) freq2(dir2) ...
    # The values after the time are direction values at each frequency
    entries = []
    # We need the .data_spec file for energy and swdir file for directions
    return entries


def parse_data_spec(spec_text, swdir_text):
    """Parse spectral density + direction files for compass rose."""
    spec_lines = spec_text.strip().splitlines()
    swdir_lines = swdir_text.strip().splitlines()
    if len(spec_lines) < 3 or len(swdir_lines) < 3:
        return []

    # Parse frequency bins from header
    spec_header = spec_lines[0].replace("#", "").replace("(", " ").replace(")", " ").split()
    # First 5 cols are YY MM DD hh mm, rest are freq values in parens
    # Actually format is: freq(density) or just densities with freq header
    
    # Simpler approach: parse the raw line
    spec_row = spec_lines[2].split()
    swdir_row = swdir_lines[2].split()
    
    if len(spec_row) < 7 or len(swdir_row) < 7:
        return []
    
    time_str = f"{spec_row[0]}-{spec_row[1]}-{spec_row[2]}T{spec_row[3]}:{spec_row[4]}Z"
    
    # Extract frequency bins from the header line
    # Format: #YY MM DD hh mm  0.0325(freq1) 0.0375(freq2) ...
    freq_parts = spec_lines[1].replace("#", "").replace("(", " ").replace(")", " ").split()
    freqs = []
    for p in freq_parts[5:]:  # skip YY MM DD hh mm
        f = safe_float(p)
        if f is not None:
            freqs.append(f)
    
    densities = []
    directions = []
    for v in spec_row[5:]:
        d = safe_float(v)
        densities.append(d)
    for v in swdir_row[5:]:
        d = safe_float(v)
        directions.append(d)
    
    entries = []
    for i in range(min(len(freqs), len(densities), len(directions))):
        if densities[i] is not None and directions[i] is not None and freqs[i] is not None:
            period = 1.0 / freqs[i] if freqs[i] > 0 else None
            entries.append({
                "freq": round(freqs[i], 4),
                "period_s": round(period, 1) if period else None,
                "energy": round(densities[i], 2),
                "direction_deg": round(directions[i], 1),
            })
    
    return entries


def fetch_buoy():
    """Fetch all buoy data and write data/buoy.json."""
    print(f"[buoy] Fetching NDBC {BUOY} ...")
    
    stdmet_text = fetch_text(f"{NDBC_BASE}/{BUOY}.txt")
    stdmet = parse_stdmet(stdmet_text)
    
    # Spectral data
    spectral = []
    try:
        spec_text = fetch_text(f"{NDBC_BASE}/{BUOY}.data_spec")
        swdir_text = fetch_text(f"{NDBC_BASE}/{BUOY}.swdir")
        spectral = parse_data_spec(spec_text, swdir_text)
        print(f"[buoy] Got {len(spectral)} spectral bins")
    except Exception as e:
        print(f"[buoy] Spectral fetch failed: {e}")
    
    result = {
        "fetched": datetime.now(timezone.utc).isoformat(),
        "buoy": BUOY,
        "stdmet": stdmet,
        "spectral": spectral,
    }
    
    out = DATA_DIR / "buoy.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"[buoy] Wrote {out}")
    return result


# ---------------------------------------------------------------------------
# Part 2 — GFS-Wave Forecast (GRIB2 from NOMADS)
# ---------------------------------------------------------------------------

NOMADS_BASE = "https://nomads.ncep.noaa.gov/dods/wave/gfswave"
TARGET_LAT = 41.003
TARGET_LON = -71.6  # stored as 288.4 in GFS (360 + lon)

def fetch_forecast():
    """Fetch GFS-Wave forecast via OPeNDAP and write data/forecast.json."""
    print("[forecast] Fetching GFS-Wave from NOMADS ...")
    
    try:
        import xarray as xr
    except ImportError:
        print("[forecast] ERROR: xarray not installed. Run: pip install xarray netCDF4")
        sys.exit(1)
    
    # Find latest available run
    now = datetime.now(timezone.utc)
    # Try today's runs first, then yesterday
    for day_offset in range(0, 3):
        from datetime import timedelta
        d = now - timedelta(days=day_offset)
        date_str = d.strftime("%Y%m%d")
        for cycle in ["12", "06", "00"]:
            url = f"{NOMADS_BASE}/{date_str}/gfswave.global.0p16_{cycle}z"
            print(f"[forecast] Trying {url} ...")
            try:
                ds = xr.open_dataset(url, engine="netcdf4")
                break
            except Exception:
                ds = None
                continue
        if ds is not None:
            break
    
    if ds is None:
        print("[forecast] ERROR: Could not find any available GFS-Wave run")
        sys.exit(1)
    
    # Select nearest grid point
    gfs_lon = 360 + TARGET_LON  # convert to 0-360
    ds_point = ds.sel(lat=TARGET_LAT, lon=gfs_lon, method="nearest")
    
    hours = []
    for t in ds_point.time.values:
        ts = str(t)
        rec = {"time": ts}
        for var in ["htsgwsfc", "perpwsfc", "dirpwsfc", "wvhgtsfc", "wvpersfc", "wvdirsfc",
                     "swell_1", "swper_1", "swdir_1", "swell_2", "swper_2", "swdir_2",
                     "wndspdsfc", "wnddirsfc"]:
            if var in ds_point:
                val = float(ds_point[var].sel(time=t).values)
                rec[var] = round(val, 2) if val == val else None  # NaN check
            
        # Validate wave height
        if rec.get("htsgwsfc") and rec["htsgwsfc"] > 8.0:
            rec["htsgwsfc"] = None
            
        hours.append(rec)
    
    ds.close()
    
    result = {
        "fetched": datetime.now(timezone.utc).isoformat(),
        "model": "GFS-Wave",
        "run": url.split("/")[-1],
        "lat": TARGET_LAT,
        "lon": TARGET_LON,
        "hours": hours,
    }
    
    out = DATA_DIR / "forecast.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"[forecast] Wrote {out} ({len(hours)} hours)")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SurfCheck data pipeline")
    parser.add_argument("--buoy-only", action="store_true", help="Fetch buoy data only")
    args = parser.parse_args()
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    fetch_buoy()
    
    if not args.buoy_only:
        fetch_forecast()
    
    print("[done]")


if __name__ == "__main__":
    main()
