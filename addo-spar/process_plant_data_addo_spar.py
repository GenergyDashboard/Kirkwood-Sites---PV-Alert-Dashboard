"""
process_plant_data_addo_spar.py

Enhanced processor for Addo Spar that:
- Stores historical daily data
- Fetches irradiation from Open-Meteo API
- Calculates 30-day hourly averages (bell curve baseline)
- Calculates 30-day min/max bands and percentiles (p10/p25/p75/p90)
- Sends Telegram alerts for underperformance
- Produces dashboard-ready JSON

All data stored with timestamps so you can track underperforming days.
"""

import json
import math
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# ✏️  SITE THRESHOLDS — only these two values change between sites
# =============================================================================
DAILY_EXPECTED_KWH = 1400.0   # Average good day for this site (kWh)
DAILY_LOW_KWH      = 304.0    # Known worst/low production day (kWh)

# PV Yield column fallback — 0-based (A=0, B=1, C=2, D=3, E=4, F=5...)
PV_COLUMN_INDEX    = 4        # default = column E

# =============================================================================
# 🔒 SECRETS — set in GitHub repo Settings → Secrets → Actions
# =============================================================================
PLANT_NAME         = os.environ.get("PLANT_NAME", "Addo Spar")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# Site location for irradiation (Addo, Eastern Cape)
SITE_LATITUDE  = -33.5733
SITE_LONGITUDE = 25.7467

# =============================================================================
# FIXED CONFIG
# =============================================================================
PACE_THRESHOLD_PCT = 0.30    # check 1: alert if actual < 30% of curve-expected
OFFLINE_THRESHOLD  = 0.01    # kWh — treat as offline below this
HISTORY_DAYS       = 30      # Days to keep in rolling history

_HERE       = Path(__file__).parent
RAW_FILE    = _HERE / "data" / "raw_report.xlsx"
OUTPUT_FILE = _HERE / "data" / "processed.json"
HISTORY_FILE = _HERE / "data" / "history.json"
STATE_FILE  = _HERE / "data" / "alert_state.json"

SAST = timezone(timedelta(hours=2))


# =============================================================================
# Solar curve — Johannesburg seasonal sunrise/sunset + sine bell
# =============================================================================

def solar_window(month: int) -> tuple:
    """Seasonal sunrise/sunset for Johannesburg (26°S)."""
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
    """Fraction of day's total PV energy expected by end of `hour`."""
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise
    if solar_day <= 0:
        return 0.0
    elapsed = (hour + 1) - sunrise
    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Fetch irradiation data from Open-Meteo
# =============================================================================

def fetch_irradiation(date_str: str) -> list:
    """
    Fetch hourly GHI (Global Horizontal Irradiance) for the given date.
    Returns list of 24 hourly values in W/m².
    """
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": SITE_LATITUDE,
            "longitude": SITE_LONGITUDE,
            "hourly": "shortwave_radiation",
            "timezone": "Africa/Johannesburg",
            "start_date": date_str,
            "end_date": date_str,
        }
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        irradiation = data.get("hourly", {}).get("shortwave_radiation", [])
        # Ensure we have exactly 24 values
        while len(irradiation) < 24:
            irradiation.append(0)
        return [round(v if v else 0, 1) for v in irradiation[:24]]
    
    except Exception as e:
        print(f"  ⚠️  Irradiation fetch failed: {e}")
        return [0] * 24


# =============================================================================
# Parse the xlsx
# =============================================================================

def parse_report(filepath: Path) -> dict:
    df      = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = [str(h).strip() if not pd.isna(h) else "" for h in df.iloc[1].tolist()]

    pv_col = next(
        (i for i, h in enumerate(headers) if "PV Yield" in h),
        PV_COLUMN_INDEX,
    )
    print(f"  ℹ️  PV Yield column: index {pv_col} — '{headers[pv_col]}'")

    hourly      = [0.0] * 24
    total       = 0.0
    last_hour   = 0
    row_count   = 0
    report_date = None

    for idx in range(2, len(df)):
        row    = df.iloc[idx]
        ts_raw = row.iloc[0]
        if pd.isna(ts_raw):
            continue
        try:
            ts   = pd.Timestamp(ts_raw)
            hour = ts.hour
            if report_date is None:
                report_date = ts.strftime("%Y-%m-%d")
        except Exception:
            continue

        pv_val       = float(row.iloc[pv_col]) if not pd.isna(row.iloc[pv_col]) else 0.0
        hourly[hour] = round(pv_val, 4)
        total       += pv_val
        last_hour    = hour
        row_count   += 1

    return {
        "date":       report_date or datetime.now(SAST).strftime("%Y-%m-%d"),
        "total_kwh":  round(total, 3),
        "hourly":     hourly,
        "last_hour":  last_hour,
        "row_count":  row_count,
    }


# =============================================================================
# Historical data management
# =============================================================================

def load_history() -> dict:
    """Load historical data, returns dict of {date: {data}}"""
    if not HISTORY_FILE.exists():
        return {}
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Could not load history: {e}")
        return {}


def save_history(history: dict):
    """Save historical data and prune to last HISTORY_DAYS"""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Prune old entries
    cutoff = (datetime.now(SAST) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    history = {k: v for k, v in history.items() if k >= cutoff}
    
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def percentile(sorted_vals: list, p: float) -> float:
    """Calculate percentile from a pre-sorted list (p in 0-100)."""
    if not sorted_vals:
        return 0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    d = k - f
    return sorted_vals[f] + d * (sorted_vals[c] - sorted_vals[f])


def calculate_30day_stats(history: dict, exclude_date: str = None) -> dict:
    """
    Calculate 30-day statistics:
    - hourly_avg: average PV for each hour [0-23] over last 30 days
    - hourly_min: minimum PV for each hour
    - hourly_max: maximum PV for each hour
    - hourly_p10, hourly_p25, hourly_p75, hourly_p90: percentile bands
    - daily_min: lowest daily total
    - daily_max: highest daily total
    - daily_avg: average daily total
    """
    if not history:
        return {
            "hourly_avg": [0] * 24,
            "hourly_min": [0] * 24,
            "hourly_max": [0] * 24,
            "hourly_p10": [0] * 24,
            "hourly_p25": [0] * 24,
            "hourly_p75": [0] * 24,
            "hourly_p90": [0] * 24,
            "hourly_irrad_avg": [0] * 24,
            "daily_min": 0,
            "daily_max": 0,
            "daily_avg": 0,
            "sample_days": 0,
        }
    
    # Collect all hourly values across days
    hourly_values = [[] for _ in range(24)]  # 24 lists, one per hour
    daily_totals = []
    
    for date, day_data in history.items():
        if date == exclude_date:
            continue  # Skip today's partial data
        hourly = day_data.get("hourly", [0] * 24)
        total = day_data.get("total_kwh", 0)
        
        if total > 0:  # Only include days with actual data
            daily_totals.append(total)
            for hour in range(24):
                if hour < len(hourly):
                    hourly_values[hour].append(hourly[hour])
    
    # Calculate stats
    hourly_avg = [
        round(sum(vals) / len(vals), 2) if vals else 0
        for vals in hourly_values
    ]
    hourly_min = []
    for h in range(24):
        vals = hourly_values[h]
        if not vals:
            hourly_min.append(0)
        else:
            # For solar hours, exclude zeros (outage days) from min
            nonzero = [v for v in vals if v > 0]
            if nonzero:
                hourly_min.append(round(min(nonzero), 2))
            else:
                hourly_min.append(0)
    hourly_max = [
        round(max(vals), 2) if vals else 0
        for vals in hourly_values
    ]
    
    # Calculate percentile bands
    hourly_p10 = []
    hourly_p25 = []
    hourly_p75 = []
    hourly_p90 = []
    
    for hour in range(24):
        vals = sorted(hourly_values[hour])
        hourly_p10.append(round(percentile(vals, 10), 2))
        hourly_p25.append(round(percentile(vals, 25), 2))
        hourly_p75.append(round(percentile(vals, 75), 2))
        hourly_p90.append(round(percentile(vals, 90), 2))
    
    # Calculate average irradiation per hour
    irrad_values = [[] for _ in range(24)]
    for date, day_data in history.items():
        irrad = day_data.get("irradiation", [0] * 24)
        total = day_data.get("total_kwh", 0)
        if total > 0:
            for hour in range(24):
                if hour < len(irrad):
                    irrad_values[hour].append(irrad[hour])
    
    hourly_irrad_avg = [
        round(sum(v) / len(v), 1) if v else 0
        for v in irrad_values
    ]
    
    return {
        "hourly_avg": hourly_avg,
        "hourly_min": hourly_min,
        "hourly_max": hourly_max,
        "hourly_p10": hourly_p10,
        "hourly_p25": hourly_p25,
        "hourly_p75": hourly_p75,
        "hourly_p90": hourly_p90,
        "hourly_irrad_avg": hourly_irrad_avg,
        "daily_min": round(min(daily_totals), 1) if daily_totals else 0,
        "daily_max": round(max(daily_totals), 1) if daily_totals else 0,
        "daily_avg": round(sum(daily_totals) / len(daily_totals), 1) if daily_totals else 0,
        "sample_days": len(daily_totals),
    }


# =============================================================================
# Status checks
# =============================================================================

def determine_status(data: dict, month: int, stats: dict, irradiation: list = None) -> tuple:
    """
    Returns (status, alerts, debug).
    Uses 30-day stats and irradiation for smarter thresholds.
    If today's irradiation is low compared to the 30-day average,
    scale down expectations proportionally.
    """
    total           = data["total_kwh"]
    hour            = data["last_hour"]
    sunrise, sunset = solar_window(month)
    alerts          = {"offline": False, "pace_low": False, "total_low": False}

    # Offline
    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {
            "reason": "no generation detected",
            "curve_fraction": 0.0, "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "irrad_factor": 1.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    curve_frac = solar_curve_fraction(hour, month)

    # Too early — less than 10% of day's energy expected yet
    if curve_frac < 0.10:
        return "ok", alerts, {
            "reason": "too early to assess",
            "curve_fraction": round(curve_frac, 3),
            "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "irrad_factor": 1.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    # ── Irradiation scaling ────────────────────────────────────────
    # Compare today's cumulative irradiation (up to current hour) against
    # the 30-day average cumulative irradiation at the same hour.
    # If today's irradiation is e.g. 60% of average, scale expected
    # generation down by the same factor — low sun = low output is normal.
    irrad_factor = 1.0
    if irradiation and stats.get("hourly_irrad_avg"):
        avg_irrad = stats["hourly_irrad_avg"]
        today_cum = sum(irradiation[:hour + 1])
        avg_cum   = sum(avg_irrad[:hour + 1])
        if avg_cum > 0:
            irrad_factor = min(today_cum / avg_cum, 1.5)  # cap at 1.5×
            irrad_factor = max(irrad_factor, 0.1)          # floor at 0.1×
            print(f"  🌤️  Irrad factor: {irrad_factor:.2f} (today {today_cum:.0f} vs avg {avg_cum:.0f} W/m² cumulative)")

    expected_by_now  = effective_expected * curve_frac * irrad_factor
    pace_trigger     = expected_by_now * PACE_THRESHOLD_PCT
    projected_total  = total / curve_frac

    # Check 1: hourly pace (scaled by irradiation)
    if total < pace_trigger:
        alerts["pace_low"] = True

    # Check 2: projected daily total below known low day
    daily_min = stats.get("daily_min", DAILY_LOW_KWH)
    if daily_min < 1:
        daily_min = DAILY_LOW_KWH
    
    if projected_total < daily_min:
        alerts["total_low"] = True

    debug = {
        "curve_fraction":  round(curve_frac, 3),
        "expected_by_now": round(expected_by_now, 1),
        "irrad_factor":    round(irrad_factor, 3),
        "actual_kwh":      round(total, 2),
        "pace_trigger":    round(pace_trigger, 1),
        "projected_total": round(projected_total, 1),
        "low_day_kwh":     daily_min,
        "sunrise":         round(sunrise, 2),
        "sunset":          round(sunset, 2),
        "checks": {
            "pace_low":  alerts["pace_low"],
            "total_low": alerts["total_low"],
        },
    }

    status = "low" if (alerts["pace_low"] or alerts["total_low"]) else "ok"
    return status, alerts, debug


# =============================================================================
# Telegram
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠️  Telegram not configured — skipping")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("  ✅ Telegram alert sent")
            return True
        print(f"  ❌ Telegram error {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"  ❌ Telegram request failed: {e}")
        return False


def send_alerts(status: str, alerts: dict, data: dict, debug: dict):
    """Fires Telegram messages for issues and recovery."""
    now_str          = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total            = data["total_kwh"]
    hour             = data["last_hour"]
    expected_by_now  = debug.get("expected_by_now", 0)
    projected_total  = debug.get("projected_total", 0)
    low_day_kwh      = debug.get("low_day_kwh", DAILY_LOW_KWH)

    # Load previous status for recovery detection
    prev_status = "ok"
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    if alerts["offline"]:
        send_telegram(
            f"🔴 <b>{PLANT_NAME} — OFFLINE</b>\n"
            f"No generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"🕐 {now_str}"
        )

    else:
        # Check 1: pace alert
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{PLANT_NAME} — LOW PACE</b>\n"
                f"Generation is well behind the expected curve.\n"
                f"Actual so far:    <b>{total:.1f} kWh</b>\n"
                f"Expected by now:  <b>~{expected_by_now:.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )

        # Check 2: projected total alert
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{PLANT_NAME} — POOR DAY PROJECTED</b>\n"
                f"At current pace, today will finish below the 30-day minimum.\n"
                f"Actual so far:      <b>{total:.1f} kWh</b>\n"
                f"Projected end-day:  <b>~{projected_total:.0f} kWh</b>\n"
                f"30-day minimum:     <b>{low_day_kwh:.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )

        # Recovery: was bad, now ok
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
                f"System is back within normal range.\n"
                f"Total today: <b>{total:.1f} kWh</b> (as of {hour:02d}:00)\n"
                f"🕐 {now_str}"
            )

        # All clear
        if not alerts["pace_low"] and not alerts["total_low"] and status == "ok":
            print(f"  ✅ All checks passed — no alert needed")

    # Save state
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing: {PLANT_NAME}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not RAW_FILE.exists():
        print(f"❌ Raw file not found: {RAW_FILE}")
        sys.exit(1)

    now             = datetime.now(SAST)
    month           = now.month
    sunrise, sunset = solar_window(month)

    # Parse today's data
    print(f"📥 Reading: {RAW_FILE}")
    data = parse_report(RAW_FILE)
    
    # Fetch irradiation
    print(f"🌤️  Fetching irradiation data...")
    irradiation = fetch_irradiation(data["date"])
    
    # Load and update history
    print(f"📚 Loading historical data...")
    history = load_history()
    
    # Add today's data to history
    history[data["date"]] = {
        "total_kwh": data["total_kwh"],
        "hourly": data["hourly"],
        "irradiation": irradiation,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "last_hour": data["last_hour"],
    }
    
    save_history(history)
    
    # Calculate 30-day stats
    print(f"📊 Calculating 30-day statistics...")
    stats = calculate_30day_stats(history, exclude_date=data["date"])
    
    # Determine status
    status, alerts, debug = determine_status(data, month, stats, irradiation)

    print(f"  📅 Date:               {data['date']}")
    print(f"  ⚡ PV Yield:           {data['total_kwh']:.3f} kWh")
    print(f"  🕐 Last hour:          {data['last_hour']:02d}:00")
    print(f"  🌅 Solar window:       {sunrise:.1f}h – {sunset:.1f}h")
    print(f"  📈 30-day avg:         {stats['daily_avg']:.1f} kWh  (min: {stats['daily_min']:.1f}, max: {stats['daily_max']:.1f})")
    print(f"  📉 Sample days:        {stats['sample_days']}")
    print(f"  🎯 Expected by now:    {debug.get('expected_by_now', 0.0):.1f} kWh")
    print(f"  📊 Projected total:    {debug.get('projected_total', 0.0):.1f} kWh")
    print(f"  🚦 Status:             {status.upper()}")

    send_alerts(status, alerts, data, debug)

    # Prepare dashboard output
    output = {
        "plant": PLANT_NAME,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "date": data["date"],
        "total_kwh": data["total_kwh"],
        "last_hour": data["last_hour"],
        "status": status,
        "alerts": alerts,
        
        # Current day data
        "today": {
            "hourly_pv": data["hourly"],
            "irradiation": irradiation,
        },
        
        # ── FLAT ALIASES for backward-compat with overview dashboard ──
        "hourly_pv": data["hourly"],
        "irradiation": irradiation,
        
        # 30-day statistics (for chart baselines)
        "stats_30day": stats,
        
        # Historical data (last 30 days for export/filtering)
        "history": history,
        
        # Thresholds and debug
        "thresholds": {
            "expected_daily_kwh": DAILY_EXPECTED_KWH,
            "low_day_kwh": stats.get("daily_min", DAILY_LOW_KWH),
            "pace_threshold_pct": PACE_THRESHOLD_PCT,
            "solar_window": {
                "sunrise": round(sunrise, 2),
                "sunset": round(sunset, 2),
            },
        },
        "debug": debug,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Saved: {OUTPUT_FILE}")
    print("✅ Done!")


if __name__ == "__main__":
    main()
