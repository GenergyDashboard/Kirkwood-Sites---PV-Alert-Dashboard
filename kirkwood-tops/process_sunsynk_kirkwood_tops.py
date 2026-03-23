"""
process_sunsynk_kirkwood_tops.py

Enhanced Sunsynk processor for Kirkwood Tops that:
- Uses snapshot-based hourly delta calculation (Sunsynk-specific)
- Stores historical daily data
- Fetches irradiation from Open-Meteo API
- Calculates 30-day hourly averages (bell curve baseline)
- Calculates 30-day min/max bands and percentiles (p10/p25/p75/p90)
- Sends Telegram alerts for underperformance
- Produces dashboard-ready JSON

Works with download_sunsynk_kirkwood_tops.py which saves snapshots every run.
"""

import json
import math
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# =============================================================================
# ✏️  SITE THRESHOLDS — edit directly here, do NOT set as GitHub secrets
# =============================================================================
DAILY_EXPECTED_KWH = 37.0    # Average good day for this site (kWh)
DAILY_LOW_KWH      = 17.0    # Known worst/low production day (kWh)

# =============================================================================
# 🔒 SECRETS — set in GitHub repo Settings → Secrets → Actions
# =============================================================================
PLANT_NAME         = os.environ.get("PLANT_NAME", "Kirkwood Tops")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# Site location for irradiation (Kirkwood, Eastern Cape)
SITE_LATITUDE  = -33.40047315873728
SITE_LONGITUDE = 25.44748345729338

# =============================================================================
# FIXED CONFIG
# =============================================================================
PACE_THRESHOLD_PCT = 0.30
OFFLINE_THRESHOLD  = 0.01
HISTORY_DAYS       = 30

SAST         = timezone(timedelta(hours=2))
_HERE        = Path(__file__).parent
SNAPSHOT     = _HERE / "data" / "sunsynk_snapshot.json"
PREV_SNAP    = _HERE / "data" / "sunsynk_snapshot_prev.json"
HOURLY_FILE  = _HERE / "data" / "sunsynk_hourly.json"
OUTPUT_FILE  = _HERE / "data" / "processed.json"
HISTORY_FILE = _HERE / "data" / "history.json"
STATE_FILE   = _HERE / "data" / "alert_state.json"


# =============================================================================
# Solar curve
# =============================================================================

def solar_window(month: int) -> tuple:
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
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
        while len(irradiation) < 24:
            irradiation.append(0)
        return [round(v if v else 0, 1) for v in irradiation[:24]]
    
    except Exception as e:
        print(f"  ⚠️  Irradiation fetch failed: {e}")
        return [0] * 24


# =============================================================================
# Hourly delta calculation (Sunsynk-specific)
# =============================================================================

def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Could not load {path}: {e}")
        return None


def build_hourly(current: dict, prev: dict | None, today: str) -> list:
    """
    Load persisted hourly accumulator for today, then apply the latest delta.
    Returns a 24-element list of kWh per hour.
    """
    acc_data = load_json(HOURLY_FILE)
    if acc_data and acc_data.get("date") == today:
        hourly = acc_data["hourly"]
    else:
        print(f"  ℹ️  New day ({today}) — resetting hourly accumulator")
        hourly = [0.0] * 24

    current_hour = current["hour"]

    if prev is None:
        print("  ℹ️  No previous snapshot — delta = 0 for this run")
        delta = 0.0
    elif prev.get("date") != today:
        print(f"  ℹ️  Previous snapshot is from {prev.get('date')} — delta = 0 (day rollover)")
        delta = 0.0
    else:
        delta = current["total_kwh"] - prev["total_kwh"]
        if delta < 0:
            print(f"  ⚠️  Negative delta ({delta:.3f} kWh) — clamping to 0")
            delta = 0.0
        print(f"  ⚡ Delta this run: {delta:.3f} kWh → hour {current_hour:02d}:00")

    hourly[current_hour] = round(hourly[current_hour] + delta, 4)

    HOURLY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HOURLY_FILE, "w") as f:
        json.dump({"date": today, "hourly": hourly}, f, indent=2)
    print(f"  💾 Hourly accumulator saved: {HOURLY_FILE}")

    return hourly


# =============================================================================
# Historical data management
# =============================================================================

def load_history() -> dict:
    if not HISTORY_FILE.exists():
        return {}
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Could not load history: {e}")
        return {}


def save_history(history: dict):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
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
    
    hourly_values = [[] for _ in range(24)]
    daily_totals = []
    
    for date, day_data in history.items():
        if date == exclude_date:
            continue  # Skip today's partial data
        hourly = day_data.get("hourly", [0] * 24)
        total = day_data.get("total_kwh", 0)
        
        if total > 0:
            daily_totals.append(total)
            for hour in range(24):
                if hour < len(hourly):
                    hourly_values[hour].append(hourly[hour])
    
    hourly_avg = [round(sum(v)/len(v), 2) if v else 0 for v in hourly_values]
    hourly_min = [round(min(v), 2) if v else 0 for v in hourly_values]
    hourly_max = [round(max(v), 2) if v else 0 for v in hourly_values]
    
    hourly_p10, hourly_p25, hourly_p75, hourly_p90 = [], [], [], []
    for hour in range(24):
        vals = sorted(hourly_values[hour])
        hourly_p10.append(round(percentile(vals, 10), 2))
        hourly_p25.append(round(percentile(vals, 25), 2))
        hourly_p75.append(round(percentile(vals, 75), 2))
        hourly_p90.append(round(percentile(vals, 90), 2))
    
    # Calculate average irradiation per hour
    irrad_values = [[] for _ in range(24)]
    for date, day_data in history.items():
        if date == exclude_date:
            continue
        irrad = day_data.get("irradiation", [0] * 24)
        total_chk = day_data.get("total_kwh", 0)
        if total_chk > 0:
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

def determine_status(total: float, last_hour: int, month: int, stats: dict, irradiation: list = None) -> tuple:
    alerts = {"offline": False, "pace_low": False, "total_low": False}
    sunrise, sunset = solar_window(month)

    # Nighttime — no alerts outside solar window
    if last_hour < int(sunrise) or last_hour >= int(sunset):
        return "ok", alerts, {
            "reason": "outside solar window (nighttime)",
            "curve_fraction": 0.0, "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "irrad_factor": 1.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    # Offline (during daylight)
    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {
            "reason": "no generation detected during daylight",
            "curve_fraction": 0.0, "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "irrad_factor": 1.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    curve_frac = solar_curve_fraction(last_hour, month)

    if curve_frac < 0.10:
        return "ok", alerts, {
            "reason": "too early to assess",
            "curve_fraction": round(curve_frac, 3),
            "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "irrad_factor": 1.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    # ── Dynamic thresholds from 30-day history ────────────────────
    effective_expected = stats.get("daily_avg", 0)
    if effective_expected < 1:
        effective_expected = DAILY_EXPECTED_KWH
    
    # ── Irradiation scaling ────────────────────────────────────────
    irrad_factor = 1.0
    if irradiation and stats.get("hourly_irrad_avg"):
        avg_irrad = stats["hourly_irrad_avg"]
        today_cum = sum(irradiation[:last_hour + 1])
        avg_cum   = sum(avg_irrad[:last_hour + 1])
        if avg_cum > 0:
            irrad_factor = min(today_cum / avg_cum, 1.5)
            irrad_factor = max(irrad_factor, 0.1)
            print(f"  🌤️  Irrad factor: {irrad_factor:.2f} (today {today_cum:.0f} vs avg {avg_cum:.0f} W/m² cumulative)")

    expected_by_now = effective_expected * curve_frac * irrad_factor
    pace_trigger    = expected_by_now * PACE_THRESHOLD_PCT
    projected_total = total / curve_frac

    if total < pace_trigger:
        alerts["pace_low"] = True
    
    daily_min = stats.get("daily_min", DAILY_LOW_KWH)
    if daily_min < 1:
        daily_min = DAILY_LOW_KWH
    
    # Scale minimum threshold by irradiation — cloudy day = lower floor
    adjusted_min = daily_min * irrad_factor if irrad_factor < 1.0 else daily_min
    if projected_total < adjusted_min:
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


def send_alerts(status: str, alerts: dict, total: float, last_hour: int, debug: dict):
    now_str         = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    expected_by_now = debug.get("expected_by_now", 0)
    projected_total = debug.get("projected_total", 0)
    low_day_kwh     = debug.get("low_day_kwh", DAILY_LOW_KWH)

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
            f"Total today: <b>{total:.2f} kWh</b> (as of {last_hour:02d}:00)\n"
            f"🕐 {now_str}"
        )
    else:
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{PLANT_NAME} — LOW PACE</b>\n"
                f"Generation is well behind the expected curve.\n"
                f"Actual so far:   <b>{total:.1f} kWh</b>\n"
                f"Expected by now: <b>~{expected_by_now:.0f} kWh</b>\n"
                f"Hour: {last_hour:02d}:00 | 🕐 {now_str}"
            )
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{PLANT_NAME} — POOR DAY PROJECTED</b>\n"
                f"At current pace, today will finish below the 30-day minimum.\n"
                f"Actual so far:     <b>{total:.1f} kWh</b>\n"
                f"Projected end-day: <b>~{projected_total:.0f} kWh</b>\n"
                f"30-day minimum:    <b>{low_day_kwh:.0f} kWh</b>\n"
                f"Hour: {last_hour:02d}:00 | 🕐 {now_str}"
            )
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
                f"System is back within normal range.\n"
                f"Total today: <b>{total:.1f} kWh</b> (as of {last_hour:02d}:00)\n"
                f"🕐 {now_str}"
            )
        if not alerts["pace_low"] and not alerts["total_low"] and status == "ok":
            print(f"  ✅ All checks passed — no alert needed")

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing Sunsynk: {PLANT_NAME}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    current = load_json(SNAPSHOT)
    if not current:
        print(f"❌ Snapshot not found: {SNAPSHOT}")
        sys.exit(1)

    prev    = load_json(PREV_SNAP)
    now     = datetime.now(SAST)
    month   = now.month
    today   = now.strftime("%Y-%m-%d")
    sunrise, sunset = solar_window(month)

    total     = current["total_kwh"]
    last_hour = current["hour"]

    print(f"  📅 Date:      {today}")
    print(f"  ⚡ Total kWh: {total}")
    print(f"  🕐 Hour:      {last_hour:02d}:00")
    if prev:
        print(f"  📦 Prev snap: {prev.get('timestamp','?')} → {prev.get('total_kwh','?')} kWh")

    hourly = build_hourly(current, prev, today)
    
    print(f"🌤️  Fetching irradiation data...")
    irradiation = fetch_irradiation(today)
    
    print(f"📚 Loading historical data...")
    history = load_history()
    
    history[today] = {
        "total_kwh": total,
        "hourly": hourly,
        "irradiation": irradiation,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "last_hour": last_hour,
    }
    
    save_history(history)
    
    print(f"📊 Calculating 30-day statistics...")
    stats = calculate_30day_stats(history, exclude_date=today)

    status, alerts, debug = determine_status(total, last_hour, month, stats, irradiation)

    print(f"  🌅 Solar window:    {sunrise:.1f}h – {sunset:.1f}h")
    print(f"  📈 30-day avg:      {stats['daily_avg']:.1f} kWh  (min: {stats['daily_min']:.1f}, max: {stats['daily_max']:.1f})")
    print(f"  📉 Sample days:     {stats['sample_days']}")
    print(f"  🎯 Expected by now: {debug.get('expected_by_now', 0.0):.1f} kWh")
    print(f"  📊 Projected total: {debug.get('projected_total', 0.0):.1f} kWh")
    print(f"  🚦 Status:          {status.upper()}")

    send_alerts(status, alerts, total, last_hour, debug)

    output = {
        "plant":        PLANT_NAME,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "date":         today,
        "total_kwh":    total,
        "last_hour":    last_hour,
        "status":       status,
        "alerts":       alerts,
        
        "today": {
            "hourly_pv": hourly,
            "irradiation": irradiation,
        },
        
        # ── FLAT ALIASES for backward-compat with overview dashboard ──
        "hourly_pv": hourly,
        "irradiation": irradiation,
        
        "stats_30day": stats,
        "history": history,
        
        "thresholds": {
            "expected_daily_kwh": DAILY_EXPECTED_KWH,
            "low_day_kwh":        stats.get("daily_min", DAILY_LOW_KWH),
            "pace_threshold_pct": PACE_THRESHOLD_PCT,
            "solar_window": {
                "sunrise": round(sunrise, 2),
                "sunset":  round(sunset,  2),
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
