"""
download_plant_data.py

Downloads the daily plant report from FusionSolar for a configured site.
Site name, credentials, and paths are all configurable via environment
variables or the SITE_CONFIG dict below.

Environment variables (set as secrets):
  FUSIONSOLAR_USERNAME  - your FusionSolar username
  FUSIONSOLAR_PASSWORD  - your FusionSolar password

Optional overrides:
  PLANT_NAME            - override the plant name to search for
  OUTPUT_FILE           - override the output xlsx path (default: data/raw_report.xlsx)
"""

import time
import random
import os
import sys
import subprocess
import socket
from pathlib import Path
from playwright.sync_api import sync_playwright

# =============================================================================
# ✏️  SITE CONFIGURATION — Edit this section to change the monitored site
# =============================================================================
SITE_CONFIG = {
    "plant_name": os.environ.get("PLANT_NAME", "Addo Spar"),
    # Output path is relative to this script's directory
    "output_file": os.environ.get(
        "OUTPUT_FILE",
        str(Path(__file__).parent / "data" / "raw_report.xlsx")
    ),
}

# =============================================================================
# FusionSolar URLs (rarely need changing)
# =============================================================================
FUSIONSOLAR_HOST = "intl.fusionsolar.huawei.com"
FUSIONSOLAR_BASE = f"https://{FUSIONSOLAR_HOST}"
LOGIN_URL        = FUSIONSOLAR_BASE
PORTAL_HOME      = (
    f"{FUSIONSOLAR_BASE}/uniportal/pvmswebsite/assets/build/cloud.html"
    "?app-id=smartpvms&instance-id=smartpvms"
    "&zone-id=region-7-075ad9fd-a8fc-46e6-8d88-e829f96a09b7"
    "#/home/list"
)
FALLBACK_IP = "119.8.160.213"


# =============================================================================
# Helpers
# =============================================================================

def fix_dns_resolution():
    """Ensure intl.fusionsolar.huawei.com resolves — patch /etc/hosts if needed."""
    print(f"🔍 Checking DNS for {FUSIONSOLAR_HOST}...")
    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS OK: {FUSIONSOLAR_HOST} → {ip}")
        return
    except socket.gaierror:
        print(f"  ⚠️  DNS failed, trying Google DNS fallback...")

    resolved_ip = None
    try:
        result = subprocess.run(
            ["dig", "+short", FUSIONSOLAR_HOST, "@8.8.8.8"],
            capture_output=True, text=True, timeout=10,
        )
        ips = [l.strip() for l in result.stdout.strip().split("\n")
               if l.strip() and not l.strip().endswith(".")]
        if ips:
            resolved_ip = ips[0]
            print(f"  ✅ Resolved via Google DNS: {resolved_ip}")
    except Exception:
        pass

    if not resolved_ip:
        resolved_ip = FALLBACK_IP
        print(f"  ⚠️  Using fallback IP: {resolved_ip}")

    hosts_entry = f"{resolved_ip} {FUSIONSOLAR_HOST}\n"
    try:
        with open("/etc/hosts", "r") as f:
            if FUSIONSOLAR_HOST in f.read():
                print("  ℹ️  Host entry already exists")
                return
        try:
            result = subprocess.run(
                ["sudo", "tee", "-a", "/etc/hosts"],
                input=hosts_entry, capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                raise RuntimeError("sudo tee failed")
        except Exception:
            with open("/etc/hosts", "a") as f:
                f.write(hosts_entry)
        print(f"  ✅ Added to /etc/hosts: {hosts_entry.strip()}")
    except Exception as e:
        print(f"  ❌ Could not fix DNS: {e}")
        sys.exit(1)

    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS now resolves: {FUSIONSOLAR_HOST} → {ip}")
    except socket.gaierror:
        print(f"  ❌ DNS still failing after patch")
        sys.exit(1)


def human_delay(min_s=3, max_s=7):
    delay = random.uniform(min_s, max_s)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def random_mouse_movement(page):
    try:
        vs = page.viewport_size
        if vs:
            page.mouse.move(
                random.randint(100, vs["width"] - 100),
                random.randint(100, vs["height"] - 100),
            )
    except Exception:
        pass


def type_human_like(field, text):
    for char in text:
        field.type(char, delay=random.randint(50, 150))


def find_search_field(page):
    """Try multiple strategies to locate the plant-search input."""
    strategies = [
        ("role textbox 'Plant name'",    lambda: page.get_by_role("textbox", name="Plant name")),
        ("placeholder 'Plant name'",     lambda: page.locator("input[placeholder*='Plant name']").first),
        ("placeholder 'plant' (lower)",  lambda: page.locator("input[placeholder*='plant']").first),
        ("placeholder 'search' (ci)",    lambda: page.locator("input[placeholder*='search' i]").first),
        ("role searchbox",               lambda: page.get_by_role("searchbox").first),
        ("visible text input",           lambda: page.locator("input[type='text']:visible").first),
        ("any visible input",            lambda: page.locator("input:visible").first),
    ]
    for name, strategy in strategies:
        try:
            field = strategy()
            if field.is_visible(timeout=3000):
                print(f"  ✅ Search field found via: {name}")
                return field
        except Exception:
            continue
    return None


# =============================================================================
# Main download function
# =============================================================================

def download_plant_data():
    plant_name  = SITE_CONFIG["plant_name"]
    output_file = Path(SITE_CONFIG["output_file"])

    print(f"🚀 Starting download for plant: '{plant_name}'")
    print(f"📁 Output file: {output_file}")

    fix_dns_resolution()

    username = os.environ.get("FUSIONSOLAR_USERNAME")
    password = os.environ.get("FUSIONSOLAR_PASSWORD")
    if not username or not password:
        print("❌ FUSIONSOLAR_USERNAME and FUSIONSOLAR_PASSWORD must be set as environment variables / secrets")
        sys.exit(1)
    print(f"🔐 Username: {username[:4]}***")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        print("🌐 Launching browser (headless)...")
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Johannesburg",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()

        try:
            # ── Step 1: Login ──────────────────────────────────────────────
            print("📱 Step 1: Navigating to FusionSolar login...")
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
            human_delay(5, 8)

            print("👤 Step 2: Entering credentials...")
            page.get_by_role("textbox", name="Username or email").fill(username)
            human_delay(1, 2)
            page.get_by_role("textbox", name="Password").click()
            page.get_by_role("textbox", name="Password").fill(password)
            human_delay(1, 2)
            page.get_by_text("Log In").click()
            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(7, 10)
            print(f"  📍 After login: {page.url[:80]}")

            # ── Step 2: Portal ─────────────────────────────────────────────
            print("🏠 Step 3: Navigating to portal...")
            page.goto(PORTAL_HOME, wait_until="networkidle", timeout=60000)
            human_delay(5, 8)
            random_mouse_movement(page)

            # ── Dismiss any modal dialogs before interacting ───────────────
            print("🔔 Checking for modal dialogs...")
            modal_dismissed = False
            modal_selectors = [
                # Close button inside dpdesign modal (the one seen in logs)
                ".dpdesign-modal-wrap .dpdesign-modal-close",
                ".dpdesign-modal-wrap button[aria-label='Close']",
                ".dpdesign-modal-wrap .dpdesign-icon-close",
                # Generic ant-design / common close patterns
                ".ant-modal-close",
                ".ant-modal-close-x",
                "button[aria-label='Close']",
                ".modal-close",
                # Any visible × / close button
                "button:has-text('×')",
                "button:has-text('✕')",
                "button:has-text('Close')",
                "button:has-text('OK')",
                "button:has-text('Got it')",
                "button:has-text('Confirm')",
            ]
            for sel in modal_selectors:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=2000):
                        btn.click()
                        print(f"  ✅ Dismissed modal via: {sel}")
                        modal_dismissed = True
                        human_delay(1, 2)
                        break
                except Exception:
                    continue
            if not modal_dismissed:
                # Try pressing Escape as a last resort
                try:
                    page.keyboard.press("Escape")
                    human_delay(0.5, 1)
                    print("  ℹ️  No modal found (or dismissed via Escape)")
                except Exception:
                    pass

            # ── Step 3: Search for plant ───────────────────────────────────
            print(f"🔎 Step 4: Searching for '{plant_name}'...")
            search_field = find_search_field(page)
            if not search_field:
                raise RuntimeError("Could not find a search/input field on the portal page")

            search_field.click()
            human_delay(1, 2)
            type_human_like(search_field, plant_name)
            human_delay(2, 3)

            # Click Search button or press Enter
            try:
                page.get_by_role("button", name="Search").click()
            except Exception:
                try:
                    page.locator("button:has-text('Search')").first.click()
                except Exception:
                    search_field.press("Enter")

            page.wait_for_load_state("networkidle", timeout=30000)
            human_delay(5, 8)

            # ── Step 4: Open plant page ────────────────────────────────────
            print(f"🏢 Step 5: Selecting '{plant_name}'...")
            try:
                page.get_by_role("link", name=plant_name).click()
            except Exception:
                page.get_by_text(plant_name).first.click()

            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(5, 8)
            random_mouse_movement(page)

            # ── Step 5: Report Management ──────────────────────────────────
            print("📊 Step 6: Opening Report Management...")
            page.get_by_text("Report Management").click()
            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(5, 8)
            random_mouse_movement(page)

            # ── Step 6: Export ─────────────────────────────────────────────
            print("📤 Step 7: Clicking Export...")
            page.get_by_role("button", name="Export").click()
            human_delay(5, 8)

            # ── Step 7: Download ───────────────────────────────────────────
            print("💾 Step 8: Downloading file...")
            with page.expect_download(timeout=30000) as dl_info:
                page.get_by_title("Download").first.click()
            download = dl_info.value
            download.save_as(output_file)
            print(f"✅ Downloaded to: {output_file}")

            # ── Step 8: Close dialog ───────────────────────────────────────
            try:
                page.get_by_role("button", name="Close").click()
            except Exception:
                pass
            human_delay(2, 4)

            print("✅ Download complete!")
            return str(output_file)

        except Exception as err:
            print(f"❌ Download failed: {err}")
            try:
                page.screenshot(path="error_screenshot.png", full_page=True)
                Path("error_page.html").write_text(page.content())
                print("📸 Debug files saved: error_screenshot.png, error_page.html")
            except Exception:
                pass
            raise

        finally:
            human_delay(2, 3)
            context.close()
            browser.close()
            print("🔒 Browser closed")


if __name__ == "__main__":
    try:
        download_plant_data()
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
