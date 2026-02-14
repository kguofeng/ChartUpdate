"""
Crawl historical MMF Total AUM data from KOFIA FreeSIS.

Source: https://freesis.kofia.or.kr/stat/FreeSIS.do
Path:   Statistics > Fund > AUM of MMF by Period

Uses the "Trend" tab to fetch the full time series in one query,
rather than scraping day-by-day. Extracts the "Total" column under
"MMF Status" for every row (reference date).

Usage:
    # Normal run (headless)
    python crawl_mmf_aum.py

    # Show browser for debugging
    python crawl_mmf_aum.py --no-headless

    # Test mode with verbose logging
    python crawl_mmf_aum.py --test --no-headless

    # Custom date range
    python crawl_mmf_aum.py --start 2020-01-01 --end 2024-12-31

Requirements:
    pip install selenium pandas matplotlib
    Chrome/Chromium + matching chromedriver on PATH
"""

import os
import re
import sys
import time
import json
import argparse
import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KOFIA_MMF_URL = (
    "https://freesis.kofia.or.kr/stat/FreeSIS.do"
    "?parentDivId=MSIS40300000000000"
    "&serviceId=STATFND0400000050"
)

SCRIPT_DIR = Path(__file__).parent
CACHE_FILE = SCRIPT_DIR / "mmf_aum_cache.csv"
OUTPUT_FILE = SCRIPT_DIR / "mmf_aum_history.csv"
DEBUG_DIR = SCRIPT_DIR / "debug_screenshots"
CHART_FILE = SCRIPT_DIR / "mmf_aum_chart.png"

DEFAULT_START_DATE = "2017-01-01"
DEFAULT_END_DATE = datetime.today().strftime("%Y-%m-%d")

PAGE_LOAD_WAIT = 15
POST_CLICK_WAIT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_debug_steps = False
_step_counter = 0


def _step(driver, label: str, save_screenshot: bool = True):
    """Log a numbered step and optionally save a screenshot."""
    global _step_counter
    _step_counter += 1
    step_id = f"STEP-{_step_counter:04d}"
    logger.info("[%s] %s", step_id, label)

    if _debug_steps and save_screenshot and driver is not None:
        DEBUG_DIR.mkdir(exist_ok=True)
        ss_name = re.sub(r'[^\w\-]', '_', label[:40])
        ss_path = DEBUG_DIR / f"{step_id}_{ss_name}.png"
        try:
            driver.save_screenshot(str(ss_path))
        except Exception:
            pass
    return step_id


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def load_cache() -> pd.DataFrame:
    if CACHE_FILE.exists():
        df = pd.read_csv(CACHE_FILE, parse_dates=["date"])
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        return df
    return pd.DataFrame(columns=["date", "mmf_total_aum"])


def save_cache(df: pd.DataFrame) -> None:
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df.to_csv(CACHE_FILE, index=False)
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info("Cache saved (%d records) -> %s", len(df), OUTPUT_FILE)


# ---------------------------------------------------------------------------
# Browser setup
# ---------------------------------------------------------------------------

def create_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(3)
    return driver


# ---------------------------------------------------------------------------
# Helper: parse a number from text
# ---------------------------------------------------------------------------

def _parse_number(val) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-", ""):
        return None
    s = s.replace(",", "").replace(" ", "").replace("\xa0", "").replace("\u3000", "")
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Helper: click element robustly
# ---------------------------------------------------------------------------

def _click(driver, element, description="element"):
    """Click an element, falling back to JS click."""
    try:
        element.click()
        logger.info("  Clicked %s (direct)", description)
        return True
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].click();", element)
        logger.info("  Clicked %s (JS)", description)
        return True
    except Exception as e:
        logger.warning("  Failed to click %s: %s", description, e)
        return False


# ---------------------------------------------------------------------------
# Helper: set date in an input field
# ---------------------------------------------------------------------------

def _set_date_input(driver, element, date_str, label="date"):
    """Set a date value in an input element."""
    try:
        element.click()
        time.sleep(0.1)
        element.send_keys(Keys.CONTROL, "a")
        time.sleep(0.05)
        element.send_keys(date_str)
        element.send_keys(Keys.TAB)
        time.sleep(0.3)
        new_val = element.get_attribute("value") or ""
        logger.info("  Set %s: wanted='%s', got='%s'", label, date_str, new_val)
        return True
    except Exception as e:
        logger.warning("  Failed to set %s via send_keys: %s", label, e)

    # JS fallback
    try:
        driver.execute_script(
            """
            var el = arguments[0], val = arguments[1];
            var setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value').set;
            setter.call(el, val);
            el.dispatchEvent(new Event('input', {bubbles: true}));
            el.dispatchEvent(new Event('change', {bubbles: true}));
            """,
            element, date_str
        )
        time.sleep(0.3)
        new_val = element.get_attribute("value") or ""
        logger.info("  Set %s via JS: wanted='%s', got='%s'", label, date_str, new_val)
        return True
    except Exception as e:
        logger.warning("  Failed to set %s via JS: %s", label, e)
        return False


# ---------------------------------------------------------------------------
# Main crawl logic: use Trend tab for bulk time series
# ---------------------------------------------------------------------------

def crawl_mmf_trend(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    headless: bool = True,
) -> pd.DataFrame:
    """
    Navigate to the MMF AUM page, click the Trend tab,
    set the date range, click search, and extract the full time series.

    Uses 1-week blocks to ensure all data is captured.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Build 1-week windows
    CHUNK_DAYS = 7
    windows = []
    cur = start_dt
    while cur < end_dt:
        win_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end_dt)
        windows.append((cur, win_end))
        cur = win_end + timedelta(days=1)

    logger.info("Date range %s to %s -> %d query windows of %d days each",
                start_date, end_date, len(windows), CHUNK_DAYS)

    driver = create_driver(headless=headless)
    all_records = []

    try:
        # === Step 1: Load the page ===
        _step(driver, f"Loading {KOFIA_MMF_URL}")
        driver.get(KOFIA_MMF_URL)
        _step(driver, f"Waiting {PAGE_LOAD_WAIT}s for page load...", save_screenshot=False)
        time.sleep(PAGE_LOAD_WAIT)

        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass
        _step(driver, "Page loaded")

        # === Step 2: Click the Trend tab ===
        _step(driver, "Looking for Trend tab...")
        trend_tab = _find_and_click_tab(driver, "\ucd94\uc774")
        if not trend_tab:
            _step(driver, "ERROR: Could not find Trend tab")
            _dump_page_state(driver)
            return pd.DataFrame()

        time.sleep(3)
        _step(driver, "Trend tab clicked, waiting for tab content...")

        # === Step 3: Loop through weekly windows ===
        consecutive_failures = 0
        for win_idx, (win_start, win_end) in enumerate(windows):
            win_start_str = win_start.strftime("%Y-%m-%d")
            win_end_str = win_end.strftime("%Y-%m-%d")

            if win_idx % 10 == 0 or win_idx == len(windows) - 1:
                _step(driver, f"Window [{win_idx+1}/{len(windows)}] "
                      f"{win_start_str} to {win_end_str}", save_screenshot=False)

            # Set filters for this window
            _set_filters(driver, win_start_str, win_end_str)
            time.sleep(0.3)

            # Click search
            search_clicked = _click_search(driver)
            if not search_clicked:
                consecutive_failures += 1
                if consecutive_failures >= 10:
                    logger.error("Too many consecutive failures, stopping")
                    break
                continue

            # Brief wait for data to load
            time.sleep(1)

            # Extract data from this window
            df = _extract_trend_data(driver)
            if df is not None and len(df) > 0:
                all_records.append(df)
                consecutive_failures = 0

                # Log progress every 10 windows
                if win_idx % 10 == 0:
                    logger.info("  Window [%d]: got %d rows (%s to %s)",
                                win_idx + 1, len(df),
                                df['date'].min().strftime('%Y-%m-%d'),
                                df['date'].max().strftime('%Y-%m-%d'))

                # Periodic cache save every 50 windows
                if len(all_records) % 50 == 0:
                    combined = pd.concat(all_records, ignore_index=True)
                    combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
                    save_cache(combined)
                    logger.info("  Checkpoint: %d total records saved", len(combined))
            else:
                consecutive_failures += 1
                if consecutive_failures >= 10:
                    logger.error("Too many consecutive failures, stopping")
                    _dump_page_state(driver)
                    break

            # Small delay between requests
            time.sleep(0.5)

        # Combine all results
        if all_records:
            result = pd.concat(all_records, ignore_index=True)
            result = result.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            _step(driver, f"Total: extracted {len(result)} unique data points from "
                  f"{len(all_records)} windows")
            return result
        else:
            _step(driver, "No data extracted from any window")
            _dump_page_state(driver)
            return pd.DataFrame()

    finally:
        driver.quit()


def _find_and_click_tab(driver, tab_text: str) -> bool:
    """Find and click a tab by its Korean text."""
    # First try: find by visible text using XPath
    try:
        xpath_patterns = [
            f"//a[contains(text(), '{tab_text}')]",
            f"//span[contains(text(), '{tab_text}')]",
            f"//li[contains(text(), '{tab_text}')]",
            f"//div[contains(text(), '{tab_text}')]",
            f"//button[contains(text(), '{tab_text}')]",
            f"//*[contains(text(), '{tab_text}')]",
        ]
        for xpath in xpath_patterns:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                try:
                    text = el.text.strip()
                    if tab_text in text and len(text) < 20:
                        logger.info("  Found tab element: <%s> text='%s'",
                                   el.tag_name, text)
                        if _click(driver, el, f"tab '{tab_text}'"):
                            return True
                except (StaleElementReferenceException, WebDriverException):
                    continue
    except Exception as e:
        logger.debug("  XPath tab search failed: %s", e)

    # Second try: use JS to find by textContent
    try:
        result = driver.execute_script(f"""
            var allEls = document.querySelectorAll('a, span, li, button, div');
            for (var i = 0; i < allEls.length; i++) {{
                var el = allEls[i];
                var tc = el.textContent.trim();
                if (tc === '{tab_text}' || (tc.indexOf('{tab_text}') >= 0 && tc.length < 20)) {{
                    // Make sure it looks like a tab (clickable, not too deep)
                    if (el.offsetParent !== null) {{
                        el.click();
                        return true;
                    }}
                }}
            }}
            return false;
        """)
        if result:
            logger.info("  Clicked tab '%s' via JS textContent scan", tab_text)
            return True
    except Exception as e:
        logger.debug("  JS tab search failed: %s", e)

    logger.warning("  Could not find tab '%s'", tab_text)
    return False


def _set_filters(driver, start_date: str, end_date: str):
    """Set the search filters: date range, period type, etc."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # IMPORTANT: First select custom period mode
    _step(driver, "Selecting custom period mode...", save_screenshot=False)

    # Try multiple approaches to click custom period
    jijung_clicked = False

    # Approach 1: Find radio button with custom period label
    try:
        result = driver.execute_script("""
            // Find all radio buttons
            var radios = document.querySelectorAll('input[type="radio"]');
            for (var i = 0; i < radios.length; i++) {
                var radio = radios[i];
                var label = null;

                // Check for associated label
                if (radio.id) {
                    label = document.querySelector('label[for="' + radio.id + '"]');
                }
                // Check parent/sibling text
                var parentText = radio.parentElement ? radio.parentElement.textContent : '';

                if ((label && label.textContent.indexOf('\uc9c0\uc815') >= 0) ||
                    parentText.indexOf('\uc9c0\uc815') >= 0) {
                    radio.click();
                    radio.checked = true;
                    // Dispatch change event
                    radio.dispatchEvent(new Event('change', {bubbles: true}));
                    radio.dispatchEvent(new Event('click', {bubbles: true}));
                    return 'clicked radio for custom period';
                }
            }

            // Try clicking any element with custom period text
            var allEls = document.querySelectorAll('label, span, a, td, div');
            for (var i = 0; i < allEls.length; i++) {
                var el = allEls[i];
                var text = el.textContent.trim();
                if (text === '\uc9c0\uc815' || (text.indexOf('\uc9c0\uc815') >= 0 && text.length < 10)) {
                    el.click();
                    return 'clicked element: ' + text;
                }
            }
            return 'not found';
        """)
        logger.info("  Custom period selection: %s", result)
        if 'clicked' in result:
            jijung_clicked = True
            time.sleep(1)  # Wait for UI to update
    except Exception as e:
        logger.debug("  Custom period selection failed: %s", e)

    # Set date range
    _step(driver, "Setting date range...", save_screenshot=False)

    # Format dates for the Trend tab (uses YYYY/MM/DD)
    start_fmt = start_dt.strftime("%Y/%m/%d")
    end_fmt = end_dt.strftime("%Y/%m/%d")

    # Find date inputs - look for the ones with slash format (Trend tab)
    date_inputs = _find_date_inputs(driver)
    logger.info("  Found %d date inputs total", len(date_inputs))

    slash_inputs = []
    for inp in date_inputs:
        val = inp.get_attribute("value") or ""
        logger.info("  date_input: value='%s'", val)
        if "/" in val:
            slash_inputs.append(inp)

    if len(slash_inputs) >= 2:
        # Set start date with proper event dispatching
        _set_date_input_with_events(driver, slash_inputs[0], start_fmt, "start date")
        time.sleep(0.3)
        # Set end date
        _set_date_input_with_events(driver, slash_inputs[1], end_fmt, "end date")
        time.sleep(0.3)
    elif len(date_inputs) >= 2:
        _set_date_input_with_events(driver, date_inputs[0], start_fmt, "start date")
        time.sleep(0.3)
        _set_date_input_with_events(driver, date_inputs[1], end_fmt, "end date")
        time.sleep(0.3)

    # Ensure original setting is selected
    _step(driver, "Checking original setting selection...", save_screenshot=False)
    try:
        result = driver.execute_script("""
            var radios = document.querySelectorAll('input[type="radio"]');
            for (var i = 0; i < radios.length; i++) {
                var radio = radios[i];
                var parentText = radio.parentElement ? radio.parentElement.textContent : '';
                if (parentText.indexOf('\uc124\uc815\uc6d0\ubcf8') >= 0) {
                    if (!radio.checked) {
                        radio.click();
                        radio.checked = true;
                        radio.dispatchEvent(new Event('change', {bubbles: true}));
                        return 'clicked original setting radio';
                    }
                    return 'already selected';
                }
            }
            return 'not found';
        """)
        logger.info("  Original setting result: %s", result)
    except Exception as e:
        logger.debug("  Original setting selection failed: %s", e)

    _step(driver, "Filters configured")


def _set_date_input_with_events(driver, element, date_str, label="date"):
    """Set a date value and dispatch proper events to trigger updates."""
    try:
        # Clear and set value using JavaScript with proper event dispatching
        driver.execute_script("""
            var el = arguments[0];
            var val = arguments[1];

            // Focus the element
            el.focus();

            // Clear existing value
            el.value = '';

            // Set new value using native setter to trigger React/Vue bindings
            var nativeSetter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value').set;
            nativeSetter.call(el, val);

            // Dispatch all relevant events
            el.dispatchEvent(new Event('input', {bubbles: true}));
            el.dispatchEvent(new Event('change', {bubbles: true}));
            el.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true}));
            el.dispatchEvent(new Event('blur', {bubbles: true}));
        """, element, date_str)

        time.sleep(0.2)
        new_val = element.get_attribute("value") or ""
        logger.info("  Set %s: wanted='%s', got='%s'", label, date_str, new_val)
        return True
    except Exception as e:
        logger.warning("  Failed to set %s: %s", label, e)
        # Fallback to send_keys
        try:
            element.clear()
            element.send_keys(date_str)
            element.send_keys(Keys.TAB)
            return True
        except Exception:
            pass
        return False


def _find_date_inputs(driver) -> list:
    """Find all visible date input fields."""
    inputs = driver.find_elements(By.TAG_NAME, "input")
    date_inputs = []
    for inp in inputs:
        try:
            inp_type = (inp.get_attribute("type") or "").lower()
            if inp_type in ("hidden", "submit", "button", "checkbox", "radio", "image"):
                continue
            val = inp.get_attribute("value") or ""
            name = (inp.get_attribute("name") or "").lower()
            inp_id = (inp.get_attribute("id") or "").lower()
            cls = (inp.get_attribute("class") or "").lower()

            is_date = False
            # Check by value pattern
            if len(val) in (8, 10) and val[:4].isdigit():
                is_date = True
            # Check by name/id/class keywords
            date_kw = ["date", "dt", "ymd", "cal", "day", "yyyymmdd", "std_dt",
                       "search_dt", "from", "to", "start", "end", "strt", "sdate", "edate"]
            if any(kw in f"{name} {inp_id} {cls}" for kw in date_kw):
                is_date = True

            if is_date:
                try:
                    visible = inp.is_displayed()
                except Exception:
                    visible = True
                if visible:
                    logger.info("  Date input: name='%s' id='%s' value='%s'",
                               name, inp_id, val)
                    date_inputs.append(inp)
        except StaleElementReferenceException:
            continue
    return date_inputs


def _click_search(driver) -> bool:
    """Find and click the search button and wait for content refresh."""

    # First, dismiss any popup that might be open
    try:
        driver.execute_script("""
            var cancelBtns = document.querySelectorAll('a, button, input');
            for (var i = 0; i < cancelBtns.length; i++) {
                var btn = cancelBtns[i];
                var text = (btn.textContent || btn.value || '').trim();
                if (text === '\ucde8\uc18c' || text === 'Cancel') {
                    if (btn.offsetParent !== null) {
                        btn.click();
                        console.log('Clicked cancel button');
                    }
                }
            }
        """)
    except Exception:
        pass

    # Capture current state to detect changes
    old_state = driver.execute_script("""
        var text = document.body.textContent || '';
        var dates = text.match(/\\d{4}\\/\\d{2}\\/\\d{2}/g) || [];
        return {dates: dates.slice(0, 5).join(','), len: dates.length};
    """)

    # Click ONLY the search button
    try:
        result = driver.execute_script("""
            var btns = document.querySelectorAll('a, button, input[type=button], input[type=submit], img');
            for (var i = 0; i < btns.length; i++) {
                var btn = btns[i];
                if (btn.offsetParent === null) continue; // skip hidden

                var text = (btn.textContent || btn.value || btn.alt || btn.title || '').trim();

                // ONLY click search button
                if (text === '\uc870\ud68c') {
                    console.log('Clicking search button');
                    btn.click();
                    return 'clicked search';
                }
            }
            return 'no search button found';
        """)
        logger.info("  Search button: %s", result)
    except Exception as e:
        logger.debug("  Button click failed: %s", e)
        return False

    # Wait briefly for data to load
    time.sleep(2)

    # Check if a popup appeared and close it
    try:
        popup_closed = driver.execute_script("""
            // Check for popup with company search in title/header
            var popupTexts = ['\uc6b4\uc6a9/\ud310\ub9e4 \ud68c\uc0ac\uac80\uc0c9', '\ud68c\uc0ac\uac80\uc0c9', '\uc6b4\uc6a9\uc0ac\uac80\uc0c9'];
            var bodyText = document.body.textContent || '';

            for (var i = 0; i < popupTexts.length; i++) {
                if (bodyText.indexOf(popupTexts[i]) >= 0) {
                    // Find and click cancel button
                    var cancelBtns = document.querySelectorAll('a, button, input');
                    for (var j = 0; j < cancelBtns.length; j++) {
                        var btn = cancelBtns[j];
                        var text = (btn.textContent || btn.value || '').trim();
                        if (text === '\ucde8\uc18c' || text === 'Cancel' || text === '\ub2eb\uae30' || text === 'Close') {
                            if (btn.offsetParent !== null) {
                                btn.click();
                                return 'closed popup: ' + popupTexts[i];
                            }
                        }
                    }
                }
            }
            return 'no popup';
        """)
        if 'closed' in popup_closed:
            logger.info("  %s", popup_closed)
            time.sleep(0.5)
    except Exception:
        pass

    # Check if content changed
    new_state = driver.execute_script("""
        var text = document.body.textContent || '';
        var dates = text.match(/\\d{4}\\/\\d{2}\\/\\d{2}/g) || [];
        return {dates: dates.slice(0, 5).join(','), len: dates.length};
    """)

    if new_state.get('dates') != old_state.get('dates'):
        logger.info("  Content changed after clicking search")
        return True

    return True  # Continue anyway


# ---------------------------------------------------------------------------
# Data extraction from the grid
# ---------------------------------------------------------------------------

def _extract_trend_data(driver) -> pd.DataFrame | None:
    """
    Extract the time series data from the grid.
    Tries multiple methods: grid API, table HTML, div cells, page text.
    """
    # Method 1: Try to find and use the grid library's API directly
    _step(driver, "Method 1: Detecting grid library...", save_screenshot=False)
    df = _extract_via_grid_api(driver)
    if df is not None and len(df) > 0:
        return df

    # Method 2: Try pd.read_html on the page source
    _step(driver, "Method 2: Trying pd.read_html...", save_screenshot=False)
    df = _extract_via_read_html(driver)
    if df is not None and len(df) > 0:
        return df

    # Method 3: Inspect div grid DOM via JavaScript
    _step(driver, "Method 3: Inspecting div grid DOM...", save_screenshot=False)
    df = _extract_via_div_grid(driver)
    if df is not None and len(df) > 0:
        return df

    # Method 4: Full text content parsing
    _step(driver, "Method 4: Full text parsing...", save_screenshot=False)
    df = _extract_via_text(driver)
    if df is not None and len(df) > 0:
        return df

    return None


def _extract_via_grid_api(driver) -> pd.DataFrame | None:
    """Try to extract data from the grid's JavaScript API."""
    try:
        data = driver.execute_script("""
            var result = {lib: null, data: [], headers: []};

            // Scan window for grid/sheet objects with data methods
            var gridObj = null;
            var gridKey = null;
            for (var key in window) {
                try {
                    var obj = window[key];
                    if (!obj || typeof obj !== 'object') continue;

                    // IBSheet: GetCellValue method
                    if (typeof obj.GetCellValue === 'function') {
                        gridObj = obj;
                        gridKey = key;
                        result.lib = 'IBSheet/' + key;
                        break;
                    }
                    // TOAST UI Grid: getData method
                    if (typeof obj.getData === 'function' && typeof obj.getColumns === 'function') {
                        result.lib = 'tui-grid/' + key;
                        try {
                            var cols = obj.getColumns();
                            result.headers = cols.map(function(c) { return c.name || c.header; });
                            var rows = obj.getData();
                            result.data = rows.map(function(r) {
                                return result.headers.map(function(h) {
                                    return String(r[h] || '');
                                });
                            });
                        } catch(e) {}
                        if (result.data.length > 0) return result;
                    }
                } catch(e) {}
            }

            // If IBSheet found, extract systematically
            if (gridObj) {
                var colCount = 0;
                try {
                    if (typeof gridObj.ColCount === 'function') colCount = gridObj.ColCount();
                    else if (typeof gridObj.LastCol === 'function') colCount = gridObj.LastCol();
                    else colCount = 30;
                } catch(e) { colCount = 30; }

                // Get headers from row 0
                for (var c = 0; c < colCount; c++) {
                    try {
                        var h = gridObj.GetCellValue(0, c);
                        if (h === null || h === undefined) break;
                        result.headers.push(String(h));
                    } catch(e) { break; }
                }

                var rowCount = 0;
                try {
                    if (typeof gridObj.RowCount === 'function') rowCount = gridObj.RowCount();
                    else if (typeof gridObj.LastRow === 'function') rowCount = gridObj.LastRow();
                } catch(e) {}

                for (var r = 1; r <= Math.min(rowCount, 10000); r++) {
                    var row = [];
                    for (var c = 0; c < result.headers.length; c++) {
                        try {
                            row.push(String(gridObj.GetCellValue(r, c) || ''));
                        } catch(e) { row.push(''); }
                    }
                    result.data.push(row);
                }
            }

            return result;
        """)

        if not data:
            logger.info("  No grid API found")
            return None

        lib = data.get("lib")
        headers = data.get("headers", [])
        rows = data.get("data", [])
        logger.info("  Grid API: lib=%s, headers=%d, rows=%d", lib, len(headers), len(rows))

        if headers:
            logger.info("  Headers: %s", headers[:15])
        if rows:
            logger.info("  First row: %s", [s[:20] for s in rows[0][:15]] if rows[0] else [])
            logger.info("  Last row: %s", [s[:20] for s in rows[-1][:15]] if rows[-1] else [])

        if not rows or not headers:
            return None

        return _build_dataframe(headers, rows)

    except Exception as e:
        logger.info("  Grid API extraction failed: %s", e)
        return None


def _extract_via_read_html(driver) -> pd.DataFrame | None:
    """Try pd.read_html on the page source."""
    try:
        page_source = driver.page_source
        tables = pd.read_html(StringIO(page_source))
        logger.info("  pd.read_html found %d tables", len(tables))

        for i, df in enumerate(tables):
            full_text = df.to_string().lower()
            if "mmf" in full_text or "\uba38\ub2c8\ub9c8\ucf13" in full_text:
                logger.info("  Table[%d] has MMF data: shape=%s", i, df.shape)
                logger.info("  Columns: %s", [str(c)[:30] for c in df.columns][:15])
                if _debug_steps:
                    logger.info("  Content:\n%s", df.head(5).to_string())
                return _process_pandas_table(df)

        # If no MMF-specific table found, log what we have
        for i, df in enumerate(tables):
            logger.info("  Table[%d]: shape=%s cols=%s", i, df.shape,
                        [str(c)[:25] for c in df.columns][:10])
    except Exception as e:
        logger.info("  pd.read_html failed: %s", e)
    return None


def _extract_via_div_grid(driver) -> pd.DataFrame | None:
    """Extract data from div-based grid by inspecting DOM deeply."""
    try:
        data = driver.execute_script("""
            var result = {headers: [], rows: [], gridClasses: []};

            // Get unique grid class names
            var gridDivs = document.querySelectorAll(
                "div[class*='grid'], div[class*='Grid'], div[class*='sheet'], div[class*='Sheet']"
            );
            var classSet = new Set();
            for (var i = 0; i < gridDivs.length; i++) {
                classSet.add(gridDivs[i].className);
            }
            result.gridClasses = Array.from(classSet).slice(0, 30);

            // Look for header cells
            var headerSels = [
                "div[class*='header'] div[class*='cell']",
                "div[class*='Header'] div[class*='Cell']",
                "div[class*='hd'] div[class*='cell']",
                "thead th", "thead td",
                "div[class*='grid-header'] span",
            ];
            for (var s = 0; s < headerSels.length; s++) {
                var hdrs = document.querySelectorAll(headerSels[s]);
                if (hdrs.length > 2) {
                    for (var i = 0; i < hdrs.length; i++) {
                        var t = hdrs[i].textContent.trim();
                        if (t) result.headers.push(t);
                    }
                    break;
                }
            }

            // Look for data cells
            var dataSels = [
                "div[class*='body'] div[class*='row']",
                "div[class*='Body'] div[class*='Row']",
                "tbody tr",
                "div[class*='grid-body'] div[class*='row']",
            ];
            for (var s = 0; s < dataSels.length; s++) {
                var dataRows = document.querySelectorAll(dataSels[s]);
                if (dataRows.length > 0) {
                    for (var r = 0; r < Math.min(dataRows.length, 10000); r++) {
                        var cells = dataRows[r].querySelectorAll(
                            "div[class*='cell'], td, span[class*='cell']"
                        );
                        if (cells.length === 0) {
                            // Try children directly
                            cells = dataRows[r].children;
                        }
                        var row = [];
                        for (var c = 0; c < cells.length; c++) {
                            row.push(cells[c].textContent.trim());
                        }
                        if (row.length > 0 && row.some(function(v) { return v.length > 0; })) {
                            result.rows.push(row);
                        }
                    }
                    if (result.rows.length > 0) break;
                }
            }

            // Also get full textContent for analysis
            result.textContent = document.body.textContent.substring(0, 50000);

            return result;
        """)

        if data:
            logger.info("  Grid classes: %s", data.get("gridClasses", [])[:10])
            logger.info("  Headers found: %d -> %s", len(data.get("headers", [])),
                        data.get("headers", [])[:15])
            logger.info("  Data rows found: %d", len(data.get("rows", [])))

            if data.get("rows"):
                logger.info("  First row: %s",
                           [s[:20] for s in data["rows"][0]][:15] if data["rows"][0] else [])

            if _debug_steps and data.get("textContent"):
                DEBUG_DIR.mkdir(exist_ok=True)
                with open(DEBUG_DIR / "page_textContent.txt", "w", encoding="utf-8") as f:
                    f.write(data["textContent"])
                logger.info("  Page textContent saved to debug_screenshots/page_textContent.txt")

            headers = data.get("headers", [])
            rows = data.get("rows", [])
            if rows and len(rows) > 0:
                return _build_dataframe(headers, rows)

    except Exception as e:
        logger.info("  Div grid extraction failed: %s", e)
    return None


def _extract_via_text(driver) -> pd.DataFrame | None:
    """Parse page text for date + numeric patterns, focusing on grid content."""
    try:
        # Try to extract only from the visible grid area first
        text = driver.execute_script("""
            // Look for visible grid content containers
            var gridSelectors = [
                'div[class*="grid-body"]',
                'div[class*="GridBody"]',
                'div[class*="sheet-body"]',
                'div[class*="SheetBody"]',
                'div[class*="body"][class*="grid"]',
                'div[id*="grid"] div[class*="body"]',
                'div[class*="tui-grid"] div[class*="body"]',
            ];

            for (var i = 0; i < gridSelectors.length; i++) {
                var el = document.querySelector(gridSelectors[i]);
                if (el && el.offsetParent !== null && el.textContent.length > 100) {
                    return el.textContent;
                }
            }

            // Try to find by content - look for container with many date patterns
            var allDivs = document.querySelectorAll('div');
            var bestDiv = null;
            var bestCount = 0;
            for (var i = 0; i < allDivs.length; i++) {
                var div = allDivs[i];
                if (div.offsetParent === null) continue; // skip hidden
                var tc = div.textContent;
                if (tc.length < 100 || tc.length > 500000) continue;
                var dateMatches = tc.match(/\\d{4}\\/\\d{2}\\/\\d{2}/g);
                if (dateMatches && dateMatches.length > bestCount) {
                    // Make sure this div has actual data, not just menus
                    if (tc.indexOf('MMF\ud604\ud669') >= 0 || tc.indexOf('\uc804\uccb4') >= 0) {
                        bestCount = dateMatches.length;
                        bestDiv = div;
                    }
                }
            }
            if (bestDiv && bestCount >= 3) {
                return bestDiv.textContent;
            }

            // Fallback to body text
            return document.body.textContent || '';
        """)
        logger.info("  Extracted text length: %d chars", len(text))

        if _debug_steps:
            DEBUG_DIR.mkdir(exist_ok=True)
            with open(DEBUG_DIR / "full_text.txt", "w", encoding="utf-8") as f:
                f.write(text)

        # The page text has rows like:
        # 2026/02/032,326,634216,9572,109,676-64,20062,262
        # The date is YYYY/MM/DD and the first number after it is total MMF AUM
        # Numbers are comma-formatted (e.g., 2,326,634) and directly concatenated

        # Strategy: find all date occurrences and extract the number that follows
        # Pattern: date followed by a comma-formatted number
        # The numbers are concatenated, so "2,326,634216,957" means 2326634 then 216957
        # We need to find date + first large number

        records = []

        # Split text into chunks starting with dates
        date_pattern = r'(\d{4}/\d{2}/\d{2})'
        parts = re.split(date_pattern, text)

        for i in range(1, len(parts), 2):
            date_str = parts[i]
            if i + 1 < len(parts):
                after_date = parts[i + 1]
                # The first number after the date is the total value
                # Numbers use comma separators: e.g., "2,326,634"
                # They're concatenated: "2,326,634216,957" = 2326634 then 216957
                # A comma-formatted number: 1-3 digits, then (comma + 3 digits) repeated
                # This stops at "216" because it's not preceded by a comma
                num_match = re.match(r'(\d{1,3}(?:,\d{3})*)', after_date)
                if num_match:
                    val = _parse_number(num_match.group(1))
                    if val is not None and val > 1000:  # AUM should be large
                        try:
                            dt = pd.to_datetime(date_str)
                            records.append({"date": dt, "mmf_total_aum": val})
                            if len(records) <= 3 or len(records) % 100 == 0:
                                logger.info("  Parsed: %s -> %s", date_str, f"{val:,.0f}")
                        except Exception:
                            continue

        if records:
            logger.info("  Extracted %d date-value pairs from text", len(records))
            return pd.DataFrame(records)

        # Fallback: simpler pattern for date followed by whitespace and number
        pattern = r'(\d{4}[-/]\d{2}[-/]\d{2})\s+([\d,]+(?:\.\d+)?)'
        matches = re.findall(pattern, text)
        if matches:
            logger.info("  Found %d date-value pairs (fallback)", len(matches))
            for date_str, val_str in matches:
                val = _parse_number(val_str)
                if val is not None and val > 100:
                    try:
                        dt = pd.to_datetime(date_str)
                        records.append({"date": dt, "mmf_total_aum": val})
                    except Exception:
                        continue
            if records:
                return pd.DataFrame(records)

    except Exception as e:
        logger.info("  Text extraction failed: %s", e)
    return None


# ---------------------------------------------------------------------------
# DataFrame processing helpers
# ---------------------------------------------------------------------------

def _build_dataframe(headers: list, rows: list) -> pd.DataFrame | None:
    """Build a DataFrame from headers and rows, find date and MMF total columns."""
    if not rows:
        return None

    # Ensure all rows have the same length as headers
    max_cols = max(len(headers), max(len(r) for r in rows) if rows else 0)
    while len(headers) < max_cols:
        headers.append(f"col_{len(headers)}")

    # Pad short rows
    padded_rows = []
    for row in rows:
        r = list(row) + [""] * (max_cols - len(row))
        padded_rows.append(r[:max_cols])

    df = pd.DataFrame(padded_rows, columns=headers[:max_cols])
    logger.info("  Built DataFrame: shape=%s", df.shape)
    logger.info("  Columns: %s", list(df.columns)[:15])

    # Find date column
    date_col = None
    for col in df.columns:
        cl = str(col).lower()
        if "\uae30\uc900\uc77c\uc790" in cl or "\uc77c\uc790" in cl or "date" in cl or "\ub0a0\uc9dc" in cl:
            date_col = col
            break
    if date_col is None:
        # Try first column if it looks like dates
        first_col = df.columns[0]
        sample_vals = df[first_col].dropna().head(5).astype(str)
        if sample_vals.str.match(r'\d{4}[-/]\d{2}[-/]\d{2}').any():
            date_col = first_col

    # Find MMF total column
    mmf_col = None
    for col in df.columns:
        cl = str(col)
        if "\uc804\uccb4" in cl and ("mmf" in cl.lower() or "\uba38\ub2c8\ub9c8\ucf13" in cl.lower()):
            mmf_col = col
            break
    if mmf_col is None:
        for col in df.columns:
            cl = str(col)
            if "\uc804\uccb4" in cl:
                mmf_col = col
                break
    if mmf_col is None:
        for col in df.columns:
            cl = str(col).lower()
            if "mmf" in cl or "\uba38\ub2c8\ub9c8\ucf13" in cl:
                mmf_col = col
                break

    logger.info("  Date column: %s", date_col)
    logger.info("  MMF column: %s", mmf_col)

    if date_col is None or mmf_col is None:
        logger.info("  Could not identify date or MMF column, returning raw data")
        # Try to return what we can
        if date_col and mmf_col is None:
            # Use the second numeric column
            for col in df.columns[1:]:
                if df[col].astype(str).str.replace(",", "").str.match(r'^\d+\.?\d*$').any():
                    mmf_col = col
                    break

    if date_col and mmf_col:
        result = pd.DataFrame()
        result["date"] = pd.to_datetime(df[date_col], errors="coerce")
        result["mmf_total_aum"] = df[mmf_col].apply(_parse_number)
        result = result.dropna(subset=["date", "mmf_total_aum"])
        logger.info("  Final result: %d rows", len(result))
        if len(result) > 0:
            logger.info("  Date range: %s to %s", result["date"].min(), result["date"].max())
            logger.info("  AUM range: %s to %s",
                        f"{result['mmf_total_aum'].min():,.0f}",
                        f"{result['mmf_total_aum'].max():,.0f}")
        return result

    return None


def _process_pandas_table(df: pd.DataFrame) -> pd.DataFrame | None:
    """Process a pandas-parsed table into date + mmf_total_aum format."""
    # Flatten multi-index columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

    return _build_dataframe(list(df.columns.astype(str)), df.values.tolist())


# ---------------------------------------------------------------------------
# Debugging helper
# ---------------------------------------------------------------------------

def _dump_page_state(driver):
    """Save comprehensive debug info about the current page state."""
    DEBUG_DIR.mkdir(exist_ok=True)

    try:
        driver.save_screenshot(str(DEBUG_DIR / "debug_screenshot.png"))
    except Exception:
        pass

    try:
        with open(DEBUG_DIR / "page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info("  Page source saved to debug_screenshots/page_source.html")
    except Exception:
        pass

    try:
        text = driver.execute_script("return document.body.textContent || ''")
        with open(DEBUG_DIR / "page_text.txt", "w", encoding="utf-8") as f:
            f.write(text)
        logger.info("  Page text saved (%d chars)", len(text))
    except Exception:
        pass

    # Log all visible elements
    try:
        info = driver.execute_script("""
            var result = {
                title: document.title,
                url: window.location.href,
                inputs: [],
                links: [],
                gridDivs: 0,
                tables: document.querySelectorAll('table').length,
                iframes: document.querySelectorAll('iframe').length,
            };

            var inputs = document.querySelectorAll('input');
            for (var i = 0; i < inputs.length; i++) {
                var inp = inputs[i];
                if (inp.type !== 'hidden') {
                    result.inputs.push({
                        type: inp.type, name: inp.name, id: inp.id,
                        value: (inp.value || '').substring(0, 30),
                        class: (inp.className || '').substring(0, 50)
                    });
                }
            }

            var links = document.querySelectorAll('a');
            for (var i = 0; i < Math.min(links.length, 30); i++) {
                var a = links[i];
                var t = a.textContent.trim();
                if (t) {
                    result.links.push(t.substring(0, 50));
                }
            }

            result.gridDivs = document.querySelectorAll(
                "div[class*='grid'], div[class*='Grid']"
            ).length;

            return result;
        """)
        logger.info("  Page state: title='%s' url='%s'", info["title"], info["url"])
        logger.info("  Tables: %d, Iframes: %d, Grid divs: %d",
                    info["tables"], info["iframes"], info["gridDivs"])
        logger.info("  Inputs: %s", info["inputs"])
        logger.info("  Links: %s", info["links"][:20])
    except Exception as e:
        logger.info("  Page state dump failed: %s", e)


# ---------------------------------------------------------------------------
# Chart plotting
# ---------------------------------------------------------------------------

def plot_mmf_chart(df: pd.DataFrame, output_path: Path = CHART_FILE):
    """Plot MMF Total AUM over time."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib.ticker as mticker
    except ImportError:
        logger.warning("matplotlib not installed. Run: pip install matplotlib")
        return

    if df.empty:
        logger.warning("No data to plot")
        return

    df = df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])

    # Remove duplicates, keeping last value per date
    df = df.drop_duplicates(subset=["date"], keep="last")

    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot as markers + line, but break line at gaps > 5 days
    # This prevents linear interpolation across weekends/holidays
    dates = df["date"].values
    values = df["mmf_total_aum"].values

    # Find segments (break when gap > 5 days)
    segments_x = []
    segments_y = []
    current_x = [dates[0]]
    current_y = [values[0]]

    for i in range(1, len(dates)):
        gap_days = (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i-1])).days
        if gap_days > 5:
            # End current segment, start new one
            segments_x.append(current_x)
            segments_y.append(current_y)
            current_x = [dates[i]]
            current_y = [values[i]]
        else:
            current_x.append(dates[i])
            current_y.append(values[i])

    # Add final segment
    segments_x.append(current_x)
    segments_y.append(current_y)

    # Plot each segment
    for seg_x, seg_y in zip(segments_x, segments_y):
        ax.plot(seg_x, seg_y, linewidth=1.0, color="#1f77b4")

    # Add light fill under the full data
    ax.fill_between(df["date"], df["mmf_total_aum"], alpha=0.1, color="#1f77b4")

    ax.set_title("Korea MMF Total AUM Over Time", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("MMF Total AUM")

    # Format y-axis with commas
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f"{x:,.0f}"
    ))

    # Format x-axis dates
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    plt.xticks(rotation=45)

    ax.grid(True, alpha=0.3)
    ax.set_xlim(df["date"].min(), df["date"].max())

    # Add last data date annotation
    last_date = df["date"].max()
    last_val = df.loc[df["date"] == last_date, "mmf_total_aum"].values[0]
    fig.text(0.98, 0.98, f"Last data: {last_date.strftime('%d %b %Y')}", transform=fig.transFigure,
             fontsize=10, ha='right', va='top',
             bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))

    # Add latest value annotation
    ax.annotate(f"{last_val:,.0f}", xy=(last_date, last_val),
                xytext=(5, 0), textcoords='offset points', fontsize=9, color="#1f77b4",
                bbox=dict(facecolor='white', edgecolor="#1f77b4", alpha=0.8, boxstyle='round,pad=0.2'))

    plt.tight_layout()
    plt.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Chart saved to %s", output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    global _debug_steps

    parser = argparse.ArgumentParser(
        description="Crawl historical MMF Total AUM from KOFIA FreeSIS (Trend tab)"
    )
    parser.add_argument("--start", default=None,
                        help="Start date YYYY-MM-DD (default: auto from cache or 2007-01-01)")
    parser.add_argument("--end", default=DEFAULT_END_DATE,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show the browser window")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: verbose DEBUG logging + screenshots")
    parser.add_argument("--no-chart", action="store_true",
                        help="Skip chart generation")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore cache and fetch full date range from 2007")
    args = parser.parse_args()

    if args.test:
        logging.getLogger().setLevel(logging.DEBUG)
        _debug_steps = True
        logger.info("TEST MODE: verbose logging enabled")
        logger.info("Screenshots will be saved to %s", DEBUG_DIR)

    # Load existing cache
    cache_df = load_cache()
    logger.info("Existing cache: %d records", len(cache_df))

    # Determine start date: use cache's latest date if available (incremental update)
    if args.start:
        start_date = args.start
        logger.info("Using explicit start date: %s", start_date)
    elif args.full_refresh or len(cache_df) == 0:
        start_date = DEFAULT_START_DATE
        logger.info("Full refresh from: %s", start_date)
    else:
        # Incremental: start from day after cache's latest date
        cache_max_date = cache_df["date"].max()
        start_date = (cache_max_date + timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("Incremental update: cache has data up to %s, fetching from %s",
                    cache_max_date.strftime("%Y-%m-%d"), start_date)

        # Check if we already have today's data
        end_dt = datetime.strptime(args.end, "%Y-%m-%d")
        if cache_max_date >= end_dt:
            logger.info("Cache already has data up to %s, nothing to fetch",
                        cache_max_date.strftime("%Y-%m-%d"))
            if not args.no_chart:
                plot_mmf_chart(cache_df)
            return

    # Crawl new data
    new_df = crawl_mmf_trend(
        start_date=start_date,
        end_date=args.end,
        headless=not args.no_headless,
    )

    # Merge with cache
    if new_df is not None and len(new_df) > 0:
        if len(cache_df) > 0:
            combined = pd.concat([cache_df, new_df], ignore_index=True)
        else:
            combined = new_df
        save_cache(combined)
        logger.info("Done. Total records: %d", len(combined))
        logger.info("Date range: %s to %s",
                    combined["date"].min(), combined["date"].max())

        # Plot chart
        if not args.no_chart:
            plot_mmf_chart(combined)
    else:
        logger.warning("No new data extracted.")
        if len(cache_df) > 0:
            logger.info("Using existing cache (%d records)", len(cache_df))
            if not args.no_chart:
                plot_mmf_chart(cache_df)


if __name__ == "__main__":
    main()
