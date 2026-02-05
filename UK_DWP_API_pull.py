import os
import time
import random
import re
import itertools
from datetime import datetime
from pathlib import Path
import matplotlib.dates as mdates
import requests
import pandas as pd
import matplotlib.pyplot as plt
from xbbg import blp


G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = Path(r"O:\Tian\Portal\Charts\ChartDataBase")  # general chart directory
FONTSIZE = 14

# -----------------------------
# Stat-Xplore Open Data API
# -----------------------------
BASE_URL = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"

DB_ID = "str:database:UC_Monthly"  # People on Universal Credit

MEASURE_ID = "str:count:UC_Monthly:V_F_UC_CASELOAD_FULL"

# IMPORTANT: Month/time comes from the date dimension table, not V_F_UC_CASELOAD_FULL
MONTH_FIELD_ID = "str:field:UC_Monthly:F_UC_DATE:DATE_NAME"

COND_FIELD_ID = "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME"

# From your console output:
NO_WORK_VALUE_ID = (
    "str:value:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME:"
    "C_UC_CONDITIONALITY_REGIME:BC"
)

API_KEY = '65794a30655841694f694a4b563151694c434a68624763694f694a49557a49314e694a392e65794a7063334d694f694a7a644849756333526c6247786863694973496e4e3159694936496d746e6457396d5a57356e4d5445785147647459576c734c6d4e7662534973496d6c68644349364d5463324e7a4d7a4f5441774d4377695958566b496a6f69633352794c6d396b59534a392e775a546b38737577764158553750433476376c327155796f6a714e676f6e334c4463497179357438617673'

if not API_KEY:
    raise RuntimeError("Set STATXPLORE_API_KEY env var first.")

session = requests.Session()
session.headers.update({
    "APIKey": API_KEY,
    "Accept-Language": "en",
    "User-Agent": "statxplore-python/1.0",
})


RETRIABLE = {429, 502, 503, 504}

def request_json(method: str, url: str, *, params=None, json_body=None, timeout=180, max_attempts=8):
    """HTTP with retry/backoff for occasional Stat-Xplore 503s."""
    for attempt in range(1, max_attempts + 1):
        resp = session.request(method, url, params=params, json=json_body, timeout=timeout)

        if resp.status_code in RETRIABLE:
            ra = resp.headers.get("Retry-After")
            if ra and ra.isdigit():
                delay = int(ra)
            else:
                delay = min(60, 2 ** (attempt - 1)) + random.uniform(0, 0.5)

            if attempt == max_attempts:
                raise requests.HTTPError(
                    f"{resp.status_code} from {url}\n{resp.text[:2000]}",
                    response=resp
                )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError("Unreachable")


def find_cube_values(obj):
    """Find the first flat list of numeric-like values inside the cube."""
    def looks_like_values_list(x):
        if not isinstance(x, list) or any(isinstance(v, (list, dict)) for v in x):
            return False
        for v in x:
            if v is None:
                continue
            if isinstance(v, (int, float)):
                continue
            if isinstance(v, str):
                # allow numeric strings and some common non-numeric markers
                try:
                    float(v)
                except ValueError:
                    return False
            else:
                return False
        return True

    if looks_like_values_list(obj):
        return obj

    if isinstance(obj, dict):
        for k in ("values", "value", "cells", "data"):
            if k in obj:
                hit = find_cube_values(obj[k])
                if hit is not None:
                    return hit
        for v in obj.values():
            hit = find_cube_values(v)
            if hit is not None:
                return hit

    if isinstance(obj, list):
        for v in obj:
            hit = find_cube_values(v)
            if hit is not None:
                return hit

    return None


def get_first_cube(resp_json: dict):
    cubes = resp_json.get("cubes")
    if cubes is None:
        raise RuntimeError(f"No 'cubes' in response. Full response keys: {list(resp_json.keys())}")

    if isinstance(cubes, list):
        if not cubes:
            raise RuntimeError("Response 'cubes' is an empty list.")
        return cubes[0]

    if isinstance(cubes, dict):
        # sometimes APIs return dict keyed by measure
        first = next(iter(cubes.values()), None)
        if first is None:
            raise RuntimeError("Response 'cubes' is an empty dict.")
        return first

    raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")


import itertools
import pandas as pd

def _flatten_listlike(x):
    """Recursively flatten nested Python lists/tuples into a single flat list."""
    out = []
    def rec(v):
        if isinstance(v, (list, tuple)):
            for w in v:
                rec(w)
        else:
            out.append(v)
    rec(x)
    return out

def _extract_cube_values_flat(resp_json: dict, measure_idx: int = 0):
    """
    Stat-Xplore 'cubes' often looks like:
      cubes: [ { "<measure-uri>": [[...values...]] } ]
    or other small variations. This returns a flat list of cell values.
    """
    cubes = resp_json.get("cubes")

    if cubes is None:
        raise RuntimeError(f"No 'cubes' in response. Keys: {list(resp_json.keys())}")

    # cubes can be list or dict depending on parser/endpoint variants
    if isinstance(cubes, list):
        cube = cubes[measure_idx]
    elif isinstance(cubes, dict):
        cube = list(cubes.values())[measure_idx]
    else:
        raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")

    # cube item is often a dict keyed by measure uri
    if isinstance(cube, dict):
        if len(cube) == 1:
            cube_payload = next(iter(cube.values()))
        elif "values" in cube:
            cube_payload = cube["values"]
        else:
            # fallback: take first value
            cube_payload = next(iter(cube.values()))
    else:
        cube_payload = cube

    # flatten whatever nesting exists (R does unlist())
    return _flatten_listlike(cube_payload)

def _expand_grid_like_tidyr(lists):
    """
    Mimic tidyr::expand_grid ordering used by statxplorer:
    first field varies fastest.
    """
    rev = lists[::-1]
    for prod in itertools.product(*rev):
        yield prod[::-1]

def table_to_df(resp_json: dict) -> pd.DataFrame:
    fields = resp_json["fields"]
    field_labels = [f["label"] for f in fields]

    items_labels = []
    for f in fields:
        labels = []
        for it in f["items"]:
            lbl = it.get("labels")
            if isinstance(lbl, list):
                labels.append(" / ".join(map(str, lbl)))
            else:
                labels.append(str(lbl))
        items_labels.append(labels)

    combos = list(_expand_grid_like_tidyr(items_labels))
    values = _extract_cube_values_flat(resp_json, measure_idx=0)

    if len(values) != len(combos):
        # useful debugging if it ever happens again
        raise RuntimeError(
            f"Mismatch: {len(values)} values vs {len(combos)} combos. "
            f"Field sizes = {[len(x) for x in items_labels]}. "
            f"First cube type = {type(resp_json.get('cubes'))}."
        )

    df = pd.DataFrame(combos, columns=field_labels)
    measure_label = resp_json["measures"][0]["label"]
    df[measure_label] = pd.to_numeric(values, errors="coerce")
    return df



def parse_month(s: str):
    s = str(s).strip()
    # Typical Stat-Xplore month labels are like "May 2013"
    for fmt in ("%b %Y", "%B %Y", "%Y-%m", "%b-%Y"):
        try:
            return pd.to_datetime(datetime.strptime(s, fmt).date())
        except ValueError:
            pass
    return pd.to_datetime(s, errors="coerce")

def _field_label_by_id(resp_json: dict, field_id: str, fallback_idx: int) -> str:
    """
    Best-effort lookup of a field label by id/uri/name.
    Falls back to the field at fallback_idx if no match found.
    """
    for f in resp_json.get("fields", []):
        if f.get("id") == field_id or f.get("uri") == field_id or f.get("name") == field_id:
            return f.get("label", resp_json["fields"][fallback_idx]["label"])
    return resp_json["fields"][fallback_idx]["label"]

def _normalize_monthly_series(s: pd.Series, name: str) -> pd.Series:
    if s is None or s.empty:
        raise RuntimeError(f"{name} series is empty after pull.")
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]
    s.index = s.index.to_period("M").to_timestamp("M")
    return s

# Build query:
# Month on one axis, Conditionality on the other, but recoded to ONLY "No work requirements".
query = {
    "database": DB_ID,
    "measures": [MEASURE_ID],
    "recodes": {
        COND_FIELD_ID: {
            "map": [[NO_WORK_VALUE_ID]],
            "total": False
        }
    },
    "dimensions": [
        [MONTH_FIELD_ID],
        [COND_FIELD_ID]
    ]
}

resp = request_json("POST", f"{BASE_URL}/table", json_body=query)
df = table_to_df(resp)

month_col = _field_label_by_id(resp, MONTH_FIELD_ID, fallback_idx=0)
cond_col = _field_label_by_id(resp, COND_FIELD_ID, fallback_idx=1)
measure_label = resp["measures"][0]["label"]

# Keep only the single conditionality value (if multiple returned); drop totals; parse month
if cond_col in df.columns and df[cond_col].nunique(dropna=True) > 1:
    df = df[df[cond_col].str.lower().str.contains("no work", na=False)].copy()
df = df[df[month_col].str.lower() != "total"].copy()
df["Month"] = df[month_col].map(parse_month)
df = df.dropna(subset=["Month"]).sort_values("Month")

# --- Build the DWP series (from your df) ---
dwp_series = pd.Series(df[measure_label].values, index=pd.to_datetime(df["Month"]))
dwp_series.name = "Universal Claimants: No work requirements"
dwp_series = _normalize_monthly_series(dwp_series, "DWP")
if dwp_series.dropna().empty:
    raise RuntimeError("DWP series contains no valid data after cleaning.")
last_dwp = dwp_series.dropna().index.max()

# --- Pull Bloomberg series ---
START_DATE = dwp_series.index.min().to_pydatetime()
END_DATE = datetime.today()

tickers = ["UKLFLF69 Index"]
bbg = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
if isinstance(bbg.columns, pd.MultiIndex):
    bbg.columns = bbg.columns.get_level_values(0)
bbg.index = pd.to_datetime(bbg.index)

bbg_series = bbg["UKLFLF69 Index"].dropna()
bbg_series.name = "UK Inactive Long-Term Sick"
if bbg_series.empty:
    raise RuntimeError("Bloomberg series 'UKLFLF69 Index' returned no data.")
last_bbg = bbg_series.index[-1]

# --- Plot ---
PLOT_START = datetime(2015, 1, 1)

fig, ax = plt.subplots(figsize=(12, 6))

# Left axis: DWP
mask_dwp = dwp_series.index >= PLOT_START
ax.plot(
    dwp_series.index[mask_dwp],
    dwp_series[mask_dwp],
    color="#1f77b4",
    linewidth=2,
    label="Universal Claimants: No work requirements"
)
ax.set_xlabel("Date", fontsize=12)
ax.set_ylabel("Universal Claimants", color="#1f77b4", fontsize=12)
ax.tick_params(axis="y", labelcolor="#1f77b4")

ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.xticks(rotation=45)

# Right axis: Bloomberg
ax2 = ax.twinx()
mask_bbg = bbg_series.index >= PLOT_START
ax2.plot(
    bbg_series.index[mask_bbg],
    bbg_series[mask_bbg],
    color="#ff7f0e",
    linewidth=2,
    alpha=0.9,
    label="UK Inactive Long-Term Sick"
)
ax2.set_ylabel("UK Inactive Long-Term Sick", color="#ff7f0e", fontsize=12)
ax2.tick_params(axis="y", labelcolor="#ff7f0e")

# X limits
ax.set_xlim(PLOT_START, max(last_dwp, last_bbg))

ax.set_title(
    "Universal Claimants (No work requirements) vs UK Inactive Long-Term Sick",
    fontsize=14
)

# Combined legend
lines = ax.get_lines() + ax2.get_lines()
labels = [l.get_label() for l in lines]
ax.legend(lines, labels, loc="upper left", fontsize=10)

# Last data dates box
plt.text(
    0.95, 0.95,
    f"Last DWP data: {last_dwp.strftime('%Y-%m-%d')}\n"
    f"Last BBG data: {last_bbg.strftime('%Y-%m-%d')}",
    horizontalalignment="right",
    verticalalignment="top",
    transform=ax.transAxes,
    fontsize=10,
    bbox=dict(facecolor="white", alpha=0.7)
)

plt.tight_layout()
plt.savefig(Path(G_CHART_DIR, "UniversalClaimants_NoWorkReq_vs_UKInactive_LongTermSick.png"),
            bbox_inches="tight")
plt.show()

del bbg, bbg_series, dwp_series, ax, ax2, fig
