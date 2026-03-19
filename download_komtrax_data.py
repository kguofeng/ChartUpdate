"""
Download Komatsu KOMTRAX Monthly Utilization Data

This script downloads the KOMTRAX PDF from Komatsu's IR website and extracts
monthly average hours of machine use per unit of Komtrax-installed construction
equipment (excluding mini and mining equipment).

Source: https://www.komatsu.jp/en/ir/library/demand/
PDF URL: https://www.komatsu.jp/en/-/media/home/ir/library/demand-orders/en/komtrax_e.pdf

Regions covered: Japan, North America, Europe, Indonesia

Data structure:
    - Column [A]: Prior fiscal year period (e.g. 24/03-25/02)
    - Column [B]: Current fiscal year period (e.g. 25/03-26/02)
    - Column [A] vs [B]: YoY % change

Usage:
    from download_komtrax_data import get_komtrax_data
    df = get_komtrax_data()   # Returns DataFrame with columns per region

    # Using a manually downloaded PDF
    from download_komtrax_data import load_from_local_file
    df = load_from_local_file("/path/to/komtrax_e.pdf")

Manual Download Instructions:
    If automatic download fails (network restrictions, proxy issues):
    1. Go to: https://www.komatsu.jp/en/ir/library/demand/
    2. Click on the KOMTRAX PDF link
    3. Save as: buffer_komtrax/komtrax_e.pdf
    4. Re-run this script
"""

import io
import re
import time
import pickle
from pathlib import Path
from datetime import datetime
from typing import Optional, Union

import pandas as pd
import requests

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import tabula
    HAS_TABULA = True
except ImportError:
    HAS_TABULA = False


# Configuration
KOMTRAX_PDF_URL = (
    "https://www.komatsu.jp/en/-/media/home/ir/library/demand-orders/en/komtrax_e.pdf"
)
BUFFER_DIR = Path(__file__).parent / "buffer_komtrax"
CACHE_FILE = BUFFER_DIR / "komtrax_data.pkl"
PDF_CACHE_FILE = BUFFER_DIR / "komtrax_e.pdf"
CSV_CACHE_FILE = BUFFER_DIR / "komtrax_data.csv"

# Regions in the PDF table
REGIONS = ["Japan", "North America", "Europe", "Indonesia"]

# Month order in the Komatsu fiscal year (April start)
FISCAL_MONTHS = ["Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                 "Jan", "Feb"]

# Browser-like headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}


def download_pdf(
    url: str = KOMTRAX_PDF_URL,
    max_retries: int = 4,
    save_path: Optional[Path] = None,
) -> bytes:
    """
    Download the KOMTRAX PDF from Komatsu with retry logic.

    Returns:
        Raw bytes of the PDF file
    """
    delays = [2, 4, 8, 16]

    for attempt in range(max_retries + 1):
        try:
            print(f"Downloading {url}... (attempt {attempt + 1})")
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()

            content = response.content
            print(f"Downloaded {len(content):,} bytes")

            if save_path:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(content)
                print(f"Saved to {save_path}")

            return content

        except requests.RequestException as e:
            if attempt < max_retries:
                delay = delays[min(attempt, len(delays) - 1)]
                print(f"Download failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


def _parse_month_to_calendar_date(month_str: str, fiscal_year_start: int) -> datetime:
    """
    Convert a fiscal-year month string to a calendar date.

    Komatsu fiscal year runs Mar-Feb. The column header shows e.g. "25/03-26/02"
    meaning the fiscal year starts March 2025 and ends February 2026.

    Args:
        month_str: Month abbreviation (e.g. "Mar", "Jan")
        fiscal_year_start: Calendar year of the fiscal year start month (March)

    Returns:
        datetime for the 1st of that calendar month
    """
    month_map = {
        "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
        "Nov": 11, "Dec": 12, "Jan": 1, "Feb": 2,
    }
    month_num = month_map[month_str]
    # Jan and Feb belong to the next calendar year
    if month_num <= 2:
        year = fiscal_year_start + 1
    else:
        year = fiscal_year_start
    return datetime(year, month_num, 1)


def _parse_fiscal_year_header(header_str: str) -> int:
    """
    Parse fiscal year header like '[A]24/03-25/02' or '[B]25/03-26/02'
    and return the start calendar year (full 4-digit).

    '24/03-25/02' -> start year = 2024 (March 2024)
    '25/03-26/02' -> start year = 2025 (March 2025)
    """
    match = re.search(r'(\d{2})/03', header_str)
    if match:
        yy = int(match.group(1))
        return 2000 + yy
    raise ValueError(f"Cannot parse fiscal year from header: {header_str}")


def parse_pdf_with_pdfplumber(pdf_content: bytes) -> pd.DataFrame:
    """
    Parse the KOMTRAX PDF using pdfplumber to extract table data.

    Returns:
        DataFrame with columns: Region, Month, Date, Hours_A, Hours_B, YoY_pct
    """
    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()

    if not tables:
        raise ValueError("No tables found in KOMTRAX PDF")

    # Find the main data table (the one with hours data)
    main_table = None
    for table in tables:
        # Look for table with "hours" or region names
        flat = str(table).lower()
        if "japan" in flat or "hours" in flat:
            main_table = table
            break

    if main_table is None:
        main_table = tables[0]

    print(f"Extracted table with {len(main_table)} rows, {len(main_table[0])} cols")

    # Parse the table structure
    # The PDF has a side-by-side layout: left half (Japan/North America) and right half (Europe/Indonesia)
    records = []

    # Detect header row to get fiscal year periods
    header_a = None  # e.g. "[A]24/03-25/02"
    header_b = None  # e.g. "[B]25/03-26/02"

    for row in main_table:
        row_str = " ".join(str(c) for c in row if c)
        if "24/03" in row_str or "25/03" in row_str:
            # This is a header row
            for cell in row:
                if cell and "24/03" in str(cell):
                    header_a = str(cell)
                if cell and "25/03" in str(cell):
                    header_b = str(cell)
            break

    if header_a and header_b:
        fy_start_a = _parse_fiscal_year_header(header_a)
        fy_start_b = _parse_fiscal_year_header(header_b)
        print(f"Fiscal years: A starts {fy_start_a}, B starts {fy_start_b}")
    else:
        # Fallback: infer from current date
        now = datetime.now()
        fy_start_b = now.year if now.month >= 3 else now.year - 1
        fy_start_a = fy_start_b - 1
        print(f"Could not parse headers, inferring: A={fy_start_a}, B={fy_start_b}")

    # Parse data rows
    current_region = None
    month_abbrevs = {"Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb"}

    for row in main_table:
        if not row or all(c is None or str(c).strip() == "" for c in row):
            continue

        cells = [str(c).strip() if c else "" for c in row]

        # Check if this row contains region info
        for region in REGIONS:
            if region.lower() in " ".join(cells).lower():
                current_region = region
                break

        # Try to find month data in this row
        # The row format varies but typically: [region], Month, val_a, val_b, yoy%, ...
        for i, cell in enumerate(cells):
            if cell in month_abbrevs:
                month = cell
                # Look for numeric values after the month
                vals = []
                for j in range(i + 1, min(i + 4, len(cells))):
                    v = cells[j].replace("%", "").replace(",", "").strip()
                    try:
                        vals.append(float(v))
                    except (ValueError, TypeError):
                        vals.append(None)

                if len(vals) >= 2 and vals[0] is not None and vals[1] is not None:
                    date_a = _parse_month_to_calendar_date(month, fy_start_a)
                    date_b = _parse_month_to_calendar_date(month, fy_start_b)
                    yoy = vals[2] if len(vals) >= 3 and vals[2] is not None else None

                    records.append({
                        "Region": current_region or "Unknown",
                        "Month": month,
                        "Date_A": date_a,
                        "Date_B": date_b,
                        "Hours_A": vals[0],
                        "Hours_B": vals[1],
                        "YoY_pct": yoy,
                    })

    if not records:
        raise ValueError("Could not extract any data records from the KOMTRAX PDF table")

    df = pd.DataFrame(records)
    print(f"Extracted {len(df)} data records across {df['Region'].nunique()} regions")
    return df


def parse_komtrax_from_known_structure(
    fiscal_year_start_a: int = 2024,
    fiscal_year_start_b: int = 2025,
) -> pd.DataFrame:
    """
    Build the KOMTRAX DataFrame from the known table structure.
    This is used as a fallback when PDF parsing is unavailable.

    The data is sourced from the latest KOMTRAX PDF on the Komatsu IR website.
    Users should update this function when new data is released.

    Returns:
        DataFrame with columns: Region, Month, Date_A, Date_B, Hours_A, Hours_B, YoY_pct
    """
    # Data extracted from the KOMTRAX PDF (screenshot verified)
    # Fiscal year A: 24/03-25/02 (March 2024 - February 2025)
    # Fiscal year B: 25/03-26/02 (March 2025 - February 2026)
    raw_data = {
        "Japan": [
            ("Mar", 44.9, 44.4, -1.0),
            ("Apr", 42.0, 41.8, -0.5),
            ("May", 41.1, 40.3, -1.9),
            ("Jun", 43.2, 42.8, -0.8),
            ("Jul", 47.8, 48.2, 0.8),
            ("Aug", 40.4, 39.0, -3.4),
            ("Sep", 45.2, 44.2, -2.2),
            ("Oct", 46.2, 45.6, -1.3),
            ("Nov", 44.4, 41.4, -6.9),
            ("Dec", 49.1, 45.6, -7.1),
            ("Jan", 49.2, 54.9, 11.5),
            ("Feb", 52.4, 48.5, -7.4),
        ],
        "North America": [
            ("Mar", 61.1, 64.6, 5.9),
            ("Apr", 65.9, 65.2, -1.1),
            ("May", 66.1, 62.6, -5.3),
            ("Jun", 67.2, 67.0, -0.2),
            ("Jul", 68.3, 71.5, 4.7),
            ("Aug", 72.4, 68.4, -5.5),
            ("Sep", 64.3, 69.0, 7.3),
            ("Oct", 75.1, 70.8, -5.7),
            ("Nov", 58.7, 59.0, 0.6),
            ("Dec", 53.8, 57.6, 7.0),
            ("Jan", 59.9, 59.6, -0.6),
            ("Feb", 58.0, 61.0, 5.2),
        ],
        "Europe": [
            ("Mar", 68.6, 73.4, 6.9),
            ("Apr", 73.3, 71.4, -2.6),
            ("May", 69.0, 70.8, 2.6),
            ("Jun", 71.5, 72.2, 1.0),
            ("Jul", 77.6, 76.3, -1.6),
            ("Aug", 67.5, 63.9, -5.3),
            ("Sep", 73.1, 76.5, 4.6),
            ("Oct", 79.3, 78.6, -0.8),
            ("Nov", 72.5, 69.3, -4.4),
            ("Dec", 52.4, 54.4, 3.9),
            ("Jan", 59.4, 54.2, -8.9),
            ("Feb", 66.3, 60.8, -8.2),
        ],
        "Indonesia": [
            ("Mar", 198.8, 184.7, -7.1),
            ("Apr", 164.3, 180.6, 9.9),
            ("May", 205.5, 211.1, 2.7),
            ("Jun", 195.6, 195.7, 0.0),
            ("Jul", 215.5, 214.7, -0.3),
            ("Aug", 211.2, 206.0, -2.5),
            ("Sep", 211.5, 205.0, -3.1),
            ("Oct", 219.9, 211.7, -3.7),
            ("Nov", 204.8, 202.7, -1.0),
            ("Dec", 193.3, 194.5, 0.6),
            ("Jan", 188.9, 194.8, 3.1),
            ("Feb", 189.3, 189.2, -0.1),
        ],
    }

    records = []
    for region, rows in raw_data.items():
        for month, hours_a, hours_b, yoy in rows:
            date_a = _parse_month_to_calendar_date(month, fiscal_year_start_a)
            date_b = _parse_month_to_calendar_date(month, fiscal_year_start_b)
            records.append({
                "Region": region,
                "Month": month,
                "Date_A": date_a,
                "Date_B": date_b,
                "Hours_A": hours_a,
                "Hours_B": hours_b,
                "YoY_pct": yoy,
            })

    df = pd.DataFrame(records)
    print(f"Built KOMTRAX data: {len(df)} records across {df['Region'].nunique()} regions")
    return df


def get_komtrax_data(
    use_cache: bool = True,
    force_refresh: bool = False,
    local_file: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """
    Get KOMTRAX monthly utilization data.

    Tries in order:
    1. Pickle cache (if valid and not force_refresh)
    2. PDF download + parse (if pdfplumber available)
    3. Cached PDF file + parse
    4. Hardcoded fallback data (from latest known PDF snapshot)

    Returns:
        DataFrame with columns: Region, Month, Date_A, Date_B, Hours_A, Hours_B, YoY_pct
    """
    BUFFER_DIR.mkdir(parents=True, exist_ok=True)

    # Use local file if provided
    if local_file is not None:
        return load_from_local_file(local_file)

    # Check pickle cache
    if use_cache and not force_refresh and CACHE_FILE.exists():
        cache_mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        cache_age_days = (datetime.now() - cache_mtime).days
        if cache_age_days < 30:  # KOMTRAX updates monthly
            print(f"Loading cached KOMTRAX data from {CACHE_FILE} (age: {cache_age_days} days)")
            with open(CACHE_FILE, 'rb') as f:
                return pickle.load(f)
        else:
            print(f"Cache is {cache_age_days} days old, refreshing...")

    # Try PDF download + parse
    if HAS_PDFPLUMBER:
        try:
            pdf_content = download_pdf(save_path=PDF_CACHE_FILE)
            df = parse_pdf_with_pdfplumber(pdf_content)
            _save_to_cache(df)
            return df
        except Exception as e:
            print(f"PDF download/parse failed: {e}")
            # Try cached PDF
            if PDF_CACHE_FILE.exists():
                print(f"Trying cached PDF: {PDF_CACHE_FILE}")
                try:
                    pdf_content = PDF_CACHE_FILE.read_bytes()
                    df = parse_pdf_with_pdfplumber(pdf_content)
                    _save_to_cache(df)
                    return df
                except Exception as e2:
                    print(f"Cached PDF parse failed: {e2}")

    # Fallback to hardcoded data
    print("Using hardcoded KOMTRAX data (update download_komtrax_data.py when new PDF is released)")
    df = parse_komtrax_from_known_structure()
    _save_to_cache(df)
    return df


def load_from_local_file(
    file_path: Union[str, Path],
    save_to_cache: bool = True,
) -> pd.DataFrame:
    """
    Load and parse KOMTRAX data from a locally downloaded PDF file.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    print(f"Loading from local file: {file_path}")

    if not HAS_PDFPLUMBER:
        raise ImportError(
            "pdfplumber is required to parse PDF files. "
            "Install it with: pip install pdfplumber"
        )

    pdf_content = file_path.read_bytes()
    df = parse_pdf_with_pdfplumber(pdf_content)

    if save_to_cache:
        _save_to_cache(df)

    return df


def _save_to_cache(df: pd.DataFrame) -> None:
    """Save DataFrame to pickle and CSV cache."""
    BUFFER_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(df, f)
    df.to_csv(CSV_CACHE_FILE, index=False)
    print(f"Cached KOMTRAX data to {CACHE_FILE}")


def get_komtrax_timeseries(
    df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Convert the raw KOMTRAX table into a time-series DataFrame suitable for charting.

    Returns a DataFrame indexed by Date with columns for each region's usage hours.
    Combines both fiscal year A and B data into a single time series.

    Returns:
        DataFrame with DatetimeIndex and columns like 'Japan', 'North America', etc.
    """
    if df is None:
        df = get_komtrax_data()

    all_records = []

    for _, row in df.iterrows():
        region = row["Region"]
        # Period A data
        all_records.append({
            "Date": row["Date_A"],
            "Region": region,
            "Hours": row["Hours_A"],
        })
        # Period B data
        all_records.append({
            "Date": row["Date_B"],
            "Region": region,
            "Hours": row["Hours_B"],
        })

    ts_df = pd.DataFrame(all_records)
    # Pivot to wide format
    ts_wide = ts_df.pivot_table(index="Date", columns="Region", values="Hours", aggfunc="last")
    ts_wide.index = pd.to_datetime(ts_wide.index)
    ts_wide = ts_wide.sort_index()

    # Reorder columns
    col_order = [r for r in REGIONS if r in ts_wide.columns]
    ts_wide = ts_wide[col_order]

    return ts_wide


def get_komtrax_yoy(
    df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Extract YoY % change data from the KOMTRAX table for the current fiscal year.

    Returns a DataFrame indexed by Date_B with columns for each region's YoY change.
    """
    if df is None:
        df = get_komtrax_data()

    records = []
    for _, row in df.iterrows():
        records.append({
            "Date": row["Date_B"],
            "Region": row["Region"],
            "YoY_pct": row["YoY_pct"],
        })

    yoy_df = pd.DataFrame(records)
    yoy_wide = yoy_df.pivot_table(index="Date", columns="Region", values="YoY_pct", aggfunc="last")
    yoy_wide.index = pd.to_datetime(yoy_wide.index)
    yoy_wide = yoy_wide.sort_index()

    col_order = [r for r in REGIONS if r in yoy_wide.columns]
    yoy_wide = yoy_wide[col_order]

    return yoy_wide


def main():
    """Main function to download and display KOMTRAX data."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Komatsu KOMTRAX monthly utilization data"
    )
    parser.add_argument(
        "--local-file", "-f", type=str,
        help="Path to manually downloaded komtrax_e.pdf file"
    )
    parser.add_argument(
        "--force-refresh", "-r", action="store_true",
        help="Force re-download even if cache exists"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Komatsu KOMTRAX Monthly Utilization Data")
    print("=" * 60)
    print()

    df = get_komtrax_data(
        force_refresh=args.force_refresh,
        local_file=args.local_file,
    )

    print("\n" + "=" * 60)
    print("Raw Table Data")
    print("=" * 60)
    for region in REGIONS:
        region_df = df[df["Region"] == region]
        print(f"\n{region}:")
        print(region_df[["Month", "Hours_A", "Hours_B", "YoY_pct"]].to_string(index=False))

    print("\n" + "=" * 60)
    print("Time Series (Hours)")
    print("=" * 60)
    ts = get_komtrax_timeseries(df)
    print(ts.to_string())

    print("\n" + "=" * 60)
    print("YoY % Change (Current FY)")
    print("=" * 60)
    yoy = get_komtrax_yoy(df)
    print(yoy.to_string())

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
