"""
Download Bank Indonesia Monetary Authority Balance Sheet Data (SEKI Table I.2)

This script downloads the "Neraca Analitis Otoritas Moneter (Uang Primer)" table
from Bank Indonesia's SEKI database and extracts the "Uang Primer Adjusted 1)"
(Adjusted M0/Base Money) time series.

Source: https://www.bi.go.id/id/statistik/ekonomi-keuangan/seki/Default.aspx
Table: I.2 - Neraca Analitis Otoritas Moneter (Uang Primer)
Direct Excel URL: https://www.bi.go.id/seki/tabel/TABEL1_2.xls

Usage:
    # Automatic download (requires network access to bi.go.id)
    from download_bi_monetary_data import get_adjusted_m0_data
    m0_data = get_adjusted_m0_data()  # Returns pandas Series with monthly adjusted M0

    # Using a manually downloaded file
    from download_bi_monetary_data import load_from_local_file
    m0_data = load_from_local_file("/path/to/TABEL1_2.xls")

    # Compute YoY growth
    from download_bi_monetary_data import compute_yoy_growth
    m0_yoy = compute_yoy_growth(m0_data)

Manual Download Instructions:
    If automatic download fails (network restrictions, proxy issues), manually download:
    1. Go to: https://www.bi.go.id/id/statistik/ekonomi-keuangan/seki/Default.aspx
    2. Expand "I. UANG DAN BANK" section
    3. Find "I.2. Neraca Analitis Otoritas Moneter (Uang Primer)"
    4. Click the Excel download icon next to the table name
    5. Save the file to: buffer_bi_monetary/TABEL1_2.xls
    6. Run this script again - it will use the cached file
"""

import io
import time
import pickle
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Union

import pandas as pd
import requests

# Configuration
SEKI_TABLE_URL = "https://www.bi.go.id/seki/tabel/TABEL1_2.xls"
BUFFER_DIR = Path(__file__).parent / "buffer_bi_monetary"
CACHE_FILE = BUFFER_DIR / "bi_adjusted_m0.pkl"
EXCEL_CACHE_FILE = BUFFER_DIR / "TABEL1_2.xls"

# Row containing "Uang Primer Adjusted 1)" - 0-indexed, so row 7 in Excel = index 6
ADJUSTED_M0_ROW_LABEL = "Uang Primer Adjusted 1)"

# Browser-like headers to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def download_excel_file(
    url: str = SEKI_TABLE_URL,
    max_retries: int = 4,
    save_path: Optional[Path] = None,
) -> bytes:
    """
    Download the Excel file from Bank Indonesia with retry logic.

    Args:
        url: URL to download from
        max_retries: Maximum number of retry attempts
        save_path: Optional path to save the downloaded file

    Returns:
        Raw bytes of the Excel file

    Raises:
        requests.RequestException: If download fails after all retries
    """
    delays = [2, 4, 8, 16]  # Exponential backoff delays in seconds

    for attempt in range(max_retries + 1):
        try:
            print(f"Downloading {url}... (attempt {attempt + 1})")
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()

            content = response.content
            print(f"Downloaded {len(content):,} bytes")

            # Optionally save to disk
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


def parse_excel_for_adjusted_m0(
    excel_content: bytes,
    row_label: str = ADJUSTED_M0_ROW_LABEL,
) -> pd.Series:
    """
    Parse the Bank Indonesia Excel file and extract the Adjusted M0 time series.

    The Excel file contains multiple sheets with data from different time periods:
    - 'Th 1968-1986': Data from 1968-1986 (Uang Primer only, no Adjusted)
    - 'Th 1987-2005': Data from 1987-2005 (Uang Primer only, no Adjusted)
    - 'Th 2006-2009': Data from 2006-2019 (Uang Primer only, no Adjusted)
    - 'Th 2010-2021': Data from 2010-2022 (Uang Primer only, no Adjusted)
    - 'I.2': Most recent data with "Uang Primer Adjusted 1)" row

    This function reads ALL sheets, handles the different structures, and combines
    the data into a single time series.

    Args:
        excel_content: Raw bytes of the Excel file
        row_label: The label to search for in the first column

    Returns:
        pandas Series with DatetimeIndex (monthly) and values in billions IDR
    """
    # Detect file format and use appropriate engine
    # xlsx files start with PK (ZIP signature), xls files start with D0 CF (OLE signature)
    if excel_content[:2] == b'PK':
        engine = "openpyxl"
    else:
        engine = "xlrd"
    print(f"Using Excel engine: {engine}")
    excel_file = pd.ExcelFile(io.BytesIO(excel_content), engine=engine)
    sheet_names = excel_file.sheet_names
    print(f"Available sheets: {sheet_names}")

    # Month mapping - comprehensive for different languages/formats
    month_map = {
        'jan': 1, 'januari': 1, 'january': 1,
        'feb': 2, 'februari': 2, 'february': 2,
        'mar': 3, 'maret': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5, 'mei': 5,
        'jun': 6, 'juni': 6, 'june': 6,
        'jul': 7, 'juli': 7, 'july': 7,
        'aug': 8, 'agustus': 8, 'august': 8, 'agu': 8, 'agt': 8,
        'sep': 9, 'september': 9, 'sept': 9,
        'oct': 10, 'oktober': 10, 'october': 10, 'okt': 10,
        'nov': 11, 'november': 11, 'nop': 11, 'novr': 11,
        'dec': 12, 'desember': 12, 'december': 12, 'des': 12, 'decr': 12,
    }

    all_data = {}  # date -> value mapping to combine data from all sheets

    for sheet_name in sheet_names:
        print(f"\n{'='*60}")
        print(f"Processing sheet: '{sheet_name}'")
        print(f"{'='*60}")

        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        print(f"Sheet shape: {df.shape}")

        # =====================================================================
        # Step 1: Find the year header row and month header row
        # Different sheets have different structures
        # =====================================================================
        year_row_idx = None
        month_row_idx = None

        # Look for "KETERANGAN" row which marks the header section
        for row_idx in range(min(15, df.shape[0])):
            for col_idx in range(min(5, df.shape[1])):
                cell_val = df.iloc[row_idx, col_idx]
                if pd.notna(cell_val) and isinstance(cell_val, str):
                    if 'keterangan' in cell_val.lower():
                        year_row_idx = row_idx
                        month_row_idx = row_idx + 1
                        print(f"Found KETERANGAN at row {row_idx}, col {col_idx}")
                        break
            if year_row_idx is not None:
                break

        if year_row_idx is None:
            print("WARNING: Could not find KETERANGAN row, using defaults")
            # Older sheets typically have KETERANGAN at row 5
            # Newer sheets (I.2, Th 2010-2021) have it at row 3
            if sheet_name in ['I.2', 'Th 2010-2021']:
                year_row_idx = 3
                month_row_idx = 4
            else:
                year_row_idx = 5
                month_row_idx = 6

        print(f"Using year_row={year_row_idx}, month_row={month_row_idx}")

        # =====================================================================
        # Step 2: Find the data start column
        # =====================================================================
        data_start_col = None
        for col_idx in range(df.shape[1]):
            val = df.iloc[year_row_idx, col_idx]
            if pd.notna(val) and isinstance(val, (int, float)) and 1960 <= val <= 2030:
                data_start_col = col_idx
                break

        if data_start_col is None:
            for col_idx in range(df.shape[1]):
                val = df.iloc[month_row_idx, col_idx]
                if pd.notna(val) and isinstance(val, str):
                    if any(m in val.lower() for m in month_map.keys()):
                        data_start_col = col_idx
                        break

        if data_start_col is None:
            data_start_col = 3 if sheet_name in ['I.2', 'Th 2010-2021'] else 3
            print(f"WARNING: Could not find data start column, using default {data_start_col}")

        print(f"Data starts at column: {data_start_col}")

        # =====================================================================
        # Step 3: Find the target data row
        # Prefer "Uang Primer Adjusted 1)" but fall back to "Uang Primer"
        # =====================================================================
        target_row_idx = None
        target_row_name = None

        # Priority patterns - prefer adjusted version
        # Pattern format: (pattern_string, is_adjusted, require_exact_or_start)
        search_patterns = [
            ("Uang Primer Adjusted 1)", True),
            ("Uang Primer Adjusted", True),
            ("Adjusted Base Money", True),
            ("Uang Primer", False),  # Fallback
            ("Base Money", False),   # Fallback
        ]

        for pattern, is_adjusted in search_patterns:
            if target_row_idx is not None:
                break
            for col_idx in range(min(5, df.shape[1])):
                for row_idx in range(df.shape[0]):
                    cell_value = df.iloc[row_idx, col_idx]
                    if pd.notna(cell_value) and isinstance(cell_value, str):
                        cell_lower = cell_value.lower().strip()
                        # Skip title/header/footnote rows (long text, certain keywords)
                        if len(cell_value) > 100:  # Footnotes are typically long
                            continue
                        if any(skip in cell_lower for skip in [
                            'analytical', 'neraca', 'table', 'tabel', 'i.2', 'billion',
                            'miliar', 'faktor', 'mempengaruhi', 'sejak', 'dilakukan',
                            'reklasifikasi', 'revisi', 'sementara', 'penyesuaian',
                            'komponen', 'gwm', 'sekunder'
                        ]):
                            continue
                        # Check for exact match or pattern at start of cell
                        pattern_lower = pattern.lower()
                        if cell_lower == pattern_lower or cell_lower.startswith(pattern_lower):
                            # For non-adjusted patterns, ensure we don't accidentally match adjusted
                            if not is_adjusted and 'adjusted' in cell_lower:
                                continue
                            target_row_idx = row_idx
                            target_row_name = cell_value
                            print(f"Found target row: '{cell_value}' at row {row_idx}, col {col_idx}")
                            break
                    if target_row_idx is not None:
                        break
                if target_row_idx is not None:
                    break

        if target_row_idx is None:
            print(f"WARNING: Could not find data row in sheet '{sheet_name}', skipping")
            continue

        # =====================================================================
        # Step 4: Build column-to-date mapping with year rollover detection
        # Key fix: When month goes from Dec to Jan (or high to low), increment year
        # =====================================================================
        year_row = df.iloc[year_row_idx, data_start_col:].tolist()
        month_row = df.iloc[month_row_idx, data_start_col:].tolist()
        data_row = df.iloc[target_row_idx, data_start_col:].tolist()

        current_year = None
        prev_month = None
        extracted_count = 0

        for i, (year_val, month_val, data_val) in enumerate(zip(year_row, month_row, data_row)):
            col_idx = data_start_col + i

            # Track if we got an explicit year value in this column
            explicit_year = False

            # Update year if we see a valid year value
            if pd.notna(year_val):
                if isinstance(year_val, (int, float)) and 1960 <= year_val <= 2030:
                    current_year = int(year_val)
                    explicit_year = True
                elif isinstance(year_val, str):
                    # Try to parse year from string (handles "2021 r", "2022", etc.)
                    try:
                        # First try extracting first word/number (for "2021 r" format)
                        year_str = year_val.strip().split()[0]
                        y = int(year_str)
                        if 1960 <= y <= 2030:
                            current_year = y
                            explicit_year = True
                    except (ValueError, IndexError):
                        pass

            # Parse month
            month = None
            if pd.notna(month_val) and isinstance(month_val, str):
                # Clean month string (remove asterisks like "Nov*")
                month_clean = month_val.lower().strip().rstrip('*').strip()
                for m_name, m_num in month_map.items():
                    if m_name in month_clean or month_clean == m_name:
                        month = m_num
                        break
            elif pd.notna(month_val) and isinstance(month_val, (int, float)) and 1 <= month_val <= 12:
                month = int(month_val)

            # Skip if no valid month (could be annual total column)
            # BUT don't reset prev_month - we need it for year rollover detection
            if month is None:
                continue

            # Year rollover detection: if month resets from high to low (Dec->Jan)
            # This handles cases where the year cell is blank but months reset
            # ONLY apply if we didn't get an explicit year value this column
            if current_year is not None and prev_month is not None and not explicit_year:
                if prev_month >= 10 and month <= 4:  # Dec/Nov/Oct -> Jan/Feb/Mar/Apr
                    current_year += 1
                    print(f"  Year rollover detected at col {col_idx}: {prev_month} -> {month}, advancing to {current_year}")

            prev_month = month

            # Skip if still no year
            if current_year is None:
                continue

            # Parse data value
            if pd.isna(data_val):
                continue

            try:
                if isinstance(data_val, (int, float)) and not isinstance(data_val, bool):
                    value = float(data_val)
                elif isinstance(data_val, str):
                    clean_val = data_val.replace(',', '').replace(' ', '').strip()
                    if clean_val.startswith('(') and clean_val.endswith(')'):
                        clean_val = '-' + clean_val[1:-1]
                    if clean_val in ['', '-', '*']:
                        continue
                    value = float(clean_val)
                else:
                    continue

                date = datetime(current_year, month, 1)

                # Store the data (newer sheets will overwrite older if same date)
                if date not in all_data or sheet_name == 'I.2':
                    all_data[date] = value
                    extracted_count += 1

            except (ValueError, TypeError) as e:
                continue

        print(f"Extracted {extracted_count} data points from sheet '{sheet_name}'")
        if extracted_count > 0:
            dates_in_sheet = [d for d in all_data.keys()]
            dates_in_sheet.sort()
            sheet_dates = [d for d in dates_in_sheet if all_data.get(d) is not None]
            if sheet_dates:
                print(f"  Date range: {min(sheet_dates).strftime('%Y-%m')} to {max(sheet_dates).strftime('%Y-%m')}")

    # =========================================================================
    # Combine all data into a Series
    # =========================================================================
    if not all_data:
        raise ValueError("No valid date-value pairs found in any sheet")

    dates = sorted(all_data.keys())
    values = [all_data[d] for d in dates]

    series = pd.Series(values, index=pd.DatetimeIndex(dates), name="Adjusted_M0_BnIDR")
    series = series.sort_index()

    # Normalize to month-end timestamps
    series.index = series.index.to_period('M').to_timestamp('M')

    # Remove duplicates (keep last)
    series = series[~series.index.duplicated(keep='last')]

    # Validate: check for gaps in the data
    # Use 'ME' for pandas >= 2.2, fallback to 'M' for older versions
    try:
        expected_months = pd.date_range(series.index.min(), series.index.max(), freq='ME')
    except ValueError:
        expected_months = pd.date_range(series.index.min(), series.index.max(), freq='M')
    missing_months = expected_months.difference(series.index)
    if len(missing_months) > 0:
        print(f"\nWARNING: Missing {len(missing_months)} months in the data:")
        for m in missing_months[:10]:
            print(f"  - {m.strftime('%Y-%m')}")
        if len(missing_months) > 10:
            print(f"  ... and {len(missing_months) - 10} more")

    print(f"\n{'='*60}")
    print(f"FINAL RESULT: Extracted {len(series)} monthly data points")
    print(f"Date range: {series.index.min().date()} to {series.index.max().date()}")
    print(f"{'='*60}")

    return series


def get_adjusted_m0_data(
    use_cache: bool = True,
    force_refresh: bool = False,
    local_file: Optional[Union[str, Path]] = None,
) -> pd.Series:
    """
    Get the Adjusted M0 (Base Money) data from Bank Indonesia.

    This is the main function to use for accessing the data.

    Args:
        use_cache: If True, use cached data if available
        force_refresh: If True, force re-download even if cache exists
        local_file: Optional path to a manually downloaded Excel file

    Returns:
        pandas Series with monthly Adjusted M0 data in billions IDR

    Raises:
        RuntimeError: If download fails and no cached/local file available
    """
    BUFFER_DIR.mkdir(parents=True, exist_ok=True)

    # If local file provided, use it directly
    if local_file is not None:
        return load_from_local_file(local_file)

    # Check cache first (unless force refresh)
    if use_cache and not force_refresh and CACHE_FILE.exists():
        cache_mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        cache_age_days = (datetime.now() - cache_mtime).days

        if cache_age_days < 7:  # Cache valid for 7 days (weekly refresh)
            print(f"Loading cached data from {CACHE_FILE} (age: {cache_age_days} days)")
            with open(CACHE_FILE, 'rb') as f:
                return pickle.load(f)
        else:
            print(f"Cache is {cache_age_days} days old, refreshing...")

    # Try to download fresh data
    try:
        excel_content = download_excel_file(save_path=EXCEL_CACHE_FILE)
    except requests.RequestException as e:
        # Download failed - try cached Excel file
        if EXCEL_CACHE_FILE.exists():
            print(f"\nDownload failed, using cached Excel file: {EXCEL_CACHE_FILE}")
            excel_content = EXCEL_CACHE_FILE.read_bytes()
        else:
            # No cached file available - provide instructions
            print("\n" + "=" * 60)
            print("DOWNLOAD FAILED - Manual download required")
            print("=" * 60)
            print(f"\nError: {e}\n")
            print("Please manually download the Excel file:")
            print(f"  1. Open: {SEKI_TABLE_URL}")
            print(f"     OR go to: https://www.bi.go.id/id/statistik/ekonomi-keuangan/seki/Default.aspx")
            print("  2. Navigate to 'I. UANG DAN BANK' section")
            print("  3. Find 'I.2. Neraca Analitis Otoritas Moneter (Uang Primer)'")
            print("  4. Click the Excel download icon")
            print(f"  5. Save to: {EXCEL_CACHE_FILE}")
            print("  6. Re-run this script")
            print("=" * 60)
            raise RuntimeError(
                f"Could not download data and no cached file available. "
                f"Please manually download from {SEKI_TABLE_URL}"
            ) from e

    # Parse Excel
    series = parse_excel_for_adjusted_m0(excel_content)

    # Save to cache
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(series, f)
    print(f"Cached data to {CACHE_FILE}")

    return series


def load_from_local_file(
    file_path: Union[str, Path],
    save_to_cache: bool = True,
) -> pd.Series:
    """
    Load and parse Adjusted M0 data from a locally downloaded Excel file.

    This is useful when automatic download fails due to network restrictions.

    Args:
        file_path: Path to the downloaded TABEL1_2.xls file
        save_to_cache: If True, save parsed data to cache for future use

    Returns:
        pandas Series with monthly Adjusted M0 data in billions IDR
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    print(f"Loading from local file: {file_path}")
    excel_content = file_path.read_bytes()
    series = parse_excel_for_adjusted_m0(excel_content)

    if save_to_cache:
        BUFFER_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(series, f)
        print(f"Cached data to {CACHE_FILE}")

    return series


def compute_yoy_growth(series: pd.Series) -> pd.Series:
    """
    Compute Year-over-Year growth rate from monthly level data.

    Args:
        series: Monthly level data with DatetimeIndex

    Returns:
        YoY growth rate in percent
    """
    if series is None or series.empty:
        raise ValueError("Input series is empty; cannot compute YoY growth.")
    s = series.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    s = s[~s.index.duplicated(keep='last')]
    s.index = s.index.to_period('M').to_timestamp('M')
    yoy = s.pct_change(12) * 100.0
    yoy.name = "Adjusted_M0_YoY_pct"
    return yoy


def diagnose_excel_structure(
    excel_content: bytes,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Analyze and export the structure of the Bank Indonesia Excel file for debugging.

    This function exports detailed column-by-column data to help verify that
    all dates and values are being extracted correctly.

    Args:
        excel_content: Raw bytes of the Excel file
        output_path: Optional path to save the diagnostic CSV

    Returns:
        DataFrame with column index, year, month, date, and extracted value
    """
    # Detect file format and use appropriate engine
    if excel_content[:2] == b'PK':
        engine = "openpyxl"
    else:
        engine = "xlrd"
    excel_file = pd.ExcelFile(io.BytesIO(excel_content), engine=engine)
    sheet_names = excel_file.sheet_names

    # Find target sheet
    target_sheet = None
    for sheet in sheet_names:
        if 'I.2' in sheet or sheet == 'I.2':
            target_sheet = sheet
            break
    if target_sheet is None:
        target_sheet = sheet_names[-1]

    df = pd.read_excel(excel_file, sheet_name=target_sheet, header=None)

    print(f"Sheet: {target_sheet}, Shape: {df.shape}")

    # Month mapping
    month_map = {
        'jan': 1, 'januari': 1, 'january': 1,
        'feb': 2, 'februari': 2, 'february': 2,
        'mar': 3, 'maret': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5, 'mei': 5,
        'jun': 6, 'juni': 6, 'june': 6,
        'jul': 7, 'juli': 7, 'july': 7,
        'aug': 8, 'agustus': 8, 'august': 8, 'agu': 8, 'agt': 8,
        'sep': 9, 'september': 9, 'sept': 9,
        'oct': 10, 'oktober': 10, 'october': 10, 'okt': 10,
        'nov': 11, 'november': 11, 'nop': 11,
        'dec': 12, 'desember': 12, 'december': 12, 'des': 12,
    }

    # Find header rows
    year_row_idx = None
    month_row_idx = None

    for row_idx in range(min(15, df.shape[0])):
        for col_idx in range(min(5, df.shape[1])):
            cell_val = df.iloc[row_idx, col_idx]
            if pd.notna(cell_val) and isinstance(cell_val, str):
                if 'keterangan' in cell_val.lower():
                    year_row_idx = row_idx
                    month_row_idx = row_idx + 1
                    break
        if year_row_idx is not None:
            break

    if year_row_idx is None:
        year_row_idx = 5
        month_row_idx = 6

    # Find data start column
    data_start_col = 4
    for col_idx in range(df.shape[1]):
        val = df.iloc[year_row_idx, col_idx]
        if pd.notna(val) and isinstance(val, (int, float)) and 1960 <= val <= 2030:
            data_start_col = col_idx
            break

    # Find target row
    target_row_idx = None
    search_patterns = ["Uang Primer Adjusted 1)", "Uang Primer Adjusted", "Adjusted Base Money"]

    for col_idx in range(min(5, df.shape[1])):
        for row_idx in range(df.shape[0]):
            cell_value = df.iloc[row_idx, col_idx]
            if pd.notna(cell_value) and isinstance(cell_value, str):
                for pattern in search_patterns:
                    if pattern.lower() in cell_value.lower():
                        target_row_idx = row_idx
                        break
                if target_row_idx is not None:
                    break
        if target_row_idx is not None:
            break

    if target_row_idx is None:
        target_row_idx = 8  # Fallback

    # Build diagnostic data
    diag_data = []
    current_year = None

    for col_idx in range(data_start_col, df.shape[1]):
        year_val = df.iloc[year_row_idx, col_idx]
        month_val = df.iloc[month_row_idx, col_idx]
        data_val = df.iloc[target_row_idx, col_idx]

        # Update year if present
        if pd.notna(year_val):
            if isinstance(year_val, (int, float)) and 1960 <= year_val <= 2030:
                current_year = int(year_val)
            elif isinstance(year_val, str):
                try:
                    y = int(year_val.strip())
                    if 1960 <= y <= 2030:
                        current_year = y
                except ValueError:
                    pass

        # Parse month
        month = None
        if pd.notna(month_val):
            if isinstance(month_val, str):
                month_lower = month_val.lower().strip()
                for m_name, m_num in month_map.items():
                    if m_name in month_lower:
                        month = m_num
                        break
            elif isinstance(month_val, (int, float)) and 1 <= month_val <= 12:
                month = int(month_val)

        # Build date string
        date_str = None
        if current_year and month:
            date_str = f"{current_year}-{month:02d}"

        diag_data.append({
            'col_idx': col_idx,
            'year_raw': year_val,
            'year_parsed': current_year,
            'month_raw': month_val,
            'month_parsed': month,
            'date': date_str,
            'value_raw': data_val,
            'value_parsed': float(data_val) if pd.notna(data_val) and isinstance(data_val, (int, float)) else None
        })

    diag_df = pd.DataFrame(diag_data)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        diag_df.to_csv(output_path, index=False)
        print(f"Diagnostic data saved to: {output_path}")

    # Summary stats
    valid_dates = diag_df['date'].dropna()
    print(f"\nDiagnostic Summary:")
    print(f"  Total columns scanned: {len(diag_data)}")
    print(f"  Columns with valid dates: {len(valid_dates)}")
    print(f"  Columns with parsed values: {diag_df['value_parsed'].notna().sum()}")

    if len(valid_dates) > 0:
        print(f"  Date range: {valid_dates.min()} to {valid_dates.max()}")

    # Check for gaps
    if len(valid_dates) > 1:
        dates_sorted = sorted(pd.to_datetime(valid_dates, format='%Y-%m'))
        expected = pd.date_range(dates_sorted[0], dates_sorted[-1], freq='MS')
        actual = pd.DatetimeIndex(dates_sorted)
        missing = expected.difference(actual)
        if len(missing) > 0:
            print(f"  Missing months: {len(missing)}")
            for m in missing[:5]:
                print(f"    - {m.strftime('%Y-%m')}")
            if len(missing) > 5:
                print(f"    ... and {len(missing) - 5} more")

    return diag_df


def main():
    """
    Main function to download and display Bank Indonesia monetary data.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Bank Indonesia Adjusted M0 data from SEKI Table I.2"
    )
    parser.add_argument(
        "--local-file", "-f",
        type=str,
        help="Path to manually downloaded TABEL1_2.xls file"
    )
    parser.add_argument(
        "--force-refresh", "-r",
        action="store_true",
        help="Force re-download even if cache exists"
    )
    parser.add_argument(
        "--use-cache", "-c",
        action="store_true",
        default=True,
        help="Use cached data if available (default: True)"
    )
    parser.add_argument(
        "--diagnose", "-d",
        action="store_true",
        help="Run diagnostic mode to analyze Excel structure column-by-column"
    )
    parser.add_argument(
        "--show-sample",
        type=int,
        default=12,
        help="Number of recent months to display (default: 12)"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Bank Indonesia Monetary Data Downloader")
    print("Table I.2 - Neraca Analitis Otoritas Moneter (Uang Primer)")
    print("=" * 60)
    print()

    # Get Excel content
    excel_content = None
    if args.local_file:
        local_path = Path(args.local_file)
        if local_path.exists():
            excel_content = local_path.read_bytes()
        else:
            print(f"Error: File not found: {args.local_file}")
            return None, None
    elif EXCEL_CACHE_FILE.exists():
        excel_content = EXCEL_CACHE_FILE.read_bytes()
        print(f"Using cached Excel file: {EXCEL_CACHE_FILE}")

    # Diagnostic mode
    if args.diagnose:
        if excel_content is None:
            print("Error: Need Excel file for diagnostic mode. Use --local-file or ensure cache exists.")
            return None, None

        print("\n" + "=" * 60)
        print("DIAGNOSTIC MODE")
        print("=" * 60)
        diag_output = BUFFER_DIR / "bi_diagnostic_columns.csv"
        diagnose_excel_structure(excel_content, output_path=diag_output)
        return None, None

    # Get data
    try:
        m0_level = get_adjusted_m0_data(
            use_cache=args.use_cache,
            force_refresh=args.force_refresh,
            local_file=args.local_file
        )
    except RuntimeError as e:
        print(f"\n{e}")
        return None, None

    # Compute YoY growth
    m0_yoy = compute_yoy_growth(m0_level)

    # Display summary
    print("\n" + "=" * 60)
    print("Data Summary")
    print("=" * 60)
    print(f"\nDate range: {m0_level.index.min().date()} to {m0_level.index.max().date()}")
    print(f"Total observations: {len(m0_level)}")

    # Expected months check
    # Use 'ME' for pandas >= 2.2, fallback to 'M' for older versions
    try:
        expected_months = pd.date_range(m0_level.index.min(), m0_level.index.max(), freq='ME')
    except ValueError:
        expected_months = pd.date_range(m0_level.index.min(), m0_level.index.max(), freq='M')
    missing_count = len(expected_months) - len(m0_level)
    if missing_count > 0:
        print(f"WARNING: Missing {missing_count} months in the date range!")

    n_show = args.show_sample
    print(f"\nLast {n_show} months of Adjusted M0 (billions IDR):")
    print(m0_level.tail(n_show).to_string())

    print(f"\nLast {n_show} months of YoY Growth (%):")
    print(m0_yoy.tail(n_show).to_string())

    # Save to CSV for easy inspection
    output_csv = BUFFER_DIR / "bi_adjusted_m0_data.csv"
    output_df = pd.DataFrame({
        'Adjusted_M0_BnIDR': m0_level,
        'Adjusted_M0_YoY_pct': m0_yoy
    })
    output_df.to_csv(output_csv)
    print(f"\nSaved data to: {output_csv}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return m0_level, m0_yoy


if __name__ == "__main__":
    main()
