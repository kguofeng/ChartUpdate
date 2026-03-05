"""
Shared utility for X-13ARIMA-SEATS seasonal adjustment.

Provides get_x13_path() to locate the x13as binary,
and x13_arima_analysis() which runs X13 directly (handling both
x13as_html and x13as_ascii output conventions) with a fallback
to seasonal_decompose if the binary is unavailable.
"""

import os
import subprocess
import tempfile

import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose


def get_x13_path():
    """
    Locate the X-13ARIMA-SEATS binary. Search order:
    1. X13PATH environment variable
    2. x13binary pip package (pip install x13binary)
    3. x13as / x13as_html / x13as_ascii on system PATH
    Returns the path string, or None if not found.
    """
    # 1. Check environment variable
    env_path = os.environ.get("X13PATH", "")
    if env_path:
        if os.path.isfile(env_path):
            return env_path
        # Try it as a directory containing the binary
        for name in ("x13as", "x13as_ascii", "x13as_html", "x13as.exe"):
            candidate = os.path.join(env_path, name)
            if os.path.isfile(candidate):
                return candidate

    # 2. Try x13binary pip package
    try:
        import x13binary
        path = x13binary.find_x13_bin()
        if path and os.path.isfile(path):
            return path
    except (ImportError, Exception):
        pass

    # 3. Check system PATH
    for name in ("x13as", "x13as_ascii", "x13as_html", "x13as.exe"):
        try:
            subprocess.check_call(
                [name], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            return name
        except (OSError, subprocess.CalledProcessError):
            pass

    return None


_x13_path_cache = None


def _get_cached_x13_path():
    global _x13_path_cache
    if _x13_path_cache is None:
        _x13_path_cache = get_x13_path() or ""
    return _x13_path_cache or None


def _is_html_binary(x12path):
    """Check if the binary is the HTML variant (produces _err.html instead of .err)."""
    return "html" in os.path.basename(x12path).lower()


def _read_x13_table(filepath):
    """Read an X13 output table file (d11/d12/d13) into a Series."""
    text = open(filepath).read()
    lines = text.strip().split("\n")
    dates = []
    values = []
    for line in lines[2:]:  # skip header + separator
        parts = line.split()
        if len(parts) < 2:
            continue
        date_str = parts[0]
        try:
            value = float(parts[-1])
        except ValueError:
            continue
        # Handle YYYYMM format (e.g. "200001") or YYYY.MM format (e.g. "2000.1")
        if "." in date_str:
            year, period = date_str.split(".")
            year, period = int(year), int(period)
        elif len(date_str) == 6 and date_str.isdigit():
            year = int(date_str[:4])
            period = int(date_str[4:])
        else:
            continue
        dates.append(pd.Timestamp(year=year, month=period, day=1))
        values.append(value)
    if not dates:
        return None
    return pd.Series(values, index=pd.DatetimeIndex(dates))


def _run_x13_direct(series, x12path, tempdir=None, print_stdout=False):
    """
    Run X13-ARIMA-SEATS directly, handling both ASCII and HTML binary variants.
    Returns a Result object with seasadj, trend, seasonal, resid, out, err.
    """
    if tempdir and not os.path.exists(tempdir):
        os.makedirs(tempdir, exist_ok=True)

    # Build the spec
    freq_map = {1: 1, 3: 4, 6: 2, 12: 12}
    idx = series.index
    if hasattr(idx, "freqstr") and idx.freqstr:
        freqstr = idx.freqstr
        if freqstr in ("M", "ME", "MS"):
            period = 12
        elif freqstr in ("Q", "QE", "QS", "Q-DEC", "QE-DEC", "QS-OCT"):
            period = 4
        else:
            period = 12
    else:
        period = 12

    start_year = idx[0].year
    start_period = idx[0].month if period == 12 else (idx[0].month - 1) // 3 + 1

    spec = "series{\n"
    spec += f'  title="seasonal_adjustment"\n'
    spec += f"  start={start_year}.{start_period}\n"
    spec += f"  period={period}\n"
    spec += "  data=(\n"
    for val in series.values:
        spec += f"    {val}\n"
    spec += "  )\n}\n"
    spec += "automdl{}\n"
    spec += "outlier{}\n"
    spec += "x11{ save=(d11 d12 d13) }\n"

    # Write spec to temp file
    ftempin = tempfile.NamedTemporaryFile(
        delete=False, suffix=".spc", dir=tempdir, mode="w"
    )
    ftempout = tempfile.NamedTemporaryFile(delete=False, dir=tempdir)
    ftempout_name = ftempout.name
    ftempout.close()

    try:
        ftempin.write(spec)
        ftempin.close()

        # Run x13
        args = [x12path, ftempin.name[:-4], ftempout_name]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout_bytes = p.communicate()[0]
        if print_stdout and stdout_bytes:
            print(stdout_bytes.decode(errors="replace"))

        # Determine output file naming convention
        is_html = _is_html_binary(x12path)

        if is_html:
            err_file = ftempout_name + "_err.html"
            out_file = ftempout_name + ".html"
        else:
            err_file = ftempout_name + ".err"
            out_file = ftempout_name + ".out"

        d11_file = ftempout_name + ".d11"
        d12_file = ftempout_name + ".d12"
        d13_file = ftempout_name + ".d13"

        # Check for errors
        err_text = ""
        if os.path.exists(err_file):
            err_text = open(err_file).read()
        out_text = ""
        if os.path.exists(out_file):
            out_text = open(out_file).read()

        # For HTML error files, check the body for actual error messages
        # (the HTML header/metadata always contains "Error File" boilerplate)
        if is_html and err_text:
            import re
            body_match = re.search(r"<body[^>]*>(.*)</body>", err_text, re.DOTALL | re.IGNORECASE)
            body_text = body_match.group(1) if body_match else ""
            # Strip HTML tags to get plain text
            plain_err = re.sub(r"<[^>]+>", " ", body_text).strip()
            if plain_err and "ERROR:" in plain_err.upper():
                raise RuntimeError(f"X13 returned errors: {plain_err[:500]}")
        elif err_text and "ERROR:" in err_text.upper():
            raise RuntimeError(f"X13 returned errors: {err_text[:500]}")

        # Read results
        if not os.path.exists(d11_file):
            raise FileNotFoundError(f"X13 did not produce seasonal adjustment output ({d11_file})")

        seasadj = _read_x13_table(d11_file)
        trend = _read_x13_table(d12_file) if os.path.exists(d12_file) else None
        irregular = _read_x13_table(d13_file) if os.path.exists(d13_file) else None

        # Align with original index
        if seasadj is not None:
            seasadj.index = series.index[:len(seasadj)]
            if hasattr(series.index, "freq"):
                seasadj.index.freq = series.index.freq
        if trend is not None:
            trend.index = series.index[:len(trend)]
        if irregular is not None:
            irregular.index = series.index[:len(irregular)]

        class Result:
            pass

        res = Result()
        res.seasadj = seasadj
        res.trend = trend
        res.irregular = irregular
        res.seasonal = series - seasadj if seasadj is not None else None
        res.resid = irregular
        res.out = out_text
        res.err = err_text
        return res

    finally:
        # Clean up temp files
        for f in [ftempin.name, ftempout_name]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass
        # Clean up X13 output files
        for suffix in [".d11", ".d12", ".d13", ".err", ".out", ".html",
                       "_err.html", "_log.html", ".spc", ".log", ".udg"]:
            path = ftempout_name + suffix
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass


def x13_arima_analysis(series, x12path=None, prefer_x13=True, tempdir=None, print_stdout=False):
    """
    Run X-13ARIMA-SEATS seasonal adjustment.
    Tries the real X13 binary first; falls back to seasonal_decompose on failure.
    """
    if x12path is None:
        x12path = _get_cached_x13_path()

    if x12path is not None:
        try:
            return _run_x13_direct(series, x12path, tempdir=tempdir,
                                   print_stdout=print_stdout)
        except Exception as e:
            print(f"X13 ARIMA failed ({e}), falling back to seasonal_decompose")

    # Fallback: seasonal_decompose
    freq = getattr(series.index, "freq", None)
    period = getattr(freq, "n", 12) if freq is not None else 12

    dec = seasonal_decompose(series, model="additive", period=period)

    class Result:
        pass

    res = Result()
    res.seasadj = dec.observed - dec.seasonal
    res.trend = dec.trend
    res.seasonal = dec.seasonal
    res.resid = dec.resid
    res.out = ""
    res.err = ""
    return res
