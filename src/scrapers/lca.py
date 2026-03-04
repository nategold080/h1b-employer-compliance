"""DOL OFLC LCA (Labor Condition Application) disclosure data scraper.

Source: DOL ETA Office of Foreign Labor Certification
URL: https://www.dol.gov/agencies/eta/foreign-labor/performance
Format: Excel (.xlsx) files, quarterly releases
Records: ~800K+ LCAs per fiscal year
"""

import re
from pathlib import Path

import httpx

from src.normalization.employers import annualize_wage

# LCA disclosure file URL pattern
# Files: LCA_Disclosure_Data_FY{year}_Q{quarter}.xlsx
LCA_BASE_URL = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs"

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (H1B-Employer-Compliance-Tracker/1.0; nathanmauricegoldberg@gmail.com)"
}

# Available fiscal years (Q4 = full cumulative year)
AVAILABLE_YEARS = [2020, 2021, 2022, 2023, 2025]


def get_lca_url(fiscal_year: int, quarter: int = 4) -> str:
    """Get the LCA disclosure file URL for a given fiscal year/quarter."""
    return f"{LCA_BASE_URL}/LCA_Disclosure_Data_FY{fiscal_year}_Q{quarter}.xlsx"


def download_lca(fiscal_year: int, quarter: int = 4, force: bool = False) -> Path:
    """Download an LCA disclosure Excel file."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"lca_fy{fiscal_year}_q{quarter}.xlsx"

    if cache_file.exists() and not force:
        return cache_file

    url = get_lca_url(fiscal_year, quarter)
    resp = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=300)

    if resp.status_code == 200:
        cache_file.write_bytes(resp.content)
        return cache_file

    # Try Q1 if Q4 fails
    if quarter == 4:
        url_q1 = get_lca_url(fiscal_year, 1)
        resp2 = httpx.get(url_q1, headers=HEADERS, follow_redirects=True, timeout=300)
        if resp2.status_code == 200:
            cache_file = CACHE_DIR / f"lca_fy{fiscal_year}_q1.xlsx"
            cache_file.write_bytes(resp2.content)
            return cache_file

    raise RuntimeError(f"Failed to download LCA FY{fiscal_year} Q{quarter}: HTTP {resp.status_code}")


def parse_lca(xlsx_path: Path, fiscal_year: int | None = None) -> list[dict]:
    """Parse an LCA disclosure Excel file into normalized records."""
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required to parse LCA Excel files")

    records = []
    wb = openpyxl.load_workbook(str(xlsx_path), read_only=True, data_only=True)
    ws = wb.active

    # Get headers from first row
    rows = ws.iter_rows(values_only=True)
    headers = [str(h).strip().upper() if h else "" for h in next(rows)]
    col_map = {h: i for i, h in enumerate(headers) if h}

    for row_vals in rows:
        rec = _parse_lca_row(row_vals, col_map, fiscal_year, str(xlsx_path.name))
        if rec:
            records.append(rec)

    wb.close()
    return records


def _parse_lca_row(row: tuple, col_map: dict, fiscal_year: int | None, source: str) -> dict | None:
    """Parse a single LCA row into a normalized record."""
    def get(field: str, default=""):
        idx = col_map.get(field)
        if idx is None:
            # Try common variations
            for alt in _field_aliases(field):
                idx = col_map.get(alt)
                if idx is not None:
                    break
        if idx is None or idx >= len(row):
            return default
        val = row[idx]
        if val is None:
            return default
        return val

    case_num = _clean(get("CASE_NUMBER"))
    if not case_num:
        return None

    # Parse wages
    wage_rate = _safe_float(get("WAGE_RATE_OF_PAY_FROM"))
    if wage_rate is None:
        wage_rate = _safe_float(get("WAGE_RATE_OF_PAY"))
    wage_unit = _clean(get("WAGE_UNIT_OF_PAY"))
    if not wage_unit:
        wage_unit = _clean(get("PW_UNIT_OF_PAY"))

    pw_rate = _safe_float(get("PREVAILING_WAGE"))
    pw_unit = _clean(get("PW_UNIT_OF_PAY"))

    ann_wage = annualize_wage(wage_rate, wage_unit)
    ann_pw = annualize_wage(pw_rate, pw_unit)

    # Compute wage ratio
    wage_ratio = None
    if ann_wage and ann_pw and ann_pw > 0:
        wage_ratio = round(ann_wage / ann_pw, 3)

    # Determine fiscal year
    fy = fiscal_year
    if fy is None:
        fy_match = re.search(r"FY(\d{4})", source, re.IGNORECASE)
        if fy_match:
            fy = int(fy_match.group(1))

    return {
        "case_number": case_num,
        "case_status": _clean(get("CASE_STATUS")),
        "employer_name": _clean(get("EMPLOYER_NAME")),
        "employer_ein": _clean(get("EMPLOYER_BUSINESS_DBA")),  # DBA, not EIN in older files
        "employer_city": _clean(get("EMPLOYER_CITY")),
        "employer_state": _clean(get("EMPLOYER_STATE")),
        "employer_zip": _normalize_zip(get("EMPLOYER_POSTAL_CODE")),
        "employer_country": _clean(get("EMPLOYER_COUNTRY")),
        "naics_code": _clean(get("NAICS_CODE")),
        "soc_code": _clean(get("SOC_CODE")),
        "soc_title": _clean(get("SOC_TITLE")),
        "job_title": _clean(get("JOB_TITLE")),
        "wage_rate": wage_rate,
        "wage_unit": wage_unit,
        "annualized_wage": ann_wage,
        "prevailing_wage": pw_rate,
        "pw_unit": pw_unit,
        "annualized_pw": ann_pw,
        "wage_ratio": wage_ratio,
        "worksite_city": _clean(get("WORKSITE_CITY")),
        "worksite_state": _clean(get("WORKSITE_STATE")),
        "worksite_zip": _normalize_zip(get("WORKSITE_POSTAL_CODE")),
        "visa_class": _clean(get("VISA_CLASS")),
        "submit_date": _clean_date(get("RECEIVED_DATE")),
        "decision_date": _clean_date(get("DECISION_DATE")),
        "begin_date": _clean_date(get("BEGIN_DATE")),
        "end_date": _clean_date(get("END_DATE")),
        "total_workers": _safe_int(get("TOTAL_WORKERS")),
        "full_time": _clean(get("FULL_TIME_POSITION")),
        "fiscal_year": fy,
        "source_file": source,
    }


def _field_aliases(field: str) -> list[str]:
    """Get common field name variations for LCA files."""
    aliases = {
        "WAGE_RATE_OF_PAY_FROM": ["WAGE_RATE_OF_PAY_FROM_1", "WAGE_RATE_OF_PAY"],
        "WAGE_RATE_OF_PAY": ["WAGE_RATE_OF_PAY_FROM", "WAGE_RATE_OF_PAY_FROM_1"],
        "WAGE_UNIT_OF_PAY": ["WAGE_UNIT_OF_PAY_1"],
        "RECEIVED_DATE": ["RECEIVED_DATE_1", "CASE_SUBMITTED"],
        "EMPLOYER_BUSINESS_DBA": ["EMPLOYER_DBA", "TRADE_NAME_DBA"],
        "EMPLOYER_POSTAL_CODE": ["EMPLOYER_ZIP", "EMPLOYER_POSTAL_CODE_1"],
        "WORKSITE_POSTAL_CODE": ["WORKSITE_ZIP", "WORKSITE_POSTAL_CODE_1"],
        "TOTAL_WORKERS": ["TOTAL_WORKER_POSITIONS", "TOTAL_WORKERS_H1B"],
    }
    return aliases.get(field, [])


def _clean(val) -> str:
    """Clean a field value."""
    if val is None:
        return ""
    return str(val).strip()


def _clean_date(val) -> str:
    """Clean a date value to YYYY-MM-DD."""
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""

    # Handle datetime objects
    if hasattr(val, 'strftime'):
        return val.strftime("%Y-%m-%d")

    # Handle MM/DD/YYYY format
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"

    # Handle YYYY-MM-DD format
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s[:10]

    return s


def _normalize_zip(val) -> str:
    """Normalize ZIP code to 5 digits."""
    if val is None:
        return ""
    s = str(val).strip()
    m = re.match(r"^(\d{5})", s)
    if m:
        return m.group(1)
    if s.isdigit() and len(s) < 5:
        return s.zfill(5)
    return s


def _safe_int(val) -> int | None:
    """Safely convert to int."""
    if val is None:
        return None
    try:
        v = str(val).strip()
        if not v or v in ("", "."):
            return None
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    """Safely convert to float."""
    if val is None:
        return None
    try:
        v = str(val).strip().replace(",", "").replace("$", "")
        if not v or v in ("", "."):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None
