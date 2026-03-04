"""USCIS H-1B Employer Data Hub scraper.

Source: USCIS H-1B Employer Data Hub
URL: https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub
Format: CSV files for each fiscal year
Records: ~50K-80K employers per fiscal year
"""

import csv
import re
from io import StringIO
from pathlib import Path

import httpx

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (H1B-Employer-Compliance-Tracker/1.0; nathanmauricegoldberg@gmail.com)"
}

# USCIS provides the data as downloadable CSV files
# The data hub has files per fiscal year
USCIS_BASE_URL = "https://www.uscis.gov/sites/default/files/document/data"

AVAILABLE_YEARS = [2020, 2021, 2022, 2023]


def get_uscis_url(fiscal_year: int) -> str:
    """Get USCIS employer data hub CSV URL."""
    return f"{USCIS_BASE_URL}/h1b_datahubexport-{fiscal_year}.csv"


def download_uscis(fiscal_year: int, force: bool = False) -> Path:
    """Download USCIS employer data hub CSV."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"uscis_fy{fiscal_year}.csv"

    if cache_file.exists() and not force:
        return cache_file

    # Try multiple URL patterns since USCIS changes them
    urls = [
        f"{USCIS_BASE_URL}/h1b_datahubexport-{fiscal_year}.csv",
        f"{USCIS_BASE_URL}/H-1B_Disclosure_Data_FY{fiscal_year}.csv",
        f"{USCIS_BASE_URL}/h-1b-employer-data-hub-fy{fiscal_year}.csv",
    ]

    for url in urls:
        try:
            resp = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=300)
            if resp.status_code == 200 and len(resp.content) > 1000:
                cache_file.write_bytes(resp.content)
                return cache_file
        except httpx.HTTPError:
            continue

    raise RuntimeError(f"Failed to download USCIS data for FY{fiscal_year}")


def parse_uscis_csv(csv_path: Path, fiscal_year: int | None = None) -> list[dict]:
    """Parse USCIS employer data hub CSV into normalized records."""
    records = []
    text = csv_path.read_text(encoding="utf-8", errors="replace")
    reader = csv.DictReader(StringIO(text))

    for row in reader:
        rec = _parse_uscis_row(row, fiscal_year, str(csv_path.name))
        if rec:
            records.append(rec)

    return records


def _parse_uscis_row(row: dict, fiscal_year: int | None, source: str) -> dict | None:
    """Parse a single USCIS row."""
    employer_name = _get_field(row, [
        "Employer", "EMPLOYER", "Employer (Petitioner) Name",
        "EMPLOYER_NAME", "Petitioner Name",
    ])
    if not employer_name:
        return None

    state = _get_field(row, [
        "State", "STATE", "Employer State", "EMPLOYER_STATE",
    ])
    city = _get_field(row, [
        "City", "CITY", "Employer City", "EMPLOYER_CITY",
    ])
    zip_code = _get_field(row, [
        "ZIP", "ZIP Code", "EMPLOYER_ZIP", "Zip",
    ])
    naics = _get_field(row, [
        "NAICS", "NAICS_CODE", "Industry (NAICS) Code",
    ])

    # Petition counts
    init_app = _safe_int(_get_field(row, [
        "Initial Approvals", "INITIAL_APPROVALS", "Initial Approval",
    ]))
    init_den = _safe_int(_get_field(row, [
        "Initial Denials", "INITIAL_DENIALS", "Initial Denial",
    ]))
    cont_app = _safe_int(_get_field(row, [
        "Continuing Approvals", "CONTINUING_APPROVALS", "Continuing Approval",
    ]))
    cont_den = _safe_int(_get_field(row, [
        "Continuing Denials", "CONTINUING_DENIALS", "Continuing Denial",
    ]))

    total_app = (init_app or 0) + (cont_app or 0)
    total_den = (init_den or 0) + (cont_den or 0)
    total = total_app + total_den

    approval_rate = None
    if total > 0:
        approval_rate = round(100.0 * total_app / total, 2)

    # RFE rate if available
    rfe_rate = _safe_float(_get_field(row, [
        "RFE Rate", "RFE_RATE",
    ]))

    tax_id = _get_field(row, [
        "Tax ID", "TAX_ID", "EIN",
    ])

    # Determine fiscal year
    fy = fiscal_year
    if fy is None:
        fy_val = _get_field(row, ["Fiscal Year", "FISCAL_YEAR", "FY"])
        if fy_val:
            fy = _safe_int(fy_val)
        if fy is None:
            m = re.search(r"FY(\d{4})", source, re.IGNORECASE)
            if m:
                fy = int(m.group(1))

    # Build employer key
    key = f"{employer_name.upper().strip()}|{state or ''}|{fy or ''}"

    return {
        "employer_key": key,
        "fiscal_year": fy,
        "employer_name": employer_name.strip(),
        "employer_city": (city or "").strip(),
        "employer_state": (state or "").strip(),
        "employer_zip": _normalize_zip(zip_code),
        "naics_code": (naics or "").strip(),
        "initial_approvals": init_app or 0,
        "initial_denials": init_den or 0,
        "continuing_approvals": cont_app or 0,
        "continuing_denials": cont_den or 0,
        "total_approvals": total_app,
        "total_denials": total_den,
        "approval_rate": approval_rate,
        "rfe_rate": rfe_rate,
        "tax_id": (tax_id or "").strip(),
        "source_file": source,
    }


def _get_field(row: dict, names: list[str], default: str = "") -> str:
    """Get field value trying multiple column names."""
    for name in names:
        val = row.get(name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return default


def _normalize_zip(val: str | None) -> str:
    """Normalize ZIP code."""
    if not val:
        return ""
    s = str(val).strip()
    m = re.match(r"^(\d{5})", s)
    if m:
        return m.group(1)
    if s.isdigit() and len(s) < 5:
        return s.zfill(5)
    return s


def _safe_int(val) -> int | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(str(val).strip().replace(",", "").replace("%", ""))
    except (ValueError, TypeError):
        return None
