"""DOL Wage and Hour Division (WHD) enforcement data scraper.

Source: DOL WHD Compliance Action Database
URL: https://enforcedata.dol.gov/views/data_summary.php
Format: CSV download
Records: ~200K+ total, filter for H-1B related

Also scrapes the DOL debarment/willful violator list.
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

# WHD enforcement data API
WHD_API_URL = "https://enforcedata.dol.gov/api/enhSearch/search"

# DOL debarment list and willful violator list
DEBAR_URL = "https://www.dol.gov/agencies/whd/immigration/h1b/debarment"
WILLFUL_URL = "https://www.dol.gov/agencies/whd/immigration/h1b/willful-violator-list"


def download_whd(force: bool = False) -> Path:
    """Download WHD enforcement data via the DOL enforcement API."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / "whd_h1b.csv"

    if cache_file.exists() and not force:
        return cache_file

    # Use the DOL enforcement data CSV download endpoint
    # Filter for H-1B/H-2A/H-2B cases
    url = "https://enforcedata.dol.gov/views/data_catalogs.php"
    try:
        resp = httpx.get(
            "https://enforcedata.dol.gov/api/enhSearch",
            params={
                "db": "whd",
                "p": "h1b",
                "limit": 10000,
                "offset": 0,
            },
            headers=HEADERS,
            follow_redirects=True,
            timeout=120,
        )
        if resp.status_code == 200:
            cache_file.write_bytes(resp.content)
            return cache_file
    except httpx.HTTPError:
        pass

    raise RuntimeError("Failed to download WHD enforcement data")


def parse_whd_csv(csv_path: Path) -> list[dict]:
    """Parse WHD enforcement CSV into normalized records."""
    records = []
    text = csv_path.read_text(encoding="utf-8", errors="replace")

    # Handle both CSV and JSON responses
    if text.strip().startswith("[") or text.strip().startswith("{"):
        return _parse_whd_json(text)

    reader = csv.DictReader(StringIO(text))
    for row in reader:
        rec = _parse_whd_row(row, str(csv_path.name))
        if rec:
            records.append(rec)
    return records


def _parse_whd_json(text: str) -> list[dict]:
    """Parse WHD JSON API response."""
    import json
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []

    if isinstance(data, dict):
        data = data.get("results", data.get("data", []))

    records = []
    for item in data:
        if not isinstance(item, dict):
            continue
        rec = _parse_whd_row(item, "whd_api")
        if rec:
            records.append(rec)
    return records


def _parse_whd_row(row: dict, source: str) -> dict | None:
    """Parse a single WHD row."""
    case_id = _get(row, ["case_id", "CASE_ID", "Case ID", "case_number"])
    legal_name = _get(row, [
        "legal_name", "LEGAL_NAME", "Legal Name",
        "employer_name", "EMPLOYER_NAME",
    ])
    trade_name = _get(row, [
        "trade_name", "TRADE_NAME", "Trade Name", "dba_name",
    ])

    if not case_id and not legal_name:
        return None

    if not case_id:
        case_id = f"WHD-{hash(legal_name + source) % 100000:05d}"

    # Determine if H-1B related
    h1b_related = 0
    viol_type = _get(row, [
        "violation_type", "VIOLATION_TYPE", "Act ID", "act_id",
        "flsa_violtn_cnt", "h1b_violtn_cnt",
    ])

    # Check for H-1B related flags
    for key in ["h1b_violtn_cnt", "h1b_bw_amt", "H1B_VIOLATION"]:
        val = row.get(key)
        if val is not None and str(val).strip() not in ("", "0", "0.0"):
            h1b_related = 1
            break

    # Also check violation type text
    if viol_type and any(x in str(viol_type).upper() for x in ["H1B", "H-1B", "H1-B"]):
        h1b_related = 1

    back_wages = _safe_float(_get(row, [
        "bw_amt", "BW_AMT", "Back Wages", "back_wages",
        "h1b_bw_amt", "total_back_wages",
    ]))
    civil_penalty = _safe_float(_get(row, [
        "civil_penalty", "cmp_amt", "CMP_AMT", "Civil Money Penalty",
    ]))
    employees = _safe_int(_get(row, [
        "ee_violtd_cnt", "EE_VIOLTD_CNT", "Employees",
        "employees_affected", "nmbr_ee_violtd",
    ]))

    start_date = _get(row, [
        "findings_start_date", "FINDINGS_START_DATE",
        "invest_start_dt", "investigation_start",
    ])
    end_date = _get(row, [
        "findings_end_date", "FINDINGS_END_DATE",
        "invest_end_dt", "investigation_end",
    ])

    state = _get(row, [
        "st_cd", "ST_CD", "State", "employer_state", "state",
    ])
    city = _get(row, [
        "cty_nm", "CTY_NM", "City", "employer_city", "city",
    ])
    zip_code = _get(row, [
        "zip_cd", "ZIP_CD", "ZIP", "employer_zip",
    ])
    naics = _get(row, [
        "naics_code_description", "NAICS", "naics_cd", "naics_code",
    ])

    status = _get(row, [
        "case_status", "CASE_STATUS", "Status",
    ])

    return {
        "case_id": case_id,
        "trade_name": trade_name,
        "legal_name": legal_name,
        "employer_city": city,
        "employer_state": state,
        "employer_zip": _normalize_zip(zip_code),
        "naics_code": naics,
        "violation_type": viol_type if viol_type else ("H1B" if h1b_related else ""),
        "h1b_related": h1b_related,
        "back_wages": back_wages or 0.0,
        "civil_penalty": civil_penalty or 0.0,
        "employees_affected": employees or 0,
        "findings_start_date": _clean_date(start_date),
        "findings_end_date": _clean_date(end_date),
        "case_status": status,
        "source_file": source,
    }


def parse_debarment_html(html: str) -> list[dict]:
    """Parse DOL debarment/willful violator list from HTML."""
    records = []
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    cell_pattern = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)

    rows = row_pattern.findall(html)
    headers_found = False
    col_names = []

    for row_html in rows:
        raw_cells = cell_pattern.findall(row_html)
        cells = []
        for c in raw_cells:
            clean = re.sub(r"<[^>]+>", "", c).strip()
            clean = clean.replace("&nbsp;", "").replace("\xa0", "").strip()
            # Collapse internal whitespace
            clean = re.sub(r"\s+", " ", clean).strip()
            cells.append(clean)

        if not cells:
            continue

        # Detect header row (look for "employer" or "company" in any cell)
        if not headers_found and any("employer" in c.lower() or "company" in c.lower() for c in cells):
            col_names = [c.lower().strip() for c in cells]
            headers_found = True
            continue

        if not headers_found or len(cells) < 3:
            continue

        # Map cells to dict
        row_dict = {}
        for i, val in enumerate(cells):
            if i < len(col_names):
                row_dict[col_names[i]] = val

        # Find employer name (try multiple possible column headers)
        employer = ""
        for key in ["employer name", "employer", "company name", "company"]:
            if row_dict.get(key):
                employer = row_dict[key]
                break

        if not employer:
            continue

        city = ""
        state = ""
        start_date = ""
        end_date = ""
        violation = ""

        # Extract city/state from various column formats
        for key in ["city", "employer city"]:
            if row_dict.get(key):
                city = row_dict[key]
                break
        for key in ["state", "employer state"]:
            if row_dict.get(key):
                state = row_dict[key]
                break

        # Extract address if present (may contain city/state)
        address = row_dict.get("employer address", "")
        if address and not city:
            # Try to extract state from address
            state_match = re.search(r"\b([A-Z]{2})\b", address)
            if state_match:
                state = state_match.group(1)

        # Handle debarment period ("5/31/2024 to 5/30/2026")
        period = row_dict.get("debarment period", "")
        if period and "to" in period.lower():
            parts = re.split(r"\s+to\s+", period, flags=re.IGNORECASE)
            if len(parts) == 2:
                start_date = parts[0].strip()
                end_date = parts[1].strip()

        # Handle separate start/end columns
        for key in ["start date", "debarment start", "debar_start_date",
                     "date ofwillful violationdetermination",
                     "date of willful violation determination"]:
            if row_dict.get(key) and not start_date:
                start_date = row_dict[key]
                break

        for key in ["end date", "debarment end", "debar_end_date"]:
            if row_dict.get(key) and not end_date:
                end_date = row_dict[key]
                break

        # Willful violator flag
        willful = row_dict.get("willful violator", "")
        if willful.lower() in ("yes", "y"):
            violation = "Willful Violation"

        # Violation type
        for key in ["violation", "violation type"]:
            if row_dict.get(key):
                violation = row_dict[key]
                break

        debar_id = f"DEB-{abs(hash(employer + start_date)) % 100000:05d}"

        records.append({
            "debar_id": debar_id,
            "employer_name": employer.strip(),
            "employer_city": city.strip(),
            "employer_state": state.strip(),
            "program": "H-1B",
            "debar_start_date": _clean_date(start_date),
            "debar_end_date": _clean_date(end_date),
            "violation_type": violation.strip(),
            "source": "DOL Debarment List",
        })

    return records


def download_debarments(force: bool = False) -> list[dict]:
    """Download and parse the DOL debarment list."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    all_records = []

    # Download debarment list
    cache_debar = CACHE_DIR / "debarments.html"
    if cache_debar.exists() and not force:
        html = cache_debar.read_text(encoding="utf-8", errors="replace")
    else:
        try:
            resp = httpx.get(DEBAR_URL, headers=HEADERS, follow_redirects=True, timeout=60)
            if resp.status_code == 200:
                html = resp.text
                cache_debar.write_text(html, encoding="utf-8")
            else:
                html = ""
        except httpx.HTTPError:
            html = ""
    if html:
        all_records.extend(parse_debarment_html(html))

    # Download willful violator list
    cache_willful = CACHE_DIR / "willful_violators.html"
    if cache_willful.exists() and not force:
        html2 = cache_willful.read_text(encoding="utf-8", errors="replace")
    else:
        try:
            resp2 = httpx.get(WILLFUL_URL, headers=HEADERS, follow_redirects=True, timeout=60)
            if resp2.status_code == 200:
                html2 = resp2.text
                cache_willful.write_text(html2, encoding="utf-8")
            else:
                html2 = ""
        except httpx.HTTPError:
            html2 = ""
    if html2:
        willful = parse_debarment_html(html2)
        for rec in willful:
            rec["source"] = "DOL Willful Violator List"
            rec["violation_type"] = rec.get("violation_type") or "Willful Violation"
        all_records.extend(willful)

    return all_records


def _get(row: dict, names: list[str], default: str = "") -> str:
    for name in names:
        val = row.get(name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return default


def _normalize_zip(val: str | None) -> str:
    if not val:
        return ""
    s = str(val).strip()
    m = re.match(r"^(\d{5})", s)
    if m:
        return m.group(1)
    return s


def _clean_date(val: str | None) -> str:
    if not val:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    # MM/DD/YYYY -> YYYY-MM-DD
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s[:10]
    return s


def _safe_int(val) -> int | None:
    if val is None or str(val).strip() in ("", ".", "N/A"):
        return None
    try:
        return int(float(str(val).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    if val is None or str(val).strip() in ("", ".", "N/A"):
        return None
    try:
        return float(str(val).strip().replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None
