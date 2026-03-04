"""SEC EDGAR downloader for public company financial data.

Downloads company_tickers.json for CIK matching and XBRL companyfacts
for financial data (revenue, assets, income, employees).

Rate limit: 10 requests/sec, use 0.15s delay.
Required header: User-Agent with contact email.
"""

import json
import time
from pathlib import Path

import httpx

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "sec"
CACHE_DIR = RAW_DIR / "xbrl"

SEC_HEADERS = {
    "User-Agent": "DataFactory nathanmauricegoldberg@gmail.com",
    "Accept-Encoding": "gzip, deflate",
}

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

# XBRL concepts to extract, in priority order (first found wins)
FINANCIAL_CONCEPTS = {
    "revenue": [
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:SalesRevenueNet",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
    ],
    "total_assets": ["us-gaap:Assets"],
    "net_income": ["us-gaap:NetIncomeLoss", "us-gaap:ProfitLoss"],
    "employees": ["dei:EntityNumberOfEmployees"],
}


def download_company_tickers(force: bool = False) -> Path:
    """Download SEC company_tickers.json mapping names to CIKs."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / "company_tickers.json"

    if out_path.exists() and not force:
        return out_path

    resp = httpx.get(COMPANY_TICKERS_URL, headers=SEC_HEADERS, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    out_path.write_text(resp.text, encoding="utf-8")
    return out_path


def parse_company_tickers(path: Path) -> list[dict]:
    """Parse company_tickers.json into structured records.

    Returns list of dicts with: cik, name, ticker, exchange.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    records = []
    for entry in data.values():
        records.append({
            "cik": int(entry.get("cik_str", 0)),
            "name": entry.get("title", ""),
            "ticker": entry.get("ticker", ""),
            "exchange": entry.get("exchange", ""),
        })
    return records


def download_companyfacts(cik: int, force: bool = False) -> Path | None:
    """Download XBRL companyfacts JSON for a specific CIK.

    Returns path to cached file, or None if download fails.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CACHE_DIR / f"CIK{cik:010d}.json"

    if out_path.exists() and not force:
        return out_path

    url = COMPANYFACTS_URL.format(cik=cik)
    try:
        resp = httpx.get(url, headers=SEC_HEADERS, timeout=30, follow_redirects=True)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        out_path.write_text(resp.text, encoding="utf-8")
        return out_path
    except httpx.HTTPError:
        return None


def download_submissions(cik: int, force: bool = False) -> Path | None:
    """Download SEC submissions JSON for company metadata (SIC code, address)."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"submissions_CIK{cik:010d}.json"

    if out_path.exists() and not force:
        return out_path

    url = SUBMISSIONS_URL.format(cik=cik)
    try:
        resp = httpx.get(url, headers=SEC_HEADERS, timeout=30, follow_redirects=True)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        out_path.write_text(resp.text, encoding="utf-8")
        return out_path
    except httpx.HTTPError:
        return None


def parse_submissions(path: Path) -> dict:
    """Parse submissions JSON for company metadata."""
    data = json.loads(path.read_text(encoding="utf-8"))
    addresses = data.get("addresses", {})
    business = addresses.get("business", {})

    return {
        "sic_code": data.get("sic", ""),
        "sic_description": data.get("sicDescription", ""),
        "state_of_incorporation": data.get("stateOfIncorporation", ""),
        "business_city": business.get("city", ""),
        "business_state": business.get("stateOrCountry", ""),
    }


def extract_financials(path: Path) -> list[dict]:
    """Extract annual financial data from XBRL companyfacts JSON.

    Extracts most recent annual values for revenue, total_assets, net_income,
    and employees from 10-K filings (form="10-K", fp="FY").

    Returns list of dicts, one per fiscal year.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    facts = data.get("facts", {})

    # Collect annual values by fiscal year
    year_data = {}

    for metric_name, concept_list in FINANCIAL_CONCEPTS.items():
        for concept in concept_list:
            # Parse namespace:concept
            parts = concept.split(":")
            if len(parts) != 2:
                continue
            namespace, concept_name = parts

            ns_data = facts.get(namespace, {})
            concept_data = ns_data.get(concept_name, {})
            units = concept_data.get("units", {})

            # Financial values are in USD, employee counts are in "pure" units
            unit_key = "USD" if metric_name != "employees" else "pure"
            values = units.get(unit_key, [])

            for entry in values:
                form = entry.get("form", "")
                fp = entry.get("fp", "")

                # Only 10-K annual filings
                if form != "10-K" or fp != "FY":
                    continue

                fy = entry.get("fy")
                val = entry.get("val")
                if fy is None or val is None:
                    continue

                fy = int(fy)
                if fy not in year_data:
                    year_data[fy] = {}

                # Only set if not already set (first concept in priority list wins)
                if metric_name not in year_data[fy]:
                    year_data[fy][metric_name] = val

            # If we found data with this concept, don't try fallback concepts
            if any(metric_name in year_data.get(y, {}) for y in year_data):
                break

    # Convert to list of records
    records = []
    for fy in sorted(year_data.keys()):
        d = year_data[fy]
        if not d:
            continue
        records.append({
            "fiscal_year": fy,
            "revenue": d.get("revenue"),
            "total_assets": d.get("total_assets"),
            "net_income": d.get("net_income"),
            "employees": int(d["employees"]) if d.get("employees") else None,
        })

    return records


def batch_download_companyfacts(ciks: list[int], force: bool = False,
                                  delay: float = 0.15) -> dict[int, Path]:
    """Download companyfacts for multiple CIKs with rate limiting.

    Returns dict mapping CIK to file path (only successful downloads).
    """
    results = {}
    for i, cik in enumerate(ciks):
        path = download_companyfacts(cik, force=force)
        if path:
            results[cik] = path
        if i < len(ciks) - 1:
            time.sleep(delay)
    return results
