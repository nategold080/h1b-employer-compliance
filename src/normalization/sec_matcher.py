"""Match H-1B employer profiles to SEC public companies.

Uses normalized name matching (O(N)) to link employer profiles to SEC CIKs.
Matches are stored in employer_sec_links table and used to enrich profiles
with financial data (revenue, employees, etc.)
"""

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from src.normalization.employers import normalize_employer_name
from src.scrapers.sec_downloader import (
    parse_company_tickers, extract_financials,
    download_companyfacts, download_submissions, parse_submissions,
    CACHE_DIR,
)
from src.storage.database import (
    upsert_public_companies, upsert_company_financials,
    upsert_employer_sec_links,
)


def build_sec_company_index(tickers_path: Path) -> dict[str, list[dict]]:
    """Build normalized name → SEC company index for matching.

    Returns dict mapping normalized names to list of SEC company records.
    """
    companies = parse_company_tickers(tickers_path)
    index = defaultdict(list)

    for co in companies:
        if not co["name"]:
            continue
        norm = normalize_employer_name(co["name"])
        if norm:
            index[norm].append(co)

    return index


def match_employers_to_sec(conn: sqlite3.Connection, tickers_path: Path) -> int:
    """Match employer profiles to SEC public companies.

    Uses exact normalized name matching for O(N) performance.
    Returns count of matches found.
    """
    sec_index = build_sec_company_index(tickers_path)

    # Get all employer profiles
    profiles = conn.execute("""
        SELECT profile_id, employer_name, normalized_name, total_lcas
        FROM employer_profiles
    """).fetchall()

    # Clear old links
    conn.execute("DELETE FROM employer_sec_links")
    conn.commit()

    matches = 0
    link_records = []

    for profile in profiles:
        norm_name = profile["normalized_name"]
        if not norm_name:
            continue

        if norm_name in sec_index:
            for co in sec_index[norm_name]:
                link_records.append({
                    "employer_name": profile["employer_name"],
                    "employer_normalized": norm_name,
                    "sec_cik": co["cik"],
                    "link_method": "exact_normalized",
                    "confidence": 0.95,
                    "is_subsidiary": 0,
                })
                matches += 1

    if link_records:
        upsert_employer_sec_links(conn, link_records)

    return matches


def import_sec_companies(conn: sqlite3.Connection, tickers_path: Path) -> int:
    """Import SEC company records from tickers file into public_companies table.

    Only imports companies that match H-1B employers (to keep DB focused).
    Returns count imported.
    """
    # Get matched CIKs
    links = conn.execute("SELECT DISTINCT sec_cik FROM employer_sec_links").fetchall()
    matched_ciks = {row["sec_cik"] for row in links}

    if not matched_ciks:
        return 0

    # Parse all companies and filter to matched ones
    companies = parse_company_tickers(tickers_path)
    records = []

    for co in companies:
        if co["cik"] in matched_ciks:
            records.append({
                "sec_cik": co["cik"],
                "company_name": co["name"],
                "ticker": co["ticker"],
                "exchange": co["exchange"],
                "sic_code": "",
                "sic_description": "",
                "state_of_incorporation": "",
                "business_city": "",
                "business_state": "",
            })

    return upsert_public_companies(conn, records)


def fetch_and_import_financials(conn: sqlite3.Connection,
                                  max_companies: int = 500,
                                  delay: float = 0.15) -> int:
    """Fetch XBRL financial data for matched public companies.

    Downloads companyfacts for each matched CIK, extracts annual financials,
    and imports into company_financials table.

    Args:
        max_companies: Max number of companies to fetch (rate limit safety)
        delay: Delay between API calls in seconds

    Returns count of financial records imported.
    """
    import time

    # Get CIKs to fetch (prioritize by LCA volume)
    rows = conn.execute("""
        SELECT DISTINCT esl.sec_cik, SUM(ep.total_lcas) as total_lcas
        FROM employer_sec_links esl
        JOIN employer_profiles ep ON ep.employer_name = esl.employer_name
        GROUP BY esl.sec_cik
        ORDER BY total_lcas DESC
        LIMIT ?
    """, (max_companies,)).fetchall()

    ciks = [row["sec_cik"] for row in rows]
    total_records = 0

    for i, cik in enumerate(ciks):
        # Download companyfacts
        path = download_companyfacts(cik, force=False)
        if not path:
            if i < len(ciks) - 1:
                time.sleep(delay)
            continue

        # Extract financials
        financials = extract_financials(path)

        records = []
        for fin in financials:
            records.append({
                "sec_cik": cik,
                "fiscal_year": fin["fiscal_year"],
                "revenue": fin.get("revenue"),
                "total_assets": fin.get("total_assets"),
                "net_income": fin.get("net_income"),
                "employees": fin.get("employees"),
                "h1b_per_employee": None,  # Computed later
                "h1b_wage_to_revenue": None,  # Computed later
                "quality_score": _score_financial(fin),
            })

        if records:
            upsert_company_financials(conn, records)
            total_records += len(records)

        if i < len(ciks) - 1:
            time.sleep(delay)

    return total_records


def enrich_company_metadata(conn: sqlite3.Connection,
                              max_companies: int = 500,
                              delay: float = 0.15) -> int:
    """Fetch SEC submissions for SIC code and address data."""
    import time

    rows = conn.execute("""
        SELECT sec_cik FROM public_companies
        WHERE sic_code IS NULL OR sic_code = ''
        LIMIT ?
    """, (max_companies,)).fetchall()

    updated = 0
    for i, row in enumerate(rows):
        cik = row["sec_cik"]
        path = download_submissions(cik, force=False)
        if not path:
            if i < len(rows) - 1:
                time.sleep(delay)
            continue

        meta = parse_submissions(path)
        conn.execute("""
            UPDATE public_companies
            SET sic_code = ?, sic_description = ?, state_of_incorporation = ?,
                business_city = ?, business_state = ?
            WHERE sec_cik = ?
        """, (
            meta["sic_code"], meta["sic_description"],
            meta["state_of_incorporation"],
            meta["business_city"], meta["business_state"],
            cik,
        ))
        updated += 1

        if i < len(rows) - 1:
            time.sleep(delay)

    conn.commit()
    return updated


def compute_h1b_financial_metrics(conn: sqlite3.Connection) -> int:
    """Compute H-1B usage metrics relative to company financials.

    Updates company_financials with h1b_per_employee and h1b_wage_to_revenue.
    Also updates employer_profiles with SEC financial context fields.

    Returns count of profiles updated.
    """
    # Get employer LCA counts by SEC link
    rows = conn.execute("""
        SELECT esl.sec_cik, ep.employer_name, ep.total_lcas,
               ep.avg_wage, ep.total_workers, ep.profile_id
        FROM employer_sec_links esl
        JOIN employer_profiles ep ON ep.employer_name = esl.employer_name
    """).fetchall()

    # Aggregate LCA counts per CIK
    cik_lcas = defaultdict(lambda: {"total_lcas": 0, "avg_wage": 0, "count": 0})
    employer_cik_map = {}

    for row in rows:
        cik = row["sec_cik"]
        cik_lcas[cik]["total_lcas"] += row["total_lcas"] or 0
        if row["avg_wage"]:
            cik_lcas[cik]["avg_wage"] += row["avg_wage"]
            cik_lcas[cik]["count"] += 1
        employer_cik_map[row["profile_id"]] = cik

    # Update company_financials with H-1B metrics
    for cik, lca_data in cik_lcas.items():
        avg_wage = lca_data["avg_wage"] / lca_data["count"] if lca_data["count"] > 0 else 0
        total_lcas = lca_data["total_lcas"]

        # Get most recent financials
        fin = conn.execute("""
            SELECT * FROM company_financials
            WHERE sec_cik = ?
            ORDER BY fiscal_year DESC LIMIT 1
        """, (cik,)).fetchone()

        if not fin:
            continue

        h1b_per_emp = None
        if fin["employees"] and fin["employees"] > 0 and total_lcas > 0:
            h1b_per_emp = total_lcas / fin["employees"]

        h1b_wage_rev = None
        if fin["revenue"] and fin["revenue"] > 0 and avg_wage > 0 and total_lcas > 0:
            h1b_wage_rev = (avg_wage * total_lcas) / fin["revenue"]

        conn.execute("""
            UPDATE company_financials
            SET h1b_per_employee = ?, h1b_wage_to_revenue = ?
            WHERE sec_cik = ? AND fiscal_year = ?
        """, (h1b_per_emp, h1b_wage_rev, cik, fin["fiscal_year"]))

    # Update employer_profiles with SEC data
    updated = 0
    for profile_id, cik in employer_cik_map.items():
        # Get company info
        co = conn.execute(
            "SELECT * FROM public_companies WHERE sec_cik = ?", (cik,)
        ).fetchone()

        # Get most recent financials
        fin = conn.execute("""
            SELECT * FROM company_financials
            WHERE sec_cik = ?
            ORDER BY fiscal_year DESC LIMIT 1
        """, (cik,)).fetchone()

        ticker = co["ticker"] if co else None
        revenue = fin["revenue"] if fin else None
        employees = fin["employees"] if fin else None

        # Get profile's LCA data for metric computation
        profile = conn.execute(
            "SELECT total_lcas, avg_wage FROM employer_profiles WHERE profile_id = ?",
            (profile_id,)
        ).fetchone()

        h1b_per_1000 = None
        if employees and employees > 0 and profile and profile["total_lcas"]:
            h1b_per_1000 = (profile["total_lcas"] / employees) * 1000

        rev_per_h1b = None
        if revenue and revenue > 0 and profile and profile["total_lcas"] and profile["total_lcas"] > 0:
            rev_per_h1b = revenue / profile["total_lcas"]

        conn.execute("""
            UPDATE employer_profiles
            SET sec_cik = ?, ticker = ?, is_public = 1,
                revenue = ?, employees = ?,
                h1b_per_1000_employees = ?, revenue_per_h1b = ?
            WHERE profile_id = ?
        """, (cik, ticker, revenue, employees, h1b_per_1000, rev_per_h1b, profile_id))
        updated += 1

    conn.commit()
    return updated


def _score_financial(fin: dict) -> float:
    """Quality score for a financial record."""
    score = 0.0
    if fin.get("revenue"):
        score += 0.30
    if fin.get("total_assets"):
        score += 0.20
    if fin.get("net_income") is not None:
        score += 0.20
    if fin.get("employees"):
        score += 0.30
    return round(score, 3)
