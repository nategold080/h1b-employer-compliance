"""NAICS industry sector classification for H-1B employers.

Maps NAICS codes from LCA/USCIS data to human-readable sector and subsector names.
Uses Census Bureau NAICS hierarchy: 2-digit sector, 3-digit subsector.
"""

import sqlite3
from collections import defaultdict
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "naics_sectors.yaml"


def load_naics_config(config_path: Path | None = None) -> dict:
    """Load NAICS sector/subsector mappings from YAML config."""
    path = config_path or CONFIG_PATH
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return {
        "sectors": data.get("sectors", {}),
        "subsectors": data.get("subsectors", {}),
    }


def get_sector(naics_code: str, config: dict | None = None) -> tuple[str | None, str | None]:
    """Get sector code and name for a NAICS code.

    Returns (sector_code, sector_name) or (None, None) if not found.
    """
    if not naics_code or not isinstance(naics_code, str):
        return None, None

    code = naics_code.strip()
    if len(code) < 2:
        return None, None

    if config is None:
        config = load_naics_config()

    sector_code = code[:2]
    sector_name = config["sectors"].get(sector_code)

    return (sector_code, sector_name) if sector_name else (None, None)


def get_subsector(naics_code: str, config: dict | None = None) -> tuple[str | None, str | None]:
    """Get subsector code and name for a NAICS code.

    Returns (subsector_code, subsector_name) or (None, None) if not found.
    """
    if not naics_code or not isinstance(naics_code, str):
        return None, None

    code = naics_code.strip()
    if len(code) < 3:
        return None, None

    if config is None:
        config = load_naics_config()

    subsector_code = code[:3]
    subsector_name = config["subsectors"].get(subsector_code)

    return (subsector_code, subsector_name) if subsector_name else (None, None)


def classify_naics(naics_code: str, config: dict | None = None) -> dict:
    """Full NAICS classification for a code.

    Returns dict with sector_code, sector_name, subsector_code, subsector_name.
    """
    if config is None:
        config = load_naics_config()

    sector_code, sector_name = get_sector(naics_code, config)
    subsector_code, subsector_name = get_subsector(naics_code, config)

    return {
        "naics_code": naics_code,
        "sector_code": sector_code,
        "sector_name": sector_name,
        "subsector_code": subsector_code,
        "subsector_name": subsector_name,
    }


def load_naics_reference_table(conn: sqlite3.Connection, config: dict | None = None) -> int:
    """Load NAICS reference data into the naics_codes table.

    Populates the table with all known sector and subsector codes from config.
    Returns count of records inserted.
    """
    from src.storage.database import upsert_naics_codes

    if config is None:
        config = load_naics_config()

    records = []

    # Add sector-level entries (2-digit codes)
    for code, name in config["sectors"].items():
        records.append({
            "naics_code": code,
            "naics_description": name,
            "sector_code": code,
            "sector_name": name,
            "subsector_code": None,
            "subsector_name": None,
        })

    # Add subsector-level entries (3-digit codes)
    for code, name in config["subsectors"].items():
        sector_code = code[:2]
        sector_name = config["sectors"].get(sector_code, "Unknown")
        records.append({
            "naics_code": code,
            "naics_description": name,
            "sector_code": sector_code,
            "sector_name": sector_name,
            "subsector_code": code,
            "subsector_name": name,
        })

    return upsert_naics_codes(conn, records)


def classify_employer_profiles(conn: sqlite3.Connection, config: dict | None = None) -> int:
    """Assign industry sector to all employer profiles based on most frequent NAICS code.

    For each employer profile, finds the most common NAICS code across their LCA filings
    and maps it to a human-readable sector/subsector name.

    Returns count of profiles updated.
    """
    if config is None:
        config = load_naics_config()

    # Get primary NAICS for each employer (most frequent across LCA filings)
    rows = conn.execute("""
        SELECT employer_name, naics_code, COUNT(*) as cnt
        FROM lca_applications
        WHERE naics_code IS NOT NULL AND naics_code != ''
              AND employer_name IS NOT NULL AND employer_name != ''
        GROUP BY employer_name, naics_code
        ORDER BY employer_name, cnt DESC
    """).fetchall()

    # Pick the most frequent NAICS per employer
    employer_naics = {}
    for row in rows:
        name = row["employer_name"]
        if name not in employer_naics:
            employer_naics[name] = row["naics_code"]

    # Also check USCIS data for employers not in LCA
    uscis_rows = conn.execute("""
        SELECT employer_name, naics_code, COUNT(*) as cnt
        FROM uscis_employers
        WHERE naics_code IS NOT NULL AND naics_code != ''
              AND employer_name IS NOT NULL AND employer_name != ''
        GROUP BY employer_name, naics_code
        ORDER BY employer_name, cnt DESC
    """).fetchall()

    uscis_naics = {}
    for row in uscis_rows:
        name = row["employer_name"]
        if name not in uscis_naics:
            uscis_naics[name] = row["naics_code"]

    # Get all employer profiles
    profiles = conn.execute("""
        SELECT profile_id, employer_name
        FROM employer_profiles
    """).fetchall()

    updated = 0
    for profile in profiles:
        naics = employer_naics.get(profile["employer_name"])
        if not naics:
            naics = uscis_naics.get(profile["employer_name"])
        if not naics:
            continue

        classification = classify_naics(naics, config)
        if not classification["sector_name"]:
            continue

        conn.execute("""
            UPDATE employer_profiles
            SET primary_naics = ?,
                industry_sector = ?,
                industry_subsector = ?
            WHERE profile_id = ?
        """, (
            naics,
            classification["sector_name"],
            classification["subsector_name"],
            profile["profile_id"],
        ))
        updated += 1

    conn.commit()
    return updated
