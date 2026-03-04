"""Cross-linking engine for H-1B employer compliance data.

Links LCA applications → USCIS employer data → WHD violations → Debarments
and builds unified employer profiles.

Uses normalized name matching (O(N)) for all cross-links.
"""

import json
import sqlite3
from collections import defaultdict

from src.normalization.employers import normalize_employer_name
from src.normalization.naics_classifier import classify_naics, load_naics_config
from src.validation.quality import compute_compliance_score


def build_cross_links(conn: sqlite3.Connection) -> int:
    """Build cross-links between all data sources. Returns count of links created."""
    conn.execute("DELETE FROM cross_links")
    conn.commit()

    total = 0
    total += _link_lca_to_uscis(conn)
    total += _link_lca_to_whd(conn)
    total += _link_whd_to_debarments(conn)
    total += _link_uscis_to_whd(conn)

    conn.commit()
    return total


def _link_lca_to_uscis(conn: sqlite3.Connection) -> int:
    """Link LCA employers to USCIS employer data via normalized name matching."""
    # Get unique LCA employers
    lca_employers = conn.execute("""
        SELECT DISTINCT employer_name, employer_state, fiscal_year
        FROM lca_applications
        WHERE employer_name IS NOT NULL AND employer_name != ''
    """).fetchall()

    # Index USCIS by normalized name
    uscis_rows = conn.execute("""
        SELECT employer_key, employer_name, employer_state, fiscal_year
        FROM uscis_employers
    """).fetchall()

    uscis_index = defaultdict(list)
    for r in uscis_rows:
        norm = normalize_employer_name(r["employer_name"])
        if norm:
            uscis_index[norm].append(r)

    links = 0
    seen = set()

    for lca in lca_employers:
        norm_lca = normalize_employer_name(lca["employer_name"])
        if not norm_lca or norm_lca not in uscis_index:
            continue

        for uscis in uscis_index[norm_lca]:
            link_key = (lca["employer_name"], uscis["employer_key"])
            if link_key in seen:
                continue
            seen.add(link_key)

            conf = 0.90
            if lca["employer_state"] == uscis["employer_state"]:
                conf = 0.95
            if lca["fiscal_year"] == uscis["fiscal_year"]:
                conf = min(conf + 0.05, 1.0)

            conn.execute("""
                INSERT INTO cross_links (source_type, source_id, target_type, target_id, link_method, confidence)
                VALUES ('lca_employer', ?, 'uscis_employer', ?, 'normalized_name', ?)
            """, (lca["employer_name"], uscis["employer_key"], conf))
            links += 1

    conn.commit()
    return links


def _link_lca_to_whd(conn: sqlite3.Connection) -> int:
    """Link LCA employers to WHD violations via normalized name."""
    lca_norms = set()
    lca_rows = conn.execute("""
        SELECT DISTINCT employer_name, employer_state
        FROM lca_applications
        WHERE employer_name IS NOT NULL AND employer_name != ''
    """).fetchall()

    lca_index = defaultdict(list)
    for r in lca_rows:
        norm = normalize_employer_name(r["employer_name"])
        if norm:
            lca_index[norm].append(r)

    whd_rows = conn.execute("""
        SELECT case_id, legal_name, trade_name, employer_state
        FROM whd_violations
    """).fetchall()

    links = 0
    seen = set()

    for whd in whd_rows:
        for name_field in ["legal_name", "trade_name"]:
            name = whd[name_field]
            if not name:
                continue
            norm = normalize_employer_name(name)
            if not norm or norm not in lca_index:
                continue

            for lca in lca_index[norm]:
                link_key = (lca["employer_name"], whd["case_id"])
                if link_key in seen:
                    continue
                seen.add(link_key)

                conf = 0.85
                if lca["employer_state"] == whd["employer_state"]:
                    conf = 0.95

                conn.execute("""
                    INSERT INTO cross_links (source_type, source_id, target_type, target_id, link_method, confidence)
                    VALUES ('lca_employer', ?, 'whd_violation', ?, 'normalized_name', ?)
                """, (lca["employer_name"], whd["case_id"], conf))
                links += 1

    conn.commit()
    return links


def _link_whd_to_debarments(conn: sqlite3.Connection) -> int:
    """Link WHD violations to debarments via normalized name."""
    whd_rows = conn.execute("""
        SELECT case_id, legal_name, trade_name, employer_state
        FROM whd_violations
    """).fetchall()

    debar_rows = conn.execute("""
        SELECT debar_id, employer_name, employer_state
        FROM debarments
    """).fetchall()

    # Index debarments by normalized name
    debar_index = defaultdict(list)
    for d in debar_rows:
        norm = normalize_employer_name(d["employer_name"])
        if norm:
            debar_index[norm].append(d)

    links = 0
    seen = set()

    for whd in whd_rows:
        for name_field in ["legal_name", "trade_name"]:
            name = whd[name_field]
            if not name:
                continue
            norm = normalize_employer_name(name)
            if not norm or norm not in debar_index:
                continue

            for debar in debar_index[norm]:
                link_key = (whd["case_id"], debar["debar_id"])
                if link_key in seen:
                    continue
                seen.add(link_key)

                conf = 0.90
                if whd["employer_state"] == debar["employer_state"]:
                    conf = 0.95

                conn.execute("""
                    INSERT INTO cross_links (source_type, source_id, target_type, target_id, link_method, confidence)
                    VALUES ('whd_violation', ?, 'debarment', ?, 'normalized_name', ?)
                """, (whd["case_id"], debar["debar_id"], conf))
                links += 1

    conn.commit()
    return links


def _link_uscis_to_whd(conn: sqlite3.Connection) -> int:
    """Link USCIS employers to WHD violations via normalized name."""
    uscis_rows = conn.execute("""
        SELECT employer_key, employer_name, employer_state
        FROM uscis_employers
    """).fetchall()

    whd_rows = conn.execute("""
        SELECT case_id, legal_name, trade_name, employer_state
        FROM whd_violations
    """).fetchall()

    # Index WHD by normalized name
    whd_index = defaultdict(list)
    for r in whd_rows:
        for name_field in ["legal_name", "trade_name"]:
            name = r[name_field]
            if name:
                norm = normalize_employer_name(name)
                if norm:
                    whd_index[norm].append(r)

    links = 0
    seen = set()

    for uscis in uscis_rows:
        norm = normalize_employer_name(uscis["employer_name"])
        if not norm or norm not in whd_index:
            continue

        for whd in whd_index[norm]:
            link_key = (uscis["employer_key"], whd["case_id"])
            if link_key in seen:
                continue
            seen.add(link_key)

            conf = 0.85
            if uscis["employer_state"] == whd["employer_state"]:
                conf = 0.95

            conn.execute("""
                INSERT INTO cross_links (source_type, source_id, target_type, target_id, link_method, confidence)
                VALUES ('uscis_employer', ?, 'whd_violation', ?, 'normalized_name', ?)
            """, (uscis["employer_key"], whd["case_id"], conf))
            links += 1

    conn.commit()
    return links


def build_employer_profiles(conn: sqlite3.Connection) -> int:
    """Build unified employer profiles from all data sources."""
    conn.execute("DELETE FROM employer_profiles")
    conn.commit()

    # Group LCA applications by normalized employer name
    lca_rows = conn.execute("""
        SELECT employer_name, employer_state, employer_ein,
               annualized_wage, annualized_pw, wage_ratio,
               soc_code, worksite_state, fiscal_year, total_workers,
               case_status, naics_code
        FROM lca_applications
        WHERE employer_name IS NOT NULL AND employer_name != ''
    """).fetchall()

    employer_data = defaultdict(lambda: {
        "names": defaultdict(int), "eins": set(), "wages": [], "pws": [],
        "wage_ratios": [], "soc_codes": defaultdict(int),
        "worksites": defaultdict(int), "states": set(),
        "fiscal_years": set(), "workers": 0, "lcas": 0,
        "naics_codes": defaultdict(int),
    })

    for row in lca_rows:
        norm = normalize_employer_name(row["employer_name"])
        if not norm:
            continue
        d = employer_data[norm]
        d["names"][row["employer_name"]] += 1
        if row["employer_ein"]:
            d["eins"].add(row["employer_ein"])
        if row["annualized_wage"] and row["annualized_wage"] > 0:
            d["wages"].append(row["annualized_wage"])
        if row["annualized_pw"] and row["annualized_pw"] > 0:
            d["pws"].append(row["annualized_pw"])
        if row["wage_ratio"] and row["wage_ratio"] > 0:
            d["wage_ratios"].append(row["wage_ratio"])
        if row["soc_code"]:
            d["soc_codes"][row["soc_code"]] += 1
        if row["worksite_state"]:
            d["worksites"][row["worksite_state"]] += 1
        if row["employer_state"]:
            d["states"].add(row["employer_state"])
        if row["fiscal_year"]:
            d["fiscal_years"].add(row["fiscal_year"])
        if row["total_workers"]:
            d["workers"] += row["total_workers"]
        if row["naics_code"]:
            d["naics_codes"][row["naics_code"]] += 1
        d["lcas"] += 1

    # Index USCIS data by normalized name
    uscis_by_norm = defaultdict(lambda: {"approvals": 0, "denials": 0})
    uscis_rows = conn.execute("""
        SELECT employer_name, total_approvals, total_denials
        FROM uscis_employers
    """).fetchall()
    for row in uscis_rows:
        norm = normalize_employer_name(row["employer_name"])
        if norm:
            uscis_by_norm[norm]["approvals"] += row["total_approvals"] or 0
            uscis_by_norm[norm]["denials"] += row["total_denials"] or 0

    # Index WHD data by normalized name
    whd_by_norm = defaultdict(lambda: {"count": 0, "back_wages": 0.0, "penalties": 0.0})
    whd_rows = conn.execute("""
        SELECT legal_name, trade_name, back_wages, civil_penalty
        FROM whd_violations
    """).fetchall()
    for row in whd_rows:
        for name in [row["legal_name"], row["trade_name"]]:
            if name:
                norm = normalize_employer_name(name)
                if norm and norm in employer_data:
                    whd_by_norm[norm]["count"] += 1
                    whd_by_norm[norm]["back_wages"] += row["back_wages"] or 0
                    whd_by_norm[norm]["penalties"] += row["civil_penalty"] or 0

    # Debarment check
    debarred = set()
    debar_rows = conn.execute("SELECT employer_name FROM debarments").fetchall()
    for row in debar_rows:
        norm = normalize_employer_name(row["employer_name"])
        if norm:
            debarred.add(norm)

    # Load NAICS config for sector classification
    try:
        naics_config = load_naics_config()
    except Exception:
        naics_config = {"sectors": {}, "subsectors": {}}

    # Build profiles
    profiles = 0
    batch = []
    for norm_name, data in employer_data.items():
        if data["lcas"] == 0:
            continue

        # Pick most common original name
        display_name = max(data["names"].items(), key=lambda x: x[1])[0]

        avg_wage = sum(data["wages"]) / len(data["wages"]) if data["wages"] else None
        median_wage = sorted(data["wages"])[len(data["wages"]) // 2] if data["wages"] else None
        avg_pw = sum(data["pws"]) / len(data["pws"]) if data["pws"] else None
        avg_wr = sum(data["wage_ratios"]) / len(data["wage_ratios"]) if data["wage_ratios"] else None

        # USCIS
        uscis = uscis_by_norm.get(norm_name, {"approvals": 0, "denials": 0})
        uscis_total = uscis["approvals"] + uscis["denials"]
        approval_rate = round(100.0 * uscis["approvals"] / uscis_total, 2) if uscis_total > 0 else None

        # WHD
        whd = whd_by_norm.get(norm_name, {"count": 0, "back_wages": 0.0, "penalties": 0.0})

        # Compliance score
        compliance = compute_compliance_score(
            avg_wage_ratio=avg_wr,
            approval_rate=approval_rate,
            whd_violations=whd["count"],
            is_debarred=norm_name in debarred,
            total_back_wages=whd["back_wages"],
        )

        # Top SOC codes
        top_socs = sorted(data["soc_codes"].items(), key=lambda x: -x[1])[:5]
        top_soc_str = json.dumps([{"code": s, "count": c} for s, c in top_socs])

        # Top worksites
        top_ws = sorted(data["worksites"].items(), key=lambda x: -x[1])[:5]
        top_ws_str = json.dumps([{"state": s, "count": c} for s, c in top_ws])

        ein = next(iter(data["eins"]), "")

        source_types = []
        if data["lcas"] > 0:
            source_types.append("LCA")
        if uscis["approvals"] + uscis["denials"] > 0:
            source_types.append("USCIS")
        if whd["count"] > 0:
            source_types.append("WHD")
        if norm_name in debarred:
            source_types.append("DEBAR")

        # NAICS classification
        primary_naics = None
        industry_sector = None
        industry_subsector = None
        if data["naics_codes"]:
            primary_naics = max(data["naics_codes"].items(), key=lambda x: x[1])[0]
            classification = classify_naics(primary_naics, naics_config)
            industry_sector = classification["sector_name"]
            industry_subsector = classification["subsector_name"]

        quality = min(1.0, len(source_types) * 0.25 + (0.25 if avg_wr else 0))

        batch.append((
            display_name, norm_name, ein,
            data["lcas"], data["workers"], avg_wage, median_wage,
            avg_pw, avg_wr,
            uscis["approvals"], uscis["denials"], approval_rate,
            whd["count"], whd["back_wages"], whd["penalties"],
            1 if norm_name in debarred else 0, compliance,
            top_soc_str, top_ws_str,
            ",".join(sorted(data["states"])),
            ",".join(str(y) for y in sorted(data["fiscal_years"])),
            ",".join(source_types), quality,
            primary_naics, industry_sector, industry_subsector,
        ))
        profiles += 1

    # Batch insert
    conn.executemany("""
        INSERT INTO employer_profiles (
            employer_name, normalized_name, employer_ein,
            total_lcas, total_workers, avg_wage, median_wage,
            avg_prevailing_wage, avg_wage_ratio,
            uscis_approvals, uscis_denials, approval_rate,
            whd_violations, total_back_wages, total_penalties,
            is_debarred, compliance_score,
            top_soc_codes, top_worksites, states_active,
            fiscal_years, source_types, quality_score,
            primary_naics, industry_sector, industry_subsector
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?)
    """, batch)

    conn.commit()
    return profiles
