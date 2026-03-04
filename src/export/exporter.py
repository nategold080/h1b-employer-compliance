"""Export module for H-1B Employer Compliance data."""

import csv
import json
import sqlite3
from pathlib import Path

EXPORT_DIR = Path(__file__).parent.parent.parent / "data" / "exports"


def export_all(conn: sqlite3.Connection, output_dir: Path | None = None) -> dict:
    """Export all data to CSV, JSON, and summary markdown."""
    out = output_dir or EXPORT_DIR
    out.mkdir(parents=True, exist_ok=True)

    results = {}
    results["lca_csv"] = _export_table_csv(conn, "lca_applications", out / "lca_applications.csv")
    results["uscis_csv"] = _export_table_csv(conn, "uscis_employers", out / "uscis_employers.csv")
    results["whd_csv"] = _export_table_csv(conn, "whd_violations", out / "whd_violations.csv")
    results["debarments_csv"] = _export_table_csv(conn, "debarments", out / "debarments.csv")
    results["profiles_csv"] = _export_table_csv(conn, "employer_profiles", out / "employer_profiles.csv")
    results["cross_links_csv"] = _export_table_csv(conn, "cross_links", out / "cross_links.csv")

    results["profiles_json"] = _export_profiles_json(conn, out / "employer_profiles.json")
    results["naics_csv"] = _export_table_csv(conn, "naics_codes", out / "naics_codes.csv")
    results["public_companies_csv"] = _export_table_csv(conn, "public_companies", out / "public_companies.csv")
    results["company_financials_csv"] = _export_table_csv(conn, "company_financials", out / "company_financials.csv")
    results["employer_sec_links_csv"] = _export_table_csv(conn, "employer_sec_links", out / "employer_sec_links.csv")
    results["summary_md"] = _export_summary_md(conn, out / "summary.md")
    results["top_employers_csv"] = _export_top_employers(conn, out / "top_employers.csv")
    results["flagged_employers_csv"] = _export_flagged_employers(conn, out / "flagged_employers.csv")
    results["sector_summary_csv"] = _export_sector_summary(conn, out / "sector_summary.csv")

    return results


def _export_table_csv(conn: sqlite3.Connection, table: str, path: Path) -> int:
    """Export a table to CSV. Returns row count."""
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        return 0

    cols = [desc[0] for desc in conn.execute(f"SELECT * FROM {table} LIMIT 1").description]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(list(row))

    return len(rows)


def _export_profiles_json(conn: sqlite3.Connection, path: Path) -> int:
    """Export employer profiles to JSON."""
    rows = conn.execute("""
        SELECT * FROM employer_profiles
        ORDER BY total_lcas DESC
    """).fetchall()

    cols = [desc[0] for desc in conn.execute("SELECT * FROM employer_profiles LIMIT 1").description]
    records = []
    for row in rows:
        rec = dict(zip(cols, row))
        # Parse JSON fields
        for field in ["top_soc_codes", "top_worksites"]:
            if rec.get(field) and isinstance(rec[field], str):
                try:
                    rec[field] = json.loads(rec[field])
                except json.JSONDecodeError:
                    pass
        records.append(rec)

    path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
    return len(records)


def _export_top_employers(conn: sqlite3.Connection, path: Path) -> int:
    """Export top H-1B employers by LCA volume."""
    rows = conn.execute("""
        SELECT employer_name, normalized_name, total_lcas, total_workers,
               avg_wage, avg_wage_ratio, approval_rate, whd_violations,
               total_back_wages, is_debarred, compliance_score,
               industry_sector, states_active, fiscal_years, source_types
        FROM employer_profiles
        ORDER BY total_lcas DESC
        LIMIT 500
    """).fetchall()

    cols = [desc[0] for desc in conn.execute("""
        SELECT employer_name, normalized_name, total_lcas, total_workers,
               avg_wage, avg_wage_ratio, approval_rate, whd_violations,
               total_back_wages, is_debarred, compliance_score,
               industry_sector, states_active, fiscal_years, source_types
        FROM employer_profiles LIMIT 1
    """).description]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(list(row))

    return len(rows)


def _export_flagged_employers(conn: sqlite3.Connection, path: Path) -> int:
    """Export employers with compliance concerns."""
    rows = conn.execute("""
        SELECT employer_name, normalized_name, total_lcas, total_workers,
               avg_wage, avg_wage_ratio, approval_rate, whd_violations,
               total_back_wages, total_penalties, is_debarred, compliance_score,
               states_active, source_types
        FROM employer_profiles
        WHERE compliance_score IS NOT NULL AND (
            compliance_score < 0.5
            OR whd_violations > 0
            OR is_debarred = 1
            OR (avg_wage_ratio IS NOT NULL AND avg_wage_ratio < 1.0)
        )
        ORDER BY compliance_score ASC
    """).fetchall()

    cols = [desc[0] for desc in conn.execute("""
        SELECT employer_name, normalized_name, total_lcas, total_workers,
               avg_wage, avg_wage_ratio, approval_rate, whd_violations,
               total_back_wages, total_penalties, is_debarred, compliance_score,
               states_active, source_types
        FROM employer_profiles LIMIT 1
    """).description]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(list(row))

    return len(rows)


def _export_sector_summary(conn: sqlite3.Connection, path: Path) -> int:
    """Export industry sector summary statistics."""
    rows = conn.execute("""
        SELECT industry_sector, COUNT(*) as employers,
               SUM(total_lcas) as total_lcas,
               SUM(total_workers) as total_workers,
               ROUND(AVG(avg_wage), 0) as avg_wage,
               ROUND(AVG(avg_wage_ratio), 3) as avg_wage_ratio,
               ROUND(AVG(compliance_score), 3) as avg_compliance
        FROM employer_profiles
        WHERE industry_sector IS NOT NULL
        GROUP BY industry_sector
        ORDER BY total_lcas DESC
    """).fetchall()

    if not rows:
        return 0

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["industry_sector", "employers", "total_lcas", "total_workers",
                         "avg_wage", "avg_wage_ratio", "avg_compliance"])
        for row in rows:
            writer.writerow(list(row))

    return len(rows)


def _export_summary_md(conn: sqlite3.Connection, path: Path) -> int:
    """Export summary statistics as Markdown."""
    from src.storage.database import get_stats
    stats = get_stats(conn)

    # Top employers
    top = conn.execute("""
        SELECT employer_name, total_lcas, total_workers, avg_wage,
               compliance_score
        FROM employer_profiles
        ORDER BY total_lcas DESC LIMIT 20
    """).fetchall()

    # Flagged
    flagged = conn.execute("""
        SELECT employer_name, compliance_score, whd_violations,
               total_back_wages, is_debarred
        FROM employer_profiles
        WHERE compliance_score IS NOT NULL AND compliance_score < 0.5
        ORDER BY compliance_score ASC LIMIT 10
    """).fetchall()

    lines = [
        "# H-1B Employer Compliance Tracker — Summary",
        "",
        "## Database Statistics",
        "",
        f"- **LCA Applications:** {stats.get('lca_applications', 0):,}",
        f"- **Unique Employers:** {stats.get('unique_employers', 0):,}",
        f"- **USCIS Employer Records:** {stats.get('uscis_employers', 0):,}",
        f"- **WHD Violations:** {stats.get('whd_violations', 0):,}",
        f"- **Debarments:** {stats.get('debarments', 0):,}",
        f"- **Employer Profiles:** {stats.get('employer_profiles', 0):,}",
        f"- **Cross-Links:** {stats.get('cross_links', 0):,}",
        f"- **Fiscal Years:** {stats.get('fiscal_years', 0)}",
        f"- **States:** {stats.get('states', 0)}",
        f"- **Average Wage:** ${stats.get('avg_wage', 0):,.0f}",
        f"- **Total Workers Requested:** {stats.get('total_workers', 0):,}",
        f"- **Total Back Wages (WHD):** ${stats.get('total_back_wages', 0):,.0f}",
        f"- **Public Companies Matched:** {stats.get('public_companies', 0):,}",
        f"- **Financial Records:** {stats.get('company_financials', 0):,}",
        f"- **Industry Sectors:** {stats.get('industry_sectors', 0)}",
        "",
        "## Top 20 H-1B Employers by LCA Volume",
        "",
        "| Employer | LCAs | Workers | Avg Wage | Compliance |",
        "|----------|------|---------|----------|------------|",
    ]

    for row in top:
        wage = f"${row['avg_wage']:,.0f}" if row['avg_wage'] else "N/A"
        comp = f"{row['compliance_score']:.2f}" if row['compliance_score'] is not None else "N/A"
        lines.append(
            f"| {row['employer_name'][:40]} | {row['total_lcas']:,} | "
            f"{row['total_workers']:,} | {wage} | {comp} |"
        )

    # Industry sector summary
    sectors = conn.execute("""
        SELECT industry_sector, COUNT(*) as employers,
               SUM(total_lcas) as total_lcas,
               ROUND(AVG(avg_wage), 0) as avg_wage,
               ROUND(AVG(avg_wage_ratio), 3) as avg_wage_ratio
        FROM employer_profiles
        WHERE industry_sector IS NOT NULL
        GROUP BY industry_sector
        ORDER BY total_lcas DESC LIMIT 10
    """).fetchall()

    if sectors:
        lines.extend([
            "",
            "## Top 10 Industry Sectors by H-1B Volume",
            "",
            "| Sector | Employers | LCAs | Avg Wage | Wage Ratio |",
            "|--------|-----------|------|----------|------------|",
        ])
        for row in sectors:
            wage = f"${row['avg_wage']:,.0f}" if row['avg_wage'] else "N/A"
            wr = f"{row['avg_wage_ratio']:.3f}" if row['avg_wage_ratio'] else "N/A"
            lines.append(
                f"| {row['industry_sector'][:40]} | {row['employers']:,} | "
                f"{row['total_lcas']:,} | {wage} | {wr} |"
            )

    if flagged:
        lines.extend([
            "",
            "## Flagged Employers (Compliance Score < 0.5)",
            "",
            "| Employer | Score | WHD Violations | Back Wages | Debarred |",
            "|----------|-------|---------------|------------|----------|",
        ])
        for row in flagged:
            lines.append(
                f"| {row['employer_name'][:40]} | {row['compliance_score']:.2f} | "
                f"{row['whd_violations']} | ${row['total_back_wages']:,.0f} | "
                f"{'Yes' if row['is_debarred'] else 'No'} |"
            )

    lines.extend([
        "",
        "---",
        "",
        "Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com",
        "",
    ])

    path.write_text("\n".join(lines), encoding="utf-8")
    return 1
