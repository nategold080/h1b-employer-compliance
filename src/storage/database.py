"""SQLite database for H-1B Employer Compliance Tracker."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "h1b_compliance.db"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a database connection with WAL mode."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection | None = None) -> None:
    """Create all tables."""
    close = False
    if conn is None:
        conn = get_connection()
        close = True

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS lca_applications (
            case_number TEXT PRIMARY KEY,
            case_status TEXT,
            employer_name TEXT,
            employer_ein TEXT,
            employer_city TEXT,
            employer_state TEXT,
            employer_zip TEXT,
            employer_country TEXT,
            naics_code TEXT,
            soc_code TEXT,
            soc_title TEXT,
            job_title TEXT,
            wage_rate REAL,
            wage_unit TEXT,
            annualized_wage REAL,
            prevailing_wage REAL,
            pw_unit TEXT,
            annualized_pw REAL,
            wage_ratio REAL,
            worksite_city TEXT,
            worksite_state TEXT,
            worksite_zip TEXT,
            visa_class TEXT,
            submit_date TEXT,
            decision_date TEXT,
            begin_date TEXT,
            end_date TEXT,
            total_workers INTEGER,
            full_time TEXT,
            fiscal_year INTEGER,
            source_file TEXT,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS uscis_employers (
            employer_key TEXT PRIMARY KEY,
            fiscal_year INTEGER,
            employer_name TEXT,
            employer_city TEXT,
            employer_state TEXT,
            employer_zip TEXT,
            naics_code TEXT,
            initial_approvals INTEGER DEFAULT 0,
            initial_denials INTEGER DEFAULT 0,
            continuing_approvals INTEGER DEFAULT 0,
            continuing_denials INTEGER DEFAULT 0,
            total_approvals INTEGER DEFAULT 0,
            total_denials INTEGER DEFAULT 0,
            approval_rate REAL,
            rfe_rate REAL,
            tax_id TEXT,
            source_file TEXT,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS whd_violations (
            case_id TEXT PRIMARY KEY,
            trade_name TEXT,
            legal_name TEXT,
            employer_city TEXT,
            employer_state TEXT,
            employer_zip TEXT,
            naics_code TEXT,
            violation_type TEXT,
            h1b_related INTEGER DEFAULT 0,
            back_wages REAL DEFAULT 0.0,
            civil_penalty REAL DEFAULT 0.0,
            employees_affected INTEGER DEFAULT 0,
            findings_start_date TEXT,
            findings_end_date TEXT,
            case_status TEXT,
            source_file TEXT,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS debarments (
            debar_id TEXT PRIMARY KEY,
            employer_name TEXT,
            employer_city TEXT,
            employer_state TEXT,
            program TEXT,
            debar_start_date TEXT,
            debar_end_date TEXT,
            violation_type TEXT,
            source TEXT,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS employer_profiles (
            profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
            employer_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            employer_ein TEXT,
            total_lcas INTEGER DEFAULT 0,
            total_workers INTEGER DEFAULT 0,
            avg_wage REAL,
            median_wage REAL,
            avg_prevailing_wage REAL,
            avg_wage_ratio REAL,
            uscis_approvals INTEGER DEFAULT 0,
            uscis_denials INTEGER DEFAULT 0,
            approval_rate REAL,
            whd_violations INTEGER DEFAULT 0,
            total_back_wages REAL DEFAULT 0.0,
            total_penalties REAL DEFAULT 0.0,
            is_debarred INTEGER DEFAULT 0,
            compliance_score REAL,
            top_soc_codes TEXT,
            top_worksites TEXT,
            states_active TEXT,
            fiscal_years TEXT,
            source_types TEXT,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cross_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            link_method TEXT NOT NULL,
            confidence REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS naics_codes (
            naics_code TEXT PRIMARY KEY,
            naics_description TEXT NOT NULL,
            sector_code TEXT,
            sector_name TEXT,
            subsector_code TEXT,
            subsector_name TEXT
        );

        CREATE TABLE IF NOT EXISTS public_companies (
            sec_cik INTEGER PRIMARY KEY,
            company_name TEXT NOT NULL,
            ticker TEXT,
            exchange TEXT,
            sic_code TEXT,
            sic_description TEXT,
            state_of_incorporation TEXT,
            business_city TEXT,
            business_state TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS company_financials (
            sec_cik INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            revenue REAL,
            total_assets REAL,
            net_income REAL,
            employees INTEGER,
            h1b_per_employee REAL,
            h1b_wage_to_revenue REAL,
            quality_score REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (sec_cik, fiscal_year)
        );

        CREATE TABLE IF NOT EXISTS employer_sec_links (
            employer_name TEXT NOT NULL,
            employer_normalized TEXT,
            sec_cik INTEGER NOT NULL,
            link_method TEXT,
            confidence REAL DEFAULT 0.0,
            is_subsidiary INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (employer_name, sec_cik)
        );

        CREATE INDEX IF NOT EXISTS idx_lca_employer ON lca_applications(employer_name);
        CREATE INDEX IF NOT EXISTS idx_lca_ein ON lca_applications(employer_ein);
        CREATE INDEX IF NOT EXISTS idx_lca_state ON lca_applications(employer_state);
        CREATE INDEX IF NOT EXISTS idx_lca_visa ON lca_applications(visa_class);
        CREATE INDEX IF NOT EXISTS idx_lca_fy ON lca_applications(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_lca_soc ON lca_applications(soc_code);
        CREATE INDEX IF NOT EXISTS idx_lca_status ON lca_applications(case_status);

        CREATE INDEX IF NOT EXISTS idx_uscis_employer ON uscis_employers(employer_name);
        CREATE INDEX IF NOT EXISTS idx_uscis_fy ON uscis_employers(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_uscis_state ON uscis_employers(employer_state);

        CREATE INDEX IF NOT EXISTS idx_whd_legal ON whd_violations(legal_name);
        CREATE INDEX IF NOT EXISTS idx_whd_trade ON whd_violations(trade_name);
        CREATE INDEX IF NOT EXISTS idx_whd_state ON whd_violations(employer_state);
        CREATE INDEX IF NOT EXISTS idx_whd_h1b ON whd_violations(h1b_related);

        CREATE INDEX IF NOT EXISTS idx_debar_employer ON debarments(employer_name);

        CREATE INDEX IF NOT EXISTS idx_profile_name ON employer_profiles(normalized_name);
        CREATE INDEX IF NOT EXISTS idx_profile_ein ON employer_profiles(employer_ein);

        CREATE INDEX IF NOT EXISTS idx_xlink_source ON cross_links(source_type, source_id);
        CREATE INDEX IF NOT EXISTS idx_xlink_target ON cross_links(target_type, target_id);

        CREATE INDEX IF NOT EXISTS idx_naics_sector ON naics_codes(sector_code);
        CREATE INDEX IF NOT EXISTS idx_lca_naics ON lca_applications(naics_code);

        CREATE INDEX IF NOT EXISTS idx_public_ticker ON public_companies(ticker);
        CREATE INDEX IF NOT EXISTS idx_public_sic ON public_companies(sic_code);
        CREATE INDEX IF NOT EXISTS idx_financials_cik ON company_financials(sec_cik);
        CREATE INDEX IF NOT EXISTS idx_financials_year ON company_financials(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_employer_sec_cik ON employer_sec_links(sec_cik);
        CREATE INDEX IF NOT EXISTS idx_employer_sec_norm ON employer_sec_links(employer_normalized);
    """)

    # Add new columns to employer_profiles (idempotent)
    for col, coltype in [
        ("primary_naics", "TEXT"),
        ("industry_sector", "TEXT"),
        ("industry_subsector", "TEXT"),
        ("sec_cik", "INTEGER"),
        ("ticker", "TEXT"),
        ("is_public", "INTEGER DEFAULT 0"),
        ("revenue", "REAL"),
        ("employees", "INTEGER"),
        ("h1b_per_1000_employees", "REAL"),
        ("revenue_per_h1b", "REAL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE employer_profiles ADD COLUMN {col} {coltype}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.execute("CREATE INDEX IF NOT EXISTS idx_profile_sector ON employer_profiles(industry_sector)")
    conn.commit()

    if close:
        conn.close()


def upsert_lca(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update LCA application records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO lca_applications (
                case_number, case_status, employer_name, employer_ein,
                employer_city, employer_state, employer_zip, employer_country,
                naics_code, soc_code, soc_title, job_title,
                wage_rate, wage_unit, annualized_wage,
                prevailing_wage, pw_unit, annualized_pw, wage_ratio,
                worksite_city, worksite_state, worksite_zip,
                visa_class, submit_date, decision_date,
                begin_date, end_date, total_workers, full_time,
                fiscal_year, source_file, quality_score, updated_at
            ) VALUES (
                :case_number, :case_status, :employer_name, :employer_ein,
                :employer_city, :employer_state, :employer_zip, :employer_country,
                :naics_code, :soc_code, :soc_title, :job_title,
                :wage_rate, :wage_unit, :annualized_wage,
                :prevailing_wage, :pw_unit, :annualized_pw, :wage_ratio,
                :worksite_city, :worksite_state, :worksite_zip,
                :visa_class, :submit_date, :decision_date,
                :begin_date, :end_date, :total_workers, :full_time,
                :fiscal_year, :source_file, :quality_score, datetime('now')
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_uscis(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update USCIS employer records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO uscis_employers (
                employer_key, fiscal_year, employer_name,
                employer_city, employer_state, employer_zip,
                naics_code,
                initial_approvals, initial_denials,
                continuing_approvals, continuing_denials,
                total_approvals, total_denials,
                approval_rate, rfe_rate, tax_id,
                source_file, quality_score, updated_at
            ) VALUES (
                :employer_key, :fiscal_year, :employer_name,
                :employer_city, :employer_state, :employer_zip,
                :naics_code,
                :initial_approvals, :initial_denials,
                :continuing_approvals, :continuing_denials,
                :total_approvals, :total_denials,
                :approval_rate, :rfe_rate, :tax_id,
                :source_file, :quality_score, datetime('now')
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_whd(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update WHD violation records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO whd_violations (
                case_id, trade_name, legal_name,
                employer_city, employer_state, employer_zip,
                naics_code, violation_type, h1b_related,
                back_wages, civil_penalty, employees_affected,
                findings_start_date, findings_end_date,
                case_status, source_file, quality_score, updated_at
            ) VALUES (
                :case_id, :trade_name, :legal_name,
                :employer_city, :employer_state, :employer_zip,
                :naics_code, :violation_type, :h1b_related,
                :back_wages, :civil_penalty, :employees_affected,
                :findings_start_date, :findings_end_date,
                :case_status, :source_file, :quality_score, datetime('now')
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_debarments(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update debarment records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO debarments (
                debar_id, employer_name, employer_city, employer_state,
                program, debar_start_date, debar_end_date,
                violation_type, source, quality_score, updated_at
            ) VALUES (
                :debar_id, :employer_name, :employer_city, :employer_state,
                :program, :debar_start_date, :debar_end_date,
                :violation_type, :source, :quality_score, datetime('now')
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_public_companies(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update public company records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO public_companies (
                sec_cik, company_name, ticker, exchange,
                sic_code, sic_description, state_of_incorporation,
                business_city, business_state
            ) VALUES (
                :sec_cik, :company_name, :ticker, :exchange,
                :sic_code, :sic_description, :state_of_incorporation,
                :business_city, :business_state
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_company_financials(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update company financial records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO company_financials (
                sec_cik, fiscal_year, revenue, total_assets,
                net_income, employees, h1b_per_employee,
                h1b_wage_to_revenue, quality_score
            ) VALUES (
                :sec_cik, :fiscal_year, :revenue, :total_assets,
                :net_income, :employees, :h1b_per_employee,
                :h1b_wage_to_revenue, :quality_score
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_employer_sec_links(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update employer-to-SEC company links."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO employer_sec_links (
                employer_name, employer_normalized, sec_cik,
                link_method, confidence, is_subsidiary
            ) VALUES (
                :employer_name, :employer_normalized, :sec_cik,
                :link_method, :confidence, :is_subsidiary
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def upsert_naics_codes(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert or update NAICS code records."""
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO naics_codes (
                naics_code, naics_description, sector_code, sector_name,
                subsector_code, subsector_name
            ) VALUES (
                :naics_code, :naics_description, :sector_code, :sector_name,
                :subsector_code, :subsector_name
            )
        """, rec)
        inserted += 1
    conn.commit()
    return inserted


def get_stats(conn: sqlite3.Connection | None = None) -> dict:
    """Get database statistics."""
    close = False
    if conn is None:
        conn = get_connection()
        close = True

    stats = {}
    for table in ["lca_applications", "uscis_employers", "whd_violations",
                   "debarments", "employer_profiles", "cross_links"]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        stats[table] = row[0]

    # Quality averages
    for table in ["lca_applications", "uscis_employers", "whd_violations"]:
        row = conn.execute(
            f"SELECT AVG(quality_score) FROM {table} WHERE quality_score > 0"
        ).fetchone()
        stats[f"{table}_quality_avg"] = round(row[0], 3) if row[0] else 0.0

    # LCA-specific stats
    row = conn.execute(
        "SELECT COUNT(DISTINCT employer_name) FROM lca_applications"
    ).fetchone()
    stats["unique_employers"] = row[0]

    row = conn.execute(
        "SELECT COUNT(DISTINCT fiscal_year) FROM lca_applications WHERE fiscal_year IS NOT NULL"
    ).fetchone()
    stats["fiscal_years"] = row[0]

    row = conn.execute(
        "SELECT AVG(annualized_wage) FROM lca_applications WHERE annualized_wage > 0"
    ).fetchone()
    stats["avg_wage"] = round(row[0], 0) if row[0] else 0

    row = conn.execute(
        "SELECT SUM(total_workers) FROM lca_applications WHERE total_workers > 0"
    ).fetchone()
    stats["total_workers"] = row[0] or 0

    row = conn.execute(
        "SELECT SUM(back_wages) FROM whd_violations"
    ).fetchone()
    stats["total_back_wages"] = row[0] or 0

    row = conn.execute(
        "SELECT COUNT(DISTINCT employer_state) FROM lca_applications WHERE employer_state IS NOT NULL"
    ).fetchone()
    stats["states"] = row[0]

    # NAICS stats
    row = conn.execute("SELECT COUNT(*) FROM naics_codes").fetchone()
    stats["naics_codes"] = row[0]

    row = conn.execute(
        "SELECT COUNT(DISTINCT industry_sector) FROM employer_profiles WHERE industry_sector IS NOT NULL"
    ).fetchone()
    stats["industry_sectors"] = row[0]

    row = conn.execute(
        "SELECT COUNT(*) FROM employer_profiles WHERE industry_sector IS NOT NULL"
    ).fetchone()
    stats["profiles_with_sector"] = row[0]

    # SEC stats
    row = conn.execute("SELECT COUNT(*) FROM public_companies").fetchone()
    stats["public_companies"] = row[0]

    row = conn.execute("SELECT COUNT(*) FROM company_financials").fetchone()
    stats["company_financials"] = row[0]

    row = conn.execute("SELECT COUNT(*) FROM employer_sec_links").fetchone()
    stats["employer_sec_links"] = row[0]

    row = conn.execute(
        "SELECT COUNT(*) FROM employer_profiles WHERE is_public = 1"
    ).fetchone()
    stats["public_employer_profiles"] = row[0]

    if close:
        conn.close()
    return stats
