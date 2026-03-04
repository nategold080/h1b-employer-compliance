"""Tests for database module."""

import sqlite3
import pytest
from src.storage.database import (
    init_db, upsert_lca, upsert_uscis, upsert_whd,
    upsert_debarments, get_stats,
)


@pytest.fixture
def db():
    """Create an in-memory database."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


class TestInitDb:
    def test_creates_tables(self, db):
        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = {t["name"] for t in tables}
        assert "lca_applications" in names
        assert "uscis_employers" in names
        assert "whd_violations" in names
        assert "debarments" in names
        assert "employer_profiles" in names
        assert "cross_links" in names

    def test_idempotent(self, db):
        init_db(db)  # Should not fail
        init_db(db)


class TestUpsertLca:
    def test_insert(self, db):
        records = [{
            "case_number": "I-200-00001",
            "case_status": "Certified",
            "employer_name": "Test Corp",
            "employer_ein": "",
            "employer_city": "New York",
            "employer_state": "NY",
            "employer_zip": "10001",
            "employer_country": "US",
            "naics_code": "541511",
            "soc_code": "15-1256",
            "soc_title": "Software Developers",
            "job_title": "Software Engineer",
            "wage_rate": 120000.0,
            "wage_unit": "Year",
            "annualized_wage": 120000.0,
            "prevailing_wage": 95000.0,
            "pw_unit": "Year",
            "annualized_pw": 95000.0,
            "wage_ratio": 1.263,
            "worksite_city": "New York",
            "worksite_state": "NY",
            "worksite_zip": "10001",
            "visa_class": "H-1B",
            "submit_date": "2024-01-15",
            "decision_date": "2024-01-20",
            "begin_date": "2024-10-01",
            "end_date": "2027-09-30",
            "total_workers": 1,
            "full_time": "Y",
            "fiscal_year": 2025,
            "source_file": "test",
            "quality_score": 0.9,
        }]
        n = upsert_lca(db, records)
        assert n == 1

        row = db.execute("SELECT * FROM lca_applications").fetchone()
        assert row["case_number"] == "I-200-00001"
        assert row["employer_name"] == "Test Corp"
        assert row["annualized_wage"] == 120000.0

    def test_upsert(self, db):
        rec = {
            "case_number": "I-200-00001",
            "case_status": "Certified",
            "employer_name": "Test Corp",
            "employer_ein": "", "employer_city": "NY",
            "employer_state": "NY", "employer_zip": "10001",
            "employer_country": "US", "naics_code": "", "soc_code": "",
            "soc_title": "", "job_title": "", "wage_rate": 100000.0,
            "wage_unit": "Year", "annualized_wage": 100000.0,
            "prevailing_wage": None, "pw_unit": "", "annualized_pw": None,
            "wage_ratio": None, "worksite_city": "", "worksite_state": "NY",
            "worksite_zip": "", "visa_class": "H-1B", "submit_date": "",
            "decision_date": "", "begin_date": "", "end_date": "",
            "total_workers": 1, "full_time": "Y", "fiscal_year": 2025,
            "source_file": "test", "quality_score": 0.5,
        }
        upsert_lca(db, [rec])
        rec["wage_rate"] = 150000.0
        rec["annualized_wage"] = 150000.0
        upsert_lca(db, [rec])
        row = db.execute("SELECT * FROM lca_applications").fetchone()
        assert row["annualized_wage"] == 150000.0


class TestUpsertUscis:
    def test_insert(self, db):
        records = [{
            "employer_key": "TESTCORP-NY-2025",
            "fiscal_year": 2025,
            "employer_name": "Test Corp",
            "employer_city": "New York",
            "employer_state": "NY",
            "employer_zip": "10001",
            "naics_code": "541511",
            "initial_approvals": 50,
            "initial_denials": 5,
            "continuing_approvals": 30,
            "continuing_denials": 2,
            "total_approvals": 80,
            "total_denials": 7,
            "approval_rate": 91.95,
            "rfe_rate": 15.0,
            "tax_id": "",
            "source_file": "test",
            "quality_score": 0.9,
        }]
        n = upsert_uscis(db, records)
        assert n == 1

        row = db.execute("SELECT * FROM uscis_employers").fetchone()
        assert row["employer_name"] == "Test Corp"
        assert row["total_approvals"] == 80


class TestUpsertWhd:
    def test_insert(self, db):
        records = [{
            "case_id": "WHD-001",
            "trade_name": "Test Restaurant",
            "legal_name": "Test Corp LLC",
            "employer_city": "Chicago",
            "employer_state": "IL",
            "employer_zip": "60601",
            "naics_code": "722511",
            "violation_type": "H1B",
            "h1b_related": 1,
            "back_wages": 50000.0,
            "civil_penalty": 10000.0,
            "employees_affected": 5,
            "findings_start_date": "2023-01-01",
            "findings_end_date": "2023-12-31",
            "case_status": "Concluded",
            "source_file": "test",
            "quality_score": 0.85,
        }]
        n = upsert_whd(db, records)
        assert n == 1

        row = db.execute("SELECT * FROM whd_violations").fetchone()
        assert row["back_wages"] == 50000.0
        assert row["h1b_related"] == 1


class TestUpsertDebarments:
    def test_insert(self, db):
        records = [{
            "debar_id": "DEB-001",
            "employer_name": "Bad Corp",
            "employer_city": "Houston",
            "employer_state": "TX",
            "program": "H-1B",
            "debar_start_date": "2024-01-01",
            "debar_end_date": "2026-01-01",
            "violation_type": "Willful Failure",
            "source": "DOL Willful Violator List",
            "quality_score": 0.9,
        }]
        n = upsert_debarments(db, records)
        assert n == 1

        row = db.execute("SELECT * FROM debarments").fetchone()
        assert row["employer_name"] == "Bad Corp"
        assert row["program"] == "H-1B"


class TestGetStats:
    def test_empty(self, db):
        stats = get_stats(db)
        assert stats["lca_applications"] == 0
        assert stats["uscis_employers"] == 0

    def test_with_data(self, db):
        upsert_lca(db, [{
            "case_number": "I-200-00001", "case_status": "Certified",
            "employer_name": "Test Corp", "employer_ein": "",
            "employer_city": "NY", "employer_state": "NY",
            "employer_zip": "10001", "employer_country": "US",
            "naics_code": "", "soc_code": "", "soc_title": "",
            "job_title": "", "wage_rate": 100000.0, "wage_unit": "Year",
            "annualized_wage": 100000.0, "prevailing_wage": None,
            "pw_unit": "", "annualized_pw": None, "wage_ratio": None,
            "worksite_city": "", "worksite_state": "NY",
            "worksite_zip": "", "visa_class": "H-1B",
            "submit_date": "", "decision_date": "",
            "begin_date": "", "end_date": "",
            "total_workers": 1, "full_time": "Y",
            "fiscal_year": 2025, "source_file": "test",
            "quality_score": 0.8,
        }])
        stats = get_stats(db)
        assert stats["lca_applications"] == 1
        assert stats["unique_employers"] == 1
