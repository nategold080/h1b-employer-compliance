"""Tests for cross-linking engine."""

import sqlite3
import pytest
from src.storage.database import (
    init_db, upsert_lca, upsert_uscis, upsert_whd, upsert_debarments,
)
from src.normalization.cross_linker import build_cross_links, build_employer_profiles


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def _lca_record(name="Test Corp", state="NY", fy=2025, case_num="I-200-00001"):
    return {
        "case_number": case_num, "case_status": "Certified",
        "employer_name": name, "employer_ein": "",
        "employer_city": "New York", "employer_state": state,
        "employer_zip": "10001", "employer_country": "US",
        "naics_code": "541511", "soc_code": "15-1256",
        "soc_title": "Software Developers", "job_title": "Software Engineer",
        "wage_rate": 120000.0, "wage_unit": "Year",
        "annualized_wage": 120000.0, "prevailing_wage": 95000.0,
        "pw_unit": "Year", "annualized_pw": 95000.0, "wage_ratio": 1.263,
        "worksite_city": "New York", "worksite_state": state,
        "worksite_zip": "10001", "visa_class": "H-1B",
        "submit_date": "2024-01-15", "decision_date": "2024-01-20",
        "begin_date": "2024-10-01", "end_date": "2027-09-30",
        "total_workers": 1, "full_time": "Y",
        "fiscal_year": fy, "source_file": "test", "quality_score": 0.9,
    }


def _uscis_record(name="Test Corp", state="NY", fy=2025):
    return {
        "employer_key": f"{name.upper()}|{state}|{fy}",
        "fiscal_year": fy, "employer_name": name,
        "employer_city": "New York", "employer_state": state,
        "employer_zip": "10001", "naics_code": "541511",
        "initial_approvals": 30, "initial_denials": 3,
        "continuing_approvals": 20, "continuing_denials": 2,
        "total_approvals": 50, "total_denials": 5,
        "approval_rate": 90.9, "rfe_rate": 15.0, "tax_id": "",
        "source_file": "test", "quality_score": 0.9,
    }


class TestBuildCrossLinks:
    def test_lca_to_uscis(self, db):
        upsert_lca(db, [_lca_record()])
        upsert_uscis(db, [_uscis_record()])
        n = build_cross_links(db)
        assert n >= 1

        links = db.execute("SELECT * FROM cross_links").fetchall()
        assert len(links) >= 1
        assert links[0]["source_type"] == "lca_employer"
        assert links[0]["target_type"] == "uscis_employer"

    def test_no_match(self, db):
        upsert_lca(db, [_lca_record(name="Alpha Corp")])
        upsert_uscis(db, [_uscis_record(name="Totally Different Inc")])
        n = build_cross_links(db)
        # Should not create links for unmatched employers
        links = db.execute("""
            SELECT * FROM cross_links
            WHERE source_type='lca_employer' AND target_type='uscis_employer'
        """).fetchall()
        assert len(links) == 0

    def test_lca_to_whd(self, db):
        upsert_lca(db, [_lca_record(name="Bad Corp", state="TX")])
        upsert_whd(db, [{
            "case_id": "WHD-001", "trade_name": "", "legal_name": "Bad Corp",
            "employer_city": "Houston", "employer_state": "TX",
            "employer_zip": "77001", "naics_code": "", "violation_type": "H1B",
            "h1b_related": 1, "back_wages": 10000.0, "civil_penalty": 5000.0,
            "employees_affected": 3, "findings_start_date": "2023-01-01",
            "findings_end_date": "", "case_status": "Concluded",
            "source_file": "test", "quality_score": 0.8,
        }])
        n = build_cross_links(db)
        links = db.execute("""
            SELECT * FROM cross_links
            WHERE source_type='lca_employer' AND target_type='whd_violation'
        """).fetchall()
        assert len(links) >= 1

    def test_empty_db(self, db):
        n = build_cross_links(db)
        assert n == 0


class TestBuildEmployerProfiles:
    def test_basic_profile(self, db):
        upsert_lca(db, [_lca_record()])
        n = build_employer_profiles(db)
        assert n == 1

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["employer_name"] == "Test Corp"
        assert profile["total_lcas"] == 1
        assert profile["avg_wage"] == 120000.0

    def test_multiple_lcas(self, db):
        recs = [
            _lca_record(case_num="I-200-00001"),
            _lca_record(case_num="I-200-00002"),
            _lca_record(case_num="I-200-00003"),
        ]
        upsert_lca(db, recs)
        n = build_employer_profiles(db)
        assert n == 1

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["total_lcas"] == 3

    def test_with_uscis(self, db):
        upsert_lca(db, [_lca_record()])
        upsert_uscis(db, [_uscis_record()])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["uscis_approvals"] == 50
        assert profile["approval_rate"] == 90.91  # 50/(50+5)

    def test_compliance_score(self, db):
        upsert_lca(db, [_lca_record()])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["compliance_score"] is not None
        assert 0.0 <= profile["compliance_score"] <= 1.0

    def test_debarred_employer(self, db):
        upsert_lca(db, [_lca_record(name="Bad Corp")])
        upsert_debarments(db, [{
            "debar_id": "DEB-001", "employer_name": "Bad Corp",
            "employer_city": "Houston", "employer_state": "TX",
            "program": "H-1B", "debar_start_date": "2024-01-01",
            "debar_end_date": "2026-01-01", "violation_type": "Willful",
            "source": "test", "quality_score": 0.9,
        }])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["is_debarred"] == 1
        # Debarred employers should have lower compliance
        assert profile["compliance_score"] < 1.0

    def test_empty_db(self, db):
        n = build_employer_profiles(db)
        assert n == 0

    def test_source_types(self, db):
        upsert_lca(db, [_lca_record()])
        upsert_uscis(db, [_uscis_record()])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert "LCA" in profile["source_types"]
        assert "USCIS" in profile["source_types"]
