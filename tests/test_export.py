"""Tests for export module."""

import sqlite3
import tempfile
from pathlib import Path

import pytest
from src.storage.database import init_db, upsert_lca
from src.normalization.cross_linker import build_employer_profiles
from src.export.exporter import export_all


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def _lca_record(case_num="I-200-00001", name="Test Corp"):
    return {
        "case_number": case_num, "case_status": "Certified",
        "employer_name": name, "employer_ein": "",
        "employer_city": "New York", "employer_state": "NY",
        "employer_zip": "10001", "employer_country": "US",
        "naics_code": "541511", "soc_code": "15-1256",
        "soc_title": "Software Developers", "job_title": "Software Engineer",
        "wage_rate": 120000.0, "wage_unit": "Year",
        "annualized_wage": 120000.0, "prevailing_wage": 95000.0,
        "pw_unit": "Year", "annualized_pw": 95000.0, "wage_ratio": 1.263,
        "worksite_city": "New York", "worksite_state": "NY",
        "worksite_zip": "10001", "visa_class": "H-1B",
        "submit_date": "2024-01-15", "decision_date": "2024-01-20",
        "begin_date": "2024-10-01", "end_date": "2027-09-30",
        "total_workers": 1, "full_time": "Y",
        "fiscal_year": 2025, "source_file": "test", "quality_score": 0.9,
    }


class TestExportAll:
    def test_empty_db(self, db):
        with tempfile.TemporaryDirectory() as tmpdir:
            results = export_all(db, Path(tmpdir))
            assert results["lca_csv"] == 0

    def test_with_data(self, db):
        upsert_lca(db, [_lca_record()])
        build_employer_profiles(db)

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            results = export_all(db, out)

            assert results["lca_csv"] == 1
            assert results["profiles_csv"] >= 1
            assert results["summary_md"] == 1

            # Verify files exist
            assert (out / "lca_applications.csv").exists()
            assert (out / "employer_profiles.csv").exists()
            assert (out / "summary.md").exists()
            assert (out / "employer_profiles.json").exists()

    def test_summary_content(self, db):
        upsert_lca(db, [_lca_record()])
        build_employer_profiles(db)

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            export_all(db, out)

            content = (out / "summary.md").read_text()
            assert "H-1B Employer Compliance Tracker" in content
            assert "Test Corp" in content
            assert "Nathan Goldberg" in content

    def test_json_export(self, db):
        import json
        upsert_lca(db, [_lca_record()])
        build_employer_profiles(db)

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            export_all(db, out)

            data = json.loads((out / "employer_profiles.json").read_text())
            assert len(data) >= 1
            assert data[0]["employer_name"] == "Test Corp"

    def test_multiple_employers(self, db):
        upsert_lca(db, [
            _lca_record("I-200-00001", "Google LLC"),
            _lca_record("I-200-00002", "Apple Inc"),
            _lca_record("I-200-00003", "Microsoft Corp"),
        ])
        build_employer_profiles(db)

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            results = export_all(db, out)
            assert results["lca_csv"] == 3
            assert results["profiles_csv"] == 3
