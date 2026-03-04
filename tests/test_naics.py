"""Tests for NAICS industry sector classification."""

import sqlite3
import pytest
from src.storage.database import init_db, upsert_lca, upsert_uscis
from src.normalization.naics_classifier import (
    load_naics_config, get_sector, get_subsector, classify_naics,
    load_naics_reference_table, classify_employer_profiles,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


@pytest.fixture
def config():
    return load_naics_config()


def _lca_record(name="Test Corp", naics="541511", case_num="I-200-00001", fy=2025):
    return {
        "case_number": case_num, "case_status": "Certified",
        "employer_name": name, "employer_ein": "",
        "employer_city": "New York", "employer_state": "NY",
        "employer_zip": "10001", "employer_country": "US",
        "naics_code": naics, "soc_code": "15-1256",
        "soc_title": "Software Developers", "job_title": "Software Engineer",
        "wage_rate": 120000.0, "wage_unit": "Year",
        "annualized_wage": 120000.0, "prevailing_wage": 95000.0,
        "pw_unit": "Year", "annualized_pw": 95000.0, "wage_ratio": 1.263,
        "worksite_city": "New York", "worksite_state": "NY",
        "worksite_zip": "10001", "visa_class": "H-1B",
        "submit_date": "2024-01-15", "decision_date": "2024-01-20",
        "begin_date": "2024-10-01", "end_date": "2027-09-30",
        "total_workers": 1, "full_time": "Y",
        "fiscal_year": fy, "source_file": "test", "quality_score": 0.9,
    }


class TestLoadNaicsConfig:
    def test_loads_sectors(self, config):
        assert "sectors" in config
        assert len(config["sectors"]) > 0

    def test_loads_subsectors(self, config):
        assert "subsectors" in config
        assert len(config["subsectors"]) > 0

    def test_has_common_sectors(self, config):
        assert "54" in config["sectors"]
        assert config["sectors"]["54"] == "Professional, Scientific, and Technical Services"
        assert "62" in config["sectors"]
        assert config["sectors"]["62"] == "Health Care and Social Assistance"

    def test_has_common_subsectors(self, config):
        assert "541" in config["subsectors"]
        assert config["subsectors"]["541"] == "Professional, Scientific, and Technical Services"
        assert "622" in config["subsectors"]
        assert config["subsectors"]["622"] == "Hospitals"


class TestGetSector:
    def test_basic_sector(self, config):
        code, name = get_sector("541511", config)
        assert code == "54"
        assert name == "Professional, Scientific, and Technical Services"

    def test_manufacturing(self, config):
        code, name = get_sector("336111", config)
        assert code == "33"
        assert name == "Manufacturing"

    def test_healthcare(self, config):
        code, name = get_sector("622110", config)
        assert code == "62"
        assert name == "Health Care and Social Assistance"

    def test_finance(self, config):
        code, name = get_sector("523110", config)
        assert code == "52"
        assert name == "Finance and Insurance"

    def test_empty_code(self, config):
        code, name = get_sector("", config)
        assert code is None
        assert name is None

    def test_none_code(self, config):
        code, name = get_sector(None, config)
        assert code is None
        assert name is None

    def test_short_code(self, config):
        code, name = get_sector("5", config)
        assert code is None
        assert name is None

    def test_unknown_sector(self, config):
        code, name = get_sector("99", config)
        assert code is None
        assert name is None


class TestGetSubsector:
    def test_basic_subsector(self, config):
        code, name = get_subsector("541511", config)
        assert code == "541"
        assert name == "Professional, Scientific, and Technical Services"

    def test_hospitals(self, config):
        code, name = get_subsector("622110", config)
        assert code == "622"
        assert name == "Hospitals"

    def test_computer_manufacturing(self, config):
        code, name = get_subsector("334111", config)
        assert code == "334"
        assert name == "Computer and Electronic Product Manufacturing"

    def test_empty_code(self, config):
        code, name = get_subsector("", config)
        assert code is None
        assert name is None

    def test_too_short(self, config):
        code, name = get_subsector("54", config)
        assert code is None
        assert name is None


class TestClassifyNaics:
    def test_full_classification(self, config):
        result = classify_naics("541511", config)
        assert result["naics_code"] == "541511"
        assert result["sector_code"] == "54"
        assert result["sector_name"] == "Professional, Scientific, and Technical Services"
        assert result["subsector_code"] == "541"
        assert result["subsector_name"] == "Professional, Scientific, and Technical Services"

    def test_sector_only(self, config):
        # Unknown subsector but known sector
        result = classify_naics("54", config)
        assert result["sector_code"] == "54"
        assert result["sector_name"] == "Professional, Scientific, and Technical Services"
        assert result["subsector_code"] is None
        assert result["subsector_name"] is None

    def test_unknown_code(self, config):
        result = classify_naics("999999", config)
        assert result["sector_code"] is None
        assert result["sector_name"] is None


class TestLoadNaicsReferenceTable:
    def test_loads_records(self, db):
        n = load_naics_reference_table(db)
        assert n > 0

        rows = db.execute("SELECT COUNT(*) FROM naics_codes").fetchone()
        assert rows[0] > 0

    def test_has_sectors_and_subsectors(self, db):
        load_naics_reference_table(db)

        # Should have sector-level entries
        row = db.execute(
            "SELECT * FROM naics_codes WHERE naics_code = '54'"
        ).fetchone()
        assert row is not None
        assert row["sector_name"] == "Professional, Scientific, and Technical Services"

        # Should have subsector-level entries
        row = db.execute(
            "SELECT * FROM naics_codes WHERE naics_code = '541'"
        ).fetchone()
        assert row is not None
        assert row["subsector_name"] == "Professional, Scientific, and Technical Services"

    def test_idempotent(self, db):
        n1 = load_naics_reference_table(db)
        n2 = load_naics_reference_table(db)
        assert n1 == n2
        rows = db.execute("SELECT COUNT(*) FROM naics_codes").fetchone()
        assert rows[0] == n1


class TestClassifyEmployerProfiles:
    def test_classifies_profiles(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        upsert_lca(db, [_lca_record(naics="541511")])
        build_employer_profiles(db)

        # Profile should already have NAICS from cross_linker
        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["primary_naics"] == "541511"
        assert profile["industry_sector"] == "Professional, Scientific, and Technical Services"

    def test_uses_most_frequent_naics(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        # 3 records with 541511, 1 with 622110
        recs = [
            _lca_record(naics="541511", case_num="I-001"),
            _lca_record(naics="541511", case_num="I-002"),
            _lca_record(naics="541511", case_num="I-003"),
            _lca_record(naics="622110", case_num="I-004"),
        ]
        upsert_lca(db, recs)
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["primary_naics"] == "541511"
        assert profile["industry_sector"] == "Professional, Scientific, and Technical Services"

    def test_no_naics(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        rec = _lca_record(naics="")
        upsert_lca(db, [rec])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["primary_naics"] is None
        assert profile["industry_sector"] is None

    def test_classify_updates_existing_profiles(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        # Build profiles without NAICS first (using empty naics)
        rec = _lca_record(naics="")
        upsert_lca(db, [rec])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["industry_sector"] is None

        # Now update the LCA with NAICS and reclassify
        db.execute(
            "UPDATE lca_applications SET naics_code = '541511' WHERE case_number = 'I-200-00001'"
        )
        db.commit()

        n = classify_employer_profiles(db)
        assert n >= 1

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["industry_sector"] == "Professional, Scientific, and Technical Services"

    def test_multiple_employers_different_sectors(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        recs = [
            _lca_record(name="Tech Corp", naics="541511", case_num="I-001"),
            _lca_record(name="Hospital Inc", naics="622110", case_num="I-002"),
            _lca_record(name="Bank LLC", naics="522110", case_num="I-003"),
        ]
        upsert_lca(db, recs)
        build_employer_profiles(db)

        profiles = db.execute(
            "SELECT * FROM employer_profiles ORDER BY employer_name"
        ).fetchall()
        assert len(profiles) == 3

        sectors = {p["employer_name"]: p["industry_sector"] for p in profiles}
        assert sectors["Bank LLC"] == "Finance and Insurance"
        assert sectors["Hospital Inc"] == "Health Care and Social Assistance"
        assert sectors["Tech Corp"] == "Professional, Scientific, and Technical Services"

    def test_subsector_populated(self, db):
        from src.normalization.cross_linker import build_employer_profiles

        upsert_lca(db, [_lca_record(naics="622110")])
        build_employer_profiles(db)

        profile = db.execute("SELECT * FROM employer_profiles").fetchone()
        assert profile["industry_subsector"] == "Hospitals"
