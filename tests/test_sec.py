"""Tests for SEC EDGAR financial data integration."""

import json
import sqlite3
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.storage.database import (
    init_db, upsert_lca, upsert_public_companies,
    upsert_company_financials, upsert_employer_sec_links,
)
from src.scrapers.sec_downloader import (
    parse_company_tickers, extract_financials,
)
from src.normalization.sec_matcher import (
    build_sec_company_index, match_employers_to_sec,
    import_sec_companies, compute_h1b_financial_metrics, _score_financial,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


@pytest.fixture
def tickers_file(tmp_path):
    """Create a mock company_tickers.json file."""
    data = {
        "0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc.", "exchange": "Nasdaq"},
        "1": {"cik_str": "789019", "ticker": "MSFT", "title": "MICROSOFT CORP", "exchange": "Nasdaq"},
        "2": {"cik_str": "1652044", "ticker": "GOOGL", "title": "Alphabet Inc.", "exchange": "Nasdaq"},
        "3": {"cik_str": "1018724", "ticker": "AMZN", "title": "AMAZON COM INC", "exchange": "Nasdaq"},
        "4": {"cik_str": "1067983", "ticker": "BRK-B", "title": "BERKSHIRE HATHAWAY INC", "exchange": "NYSE"},
    }
    path = tmp_path / "company_tickers.json"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def xbrl_file(tmp_path):
    """Create a mock XBRL companyfacts JSON file."""
    data = {
        "cik": 789019,
        "entityName": "MICROSOFT CORP",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"form": "10-K", "fp": "FY", "fy": 2023, "val": 211915000000},
                            {"form": "10-K", "fp": "FY", "fy": 2022, "val": 198270000000},
                            {"form": "10-Q", "fp": "Q3", "fy": 2023, "val": 52857000000},
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            {"form": "10-K", "fp": "FY", "fy": 2023, "val": 411976000000},
                            {"form": "10-K", "fp": "FY", "fy": 2022, "val": 364840000000},
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {"form": "10-K", "fp": "FY", "fy": 2023, "val": 72361000000},
                            {"form": "10-K", "fp": "FY", "fy": 2022, "val": 67448000000},
                        ]
                    }
                },
            },
            "dei": {
                "EntityNumberOfEmployees": {
                    "units": {
                        "pure": [
                            {"form": "10-K", "fp": "FY", "fy": 2023, "val": 221000},
                            {"form": "10-K", "fp": "FY", "fy": 2022, "val": 221000},
                        ]
                    }
                }
            }
        }
    }
    path = tmp_path / "CIK0000789019.json"
    path.write_text(json.dumps(data))
    return path


def _lca_record(name="MICROSOFT CORP", case_num="I-200-00001"):
    return {
        "case_number": case_num, "case_status": "Certified",
        "employer_name": name, "employer_ein": "",
        "employer_city": "Redmond", "employer_state": "WA",
        "employer_zip": "98052", "employer_country": "US",
        "naics_code": "541511", "soc_code": "15-1256",
        "soc_title": "Software Developers", "job_title": "Software Engineer",
        "wage_rate": 180000.0, "wage_unit": "Year",
        "annualized_wage": 180000.0, "prevailing_wage": 120000.0,
        "pw_unit": "Year", "annualized_pw": 120000.0, "wage_ratio": 1.5,
        "worksite_city": "Redmond", "worksite_state": "WA",
        "worksite_zip": "98052", "visa_class": "H-1B",
        "submit_date": "2024-01-15", "decision_date": "2024-01-20",
        "begin_date": "2024-10-01", "end_date": "2027-09-30",
        "total_workers": 1, "full_time": "Y",
        "fiscal_year": 2025, "source_file": "test", "quality_score": 0.9,
    }


class TestParseCompanyTickers:
    def test_basic_parse(self, tickers_file):
        records = parse_company_tickers(tickers_file)
        assert len(records) == 5

    def test_fields(self, tickers_file):
        records = parse_company_tickers(tickers_file)
        apple = next(r for r in records if r["ticker"] == "AAPL")
        assert apple["cik"] == 320193
        assert apple["name"] == "Apple Inc."
        assert apple["exchange"] == "Nasdaq"

    def test_microsoft(self, tickers_file):
        records = parse_company_tickers(tickers_file)
        msft = next(r for r in records if r["ticker"] == "MSFT")
        assert msft["cik"] == 789019
        assert msft["name"] == "MICROSOFT CORP"


class TestExtractFinancials:
    def test_basic_extraction(self, xbrl_file):
        records = extract_financials(xbrl_file)
        assert len(records) >= 2  # 2022 and 2023

    def test_2023_values(self, xbrl_file):
        records = extract_financials(xbrl_file)
        fy2023 = next(r for r in records if r["fiscal_year"] == 2023)
        assert fy2023["revenue"] == 211915000000
        assert fy2023["total_assets"] == 411976000000
        assert fy2023["net_income"] == 72361000000
        assert fy2023["employees"] == 221000

    def test_2022_values(self, xbrl_file):
        records = extract_financials(xbrl_file)
        fy2022 = next(r for r in records if r["fiscal_year"] == 2022)
        assert fy2022["revenue"] == 198270000000

    def test_excludes_quarterly(self, xbrl_file):
        """Should only include 10-K annual data, not quarterly."""
        records = extract_financials(xbrl_file)
        fiscal_years = [r["fiscal_year"] for r in records]
        # Q3 data should not create a separate record
        assert len(records) == 2  # Only FY2022 and FY2023

    def test_empty_facts(self, tmp_path):
        path = tmp_path / "empty.json"
        path.write_text(json.dumps({"facts": {}}))
        records = extract_financials(path)
        assert records == []


class TestBuildSecCompanyIndex:
    def test_builds_index(self, tickers_file):
        index = build_sec_company_index(tickers_file)
        assert len(index) > 0

    def test_normalized_lookup(self, tickers_file):
        index = build_sec_company_index(tickers_file)
        # "MICROSOFT CORP" normalizes to "MICROSOFT"
        assert "MICROSOFT" in index
        assert index["MICROSOFT"][0]["cik"] == 789019


class TestMatchEmployersToSec:
    def test_basic_match(self, db, tickers_file):
        from src.normalization.cross_linker import build_employer_profiles

        upsert_lca(db, [_lca_record(name="MICROSOFT CORP")])
        build_employer_profiles(db)

        n = match_employers_to_sec(db, tickers_file)
        assert n >= 1

        links = db.execute("SELECT * FROM employer_sec_links").fetchall()
        assert len(links) >= 1
        assert links[0]["sec_cik"] == 789019

    def test_no_match(self, db, tickers_file):
        from src.normalization.cross_linker import build_employer_profiles

        upsert_lca(db, [_lca_record(name="TOTALLY UNKNOWN COMPANY")])
        build_employer_profiles(db)

        n = match_employers_to_sec(db, tickers_file)
        assert n == 0

    def test_multiple_matches(self, db, tickers_file):
        from src.normalization.cross_linker import build_employer_profiles

        recs = [
            _lca_record(name="MICROSOFT CORP", case_num="I-001"),
            _lca_record(name="AMAZON COM INC", case_num="I-002"),
        ]
        upsert_lca(db, recs)
        build_employer_profiles(db)

        n = match_employers_to_sec(db, tickers_file)
        assert n >= 2


class TestImportSecCompanies:
    def test_imports_matched(self, db, tickers_file):
        from src.normalization.cross_linker import build_employer_profiles

        upsert_lca(db, [_lca_record(name="MICROSOFT CORP")])
        build_employer_profiles(db)
        match_employers_to_sec(db, tickers_file)

        n = import_sec_companies(db, tickers_file)
        assert n >= 1

        co = db.execute("SELECT * FROM public_companies WHERE sec_cik = 789019").fetchone()
        assert co is not None
        assert co["ticker"] == "MSFT"

    def test_no_matches(self, db, tickers_file):
        n = import_sec_companies(db, tickers_file)
        assert n == 0


class TestUpsertFunctions:
    def test_upsert_public_companies(self, db):
        n = upsert_public_companies(db, [{
            "sec_cik": 789019, "company_name": "MICROSOFT CORP",
            "ticker": "MSFT", "exchange": "Nasdaq",
            "sic_code": "7372", "sic_description": "Prepackaged Software",
            "state_of_incorporation": "WA",
            "business_city": "REDMOND", "business_state": "WA",
        }])
        assert n == 1

        row = db.execute("SELECT * FROM public_companies WHERE sec_cik = 789019").fetchone()
        assert row["ticker"] == "MSFT"

    def test_upsert_company_financials(self, db):
        n = upsert_company_financials(db, [{
            "sec_cik": 789019, "fiscal_year": 2023,
            "revenue": 211915000000, "total_assets": 411976000000,
            "net_income": 72361000000, "employees": 221000,
            "h1b_per_employee": None, "h1b_wage_to_revenue": None,
            "quality_score": 1.0,
        }])
        assert n == 1

        row = db.execute(
            "SELECT * FROM company_financials WHERE sec_cik = 789019 AND fiscal_year = 2023"
        ).fetchone()
        assert row["revenue"] == 211915000000
        assert row["employees"] == 221000

    def test_upsert_employer_sec_links(self, db):
        n = upsert_employer_sec_links(db, [{
            "employer_name": "MICROSOFT CORP",
            "employer_normalized": "MICROSOFT",
            "sec_cik": 789019,
            "link_method": "exact_normalized",
            "confidence": 0.95,
            "is_subsidiary": 0,
        }])
        assert n == 1


class TestComputeFinancialMetrics:
    def test_computes_metrics(self, db, tickers_file):
        from src.normalization.cross_linker import build_employer_profiles

        # Set up: employer with LCA data + SEC link + financials
        recs = [_lca_record(name="MICROSOFT CORP", case_num=f"I-{i}") for i in range(100)]
        upsert_lca(db, recs)
        build_employer_profiles(db)
        match_employers_to_sec(db, tickers_file)
        import_sec_companies(db, tickers_file)

        upsert_company_financials(db, [{
            "sec_cik": 789019, "fiscal_year": 2023,
            "revenue": 211915000000, "total_assets": 411976000000,
            "net_income": 72361000000, "employees": 221000,
            "h1b_per_employee": None, "h1b_wage_to_revenue": None,
            "quality_score": 1.0,
        }])

        n = compute_h1b_financial_metrics(db)
        assert n >= 1

        profile = db.execute(
            "SELECT * FROM employer_profiles WHERE employer_name = 'MICROSOFT CORP'"
        ).fetchone()
        assert profile["is_public"] == 1
        assert profile["ticker"] == "MSFT"
        assert profile["revenue"] == 211915000000
        assert profile["employees"] == 221000
        assert profile["h1b_per_1000_employees"] is not None
        assert profile["h1b_per_1000_employees"] > 0
        assert profile["revenue_per_h1b"] is not None

    def test_no_data(self, db):
        n = compute_h1b_financial_metrics(db)
        assert n == 0


class TestScoreFinancial:
    def test_full_record(self):
        score = _score_financial({
            "revenue": 100e9, "total_assets": 200e9,
            "net_income": 30e9, "employees": 100000,
        })
        assert score == 1.0

    def test_partial_record(self):
        score = _score_financial({"revenue": 100e9, "employees": 100000})
        assert score == 0.6

    def test_empty_record(self):
        score = _score_financial({})
        assert score == 0.0
