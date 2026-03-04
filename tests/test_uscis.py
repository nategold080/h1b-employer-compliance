"""Tests for USCIS scraper."""

import csv
import tempfile
from pathlib import Path

import pytest
from src.scrapers.uscis import parse_uscis_csv


def _create_csv(rows, headers=None):
    """Create a temporary CSV file."""
    if headers is None:
        headers = [
            "Fiscal Year", "Employer", "Initial Approval", "Initial Denial",
            "Continuing Approval", "Continuing Denial", "NAICS", "Tax ID",
            "State", "City", "ZIP",
        ]
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False, newline="")
    writer = csv.writer(tmp)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    tmp.close()
    return Path(tmp.name)


class TestParseUscisCsv:
    def test_basic_parse(self):
        path = _create_csv([
            [2023, "GOOGLE LLC", 500, 10, 300, 5, "541511", "1234", "CA", "MOUNTAIN VIEW", "94043"],
        ])
        records = parse_uscis_csv(path)
        assert len(records) == 1
        assert records[0]["employer_name"] == "GOOGLE LLC"
        assert records[0]["total_approvals"] == 800
        assert records[0]["total_denials"] == 15
        assert records[0]["approval_rate"] == 98.16  # 800/815

    def test_empty_file(self):
        path = _create_csv([])
        records = parse_uscis_csv(path)
        assert len(records) == 0

    def test_multiple_records(self):
        path = _create_csv([
            [2023, "GOOGLE LLC", 500, 10, 300, 5, "541511", "", "CA", "MOUNTAIN VIEW", "94043"],
            [2023, "APPLE INC", 200, 5, 100, 2, "334111", "", "CA", "CUPERTINO", "95014"],
            [2023, "MICROSOFT CORP", 400, 8, 250, 3, "511210", "", "WA", "REDMOND", "98052"],
        ])
        records = parse_uscis_csv(path)
        assert len(records) == 3

    def test_fiscal_year_override(self):
        path = _create_csv([
            [2022, "TEST CORP", 10, 1, 5, 0, "", "", "NY", "NEW YORK", "10001"],
        ])
        records = parse_uscis_csv(path, fiscal_year=2023)
        assert records[0]["fiscal_year"] == 2023

    def test_approval_rate_calculation(self):
        path = _create_csv([
            [2023, "TEST CORP", 80, 20, 0, 0, "", "", "NY", "NYC", "10001"],
        ])
        records = parse_uscis_csv(path)
        assert records[0]["approval_rate"] == 80.0

    def test_zero_denials(self):
        path = _create_csv([
            [2023, "PERFECT CORP", 100, 0, 50, 0, "", "", "TX", "AUSTIN", "73301"],
        ])
        records = parse_uscis_csv(path)
        assert records[0]["approval_rate"] == 100.0

    def test_missing_employer(self):
        path = _create_csv([
            [2023, "", 10, 1, 5, 0, "", "", "NY", "", ""],
        ])
        records = parse_uscis_csv(path)
        assert len(records) == 0

    def test_employer_key_format(self):
        path = _create_csv([
            [2023, "Test Corp", 10, 1, 5, 0, "", "", "CA", "LA", "90001"],
        ])
        records = parse_uscis_csv(path)
        assert "TEST CORP" in records[0]["employer_key"]
        assert "CA" in records[0]["employer_key"]

    def test_zip_normalization(self):
        path = _create_csv([
            [2023, "Test Corp", 10, 1, 5, 0, "", "", "NY", "NYC", "10001-1234"],
        ])
        records = parse_uscis_csv(path)
        assert records[0]["employer_zip"] == "10001"
