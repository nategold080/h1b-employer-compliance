"""Tests for WHD scraper."""

import pytest
from src.scrapers.whd import parse_debarment_html, _parse_whd_row


class TestParseDebarmentHtml:
    def test_basic_table(self):
        html = """
        <table>
            <tr><th>Employer</th><th>City</th><th>State</th>
                <th>Start Date</th><th>End Date</th><th>Violation</th></tr>
            <tr><td>Bad Corp LLC</td><td>Houston</td><td>TX</td>
                <td>01/15/2024</td><td>01/15/2026</td><td>Willful Failure</td></tr>
            <tr><td>Evil Inc</td><td>Miami</td><td>FL</td>
                <td>06/01/2023</td><td>06/01/2025</td><td>Fraud</td></tr>
        </table>
        """
        records = parse_debarment_html(html)
        assert len(records) == 2
        assert records[0]["employer_name"] == "Bad Corp LLC"
        assert records[0]["employer_state"] == "TX"
        assert records[0]["debar_start_date"] == "2024-01-15"

    def test_empty_html(self):
        records = parse_debarment_html("")
        assert len(records) == 0

    def test_no_table(self):
        records = parse_debarment_html("<p>No data available</p>")
        assert len(records) == 0

    def test_company_name_header(self):
        html = """
        <table>
            <tr><th>Company Name</th><th>City</th><th>State</th>
                <th>Debarment Start</th><th>Debarment End</th></tr>
            <tr><td>Test Corp</td><td>LA</td><td>CA</td>
                <td>2024-01-01</td><td>2026-01-01</td></tr>
        </table>
        """
        records = parse_debarment_html(html)
        assert len(records) == 1
        assert records[0]["employer_name"] == "Test Corp"


class TestParseWhdRow:
    def test_basic_row(self):
        row = {
            "case_id": "WHD-001",
            "legal_name": "Test Corp",
            "st_cd": "NY",
            "cty_nm": "New York",
            "bw_amt": "50000",
            "cmp_amt": "10000",
            "ee_violtd_cnt": "5",
        }
        rec = _parse_whd_row(row, "test")
        assert rec is not None
        assert rec["legal_name"] == "Test Corp"
        assert rec["back_wages"] == 50000.0

    def test_h1b_detection(self):
        row = {
            "case_id": "WHD-002",
            "legal_name": "Test Corp",
            "h1b_bw_amt": "25000",
            "st_cd": "CA",
        }
        rec = _parse_whd_row(row, "test")
        assert rec["h1b_related"] == 1

    def test_empty_row(self):
        rec = _parse_whd_row({}, "test")
        assert rec is None

    def test_violation_type_h1b(self):
        row = {
            "case_id": "WHD-003",
            "legal_name": "Corp X",
            "violation_type": "H-1B Violations",
        }
        rec = _parse_whd_row(row, "test")
        assert rec["h1b_related"] == 1

    def test_date_cleaning(self):
        row = {
            "case_id": "WHD-004",
            "legal_name": "Corp Y",
            "invest_start_dt": "03/15/2023",
        }
        rec = _parse_whd_row(row, "test")
        assert rec["findings_start_date"] == "2023-03-15"
