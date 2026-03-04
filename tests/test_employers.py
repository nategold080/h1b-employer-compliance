"""Tests for employer name normalization."""

import pytest
from src.normalization.employers import (
    normalize_employer_name, normalize_ein, make_employer_key,
    match_employers, annualize_wage,
)


class TestNormalizeEmployerName:
    def test_basic(self):
        assert normalize_employer_name("Google LLC") == "GOOGLE"

    def test_inc(self):
        assert normalize_employer_name("Apple Inc.") == "APPLE"

    def test_corporation(self):
        assert normalize_employer_name("Microsoft Corporation") == "MICROSOFT"

    def test_multiple_suffixes(self):
        assert normalize_employer_name("Test Holdings Inc.") == "TEST"

    def test_dba(self):
        assert normalize_employer_name("ABC Corp DBA XYZ Store") == "ABC"

    def test_the_prefix(self):
        assert normalize_employer_name("The Boeing Company") == "BOEING"

    def test_empty(self):
        assert normalize_employer_name("") == ""
        assert normalize_employer_name(None) == ""

    def test_punctuation(self):
        assert normalize_employer_name("B.J.'s Wholesale") == "BJS WHOLESALE"

    def test_parenthetical(self):
        assert normalize_employer_name("Amazon.com (US)") == "AMAZONCOM"

    def test_abbreviations(self):
        result = normalize_employer_name("ABC Intl Tech Corp")
        assert "INTERNATIONAL" in result
        assert "TECHNOLOGY" in result

    def test_hyphen(self):
        assert normalize_employer_name("Hewlett-Packard") == "HEWLETT PACKARD"

    def test_fka(self):
        result = normalize_employer_name("New Corp FKA Old Corp")
        assert "OLD" not in result

    def test_ltd(self):
        assert normalize_employer_name("Test Ltd") == "TEST"

    def test_llp(self):
        assert normalize_employer_name("Firm LLP") == "FIRM"

    def test_preserves_core(self):
        assert normalize_employer_name("INFOSYS LIMITED") == "INFOSYS"
        assert normalize_employer_name("TATA CONSULTANCY SERVICES LIMITED") == "TATA CONSULTANCY SERVICES"

    def test_real_employer_names(self):
        """Test with real H-1B employer names."""
        assert normalize_employer_name("COGNIZANT TECHNOLOGY SOLUTIONS US CORP") == "COGNIZANT TECHNOLOGY SOLUTIONS US"
        assert normalize_employer_name("DELOITTE CONSULTING LLP") == "DELOITTE CONSULTING"
        assert normalize_employer_name("ERNST & YOUNG LLP") == "ERNST YOUNG"  # & removed, whitespace collapsed
        assert normalize_employer_name("JPMORGAN CHASE & CO.") == "JPMORGAN CHASE"


class TestNormalizeEin:
    def test_basic(self):
        assert normalize_ein("123456789") == "12-3456789"

    def test_with_dash(self):
        assert normalize_ein("12-3456789") == "12-3456789"

    def test_empty(self):
        assert normalize_ein("") == ""
        assert normalize_ein(None) == ""

    def test_short(self):
        assert normalize_ein("12345") == "12345"


class TestMakeEmployerKey:
    def test_basic(self):
        key = make_employer_key("Google LLC", "CA", "Mountain View")
        assert "GOOGLE" in key
        assert "CA" in key
        assert "MOUNTAIN VIEW" in key

    def test_name_only(self):
        key = make_employer_key("Google LLC")
        assert key == "GOOGLE"


class TestMatchEmployers:
    def test_exact(self):
        match, conf = match_employers("Google LLC", "GOOGLE INC")
        assert match is True
        assert conf == 1.0  # Both normalize to "GOOGLE"

    def test_fuzzy(self):
        match, conf = match_employers(
            "TATA CONSULTANCY SERVICES LIMITED",
            "TATA CONSULTANCY SERVICES LTD"
        )
        assert match is True
        assert conf >= 0.85

    def test_no_match(self):
        match, conf = match_employers("Google LLC", "Amazon Inc")
        assert match is False

    def test_empty(self):
        match, conf = match_employers("", "Google")
        assert match is False


class TestAnnualizeWage:
    def test_year(self):
        assert annualize_wage(120000, "Year") == 120000.0

    def test_hour(self):
        assert annualize_wage(50.0, "Hour") == 104000.0

    def test_month(self):
        assert annualize_wage(10000, "Month") == 120000.0

    def test_week(self):
        assert annualize_wage(2000, "Week") == 104000.0

    def test_biweekly(self):
        assert annualize_wage(4000, "Bi-Weekly") == 104000.0

    def test_none(self):
        assert annualize_wage(None, "Year") is None

    def test_zero(self):
        assert annualize_wage(0, "Year") is None

    def test_no_unit(self):
        assert annualize_wage(120000, None) == 120000.0
