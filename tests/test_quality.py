"""Tests for quality scoring."""

import pytest
from src.validation.quality import (
    score_lca, score_uscis, score_whd, compute_compliance_score,
)


class TestScoreLca:
    def test_full_record(self):
        rec = {
            "case_number": "I-200-00001",
            "employer_name": "Test Corp",
            "employer_state": "NY",
            "soc_code": "15-1256",
            "job_title": "Software Engineer",
            "wage_rate": 120000.0,
            "prevailing_wage": 95000.0,
            "visa_class": "H-1B",
            "worksite_state": "NY",
            "fiscal_year": 2025,
            "total_workers": 1,
            "case_status": "Certified",
        }
        score = score_lca(rec)
        assert score == 1.0

    def test_minimal_record(self):
        rec = {"case_number": "I-200-00001"}
        score = score_lca(rec)
        assert 0.0 < score < 0.5

    def test_empty_record(self):
        score = score_lca({})
        assert score == 0.0


class TestScoreUscis:
    def test_full_record(self):
        rec = {
            "employer_name": "Test Corp",
            "fiscal_year": 2025,
            "employer_state": "NY",
            "total_approvals": 50,
            "total_denials": 5,
            "approval_rate": 90.9,
            "naics_code": "541511",
            "employer_zip": "10001",
        }
        score = score_uscis(rec)
        assert score == 1.0

    def test_empty(self):
        assert score_uscis({}) == 0.0


class TestScoreWhd:
    def test_full_record(self):
        rec = {
            "case_id": "WHD-001",
            "legal_name": "Test Corp",
            "employer_state": "IL",
            "violation_type": "H1B",
            "back_wages": 50000.0,
            "employees_affected": 5,
            "findings_start_date": "2023-01-01",
            "case_status": "Concluded",
            "naics_code": "722511",
        }
        score = score_whd(rec)
        assert score == 1.0


class TestComputeComplianceScore:
    def test_perfect(self):
        score = compute_compliance_score(
            avg_wage_ratio=1.5,
            approval_rate=100.0,
            whd_violations=0,
            is_debarred=False,
            total_back_wages=0.0,
        )
        assert score == 1.0

    def test_debarred(self):
        score = compute_compliance_score(
            avg_wage_ratio=1.0,
            approval_rate=80.0,
            whd_violations=0,
            is_debarred=True,
            total_back_wages=0.0,
        )
        assert score < 1.0

    def test_many_violations(self):
        score = compute_compliance_score(
            avg_wage_ratio=1.0,
            approval_rate=50.0,
            whd_violations=10,
            is_debarred=False,
            total_back_wages=100000.0,
        )
        assert score < 0.5

    def test_none_inputs(self):
        score = compute_compliance_score(
            avg_wage_ratio=None,
            approval_rate=None,
            whd_violations=0,
            is_debarred=False,
            total_back_wages=0.0,
        )
        assert score is None

    def test_low_wage_ratio(self):
        score_low = compute_compliance_score(0.8, 80.0, 0, False, 0.0)
        score_high = compute_compliance_score(1.5, 80.0, 0, False, 0.0)
        assert score_low < score_high

    def test_low_approval(self):
        score_low = compute_compliance_score(1.2, 30.0, 0, False, 0.0)
        score_high = compute_compliance_score(1.2, 95.0, 0, False, 0.0)
        assert score_low < score_high
