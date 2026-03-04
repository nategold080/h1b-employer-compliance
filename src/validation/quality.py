"""Quality scoring for H-1B employer compliance records."""


def score_lca(rec: dict) -> float:
    """Score LCA application record quality (0.0-1.0)."""
    weights = {
        "case_number": 0.10,
        "employer_name": 0.15,
        "employer_state": 0.05,
        "soc_code": 0.10,
        "job_title": 0.10,
        "wage_rate": 0.15,
        "prevailing_wage": 0.10,
        "visa_class": 0.05,
        "worksite_state": 0.05,
        "fiscal_year": 0.05,
        "total_workers": 0.05,
        "case_status": 0.05,
    }
    score = 0.0
    for field, weight in weights.items():
        val = rec.get(field)
        if val is not None and val != "" and val != 0:
            score += weight
    return round(score, 3)


def score_uscis(rec: dict) -> float:
    """Score USCIS employer record quality (0.0-1.0)."""
    weights = {
        "employer_name": 0.20,
        "fiscal_year": 0.10,
        "employer_state": 0.10,
        "total_approvals": 0.15,
        "total_denials": 0.10,
        "approval_rate": 0.15,
        "naics_code": 0.10,
        "employer_zip": 0.10,
    }
    score = 0.0
    for field, weight in weights.items():
        val = rec.get(field)
        if val is not None and val != "" and val != 0:
            score += weight
    return round(score, 3)


def score_whd(rec: dict) -> float:
    """Score WHD violation record quality (0.0-1.0)."""
    weights = {
        "case_id": 0.10,
        "legal_name": 0.15,
        "employer_state": 0.10,
        "violation_type": 0.10,
        "back_wages": 0.15,
        "employees_affected": 0.10,
        "findings_start_date": 0.10,
        "case_status": 0.10,
        "naics_code": 0.10,
    }
    score = 0.0
    for field, weight in weights.items():
        val = rec.get(field)
        if val is not None and val != "" and val != 0:
            score += weight
    return round(score, 3)


def compute_compliance_score(
    avg_wage_ratio: float | None,
    approval_rate: float | None,
    whd_violations: int,
    is_debarred: bool,
    total_back_wages: float,
) -> float | None:
    """Compute composite employer compliance score (0.0-1.0).

    Higher = better compliance. Components:
    - Wage fairness (40%): avg wage / prevailing wage ratio (capped at 1.5)
    - USCIS approval rate (25%): petition approval rate
    - Enforcement history (25%): inverse of violations (0 = perfect, 5+ = 0)
    - Debarment status (10%): 0 if debarred, 1 otherwise
    """
    if avg_wage_ratio is None and approval_rate is None:
        return None

    score = 0.0
    components = 0.0

    # Wage ratio component (40%)
    if avg_wage_ratio is not None and avg_wage_ratio > 0:
        wage_score = min(avg_wage_ratio / 1.5, 1.0)
        score += 0.40 * wage_score
        components += 0.40

    # Approval rate component (25%)
    if approval_rate is not None:
        score += 0.25 * (approval_rate / 100.0)
        components += 0.25

    # Enforcement component (25%)
    if whd_violations == 0:
        enforcement_score = 1.0
    elif whd_violations <= 2:
        enforcement_score = 0.5
    elif whd_violations <= 5:
        enforcement_score = 0.2
    else:
        enforcement_score = 0.0
    score += 0.25 * enforcement_score
    components += 0.25

    # Debarment component (10%)
    debar_score = 0.0 if is_debarred else 1.0
    score += 0.10 * debar_score
    components += 0.10

    # Normalize if not all components present
    if components > 0:
        score = score / components
    return round(score, 3)
