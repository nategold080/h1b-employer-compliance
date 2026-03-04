"""Employer name normalization and matching for H-1B data."""

import re

from rapidfuzz import fuzz


# Legal entity suffixes to strip (only organizational/legal designations)
CORP_SUFFIXES = [
    "INCORPORATED", "CORPORATION", "COMPANY", "LIMITED",
    "HOLDINGS", "GROUP",
    "CORP", "INC", "LLC", "LLP", "LP",
    "LTD", "CO", "PC", "PA", "NA",
]

# Common abbreviation expansions
ABBREVIATIONS = {
    "INTL": "INTERNATIONAL",
    "TECH": "TECHNOLOGY",
    "SVCS": "SERVICES",
    "SVC": "SERVICE",
    "MGMT": "MANAGEMENT",
    "ASSOC": "ASSOCIATES",
    "NATL": "NATIONAL",
    "AMER": "AMERICAN",
    "INFO": "INFORMATION",
}


def normalize_employer_name(name: str) -> str:
    """Normalize employer name for matching.

    Steps:
    1. Uppercase and strip
    2. Remove DBA/TA clauses
    3. Remove punctuation (preserve spaces)
    4. Expand common abbreviations
    5. Strip corporate suffixes (3 passes)
    6. Collapse whitespace
    """
    if not name:
        return ""

    n = name.upper().strip()

    # Remove DBA/TA clauses
    for pattern in [r"\bD/?B/?A\b.*", r"\bT/?A\b\s+.*", r"\bAKA\b.*",
                    r"\bF/?K/?A\b.*", r"\bFORMERLY\b.*"]:
        n = re.sub(pattern, "", n)

    # Remove parenthetical content
    n = re.sub(r"\([^)]*\)", "", n)

    # Remove common non-name prefixes
    n = re.sub(r"^THE\s+", "", n)

    # Remove punctuation but preserve spaces and hyphens
    n = re.sub(r"[.,;:!@#$%^&*()'\"/\\]", "", n)
    n = n.replace("-", " ")

    # Expand abbreviations
    words = n.split()
    expanded = []
    for w in words:
        expanded.append(ABBREVIATIONS.get(w, w))
    n = " ".join(expanded)

    # Strip corporate suffixes (3 passes for nested)
    for _ in range(3):
        for suffix in CORP_SUFFIXES:
            pattern = rf"\b{suffix}\b\s*$"
            n = re.sub(pattern, "", n).strip()

    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()

    return n


def normalize_ein(ein: str) -> str:
    """Normalize EIN to XX-XXXXXXX format."""
    if not ein:
        return ""
    # Strip non-digits
    digits = re.sub(r"\D", "", str(ein))
    if len(digits) == 9:
        return f"{digits[:2]}-{digits[2:]}"
    return digits


def make_employer_key(name: str, state: str = "", city: str = "") -> str:
    """Create a match key for employer deduplication."""
    parts = [normalize_employer_name(name)]
    if state:
        parts.append(state.upper().strip())
    if city:
        parts.append(city.upper().strip())
    return "|".join(p for p in parts if p)


def match_employers(name1: str, name2: str, threshold: int = 85) -> tuple[bool, float]:
    """Check if two employer names match using fuzzy matching.

    Returns (is_match, confidence_score).
    """
    n1 = normalize_employer_name(name1)
    n2 = normalize_employer_name(name2)

    if not n1 or not n2:
        return False, 0.0

    # Exact match
    if n1 == n2:
        return True, 1.0

    # Token sort ratio handles word order differences
    score = fuzz.token_sort_ratio(n1, n2)
    if score >= threshold:
        return True, round(score / 100.0, 2)

    return False, round(score / 100.0, 2)


def annualize_wage(rate: float | None, unit: str | None) -> float | None:
    """Convert wage rate to annualized amount."""
    if rate is None or rate <= 0:
        return None
    if not unit:
        return rate

    unit = unit.upper().strip()
    multipliers = {
        "YEAR": 1,
        "MONTH": 12,
        "BI-WEEKLY": 26,
        "BIWEEKLY": 26,
        "WEEK": 52,
        "HOUR": 2080,  # 40 hrs/week * 52 weeks
    }
    mult = multipliers.get(unit, 1)
    return round(rate * mult, 2)
