# H-1B Employer Compliance Tracker — Methodology

## Data Sources

### 1. DOL Labor Condition Applications (LCA)
- **Source:** DOL Employment and Training Administration, Office of Foreign Labor Certification
- **URL:** https://www.dol.gov/agencies/eta/foreign-labor/performance
- **Format:** Excel (.xlsx), quarterly cumulative releases
- **Coverage:** FY2020, FY2022, FY2023, FY2025 (Q4 full-year files)
- **Records:** 480,593 applications from 65,457 unique employers
- **Key fields:** Case number, employer name/location, SOC code, job title, wage rate, prevailing wage, visa class, case status

### 2. USCIS H-1B Employer Data Hub
- **Source:** USCIS H-1B Employer Data Hub Archive
- **URL:** https://www.uscis.gov/archive/h-1b-employer-data-hub-files
- **Format:** CSV, annual releases
- **Coverage:** FY2020-2023
- **Records:** 209,348 employer-year records
- **Key fields:** Employer name, state, initial/continuing approvals/denials, NAICS code, Tax ID (last 4)

### 3. DOL WHD Enforcement Data
- **Source:** DOL Wage and Hour Division
- **Debarment list:** https://www.dol.gov/agencies/whd/immigration/h1b/debarment
- **Willful violator list:** https://www.dol.gov/agencies/whd/immigration/h1b/willful-violator-list
- **Records:** 3 debarred employers, 8 willful violators (11 total, some overlap)

## Data Pipeline

### Stage 1: Download & Cache
- Files downloaded via HTTP with polite User-Agent and 2-5 second delays
- Raw files cached in `data/raw/` to avoid re-downloading
- Automatic retry with alternative URL patterns for each source

### Stage 2: Parse & Normalize
- LCA Excel files parsed with openpyxl in read-only mode for memory efficiency
- Field name variations handled via alias mapping (LCA column names change between fiscal years)
- Wages annualized to standard annual rate (hourly×2080, weekly×52, monthly×12)
- ZIP codes normalized to 5 digits
- Dates converted to YYYY-MM-DD format

### Stage 3: Employer Name Normalization
- Uppercase and strip whitespace
- Remove DBA/FKA/AKA clauses
- Remove parenthetical content
- Strip "THE" prefix
- Remove punctuation (preserve spaces)
- Expand abbreviations (INTL→INTERNATIONAL, TECH→TECHNOLOGY, etc.)
- Strip legal entity suffixes (LLC, INC, CORP, LTD, etc.) — 3 passes
- Collapse whitespace

### Stage 4: Quality Scoring
Each record scored 0.0-1.0 based on field completeness:
- **LCA:** 12 weighted components (employer name 15%, wage rate 15%, employer state 5%, etc.)
- **USCIS:** 8 weighted components (employer name 20%, approvals 15%, approval rate 15%, etc.)

### Stage 5: Cross-Linking
Employers matched across sources using normalized name comparison:
- Normalized name exact match (O(N) via hash lookup)
- Confidence boosted for matching state (+0.05) and fiscal year (+0.05)
- 131,541 cross-links created from 4 linking functions:
  - LCA → USCIS (employer filing → petition outcome)
  - LCA → WHD (employer filing → enforcement action)
  - USCIS → WHD (petition → enforcement)
  - WHD → Debarments (violation → debarment)

### Stage 6: Employer Profile Construction
54,858 unified employer profiles built by aggregating:
- LCA filing counts, worker totals, wage statistics
- USCIS approval/denial totals and rates
- WHD violation counts and back wage amounts
- Debarment status

### Compliance Score Formula
Composite score (0.0-1.0), higher = better compliance:
- **Wage fairness (40%):** avg_wage_ratio / 1.5 (capped at 1.0)
- **USCIS approval rate (25%):** approval_rate / 100
- **Enforcement history (25%):** 0 violations = 1.0, 1-2 = 0.5, 3-5 = 0.2, 6+ = 0.0
- **Debarment status (10%):** 0 if debarred, 1 otherwise

## Coverage and Limitations

### Known Limitations
- LCA FY2021 and FY2024 not available (DOL CDN returns 405)
- USCIS data only through FY2023 (FY2024 not yet posted to archive)
- WHD general enforcement database requires API key (only debarment/willful violator lists scraped)
- Cross-linking uses exact normalized name matching only (no fuzzy matching for performance)
- Some employers may appear as separate profiles due to name variations not caught by normalization
- Debarment list is small (11 records) — enforcement data is limited without WHD API access

### Data Quality
- LCA quality average: 1.000 (all weighted fields present in DOL disclosure files)
- USCIS quality average: 0.897 (some records missing NAICS/ZIP)
- 37,119 employers successfully cross-linked between LCA and USCIS sources

## Reproducibility
The entire pipeline is reproducible from public sources using the CLI:
```bash
python3 -m src.cli pipeline
```

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com
