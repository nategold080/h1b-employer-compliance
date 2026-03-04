# H-1B Employer Compliance Tracker — Project Documentation

## Overview
Comprehensive employer risk intelligence platform cross-linking H-1B visa data from DOL LCA filings (480K+), USCIS petition outcomes (209K+), DOL enforcement actions, SEC EDGAR financial data for public companies, and NAICS industry classification for all 54,858 employer profiles.

## Architecture

### Data Pipeline
```
DOL LCA Excel → parse_lca() → score_lca() → upsert_lca()
USCIS CSV → parse_uscis_csv() → score_uscis() → upsert_uscis()
DOL WHD HTML → parse_debarment_html() → upsert_debarments()
→ build_cross_links() → build_employer_profiles() [includes NAICS classification]
→ match_employers_to_sec() → fetch_financials() → compute_h1b_financial_metrics()
→ export_all()
```

### Database Schema (SQLite, WAL mode)
- `lca_applications` — 480K+ records, PK: case_number
- `uscis_employers` — 209K records, PK: employer_key (name|state|year)
- `whd_violations` — PK: case_id (currently empty, needs WHD API key)
- `debarments` — 11 records, PK: debar_id
- `employer_profiles` — 54K+ records, PK: profile_id (with industry_sector, SEC financial fields)
- `cross_links` — 131K+ records, links between all tables
- `naics_codes` — Reference table for NAICS sector/subsector names
- `public_companies` — SEC EDGAR public company data (CIK, ticker, SIC code)
- `company_financials` — Annual financials from XBRL 10-K filings (revenue, assets, income, employees)
- `employer_sec_links` — Links between H-1B employers and SEC CIKs

### Key Modules
- `src/scrapers/lca.py` — DOL OFLC Excel download and parsing (openpyxl, read-only mode)
- `src/scrapers/uscis.py` — USCIS CSV download and parsing
- `src/scrapers/whd.py` — Debarment/willful violator HTML scraping
- `src/scrapers/sec_downloader.py` — SEC EDGAR company_tickers.json + XBRL companyfacts API
- `src/normalization/employers.py` — Name normalization, EIN formatting, fuzzy matching, wage annualization
- `src/normalization/cross_linker.py` — Cross-linking engine and profile builder (includes NAICS classification)
- `src/normalization/naics_classifier.py` — NAICS sector/subsector classification from Census Bureau codes
- `src/normalization/sec_matcher.py` — Employer-to-SEC name matching, financial metric computation
- `src/validation/quality.py` — Quality scoring (per-record) and compliance scoring (per-employer)
- `src/storage/database.py` — SQLite schema, upsert functions, stats
- `src/export/exporter.py` — CSV, JSON, Markdown exports (incl. sector summary, SEC data)
- `src/cli.py` — Click CLI with pipeline, scrape, crosslink, enrich, export, stats, dashboard commands

### Technical Notes
- LCA column names vary between fiscal years — handled via `_field_aliases()` mapping
- USCIS URL pattern: `h1b_datahubexport-{YEAR}.csv` (confirmed working FY2020-2023)
- DOL CDN uses Varnish with intermittent rate limiting (HTTP 405) — use 3-5s delays
- Cross-linking uses O(N) exact normalized name matching — fuzzy matching removed for performance
- CORP_SUFFIXES reduced to legal entity designations only (was over-stripping descriptive words)
- Employer profiles use batch INSERT for performance (~15s for 54K profiles)
- NAICS classification: most-frequent NAICS code per employer from LCA data
- SEC matching: exact normalized name matching against company_tickers.json (~10K companies)
- SEC EDGAR rate limit: 10 req/sec, use 0.15s delay, User-Agent required

### Compliance Score Formula
```
wage_component = min(avg_wage_ratio / 1.5, 1.0) × 0.40
approval_component = (approval_rate / 100) × 0.25
enforcement_component = inverse_violations × 0.25
debarment_component = (0 if debarred, 1 otherwise) × 0.10
score = sum(components) / sum(active_weights)
```

### H-1B Financial Metrics (for public companies)
```
h1b_per_1000_employees = (total_lcas / employees) × 1000
revenue_per_h1b = revenue / total_lcas
h1b_wage_bill_pct = (avg_wage × total_lcas) / revenue
```

## CLI Commands
```bash
python3 -m src.cli init          # Initialize database
python3 -m src.cli scrape-lca    # Download LCA data
python3 -m src.cli scrape-uscis  # Download USCIS data
python3 -m src.cli scrape-whd    # Download enforcement data
python3 -m src.cli crosslink     # Build cross-links and profiles
python3 -m src.cli enrich-naics  # Classify employers by NAICS industry sector
python3 -m src.cli enrich-sec    # Link to SEC EDGAR, fetch financials
python3 -m src.cli export        # Generate exports
python3 -m src.cli stats         # Show statistics
python3 -m src.cli pipeline      # Run everything
python3 -m src.cli dashboard     # Launch Streamlit
```

## Dashboard Tabs (10)
1. Overview — KPIs, top employers, filing trends, wage distribution
2. Employer Search — Name search with expandable cards, industry context
3. Top Employers — Filterable table with industry sector filter
4. Compliance Analysis — Score distribution, wage ratio vs compliance
5. Industry Analysis — Sector-level benchmarking (wage ratio, compliance, volume)
6. Financial Context — Public company financials, H-1B intensity, revenue vs filings
7. Geographic — Choropleth maps by state, avg wage by state
8. USCIS Petitions — Approval rate distribution, trends
9. Debarments — Debarred/willful violator list
10. Data Explorer — Raw table browser with search/filter

## Data Source URLs
- LCA: `https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY{YEAR}_Q{QUARTER}.xlsx`
- USCIS: `https://www.uscis.gov/sites/default/files/document/data/h1b_datahubexport-{YEAR}.csv`
- Debarments: `https://www.dol.gov/agencies/whd/immigration/h1b/debarment`
- Willful Violators: `https://www.dol.gov/agencies/whd/immigration/h1b/willful-violator-list`
- SEC Tickers: `https://www.sec.gov/files/company_tickers.json`
- SEC XBRL: `https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json`
- SEC Submissions: `https://data.sec.gov/submissions/CIK{padded_cik}.json`
- WHD API (needs key): `https://apiprod.dol.gov/v4/get/WHD/enforcement/json`
