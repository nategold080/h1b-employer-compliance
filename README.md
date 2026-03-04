# H-1B Employer Compliance Tracker

Cross-linked database of H-1B employer data from DOL LCA filings, USCIS petition outcomes, and enforcement actions. Includes composite compliance scoring for 54,800+ employers.

## Key Numbers

| Metric | Value |
|--------|-------|
| LCA Applications | 480,593 |
| Unique Employers | 65,457 |
| USCIS Employer Records | 209,348 |
| Employer Profiles | 54,858 |
| Cross-Links | 131,541 |
| Debarments/Violators | 11 |
| States/Territories | 57 |
| Fiscal Years | 4 |
| Tests | 89 |

## Data Sources

1. **DOL LCA Disclosure Data** — Labor condition applications (FY2020-2025)
2. **USCIS H-1B Employer Data Hub** — Petition approvals/denials (FY2020-2023)
3. **DOL WHD** — Debarment list and willful violator list

## Quick Start

```bash
pip install -r requirements.txt

# Run full pipeline
python3 -m src.cli pipeline

# Or run individual stages
python3 -m src.cli init
python3 -m src.cli scrape-lca
python3 -m src.cli scrape-uscis
python3 -m src.cli scrape-whd
python3 -m src.cli crosslink
python3 -m src.cli export
python3 -m src.cli stats

# Launch dashboard
python3 -m src.cli dashboard
```

## Compliance Score

Composite score (0.0-1.0) based on:
- **Wage fairness (40%)** — Actual wage / prevailing wage ratio
- **USCIS approval rate (25%)** — Petition approval rate
- **Enforcement history (25%)** — WHD violations (0 = perfect)
- **Debarment status (10%)** — 0 if debarred, 1 otherwise

## Project Structure

```
src/
├── cli.py                 # Click CLI
├── scrapers/              # LCA, USCIS, WHD scrapers
├── normalization/         # Employer matching, cross-linking
├── validation/            # Quality scoring, compliance scoring
├── storage/               # SQLite database
├── export/                # CSV, JSON, Markdown exports
└── dashboard/             # Streamlit app
```

---

Built by **Nathan Goldberg** · nathanmauricegoldberg@gmail.com · [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
