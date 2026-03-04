# H-1B Employer Compliance Tracker
## Cross-Linked Federal Data for Employer Risk Assessment

### The Problem
H-1B visa employer data is scattered across multiple federal agencies. DOL tracks labor condition applications, USCIS tracks petition approvals/denials, and WHD tracks enforcement actions. No one has systematically connected these datasets to build comprehensive employer compliance profiles.

### Our Solution
We've built the first cross-linked H-1B employer compliance database, unifying data from:

| Source | Records | Coverage |
|--------|---------|----------|
| DOL LCA Applications | 480,593 | FY2020-2025 |
| USCIS Employer Hub | 209,348 | FY2020-2023 |
| DOL Debarments/Violators | 11 | Current |
| Employer Profiles | 54,858 | Cross-linked |
| Cross-Links | 131,541 | Automated matching |

### Unique Value
- **Composite compliance scores** (0-1.0) based on wage fairness, petition outcomes, and enforcement history
- **Wage ratio analysis**: Actual wages vs. DOL prevailing wages for every employer
- **Cross-linked petition outcomes**: Connect LCA filings to USCIS approval/denial rates
- **Enforcement matching**: Identify debarred employers with active H-1B filings

### Key Insights
- Cognizant: 17,365 LCAs, only 78.7% USCIS approval rate, avg wage barely above prevailing
- Google: 5,209 LCAs, 98.8% approval, avg wage $250K (1.75x prevailing)
- 5 debarred/willful violator companies found with active LCA filings
- IT outsourcing sector shows systematically lower wage ratios than direct-hire tech

### Applications
- **Immigration law firms**: Employer due diligence and client advisory
- **Policy researchers**: H-1B labor market analysis
- **Compliance platforms**: Employer risk scoring integration
- **Journalists**: Data-driven H-1B employer investigations

### Technical Details
- Rule-based extraction pipeline (zero LLM dependency)
- SQLite database with 6 tables, 16 indexes
- 89 automated tests
- Interactive Streamlit dashboard
- CSV/JSON/Excel exports

---

**Built by Nathan Goldberg** | nathanmauricegoldberg@gmail.com | linkedin.com/in/nathanmauricegoldberg
