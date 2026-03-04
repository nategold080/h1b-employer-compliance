# Problems Tracker — H-1B Employer Compliance Tracker

## P1: CORP_SUFFIXES over-stripping descriptive words — DONE
The initial CORP_SUFFIXES list included CONSULTING, SERVICES, TECHNOLOGY, INTERNATIONAL etc. which stripped meaningful words from employer names (e.g. "DELOITTE CONSULTING LLP" → "DELOITTE" instead of "DELOITTE CONSULTING"). Fixed by reducing suffix list to legal entity designations only.

## P2: Cross-linker O(N*M) fuzzy matching too slow — DONE
Original cross-linker did fuzzy matching for all unmatched employers, causing O(N*M) comparisons across 480K LCA × 209K USCIS records. Fixed by using exact normalized name matching only (O(N) via hash lookup). Cross-linking now completes in ~14 seconds.

## P3: LCA FY2021 and FY2024 not downloadable — OPEN
DOL CDN returns HTTP 405 for some fiscal year files. FY2021 Q4 and FY2024 Q4 not available. May be CDN rate limiting or files not published. Currently have FY2020, FY2022, FY2023, FY2025.

## P4: WHD general enforcement database requires API key — OPEN
The DOL enforcement API at apiprod.dol.gov/v4 requires a free API key from dataportal.dol.gov/registration. Currently only have debarment/willful violator lists (11 records). Nathan needs to register for the key.

## P5: Debarment HTML parser fragile — DONE
Initial parser didn't handle DOL's actual HTML structure (Employer Name vs Employer, Debarment Period as date range, &nbsp; empty cells). Fixed to handle actual DOL page structure.

## P6: Some employers may have duplicate profiles — OPEN
Cross-linking uses exact normalized name only. Employers with significant name variations across sources (e.g. "AMAZON.COM SERVICES LLC" vs "AMAZON WEB SERVICES, INC.") may appear as separate profiles. Could add manual alias table for top 100 employers.

## P7: USCIS FY2024 not yet posted — OPEN
USCIS data hub archive only goes through FY2023. FY2024 may still be in the interactive tool only.

## P8: Enrichment 3 (WHD Enforcement) blocked by API key — OPEN
WHD enforcement enrichment requires the same DOL API key as P4. The enrichment plan includes full schema (whd_enforcement table), employer matching pipeline, compliance score improvements, and Enforcement Actions dashboard tab. All infrastructure is ready; awaiting API key registration at dataportal.dol.gov/registration. This is the same key needed by the employer-labor-compliance project.
