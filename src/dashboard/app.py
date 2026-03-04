"""Streamlit dashboard for H-1B Employer Compliance Tracker."""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

_DATA_DIR = Path(__file__).parent.parent.parent / "data"
_DEPLOY_DB = _DATA_DIR / "h1b_compliance_deploy.db"
DB_PATH = _DEPLOY_DB if _DEPLOY_DB.exists() else _DATA_DIR / "h1b_compliance.db"

_NO_DATA_MSG = "No data loaded yet. Run the pipeline first: `python -m src.cli pipeline`"


def _table_exists(conn, name):
    try:
        r = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return r is not None
    except Exception:
        return False


def _safe_fetchone(conn, sql, params=(), default=0):
    try:
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return default
        val = row[0] if isinstance(row, (tuple, list)) else row[row.keys()[0]]
        return val if val is not None else default
    except Exception:
        return default


@st.cache_resource
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=3600)
def query(sql, params=None):
    conn = get_db()
    try:
        if params:
            rows = conn.execute(sql, params).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        if rows:
            cols = rows[0].keys()
            return pd.DataFrame([dict(r) for r in rows], columns=cols)
    except Exception:
        pass
    return pd.DataFrame()


def main():
    st.set_page_config(
        page_title="H-1B Employer Compliance Tracker",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 H-1B Employer Compliance Tracker")
    st.caption("Cross-linked DOL LCA, USCIS petition, and enforcement data")

    tabs = st.tabs([
        "Overview", "Employer Search", "Top Employers",
        "Compliance Analysis", "Industry Analysis", "Financial Context",
        "Geographic", "USCIS Petitions", "Debarments", "Data Explorer",
    ])

    with tabs[0]:
        _overview_tab()
    with tabs[1]:
        _search_tab()
    with tabs[2]:
        _top_employers_tab()
    with tabs[3]:
        _compliance_tab()
    with tabs[4]:
        _industry_tab()
    with tabs[5]:
        _financial_tab()
    with tabs[6]:
        _geographic_tab()
    with tabs[7]:
        _uscis_tab()
    with tabs[8]:
        _debarments_tab()
    with tabs[9]:
        _explorer_tab()

    st.markdown("---")
    st.markdown(
        "Built by **Nathan Goldberg** · "
        "[Email](mailto:nathanmauricegoldberg@gmail.com) · "
        "[LinkedIn](https://www.linkedin.com/in/nathan-goldberg-62a44522a/)"
    )


def _overview_tab():
    conn = get_db()

    if not _table_exists(conn, "lca_applications"):
        st.warning(_NO_DATA_MSG)
        return

    # KPIs
    stats = {}
    for table in ["lca_applications", "uscis_employers", "whd_violations",
                   "debarments", "employer_profiles", "cross_links"]:
        stats[table] = _safe_fetchone(conn, f"SELECT COUNT(*) FROM {table}")

    stats["unique_employers"] = _safe_fetchone(conn, "SELECT COUNT(DISTINCT employer_name) FROM lca_applications")
    stats["fiscal_years"] = _safe_fetchone(conn, "SELECT COUNT(DISTINCT fiscal_year) FROM lca_applications WHERE fiscal_year IS NOT NULL")
    stats["avg_wage"] = _safe_fetchone(conn, "SELECT AVG(annualized_wage) FROM lca_applications WHERE annualized_wage > 0")
    stats["total_workers"] = _safe_fetchone(conn, "SELECT COALESCE(SUM(total_workers), 0) FROM lca_applications")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("LCA Applications", f"{stats['lca_applications']:,}")
    c2.metric("Unique Employers", f"{stats['unique_employers']:,}")
    c3.metric("USCIS Employer Records", f"{stats['uscis_employers']:,}")
    c4.metric("Cross-Links", f"{stats['cross_links']:,}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Employer Profiles", f"{stats['employer_profiles']:,}")
    c6.metric("Average Wage", f"${stats['avg_wage']:,.0f}")
    c7.metric("Total Workers", f"{stats['total_workers']:,}")
    c8.metric("Debarments", f"{stats['debarments']:,}")

    col1, col2 = st.columns(2)

    with col1:
        # Top 10 by LCA volume
        df = query("""
            SELECT employer_name, total_lcas
            FROM employer_profiles ORDER BY total_lcas DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.bar(df, x="total_lcas", y="employer_name", orientation="h",
                         title="Top 10 H-1B Employers by LCA Volume",
                         labels={"total_lcas": "LCA Applications", "employer_name": ""})
            fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # LCA by fiscal year
        df = query("""
            SELECT fiscal_year, COUNT(*) as count
            FROM lca_applications
            WHERE fiscal_year IS NOT NULL
            GROUP BY fiscal_year ORDER BY fiscal_year
        """)
        if not df.empty:
            fig = px.bar(df, x="fiscal_year", y="count",
                         title="LCA Applications by Fiscal Year",
                         labels={"count": "Applications", "fiscal_year": "Fiscal Year"})
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        # Wage distribution
        df = query("""
            SELECT CASE
                WHEN annualized_wage < 60000 THEN '<$60K'
                WHEN annualized_wage < 80000 THEN '$60-80K'
                WHEN annualized_wage < 100000 THEN '$80-100K'
                WHEN annualized_wage < 120000 THEN '$100-120K'
                WHEN annualized_wage < 150000 THEN '$120-150K'
                WHEN annualized_wage < 200000 THEN '$150-200K'
                WHEN annualized_wage < 300000 THEN '$200-300K'
                ELSE '$300K+'
            END as wage_band,
            COUNT(*) as count
            FROM lca_applications
            WHERE annualized_wage > 0
            GROUP BY wage_band
        """)
        if not df.empty:
            order = ["<$60K", "$60-80K", "$80-100K", "$100-120K",
                     "$120-150K", "$150-200K", "$200-300K", "$300K+"]
            df["wage_band"] = pd.Categorical(df["wage_band"], categories=order, ordered=True)
            df = df.sort_values("wage_band")
            fig = px.bar(df, x="wage_band", y="count",
                         title="H-1B Wage Distribution",
                         labels={"count": "Applications", "wage_band": "Wage Band"})
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Top SOC codes
        df = query("""
            SELECT soc_code, soc_title, COUNT(*) as count
            FROM lca_applications
            WHERE soc_code IS NOT NULL AND soc_code != ''
            GROUP BY soc_code, soc_title
            ORDER BY count DESC LIMIT 10
        """)
        if not df.empty:
            df["label"] = df["soc_code"] + " " + df["soc_title"].str[:30]
            fig = px.bar(df, x="count", y="label", orientation="h",
                         title="Top 10 Occupations (SOC Code)",
                         labels={"count": "Applications", "label": ""})
            fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
            st.plotly_chart(fig, use_container_width=True)


def _search_tab():
    st.subheader("Employer Search")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles"):
        st.warning(_NO_DATA_MSG)
        return

    search = st.text_input("Search employer name:", placeholder="e.g. Google, Microsoft, Infosys")

    if search and len(search) >= 2:
        df = query("""
            SELECT employer_name, total_lcas, total_workers,
                   ROUND(avg_wage, 0) as avg_wage,
                   ROUND(avg_wage_ratio, 3) as wage_ratio,
                   approval_rate, whd_violations, is_debarred,
                   ROUND(compliance_score, 3) as compliance_score,
                   industry_sector, states_active, fiscal_years, source_types
            FROM employer_profiles
            WHERE employer_name LIKE ? OR normalized_name LIKE ?
            ORDER BY total_lcas DESC
            LIMIT 50
        """, (f"%{search}%", f"%{search.upper()}%"))

        if df.empty:
            st.warning("No employers found.")
        else:
            st.success(f"Found {len(df)} employer(s)")

            for _, row in df.iterrows():
                with st.expander(f"**{row['employer_name']}** — {row['total_lcas']:,} LCAs"):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("LCA Applications", f"{row['total_lcas']:,}")
                    c2.metric("Total Workers", f"{row['total_workers']:,}")
                    c3.metric("Avg Wage", "N/A" if row['avg_wage'] is None else f"${row['avg_wage']:,.0f}")
                    c4.metric("Compliance Score", "N/A" if row['compliance_score'] is None else f"{row['compliance_score']:.3f}")

                    c5, c6, c7, c8 = st.columns(4)
                    c5.metric("Wage Ratio", "N/A" if row['wage_ratio'] is None else f"{row['wage_ratio']}")
                    c6.metric("USCIS Approval Rate", "N/A" if row['approval_rate'] is None else f"{row['approval_rate']:.1f}%")
                    c7.metric("WHD Violations", str(row['whd_violations']))
                    c8.metric("Debarred", "⚠️ YES" if row['is_debarred'] else "No")

                    st.write(f"**Industry:** {row.get('industry_sector', 'N/A')}")
                    st.write(f"**States:** {row['states_active']}")
                    st.write(f"**Fiscal Years:** {row['fiscal_years']}")
                    st.write(f"**Data Sources:** {row['source_types']}")


def _top_employers_tab():
    st.subheader("Top H-1B Employers")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles"):
        st.warning(_NO_DATA_MSG)
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        min_lcas = st.number_input("Minimum LCAs", value=10, min_value=1)
    with col2:
        SORT_OPTIONS = ["total_lcas", "total_workers", "avg_wage",
                        "compliance_score", "approval_rate", "avg_wage_ratio"]
        sort_by = st.selectbox("Sort by", SORT_OPTIONS)
    with col3:
        try:
            sector_rows = conn.execute(
                "SELECT DISTINCT industry_sector FROM employer_profiles WHERE industry_sector IS NOT NULL ORDER BY industry_sector"
            ).fetchall()
            sector_options = ["All Sectors"] + [r["industry_sector"] for r in sector_rows]
        except Exception:
            sector_options = ["All Sectors"]
        sector_filter = st.selectbox("Industry Sector", sector_options)

    # Validate sort column against whitelist
    if sort_by not in SORT_OPTIONS:
        sort_by = "total_lcas"

    sector_clause = ""
    params = [min_lcas]
    if sector_filter != "All Sectors":
        sector_clause = "AND industry_sector = ?"
        params.append(sector_filter)

    df = query(f"""
        SELECT employer_name, total_lcas, total_workers,
               ROUND(avg_wage, 0) as avg_wage,
               ROUND(avg_wage_ratio, 3) as wage_ratio,
               approval_rate, whd_violations, is_debarred,
               ROUND(compliance_score, 3) as compliance_score,
               industry_sector, source_types
        FROM employer_profiles
        WHERE total_lcas >= ? {sector_clause}
        ORDER BY {sort_by} DESC
        LIMIT 100
    """, tuple(params))

    if not df.empty:
        st.dataframe(df, use_container_width=True, height=500)

        # Download button
        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, "top_employers.csv", "text/csv")


def _compliance_tab():
    st.subheader("Compliance Analysis")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles"):
        st.warning(_NO_DATA_MSG)
        return

    col1, col2 = st.columns(2)

    with col1:
        # Compliance score distribution
        df = query("""
            SELECT CASE
                WHEN compliance_score >= 0.9 THEN 'Excellent (0.9+)'
                WHEN compliance_score >= 0.7 THEN 'Good (0.7-0.9)'
                WHEN compliance_score >= 0.5 THEN 'Fair (0.5-0.7)'
                ELSE 'Poor (<0.5)'
            END as rating, COUNT(*) as count
            FROM employer_profiles
            WHERE compliance_score IS NOT NULL
            GROUP BY rating
        """)
        if not df.empty:
            fig = px.pie(df, names="rating", values="count",
                         title="Compliance Score Distribution")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Wage ratio vs compliance
        df = query("""
            SELECT ROUND(avg_wage_ratio, 1) as wage_ratio_band,
                   AVG(compliance_score) as avg_compliance,
                   COUNT(*) as count
            FROM employer_profiles
            WHERE avg_wage_ratio IS NOT NULL AND avg_wage_ratio BETWEEN 0.5 AND 3.0
                  AND compliance_score IS NOT NULL
            GROUP BY wage_ratio_band
            HAVING count >= 5
            ORDER BY wage_ratio_band
        """)
        if not df.empty:
            fig = px.scatter(df, x="wage_ratio_band", y="avg_compliance",
                             size="count", title="Wage Ratio vs Compliance Score",
                             labels={"wage_ratio_band": "Avg Wage / Prevailing Wage",
                                     "avg_compliance": "Avg Compliance Score"})
            st.plotly_chart(fig, use_container_width=True)

    # Lowest compliance employers with significant filings
    st.subheader("Lowest Compliance Employers (50+ LCAs)")
    df = query("""
        SELECT employer_name, total_lcas, total_workers,
               ROUND(avg_wage, 0) as avg_wage,
               ROUND(avg_wage_ratio, 3) as wage_ratio,
               approval_rate, whd_violations, is_debarred,
               ROUND(compliance_score, 3) as compliance_score
        FROM employer_profiles
        WHERE compliance_score IS NOT NULL AND total_lcas >= 50
        ORDER BY compliance_score ASC
        LIMIT 25
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def _industry_tab():
    st.subheader("Industry Sector Analysis")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles"):
        st.warning(_NO_DATA_MSG)
        return

    # KPI row
    total_sectors = _safe_fetchone(conn, "SELECT COUNT(DISTINCT industry_sector) FROM employer_profiles WHERE industry_sector IS NOT NULL")
    classified = _safe_fetchone(conn, "SELECT COUNT(*) FROM employer_profiles WHERE industry_sector IS NOT NULL")
    total_profiles = _safe_fetchone(conn, "SELECT COUNT(*) FROM employer_profiles")

    c1, c2, c3 = st.columns(3)
    c1.metric("Industry Sectors", f"{total_sectors}")
    c2.metric("Classified Profiles", f"{classified:,}")
    c3.metric("Classification Rate", f"{100*classified/total_profiles:.1f}%" if total_profiles else "0%")

    col1, col2 = st.columns(2)

    with col1:
        # H-1B filing volume by sector
        df = query("""
            SELECT industry_sector, SUM(total_lcas) as total_lcas,
                   COUNT(*) as employers
            FROM employer_profiles
            WHERE industry_sector IS NOT NULL
            GROUP BY industry_sector
            ORDER BY total_lcas DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="total_lcas", y="industry_sector", orientation="h",
                         title="H-1B LCA Volume by Industry Sector",
                         labels={"total_lcas": "LCA Applications", "industry_sector": ""},
                         hover_data=["employers"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Average wage ratio by sector
        df = query("""
            SELECT industry_sector,
                   ROUND(AVG(avg_wage_ratio), 3) as avg_wage_ratio,
                   ROUND(AVG(avg_wage), 0) as avg_wage,
                   COUNT(*) as employers
            FROM employer_profiles
            WHERE industry_sector IS NOT NULL
                  AND avg_wage_ratio IS NOT NULL AND avg_wage_ratio > 0
            GROUP BY industry_sector
            HAVING employers >= 5
            ORDER BY avg_wage_ratio DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="avg_wage_ratio", y="industry_sector", orientation="h",
                         title="Avg Wage Ratio by Industry Sector",
                         labels={"avg_wage_ratio": "Avg Wage / Prevailing Wage",
                                 "industry_sector": ""},
                         hover_data=["avg_wage", "employers"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            fig.add_vline(x=1.0, line_dash="dash", line_color="red",
                          annotation_text="Prevailing Wage")
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Compliance score by sector
        df = query("""
            SELECT industry_sector,
                   ROUND(AVG(compliance_score), 3) as avg_compliance,
                   COUNT(*) as employers
            FROM employer_profiles
            WHERE industry_sector IS NOT NULL
                  AND compliance_score IS NOT NULL
            GROUP BY industry_sector
            HAVING employers >= 5
            ORDER BY avg_compliance DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="avg_compliance", y="industry_sector", orientation="h",
                         title="Avg Compliance Score by Industry Sector",
                         labels={"avg_compliance": "Avg Compliance Score",
                                 "industry_sector": ""},
                         hover_data=["employers"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Average wage by sector
        df = query("""
            SELECT industry_sector,
                   ROUND(AVG(avg_wage), 0) as avg_wage,
                   COUNT(*) as employers
            FROM employer_profiles
            WHERE industry_sector IS NOT NULL
                  AND avg_wage IS NOT NULL AND avg_wage > 0
            GROUP BY industry_sector
            HAVING employers >= 5
            ORDER BY avg_wage DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="avg_wage", y="industry_sector", orientation="h",
                         title="Avg H-1B Wage by Industry Sector",
                         labels={"avg_wage": "Average Wage ($)",
                                 "industry_sector": ""},
                         hover_data=["employers"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig, use_container_width=True)

    # Sector detail table
    st.subheader("Industry Sector Summary")
    df = query("""
        SELECT industry_sector as "Sector",
               COUNT(*) as "Employers",
               SUM(total_lcas) as "Total LCAs",
               SUM(total_workers) as "Total Workers",
               ROUND(AVG(avg_wage), 0) as "Avg Wage",
               ROUND(AVG(avg_wage_ratio), 3) as "Avg Wage Ratio",
               ROUND(AVG(compliance_score), 3) as "Avg Compliance"
        FROM employer_profiles
        WHERE industry_sector IS NOT NULL
        GROUP BY industry_sector
        ORDER BY "Total LCAs" DESC
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=400)

    # Top employers per sector (filterable)
    st.subheader("Top Employers by Sector")
    sectors = query("""
        SELECT DISTINCT industry_sector
        FROM employer_profiles
        WHERE industry_sector IS NOT NULL
        ORDER BY industry_sector
    """)
    if not sectors.empty:
        selected_sector = st.selectbox("Select sector:", sectors["industry_sector"].tolist())
        if selected_sector:
            df = query("""
                SELECT employer_name, total_lcas, total_workers,
                       ROUND(avg_wage, 0) as avg_wage,
                       ROUND(avg_wage_ratio, 3) as wage_ratio,
                       ROUND(compliance_score, 3) as compliance_score,
                       industry_subsector
                FROM employer_profiles
                WHERE industry_sector = ?
                ORDER BY total_lcas DESC
                LIMIT 25
            """, (selected_sector,))
            if not df.empty:
                st.dataframe(df, use_container_width=True)


def _financial_tab():
    st.subheader("Financial Context — Public Company H-1B Analysis")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles"):
        st.warning(_NO_DATA_MSG)
        return

    n_companies = _safe_fetchone(conn, "SELECT COUNT(*) FROM public_companies") if _table_exists(conn, "public_companies") else 0
    n_public = _safe_fetchone(conn, "SELECT COUNT(*) FROM employer_profiles WHERE is_public = 1")
    n_fin = _safe_fetchone(conn, "SELECT COUNT(*) FROM company_financials") if _table_exists(conn, "company_financials") else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Public Companies Matched", f"{n_companies:,}")
    c2.metric("Public Employer Profiles", f"{n_public:,}")
    c3.metric("Financial Records", f"{n_fin:,}")

    if n_public == 0:
        st.info("No public company data available yet. Run `enrich-sec` to link employer profiles to SEC EDGAR financial data.")
        return

    col1, col2 = st.columns(2)

    with col1:
        # Top public companies by H-1B volume
        df = query("""
            SELECT ep.employer_name, ep.ticker, ep.total_lcas,
                   ep.total_workers, ROUND(ep.avg_wage, 0) as avg_wage,
                   ROUND(ep.revenue / 1e9, 2) as revenue_b,
                   ep.employees,
                   ROUND(ep.h1b_per_1000_employees, 2) as h1b_per_1000
            FROM employer_profiles ep
            WHERE ep.is_public = 1 AND ep.revenue IS NOT NULL
            ORDER BY ep.total_lcas DESC
            LIMIT 20
        """)
        if not df.empty:
            fig = px.bar(df, x="total_lcas", y="employer_name", orientation="h",
                         title="Top 20 Public Companies by H-1B Volume",
                         labels={"total_lcas": "LCA Applications", "employer_name": ""},
                         hover_data=["ticker", "revenue_b"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Revenue vs H-1B volume scatter
        df = query("""
            SELECT ep.employer_name, ep.ticker,
                   ep.total_lcas,
                   ROUND(ep.revenue / 1e9, 2) as revenue_b,
                   ROUND(ep.avg_wage, 0) as avg_wage,
                   ROUND(ep.h1b_per_1000_employees, 2) as h1b_per_1000
            FROM employer_profiles ep
            WHERE ep.is_public = 1 AND ep.revenue IS NOT NULL
                  AND ep.total_lcas >= 10
            ORDER BY ep.revenue DESC
            LIMIT 100
        """)
        if not df.empty:
            fig = px.scatter(df, x="revenue_b", y="total_lcas",
                             hover_name="employer_name",
                             size="h1b_per_1000",
                             title="Revenue vs H-1B Filing Volume",
                             labels={"revenue_b": "Revenue ($B)",
                                     "total_lcas": "LCA Applications",
                                     "h1b_per_1000": "H-1B per 1K Employees"})
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # H-1B intensity (per 1000 employees)
        df = query("""
            SELECT ep.employer_name, ep.ticker,
                   ROUND(ep.h1b_per_1000_employees, 2) as h1b_per_1000,
                   ep.employees, ep.total_lcas
            FROM employer_profiles ep
            WHERE ep.is_public = 1
                  AND ep.h1b_per_1000_employees IS NOT NULL
                  AND ep.employees >= 1000
            ORDER BY ep.h1b_per_1000_employees DESC
            LIMIT 20
        """)
        if not df.empty:
            fig = px.bar(df, x="h1b_per_1000", y="employer_name", orientation="h",
                         title="H-1B Intensity: Applications per 1,000 Employees",
                         labels={"h1b_per_1000": "H-1B per 1K Employees",
                                 "employer_name": ""},
                         hover_data=["ticker", "employees", "total_lcas"])
            fig.update_layout(yaxis=dict(autorange="reversed"), height=500)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Public vs Private comparison
        df = query("""
            SELECT
                CASE WHEN is_public = 1 THEN 'Public' ELSE 'Private' END as company_type,
                COUNT(*) as employers,
                ROUND(AVG(avg_wage), 0) as avg_wage,
                ROUND(AVG(avg_wage_ratio), 3) as avg_wage_ratio,
                ROUND(AVG(compliance_score), 3) as avg_compliance
            FROM employer_profiles
            WHERE compliance_score IS NOT NULL AND total_lcas >= 10
            GROUP BY company_type
        """)
        if not df.empty:
            fig = px.bar(df, x="company_type", y=["avg_wage_ratio", "avg_compliance"],
                         barmode="group",
                         title="Public vs Private H-1B Employers",
                         labels={"company_type": "", "value": "Score / Ratio"})
            st.plotly_chart(fig, use_container_width=True)

    # Detailed public company table
    st.subheader("Public Company H-1B Profiles")
    df = query("""
        SELECT ep.employer_name as "Company", ep.ticker as "Ticker",
               ep.total_lcas as "LCAs", ep.total_workers as "Workers",
               ROUND(ep.avg_wage, 0) as "Avg Wage",
               ROUND(ep.avg_wage_ratio, 3) as "Wage Ratio",
               ROUND(ep.compliance_score, 3) as "Compliance",
               ROUND(ep.revenue / 1e9, 2) as "Revenue ($B)",
               ep.employees as "Employees",
               ROUND(ep.h1b_per_1000_employees, 2) as "H-1B/1K Emp",
               ROUND(ep.revenue_per_h1b / 1e6, 2) as "Rev/H-1B ($M)"
        FROM employer_profiles ep
        WHERE ep.is_public = 1
        ORDER BY ep.total_lcas DESC
        LIMIT 100
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=400)
        csv = df.to_csv(index=False)
        st.download_button("Download Public Company Data", csv,
                           "public_company_h1b.csv", "text/csv")


def _geographic_tab():
    st.subheader("Geographic Analysis")

    conn = get_db()
    if not _table_exists(conn, "lca_applications"):
        st.warning(_NO_DATA_MSG)
        return

    col1, col2 = st.columns(2)

    with col1:
        # LCAs by state
        df = query("""
            SELECT employer_state as state, COUNT(*) as count
            FROM lca_applications
            WHERE employer_state IS NOT NULL AND LENGTH(employer_state) = 2
            GROUP BY employer_state ORDER BY count DESC
        """)
        if not df.empty:
            fig = px.choropleth(df, locations="state", locationmode="USA-states",
                                color="count", scope="usa",
                                title="H-1B LCA Applications by Employer State",
                                color_continuous_scale="Blues",
                                labels={"count": "Applications"})
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Average wage by state
        df = query("""
            SELECT employer_state as state,
                   ROUND(AVG(annualized_wage), 0) as avg_wage,
                   COUNT(*) as count
            FROM lca_applications
            WHERE employer_state IS NOT NULL AND LENGTH(employer_state) = 2
                  AND annualized_wage > 0
            GROUP BY employer_state
            HAVING count >= 10
            ORDER BY avg_wage DESC
        """)
        if not df.empty:
            fig = px.choropleth(df, locations="state", locationmode="USA-states",
                                color="avg_wage", scope="usa",
                                title="Average H-1B Wage by State",
                                color_continuous_scale="Greens",
                                labels={"avg_wage": "Avg Wage ($)"})
            st.plotly_chart(fig, use_container_width=True)

    # Top states table
    df = query("""
        SELECT employer_state as state, COUNT(*) as lcas,
               COUNT(DISTINCT employer_name) as employers,
               ROUND(AVG(annualized_wage), 0) as avg_wage,
               ROUND(AVG(wage_ratio), 3) as avg_wage_ratio
        FROM lca_applications
        WHERE employer_state IS NOT NULL AND LENGTH(employer_state) = 2
              AND annualized_wage > 0
        GROUP BY employer_state
        ORDER BY lcas DESC LIMIT 20
    """)
    if not df.empty:
        st.subheader("Top 20 States by LCA Volume")
        st.dataframe(df, use_container_width=True)


def _uscis_tab():
    st.subheader("USCIS Petition Outcomes")

    conn = get_db()
    if not _table_exists(conn, "employer_profiles") and not _table_exists(conn, "uscis_employers"):
        st.warning(_NO_DATA_MSG)
        return

    col1, col2 = st.columns(2)

    with col1:
        # Approval rate distribution
        df = query("""
            SELECT CASE
                WHEN approval_rate >= 95 THEN '95-100%'
                WHEN approval_rate >= 90 THEN '90-95%'
                WHEN approval_rate >= 80 THEN '80-90%'
                WHEN approval_rate >= 70 THEN '70-80%'
                WHEN approval_rate >= 50 THEN '50-70%'
                ELSE '<50%'
            END as rate_band, COUNT(*) as count
            FROM employer_profiles
            WHERE approval_rate IS NOT NULL
            GROUP BY rate_band
        """)
        if not df.empty:
            order = ["<50%", "50-70%", "70-80%", "80-90%", "90-95%", "95-100%"]
            df["rate_band"] = pd.Categorical(df["rate_band"], categories=order, ordered=True)
            df = df.sort_values("rate_band")
            fig = px.bar(df, x="rate_band", y="count",
                         title="USCIS Approval Rate Distribution",
                         labels={"count": "Employers", "rate_band": "Approval Rate"})
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Approval vs denial totals by year
        df = query("""
            SELECT fiscal_year,
                   SUM(total_approvals) as approvals,
                   SUM(total_denials) as denials
            FROM uscis_employers
            WHERE fiscal_year IS NOT NULL
            GROUP BY fiscal_year ORDER BY fiscal_year
        """)
        if not df.empty:
            fig = px.bar(df, x="fiscal_year", y=["approvals", "denials"],
                         title="USCIS Approvals vs Denials by Year",
                         barmode="group",
                         labels={"fiscal_year": "Fiscal Year", "value": "Count"})
            st.plotly_chart(fig, use_container_width=True)

    # Lowest approval rate employers
    st.subheader("Lowest Approval Rate Employers (100+ LCAs)")
    df = query("""
        SELECT employer_name, total_lcas, uscis_approvals, uscis_denials,
               approval_rate, ROUND(compliance_score, 3) as compliance_score
        FROM employer_profiles
        WHERE approval_rate IS NOT NULL AND total_lcas >= 100
        ORDER BY approval_rate ASC
        LIMIT 25
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def _debarments_tab():
    st.subheader("Debarred & Willful Violator Employers")

    conn = get_db()
    if not _table_exists(conn, "debarments"):
        st.warning(_NO_DATA_MSG)
        return

    df = query("""
        SELECT employer_name, employer_city, employer_state, program,
               debar_start_date, debar_end_date, violation_type, source
        FROM debarments ORDER BY debar_start_date DESC
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)

    # Show debarred employers that had LCA activity
    st.subheader("Debarred Employers with H-1B Filing Activity")
    df = query("""
        SELECT ep.employer_name, ep.total_lcas, ep.total_workers,
               ROUND(ep.avg_wage, 0) as avg_wage,
               ROUND(ep.compliance_score, 3) as compliance_score,
               ep.fiscal_years, ep.states_active
        FROM employer_profiles ep
        WHERE ep.is_debarred = 1
        ORDER BY ep.total_lcas DESC
    """)
    if not df.empty:
        st.warning(f"Found {len(df)} debarred employers with LCA activity")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No debarred employers found with matching LCA filings.")


def _explorer_tab():
    st.subheader("Data Explorer")

    conn = get_db()
    ALLOWED_TABLES = ["employer_profiles", "lca_applications", "uscis_employers",
                       "debarments", "cross_links", "public_companies",
                       "company_financials", "employer_sec_links", "naics_codes"]
    # Filter to only tables that actually exist
    available_tables = [t for t in ALLOWED_TABLES if _table_exists(conn, t)]
    if not available_tables:
        st.warning(_NO_DATA_MSG)
        return

    table = st.selectbox("Table", available_tables)

    # Validate table name against whitelist
    if table not in ALLOWED_TABLES:
        st.error("Invalid table selection")
        return

    count = _safe_fetchone(conn, f"SELECT COUNT(*) FROM {table}")
    st.write(f"**{count:,}** total records")

    search = st.text_input("Filter (employer name contains):", key="explorer_search")

    limit = st.slider("Rows to display", 10, 500, 100)

    if search:
        if table == "employer_profiles":
            df = query(f"SELECT * FROM {table} WHERE employer_name LIKE ? LIMIT ?",
                       (f"%{search}%", limit))
        elif table in ("lca_applications", "uscis_employers"):
            df = query(f"SELECT * FROM {table} WHERE employer_name LIKE ? LIMIT ?",
                       (f"%{search}%", limit))
        else:
            df = query(f"SELECT * FROM {table} LIMIT ?", (limit,))
    else:
        df = query(f"SELECT * FROM {table} LIMIT ?", (limit,))

    if not df.empty:
        st.dataframe(df, use_container_width=True, height=500)
        csv = df.to_csv(index=False)
        st.download_button(f"Download {table}.csv", csv, f"{table}.csv", "text/csv")


if __name__ == "__main__":
    main()
