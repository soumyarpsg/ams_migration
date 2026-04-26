"""Spencer's MSR Dashboard — Streamlit entry point.

Run with:
    streamlit run app.py

Default admin login (change after first login under Settings):
    username: admin
    password: spencers@2026
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

import auth
import db
import ingest
import processing
import renewal
import rewards_intelligence
from config import AMS_SLABS, BRAND_DARK, BRAND_LIGHT, BRAND_RED


# ---------------------------------------------------------------------------
# Page configuration & global styling
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Spencer's MSR Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = f"""
<style>
    /* Header bar */
    .spencer-header {{
        background: linear-gradient(90deg, {BRAND_RED} 0%, #B5152A 100%);
        padding: 1rem 1.5rem;
        border-radius: 8px;
        color: white;
        margin-bottom: 1.25rem;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
    }}
    .spencer-header h1 {{
        margin: 0;
        font-size: 1.5rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }}
    .spencer-header .sub {{
        opacity: 0.85;
        font-size: 0.9rem;
        margin-top: 0.25rem;
    }}

    /* KPI cards */
    .kpi-card {{
        background: white;
        border: 1px solid #E6E6E9;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        height: 100%;
    }}
    .kpi-card .label {{
        color: #6B6B7B;
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.4rem;
    }}
    .kpi-card .value {{
        color: {BRAND_DARK};
        font-size: 1.7rem;
        font-weight: 700;
        line-height: 1;
    }}
    .kpi-card .delta {{
        font-size: 0.85rem;
        color: #6B6B7B;
        margin-top: 0.3rem;
    }}
    .kpi-card.accent {{
        border-left: 4px solid {BRAND_RED};
    }}

    /* Tables */
    .dataframe tbody tr:hover {{ background-color: #FFF5F6 !important; }}

    /* Sidebar polish */
    section[data-testid="stSidebar"] {{
        background-color: {BRAND_LIGHT};
    }}

    /* Tighten default streamlit padding */
    .block-container {{
        padding-top: 1.25rem;
        padding-bottom: 2rem;
    }}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DB init at first import
# ---------------------------------------------------------------------------
db.init_db()


# ---------------------------------------------------------------------------
# Cached fetchers (cache invalidated when underlying data changes)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def cached_ams_report(version: str) -> pd.DataFrame:
    df = db.fetch_df("SELECT * FROM ams_report_cache")
    return df


@st.cache_data(show_spinner=False)
def cached_renewal_report(version: str) -> pd.DataFrame:
    return db.fetch_df("SELECT * FROM renewal_cache")


def report_version() -> str:
    """A cache-busting key based on last build time + row count."""
    return (
        f"{db.get_meta('report_built_at', 'never')}|"
        f"{db.row_count('ams_report_cache')}|"
        f"{db.row_count('renewal_cache')}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_int(n) -> str:
    if pd.isna(n):
        return "—"
    return f"{int(n):,}"


def fmt_inr(n, decimals: int = 0) -> str:
    if pd.isna(n):
        return "—"
    if decimals == 0:
        return f"₹{n:,.0f}"
    return f"₹{n:,.{decimals}f}"


def kpi_card(label: str, value: str, delta: str | None = None, accent: bool = False) -> str:
    accent_cls = " accent" if accent else ""
    delta_html = f'<div class="delta">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card{accent_cls}">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        {delta_html}
    </div>
    """


def header(title: str, subtitle: str = "") -> None:
    sub = f'<div class="sub">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f"""
        <div class="spencer-header">
            <h1>🛒 Spencer's MSR Dashboard — {title}</h1>
            {sub}
        </div>
        """,
        unsafe_allow_html=True,
    )


def has_data() -> bool:
    return db.has_data("membership")


def current_period_label() -> str:
    return db.get_meta("current_period_label", "—") or "—"


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Sidebar  (login + nav)
# ---------------------------------------------------------------------------
def render_sidebar() -> str:
    st.sidebar.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Spencer%27s_Retail_logo.svg/200px-Spencer%27s_Retail_logo.svg.png",
        width=140,
    )
    st.sidebar.markdown("### MSR Loyalty Analytics")
    st.sidebar.caption(f"Current data period: **{current_period_label()}**")

    st.sidebar.markdown("---")

    if auth.is_admin():
        st.sidebar.success(f"👤 **{auth.current_user()}** (Admin)")
        if st.sidebar.button("🚪 Log out", use_container_width=True):
            auth.logout()
            st.rerun()
    else:
        with st.sidebar.expander("🔐 Admin login", expanded=False):
            u = st.text_input("Username", key="login_user")
            p = st.text_input("Password", type="password", key="login_pass")
            if st.button("Sign in", use_container_width=True, key="login_btn"):
                if auth.login(u, p):
                    st.rerun()
                else:
                    st.error("Invalid credentials")

    st.sidebar.markdown("---")
    nav_items = ["📊 Overview", "📋 AMS Migration Report", "🔁 Renewals", "🎯 Rewards Intelligence", "🏬 Store Master"]
    if auth.is_admin():
        nav_items.append("⬆️ Upload Data")
        nav_items.append("⚙️ Settings")
    page = st.sidebar.radio("Navigate", nav_items, label_visibility="collapsed")

    st.sidebar.markdown("---")
    if has_data():
        meta_built = db.get_meta("report_built_at", "—")
        st.sidebar.caption(f"Report last built: {meta_built}")
        st.sidebar.caption(
            f"Members: {db.row_count('membership'):,} • "
            f"Trend: {db.row_count('customer_trend'):,}"
        )
    else:
        st.sidebar.warning("No data loaded yet.\nAsk an admin to upload files.")

    return page


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------
def page_overview():
    header(
        "Overview",
        f"Loyalty programme performance snapshot · current period {current_period_label()}",
    )

    if not has_data():
        st.info("No data has been uploaded yet. An admin can upload files via the **Upload Data** menu.")
        return

    df = cached_ams_report(report_version())
    if df.empty:
        st.warning("Report cache is empty. Trigger a rebuild from the Upload page.")
        return

    # ----- KPI row -------------------------------------------------------
    total_customers = len(df)
    shopped = (df["shopper_behaviour"] == "Shopped").sum()
    shopped_pct = (shopped / total_customers * 100) if total_customers else 0
    bill_value = df["bill_value"].sum()
    eligible = df["eligible_bill_value"].sum()
    mtd_cb = df["mtd_cashback_earned"].sum()
    mtd_re = df["mtd_redemption"].sum()
    ytd_cb = df["ytd_cashback_earned"].sum()
    ytd_re = df["ytd_redemption"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Enrolled Customers", fmt_int(total_customers), accent=True), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Shopped This Month", fmt_int(shopped), f"{shopped_pct:.1f}% of base"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("MTD Bill Value", fmt_inr(bill_value)), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("MTD Eligible Bill", fmt_inr(eligible)), unsafe_allow_html=True)

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.markdown(kpi_card("MTD Cashback Earned", fmt_inr(mtd_cb)), unsafe_allow_html=True)
    with c6:
        st.markdown(kpi_card("MTD Redemption", fmt_inr(mtd_re)), unsafe_allow_html=True)
    with c7:
        st.markdown(kpi_card("YTD Cashback Earned", fmt_inr(ytd_cb), accent=True), unsafe_allow_html=True)
    with c8:
        st.markdown(kpi_card("YTD Redemption", fmt_inr(ytd_re), accent=True), unsafe_allow_html=True)

    st.markdown("&nbsp;")

    # ----- Filters -------------------------------------------------------
    with st.expander("Filters", expanded=False):
        f1, f2, f3, f4 = st.columns(4)
        regions = sorted([r for r in df["region"].dropna().unique() if r])
        formats = sorted([f for f in df["format"].dropna().unique() if f])
        clusters = sorted([c for c in df["cluster"].dropna().unique() if c])
        tiers = sorted([t for t in df["plan_tier"].dropna().unique() if t])
        sel_regions = f1.multiselect("Region", regions, default=regions)
        sel_formats = f2.multiselect("Format", formats, default=formats)
        sel_clusters = f3.multiselect("Cluster", clusters, default=clusters)
        sel_tiers = f4.multiselect("Plan Tier", tiers, default=tiers)

    fdf = df[
        df["region"].isin(sel_regions)
        & df["format"].isin(sel_formats)
        & df["cluster"].isin(sel_clusters)
        & df["plan_tier"].isin(sel_tiers)
    ]
    if fdf.empty:
        st.warning("No customers match the selected filters.")
        return

    # ----- Charts row 1: enrollment + shopper behaviour ------------------
    g1, g2 = st.columns([2, 1])
    with g1:
        enroll_trend = (
            fdf.groupby("enroll_month_label")
            .size()
            .reset_index(name="customers")
        )
        # sort by actual date
        enroll_trend["sort_key"] = pd.to_datetime(enroll_trend["enroll_month_label"], format="%b-%y", errors="coerce")
        enroll_trend = enroll_trend.sort_values("sort_key")
        fig = px.bar(
            enroll_trend, x="enroll_month_label", y="customers",
            title="Enrolment trend by month",
            labels={"enroll_month_label": "Month", "customers": "Enrolments"},
        )
        fig.update_traces(marker_color=BRAND_RED)
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=320)
        st.plotly_chart(fig, use_container_width=True)

    with g2:
        beh = fdf["shopper_behaviour"].value_counts().reset_index()
        beh.columns = ["Behaviour", "Count"]
        fig = px.pie(
            beh, names="Behaviour", values="Count",
            title="Shopper behaviour",
            color="Behaviour",
            color_discrete_map={"Shopped": BRAND_RED, "Not Shopped": "#B5B5C2"},
            hole=0.45,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=320)
        st.plotly_chart(fig, use_container_width=True)

    # ----- Charts row 2: store leaderboard + channel ---------------------
    g3, g4 = st.columns([2, 1])
    with g3:
        store_lb = (
            fdf.groupby(["store_code", "store_name"])
            .agg(customers=("msr_number", "count"),
                 bill=("bill_value", "sum"))
            .reset_index()
            .sort_values("customers", ascending=False)
            .head(15)
        )
        store_lb["label"] = store_lb["store_code"] + " · " + store_lb["store_name"].fillna("")
        fig = px.bar(
            store_lb.sort_values("customers"),
            x="customers", y="label", orientation="h",
            title="Top 15 stores by enrolment",
            labels={"label": "Store", "customers": "Enrolments"},
            hover_data={"bill": ":,.0f"},
        )
        fig.update_traces(marker_color=BRAND_RED)
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=480)
        st.plotly_chart(fig, use_container_width=True)

    with g4:
        ch = fdf["channel"].fillna("Unknown").replace("", "Unknown").value_counts().reset_index()
        ch.columns = ["Channel", "Count"]
        fig = px.pie(
            ch, names="Channel", values="Count",
            title="Channel of enrolment",
            color_discrete_sequence=[BRAND_RED, "#1F1F2E", "#B5B5C2", "#FFA17A"],
            hole=0.45,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=480)
        st.plotly_chart(fig, use_container_width=True)

    # ----- Charts row 3: bill slabs + AMS slab migration ----------------
    g5, g6 = st.columns(2)
    with g5:
        bs_order = ["<=25K", ">25K_<50K", ">50K_<75K", ">75K_<1L", ">1L"]
        bs = fdf["bill_slab"].value_counts().reindex(bs_order, fill_value=0).reset_index()
        bs.columns = ["Slab", "Customers"]
        fig = px.bar(bs, x="Slab", y="Customers", title="Bill-value slab distribution")
        fig.update_traces(marker_color=BRAND_RED)
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=340)
        st.plotly_chart(fig, use_container_width=True)

    with g6:
        ams_order = [s[0] for s in AMS_SLABS] + ["No Data"]
        past_ams = fdf["past_ams_slab"].value_counts().reindex(ams_order, fill_value=0).reset_index()
        past_ams.columns = ["Slab", "Customers"]
        fig = px.bar(past_ams, x="Slab", y="Customers", title="Past AMS slab distribution")
        fig.update_traces(marker_color="#1F1F2E")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=340, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    # ----- Charts row 4: cashback by region + sales delta -------------
    g7, g8 = st.columns(2)
    with g7:
        reg = (
            fdf.groupby("region")
            .agg(mtd_cashback=("mtd_cashback_earned", "sum"),
                 mtd_redemption=("mtd_redemption", "sum"),
                 ytd_cashback=("ytd_cashback_earned", "sum"),
                 ytd_redemption=("ytd_redemption", "sum"))
            .reset_index()
        )
        reg_long = reg.melt(id_vars="region", var_name="Metric", value_name="Amount")
        fig = px.bar(reg_long, x="region", y="Amount", color="Metric",
                     barmode="group", title="Cashback & redemption by region",
                     color_discrete_sequence=[BRAND_RED, "#FFA17A", "#1F1F2E", "#B5B5C2"])
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
        st.plotly_chart(fig, use_container_width=True)

    with g8:
        delta = pd.DataFrame({
            "Metric": ["Incremental Sales", "Lost Sales"],
            "Value": [fdf["incremental_sales"].sum(), fdf["lost_sales"].sum()],
        })
        fig = px.bar(delta, x="Metric", y="Value",
                     title="Incremental vs Lost Sales (current month)",
                     color="Metric",
                     color_discrete_map={"Incremental Sales": BRAND_RED, "Lost Sales": "#B5B5C2"},
                     text_auto=".2s")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Page: AMS Migration Report (the big table)
# ---------------------------------------------------------------------------
def page_ams_report():
    header(
        "AMS Migration Report",
        "Customer-level KPI table · download as CSV for further analysis",
    )

    if not has_data():
        st.info("No data has been uploaded yet.")
        return

    df = cached_ams_report(report_version())
    if df.empty:
        st.warning("Report cache is empty.")
        return

    # ----- Filters -------------------------------------------------------
    with st.expander("Filters", expanded=True):
        f1, f2, f3 = st.columns(3)
        regions = sorted([r for r in df["region"].dropna().unique() if r])
        clusters = sorted([c for c in df["cluster"].dropna().unique() if c])
        formats = sorted([f for f in df["format"].dropna().unique() if f])
        sel_regions = f1.multiselect("Region", regions, default=regions)
        sel_clusters = f2.multiselect("Cluster", clusters, default=clusters)
        sel_formats = f3.multiselect("Format", formats, default=formats)

        f4, f5, f6 = st.columns(3)
        store_options = sorted(df["store_code"].dropna().unique().tolist())
        sel_stores = f4.multiselect("Store Code", store_options, default=[])

        months = (
            df.dropna(subset=["enroll_month"])
            .drop_duplicates("enroll_month")
            .sort_values("enroll_month")[["enroll_month", "enroll_month_label"]]
        )
        month_options = months["enroll_month_label"].tolist()
        sel_months = f5.multiselect("Enrolment Month", month_options, default=month_options)

        beh_options = ["Shopped", "Not Shopped"]
        sel_beh = f6.multiselect("Shopper Behaviour", beh_options, default=beh_options)

    fdf = df[
        df["region"].isin(sel_regions)
        & df["cluster"].isin(sel_clusters)
        & df["format"].isin(sel_formats)
        & df["enroll_month_label"].isin(sel_months)
        & df["shopper_behaviour"].isin(sel_beh)
    ]
    if sel_stores:
        fdf = fdf[fdf["store_code"].isin(sel_stores)]

    st.caption(f"Showing **{len(fdf):,}** of {len(df):,} rows.")

    # ----- Build display frame ------------------------------------------
    fdf = fdf.sort_values("start_date", ascending=True)
    show = processing.format_for_display(fdf)

    # Round numeric columns nicely
    rounding = {
        "Bill Value": 2, "Eligible Bill Value": 2,
        "Past AMS": 2, "Past Qty": 2, "Current Qty": 2,
        "Current ASP": 2, "Past ASP": 2, "Current QPB": 2, "Past QPB": 2,
        "Post Loyalty AMS": 2,
        "MTD Cashback Earned": 2, "MTD Redemption": 2,
        "YTD Cashback Earned": 2, "YTD Redemption": 2,
        "Incremental Sales": 2, "Lost Sales": 2,
        "Past NOB": 2, "Current NOB": 2,
        "Incremental NOB": 2, "Lost NOB": 2,
    }
    for col, dec in rounding.items():
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors="coerce").round(dec)

    st.dataframe(show, use_container_width=True, hide_index=True, height=560)

    # ----- Download ------------------------------------------------------
    st.download_button(
        "⬇️ Download filtered CSV",
        data=df_to_csv_bytes(show),
        file_name=f"ams_migration_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=False,
    )

    # ----- AMS Slab Transition Matrix (waterfall view) -------------------
    st.markdown("---")
    st.markdown("### AMS Slab Transition Matrix")
    st.caption(
        "How customers moved between the Past 6-month average AMS slab (rows) and "
        "the current month AMS slab (columns). "
        "🟦 Blue = stayed in the same slab · 🟩 Green = upgraded to a higher slab · "
        "🟧 Orange = downgraded to a lower slab."
    )

    slab_order = [s[0] for s in AMS_SLABS]   # 14 ordered slabs

    # Use the filtered frame so region/cluster/store filters apply here too.
    if "past_ams_slab" in fdf.columns and "current_ams_slab" in fdf.columns and not fdf.empty:
        pivot = pd.crosstab(
            fdf["past_ams_slab"].fillna("No Data"),
            fdf["current_ams_slab"].fillna("No Data"),
            margins=True,
            margins_name="Grand Total",
        )

        # Force consistent ordering — known slabs first, "No Data" last, Grand Total at end
        def _ordered_axis(values):
            extras = [v for v in values if v not in slab_order and v != "Grand Total"]
            ordered = [s for s in slab_order if s in values] + sorted(extras)
            if "Grand Total" in values:
                ordered.append("Grand Total")
            return ordered

        pivot = pivot.reindex(
            index=_ordered_axis(pivot.index.tolist()),
            columns=_ordered_axis(pivot.columns.tolist()),
            fill_value=0,
        )
        pivot.index.name = "Past 6M AMS Slab"
        pivot.columns.name = "Current AMS Slab"

        def _color_cells(df):
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            for r in df.index:
                for c in df.columns:
                    if r == "Grand Total" or c == "Grand Total":
                        styles.loc[r, c] = (
                            "background-color: #1F1F2E; color: white; font-weight: 700;"
                        )
                    elif r in slab_order and c in slab_order:
                        ri = slab_order.index(r)
                        ci = slab_order.index(c)
                        if ri == ci:
                            styles.loc[r, c] = (
                                "background-color: #4FC3F7; color: white; font-weight: 600;"
                            )
                        elif ci > ri:
                            styles.loc[r, c] = (
                                "background-color: #66BB6A; color: white;"
                            )
                        else:
                            styles.loc[r, c] = (
                                "background-color: #F5A623; color: white;"
                            )
                    else:
                        styles.loc[r, c] = "background-color: #ECEFF4;"
            return styles

        styled = pivot.style.apply(_color_cells, axis=None).format("{:,.0f}")
        st.dataframe(styled, use_container_width=True, height=560)

        # Quick movement summary (only known slabs, ignore Grand Total + No Data)
        body = pivot.drop(index=["Grand Total"], errors="ignore").drop(columns=["Grand Total"], errors="ignore")
        upgraded = downgraded = stayed = 0
        for r in body.index:
            for c in body.columns:
                if r in slab_order and c in slab_order:
                    val = int(body.loc[r, c])
                    ri = slab_order.index(r)
                    ci = slab_order.index(c)
                    if ri == ci:
                        stayed += val
                    elif ci > ri:
                        upgraded += val
                    else:
                        downgraded += val
        total = upgraded + downgraded + stayed
        if total:
            c1, c2, c3 = st.columns(3)
            c1.markdown(
                kpi_card("Upgraded", fmt_int(upgraded), f"{upgraded/total*100:.1f}% of base"),
                unsafe_allow_html=True,
            )
            c2.markdown(
                kpi_card("Stayed Same", fmt_int(stayed), f"{stayed/total*100:.1f}% of base"),
                unsafe_allow_html=True,
            )
            c3.markdown(
                kpi_card("Downgraded", fmt_int(downgraded), f"{downgraded/total*100:.1f}% of base"),
                unsafe_allow_html=True,
            )

        st.download_button(
            "⬇️ Download transition matrix CSV",
            data=df_to_csv_bytes(pivot.reset_index()),
            file_name=f"ams_slab_transition_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )


# ---------------------------------------------------------------------------
# Page: Renewals
# ---------------------------------------------------------------------------
def page_renewals():
    header(
        "Renewals & New Acquisition",
        "Store-wise · month-wise renewal performance",
    )

    if not has_data():
        st.info("No data has been uploaded yet.")
        return

    df = cached_renewal_report(report_version())
    if df.empty:
        st.warning("Renewal cache is empty.")
        return

    # KPI row
    total_new = int(df["new_acquisitions"].sum())
    total_ren = int(df["renewals"].sum())
    total_prev = int(df["previously_registered"].sum())
    overall_pct = (total_ren / total_prev * 100) if total_prev else 0.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("New Acquisitions (lifetime)", fmt_int(total_new), accent=True), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Total Renewals", fmt_int(total_ren)), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("Memberships Expired", fmt_int(total_prev)), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Overall Renewal %", f"{overall_pct:.1f}%"), unsafe_allow_html=True)

    if total_prev == 0:
        st.info("ℹ️ No memberships have expired yet, so renewal % is N/A. The first renewals will start appearing once 12-month plans purchased in July 2025 begin to expire.")

    st.markdown("&nbsp;")

    # Filters
    with st.expander("Filters", expanded=False):
        f1, f2 = st.columns(2)
        regions = sorted([r for r in df["region"].dropna().unique() if r])
        sel_regions = f1.multiselect("Region", regions, default=regions)
        store_opts = sorted(df["store_code"].dropna().unique().tolist())
        sel_stores = f2.multiselect("Store Code", store_opts, default=[])

    fdf = df[df["region"].isin(sel_regions)]
    if sel_stores:
        fdf = fdf[fdf["store_code"].isin(sel_stores)]
    if fdf.empty:
        st.warning("No rows match the selected filters.")
        return

    # Charts
    g1, g2 = st.columns(2)
    with g1:
        trend = (
            fdf.groupby(["period", "period_label"])
            .agg(new=("new_acquisitions", "sum"), renewals=("renewals", "sum"))
            .reset_index()
            .sort_values("period")
        )
        fig = go.Figure()
        fig.add_bar(x=trend["period_label"], y=trend["new"], name="New Acquisitions", marker_color=BRAND_RED)
        fig.add_bar(x=trend["period_label"], y=trend["renewals"], name="Renewals", marker_color="#1F1F2E")
        fig.update_layout(
            barmode="group", title="Acquisitions vs renewals by month",
            margin=dict(l=10, r=10, t=40, b=10), height=380,
        )
        st.plotly_chart(fig, use_container_width=True)

    with g2:
        tier = pd.DataFrame({
            "Tier": ["Gold", "Black", "Platinum"],
            "Renewals": [int(fdf["gold_renewals"].sum()),
                         int(fdf["black_renewals"].sum()),
                         int(fdf["platinum_renewals"].sum())],
        })
        fig = px.bar(tier, x="Tier", y="Renewals", title="Renewals by plan tier",
                     color="Tier",
                     color_discrete_map={"Gold": "#D4AF37", "Black": "#1F1F2E", "Platinum": "#A6AAB1"})
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Store · month renewal table")
    show = renewal.format_renewal_for_display(fdf)
    # Type-friendly rounding
    if "Renewal %" in show.columns:
        show["Renewal %"] = pd.to_numeric(show["Renewal %"], errors="coerce").round(2)
    st.dataframe(show, use_container_width=True, hide_index=True, height=520)

    st.download_button(
        "⬇️ Download renewals CSV",
        data=df_to_csv_bytes(show),
        file_name=f"renewals_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Page: Rewards Intelligence (embedded HTML dashboard fed by AMS report)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def cached_rewards_html(version: str) -> tuple[str, int, str]:
    """Build the embedded rewards dashboard once per data version."""
    df = db.fetch_df("SELECT * FROM ams_report_cache")
    if df.empty:
        return ("", 0, "")
    return rewards_intelligence.build_for_streamlit(
        df, current_period_label=current_period_label()
    )


def page_rewards_intelligence():
    header(
        "Rewards Intelligence",
        "Live charts, KPIs and analytics — auto-fed from the AMS Migration Report",
    )

    if not has_data():
        st.info("No data has been uploaded yet. An admin can upload files via the **Upload Data** menu.")
        return

    html, n_records, rm = cached_rewards_html(report_version())
    if not html or n_records == 0:
        st.warning("Report cache is empty. Trigger a rebuild from the Upload page.")
        return

    # Lightweight info strip above the embedded dashboard
    c1, c2, c3 = st.columns([1, 1, 4])
    with c1:
        st.markdown(kpi_card("Records Loaded", fmt_int(n_records), accent=True), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Reporting Month", rm or "—"), unsafe_allow_html=True)
    with c3:
        st.markdown(
            kpi_card(
                "Return Rate Definition",
                "Enrolled before the reporting month",
                delta="Shopped this month ÷ enrolled in any prior month",
            ),
            unsafe_allow_html=True,
        )

    st.markdown("&nbsp;")

    # Embed the dashboard. Tall iframe so all 8 tabs render comfortably.
    components.html(html, height=1400, scrolling=True)

    st.caption(
        "💡 The Rewards Intelligence dashboard above is auto-populated from the "
        "AMS Migration Report — no separate upload needed. Reupload data via the "
        "**Upload Data** menu and this view will refresh automatically."
    )


# ---------------------------------------------------------------------------
# Page: Store Master
# ---------------------------------------------------------------------------
def page_store_master():
    header("Store Master", "Mapping for store name, region, cluster, city, format")


    stores = db.list_stores()

    st.dataframe(stores, use_container_width=True, hide_index=True, height=500)
    st.caption(f"{len(stores)} stores in master.")

    if not auth.is_admin():
        st.info("🔒 Sign in as an admin to add or remove stores.")
        return

    st.markdown("---")
    st.markdown("### Admin actions")

    tab_add, tab_edit, tab_delete = st.tabs(["➕ Add / update store", "✏️ Edit existing", "🗑 Delete store"])

    with tab_add:
        with st.form("add_store_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            sc = c1.text_input("Store Code")
            sn = c2.text_input("Store Name")
            fmt = c3.selectbox("Format", ["Daily", "Super", "Hyper", "Other"])
            c4, c5, c6 = st.columns(3)
            region = c4.text_input("Region", value="East")
            cluster = c5.text_input("Cluster")
            city = c6.text_input("City")
            submit = st.form_submit_button("Save store", type="primary")
            if submit:
                if not sc.strip():
                    st.error("Store code is required.")
                else:
                    db.upsert_store(sc, sn, region, city, cluster, fmt)
                    st.success(f"Saved store **{sc}**.")
                    st.rerun()

    with tab_edit:
        codes = stores["Store Code"].tolist()
        if not codes:
            st.info("No stores to edit.")
        else:
            pick = st.selectbox("Pick a store to edit", codes, key="edit_pick")
            match = stores[stores["Store Code"] == pick]
            if match.empty:
                st.info("Pick a store from the list above.")
            else:
                row = match.iloc[0]
                with st.form(f"edit_form_{pick}"):
                    c1, c2, c3 = st.columns(3)
                    sn = c1.text_input("Store Name", value=row["Store Name"])
                    fmt = c2.text_input("Format", value=row["Format"])
                    region = c3.text_input("Region", value=row["Region"])
                    c4, c5 = st.columns(2)
                    cluster = c4.text_input("Cluster", value=row["Cluster"])
                    city = c5.text_input("City", value=row["City"])
                    submit = st.form_submit_button("Update", type="primary")
                    if submit:
                        db.upsert_store(pick, sn, region, city, cluster, fmt)
                        st.success(f"Updated **{pick}**.")
                        st.rerun()

    with tab_delete:
        codes = stores["Store Code"].tolist()
        if not codes:
            st.info("No stores to delete.")
        else:
            pick_d = st.selectbox("Pick a store to delete", codes, key="delete_pick")
            confirm = st.checkbox(f"I understand this will permanently remove **{pick_d}** from the master.",
                                  key="delete_confirm")
            if st.button("Delete store", type="primary", disabled=not confirm):
                db.delete_store(pick_d)
                st.success(f"Deleted store **{pick_d}**.")
                st.rerun()


# ---------------------------------------------------------------------------
# Page: Upload Data (admin only)
# ---------------------------------------------------------------------------
def page_upload():
    header("Upload Data", "Refresh the dashboard with the latest source files")

    if not auth.is_admin():
        st.error("🔒 Admin access required.")
        return

    st.markdown(
        """
        Upload one or more of the four source files. Anything you upload is loaded
        into SQLite and **persists** until you upload a newer version (or click
        *Clear all data* below).
        """
    )

    # ----- Status panel --------------------------------------------------
    st.markdown("### Current data status")
    status = pd.DataFrame([
        {"Table": "Membership", "Rows": db.row_count("membership")},
        {"Table": "Shopping Summary", "Rows": db.row_count("shopping")},
        {"Table": "Redemption", "Rows": db.row_count("redemption")},
        {"Table": "Customer Trend", "Rows": db.row_count("customer_trend")},
    ])
    st.dataframe(status, use_container_width=True, hide_index=True)
    st.caption(f"Last report build: {db.get_meta('report_built_at', '—')}  "
               f"·  Current period: **{current_period_label()}**")

    st.markdown("---")
    st.markdown("### Upload files")

    c1, c2 = st.columns(2)
    mem_file = c1.file_uploader("Membership CSV", type=["csv"], key="upload_mem")
    shop_file = c2.file_uploader("Shopping Summary CSV", type=["csv"], key="upload_shop")
    c3, c4 = st.columns(2)
    redm_file = c3.file_uploader("Redemption CSV", type=["csv"], key="upload_red")
    trend_file = c4.file_uploader("Customer Trend CSV (UTF-16, tab-separated)", type=["csv"], key="upload_trend")

    if st.button("🔄 Process uploaded files & rebuild reports", type="primary", use_container_width=True):
        load_steps = []
        progress = st.progress(0.0, text="Starting...")
        try:
            step_n = sum(1 for f in (mem_file, shop_file, redm_file, trend_file) if f is not None) + 2
            done = 0

            if mem_file is not None:
                progress.progress(done / step_n, text="Parsing membership...")
                m = ingest.parse_membership(mem_file)
                db.replace_table("membership", m)
                load_steps.append(f"Membership: {len(m):,} rows")
                done += 1
                progress.progress(done / step_n)

            if shop_file is not None:
                progress.progress(done / step_n, text="Parsing shopping summary...")
                s = ingest.parse_shopping(shop_file)
                db.replace_table("shopping", s)
                load_steps.append(f"Shopping: {len(s):,} rows")
                done += 1
                progress.progress(done / step_n)

            if redm_file is not None:
                progress.progress(done / step_n, text="Parsing redemption...")
                r = ingest.parse_redemption(redm_file)
                db.replace_table("redemption", r)
                load_steps.append(f"Redemption: {len(r):,} rows")
                done += 1
                progress.progress(done / step_n)

            if trend_file is not None:
                progress.progress(done / step_n, text="Parsing customer trend (this can take ~20s)...")
                t = ingest.parse_customer_trend(trend_file)
                db.replace_table("customer_trend", t)
                load_steps.append(f"Customer Trend: {len(t):,} rows")
                done += 1
                progress.progress(done / step_n)

            progress.progress(done / step_n, text="Building AMS Migration Report...")
            ams = processing.build_ams_report()
            done += 1
            progress.progress(done / step_n, text="Building Renewal Report...")
            ren = renewal.build_renewal_report()
            done += 1
            progress.progress(1.0, text="Done.")

            # Bust caches
            cached_ams_report.clear()
            cached_renewal_report.clear()
            cached_rewards_html.clear()

            if load_steps:
                st.success("✅ Loaded:\n\n- " + "\n- ".join(load_steps))
            st.success(f"✅ Reports rebuilt — {len(ams):,} customer rows, {len(ren):,} store-month renewal rows.")

        except Exception as e:
            progress.empty()
            st.error(f"❌ Processing failed: {e}")
            st.exception(e)

    st.markdown("---")
    st.markdown("### Danger zone")
    with st.expander("Clear all uploaded data"):
        confirm = st.checkbox("I understand this wipes all loaded data (stores & admins are kept).",
                              key="clear_confirm")
        if st.button("Clear all data", disabled=not confirm):
            db.clear_all_data()
            cached_ams_report.clear()
            cached_renewal_report.clear()
            cached_rewards_html.clear()
            st.success("All data cleared.")
            st.rerun()


# ---------------------------------------------------------------------------
# Page: Settings
# ---------------------------------------------------------------------------
def page_settings():
    header("Settings", "Admin account management")
    if not auth.is_admin():
        st.error("🔒 Admin access required.")
        return

    st.markdown("### Change admin password")
    with st.form("change_pw_form"):
        current = st.text_input("Current password", type="password")
        new1 = st.text_input("New password", type="password")
        new2 = st.text_input("Confirm new password", type="password")
        ok = st.form_submit_button("Update password", type="primary")
        if ok:
            if not db.verify_admin(auth.current_user(), current):
                st.error("Current password is incorrect.")
            elif new1 != new2 or len(new1) < 6:
                st.error("New passwords don't match or are shorter than 6 characters.")
            else:
                db.change_admin_password(auth.current_user(), new1)
                st.success("Password updated. Please sign in again.")
                auth.logout()
                st.rerun()


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
def main():
    page = render_sidebar()
    if page.endswith("Overview"):
        page_overview()
    elif page.endswith("AMS Migration Report"):
        page_ams_report()
    elif page.endswith("Renewals"):
        page_renewals()
    elif page.endswith("Rewards Intelligence"):
        page_rewards_intelligence()
    elif page.endswith("Store Master"):
        page_store_master()
    elif page.endswith("Upload Data"):
        page_upload()
    elif page.endswith("Settings"):
        page_settings()


if __name__ == "__main__":
    main()
