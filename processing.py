"""AMS Migration Report computation.

Produces the wide table requested by the analyst: for every enrolled customer,
join Membership ↔ Shopping ↔ Customer Trend ↔ Redemption with the lookup, and
derive past / current / post-loyalty KPIs and slabs.
"""
from __future__ import annotations

from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd

from config import AMS_SLABS, BILL_SLABS, LOYALTY_START_YEAR, LOYALTY_START_MONTH
import db


# ---------------------------------------------------------------------------
# Slab helpers
# ---------------------------------------------------------------------------
def _bill_slab(value: float) -> str:
    if pd.isna(value) or value <= 0:
        return "<=25K"
    for label, lo, hi in BILL_SLABS:
        lo_ok = lo is None or value > lo
        hi_ok = hi is None or value <= hi
        if lo_ok and hi_ok:
            return label
    return BILL_SLABS[-1][0]


def _ams_slab(value: float) -> str:
    if pd.isna(value) or value == 0:
        return "0 to 500"
    for label, lo, hi in AMS_SLABS:
        lo_ok = (lo is None) or value > lo
        hi_ok = (hi is None) or value <= hi
        # The first slab "0 to 500" should include 0 — special-case
        if label == "0 to 500" and value <= 500 and value >= 0:
            return label
        if lo_ok and hi_ok:
            return label
    return "No Data"


def _mtd_cashback(eligible_bill_value: float) -> float:
    if pd.isna(eligible_bill_value) or eligible_bill_value <= 0:
        return 0.0
    v = eligible_bill_value
    if 3301 <= v <= 4000:
        return 100.0
    if 4001 <= v <= 5000:
        return round(v * 0.04, 2)
    if 5001 <= v <= 10000:
        return min(round(v * 0.06, 2), 600.0)
    if v > 10000:
        return 600.0
    return 0.0


# ---------------------------------------------------------------------------
# Past-window helpers
# ---------------------------------------------------------------------------
def _past_window(enroll_period: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return (start, end) of the 6-month window strictly *before* enrollment."""
    end = enroll_period - pd.offsets.MonthBegin(1)        # month before enrol
    start = end - pd.offsets.MonthBegin(5)                 # 5 months before that
    return start, end


# ---------------------------------------------------------------------------
# Core build
# ---------------------------------------------------------------------------
def build_ams_report() -> pd.DataFrame:
    """Compute the full AMS Migration Report by joining the five data sources.

    Returns a DataFrame with the canonical column names listed in the spec, in
    the order requested. Stores it back in `ams_report_cache` for fast reads.
    """
    membership = db.fetch_df("SELECT * FROM membership")
    shopping = db.fetch_df("SELECT * FROM shopping")
    redemption = db.fetch_df("SELECT * FROM redemption")
    trend = db.fetch_df("SELECT * FROM customer_trend")
    stores = db.fetch_df(
        "SELECT store_code, store_name, region, cluster, city, format FROM stores"
    )

    if membership.empty:
        return pd.DataFrame()

    # ----- Determine the "current" period (latest data we have) ----------
    candidate_periods: list[pd.Timestamp] = []
    if not shopping.empty:
        candidate_periods.append(pd.to_datetime(shopping["period"]).max())
    if not trend.empty:
        candidate_periods.append(pd.to_datetime(trend["month_start"]).max())
    if not redemption.empty:
        candidate_periods.append(pd.to_datetime(redemption["txn_period"]).max())
    if not candidate_periods:
        return pd.DataFrame()
    current_period = max(candidate_periods)
    current_period_str = current_period.strftime("%Y-%m-%d")

    db.set_meta("current_period", current_period_str)
    db.set_meta(
        "current_period_label",
        current_period.strftime("%b-%y"),
    )
    db.set_meta(
        "report_built_at",
        datetime.utcnow().isoformat(timespec="seconds"),
    )

    # ----- Membership: dedupe to one record per (mobile, store) ---------
    # If a mobile was enrolled multiple times, keep the *latest* enrollment as
    # the "current" record (the renewal report covers the historical view).
    membership = membership.copy()
    membership["start_date_dt"] = pd.to_datetime(membership["start_date"], errors="coerce")
    membership["end_date_dt"] = pd.to_datetime(membership["end_date"], errors="coerce")
    membership["enroll_month_dt"] = pd.to_datetime(membership["enroll_month"], errors="coerce")

    membership = membership.sort_values(["mobile_no", "start_date_dt"])
    latest = membership.drop_duplicates(subset=["mobile_no"], keep="last").copy()

    # ----- Current-month shopping aggregated per mobile -----------------
    if not shopping.empty:
        cur_shop = shopping[shopping["period"] == current_period_str]
        cur_shop = (
            cur_shop.groupby("mobile_no", as_index=False)
            .agg(bill_value=("total_bill_value", "sum"),
                 eligible_bill_value=("eligible_bill_value", "sum"))
        )
    else:
        cur_shop = pd.DataFrame(columns=["mobile_no", "bill_value", "eligible_bill_value"])

    # ----- Trend pivots --------------------------------------------------
    if not trend.empty:
        trend = trend.copy()
        trend["month_start_dt"] = pd.to_datetime(trend["month_start"])
    else:
        trend = pd.DataFrame(columns=["mobile_no", "month_start", "month_start_dt",
                                      "nob", "sales", "abv", "qty", "qpb"])

    # Current month trend stats per mobile
    cur_trend = trend[trend["month_start"] == current_period_str]
    cur_trend = cur_trend[["mobile_no", "nob", "qty"]].rename(
        columns={"nob": "current_nob", "qty": "current_qty"}
    )

    # ----- Redemption aggregates ----------------------------------------
    redemption = redemption.copy()
    if not redemption.empty:
        redemption["txn_period_dt"] = pd.to_datetime(redemption["txn_period"], errors="coerce")
        loyalty_start = pd.Timestamp(year=LOYALTY_START_YEAR, month=LOYALTY_START_MONTH, day=1)

        ytd_mask = (redemption["txn_period_dt"] >= loyalty_start) & \
                   (redemption["txn_period_dt"] <= current_period)
        mtd_mask = redemption["txn_period_dt"] == current_period

        # YTD Cashback Earned = BALANCE CREDIT total
        ytd_cashback = (
            redemption.loc[ytd_mask & (redemption["transaction_type"] == "BALANCE CREDIT")]
            .groupby("mobile_no", as_index=False)["transaction_amt"].sum()
            .rename(columns={"transaction_amt": "ytd_cashback_earned"})
        )

        # YTD Redemption = REDEMPTION total (made positive)
        ytd_redemp = (
            redemption.loc[ytd_mask & (redemption["transaction_type"] == "REDEMPTION")]
            .groupby("mobile_no", as_index=False)["transaction_amt"].sum()
            .rename(columns={"transaction_amt": "ytd_redemption"})
        )
        ytd_redemp["ytd_redemption"] = ytd_redemp["ytd_redemption"].abs()

        # MTD Redemption (current month only, from REDEMPTION rows, made positive)
        mtd_redemp = (
            redemption.loc[mtd_mask & (redemption["transaction_type"] == "REDEMPTION")]
            .groupby("mobile_no", as_index=False)["transaction_amt"].sum()
            .rename(columns={"transaction_amt": "mtd_redemption"})
        )
        mtd_redemp["mtd_redemption"] = mtd_redemp["mtd_redemption"].abs()
    else:
        ytd_cashback = pd.DataFrame(columns=["mobile_no", "ytd_cashback_earned"])
        ytd_redemp = pd.DataFrame(columns=["mobile_no", "ytd_redemption"])
        mtd_redemp = pd.DataFrame(columns=["mobile_no", "mtd_redemption"])

    # ----- Past / Post-loyalty KPI calculation per mobile ---------------
    # We compute these in a vectorised way per enrollment month to avoid
    # walking 100K customers row-by-row.
    enroll_months = sorted(latest["enroll_month_dt"].dropna().unique())

    # Pre-bucket trend by mobile so we can slice fast
    if not trend.empty:
        trend_idx = trend.set_index(["mobile_no", "month_start_dt"]).sort_index()
    else:
        trend_idx = None

    past_records = []
    for em in enroll_months:
        em_ts = pd.Timestamp(em)
        win_start, win_end = _past_window(em_ts)
        post_start, post_end = em_ts, current_period

        members_em = latest.loc[latest["enroll_month_dt"] == em_ts, "mobile_no"]
        if members_em.empty or trend_idx is None:
            continue

        # Past window slice
        past_slice = trend.loc[
            (trend["month_start_dt"] >= win_start)
            & (trend["month_start_dt"] <= win_end)
            & (trend["mobile_no"].isin(members_em))
        ]
        # mean() over each metric ignores NaN — exactly what the user asked for
        past_g = (
            past_slice.groupby("mobile_no", as_index=False)
            .agg(past_ams=("sales", "mean"),
                 past_qty=("qty", "mean"),
                 past_nob=("nob", "mean"))
        )

        # Post-loyalty window slice (enrollment month → current month inclusive)
        post_slice = trend.loc[
            (trend["month_start_dt"] >= post_start)
            & (trend["month_start_dt"] <= post_end)
            & (trend["mobile_no"].isin(members_em))
        ]
        post_g = (
            post_slice.groupby("mobile_no", as_index=False)
            .agg(post_loyalty_ams=("sales", "mean"))
        )

        merged = past_g.merge(post_g, on="mobile_no", how="outer")
        past_records.append(merged)

    if past_records:
        past_df = pd.concat(past_records, ignore_index=True)
    else:
        past_df = pd.DataFrame(columns=["mobile_no", "past_ams", "past_qty", "past_nob",
                                        "post_loyalty_ams"])

    # ----- Stitch everything together -----------------------------------
    out = latest[[
        "mobile_no", "name", "registered_store_code", "purchased_platform",
        "enroll_month", "plan_tier", "start_date", "end_date",
    ]].rename(columns={
        "mobile_no": "msr_number",
        "name": "customer_name",
        "registered_store_code": "store_code",
        "purchased_platform": "channel",
    })

    out["enroll_month_dt"] = pd.to_datetime(out["enroll_month"], errors="coerce")
    out["enroll_month_label"] = out["enroll_month_dt"].dt.strftime("%b-%y")

    # Merge store master
    stores_ren = stores.rename(columns={
        "store_code": "store_code", "store_name": "store_name",
        "region": "region", "cluster": "cluster",
        "city": "city", "format": "format",
    })
    out = out.merge(stores_ren, on="store_code", how="left")

    # Merge current shopping
    out = out.merge(cur_shop, left_on="msr_number", right_on="mobile_no", how="left")
    out.drop(columns=["mobile_no"], inplace=True, errors="ignore")
    out["bill_value"] = out["bill_value"].fillna(0.0)
    out["eligible_bill_value"] = out["eligible_bill_value"].fillna(0.0)

    # Merge current trend (NOB / Qty)
    out = out.merge(cur_trend, left_on="msr_number", right_on="mobile_no", how="left")
    out.drop(columns=["mobile_no"], inplace=True, errors="ignore")
    out["current_nob"] = out["current_nob"].fillna(0.0)
    out["current_qty"] = out["current_qty"].fillna(0.0)

    # Merge past KPIs
    out = out.merge(past_df, left_on="msr_number", right_on="mobile_no", how="left")
    out.drop(columns=["mobile_no"], inplace=True, errors="ignore")
    for c in ("past_ams", "past_qty", "past_nob", "post_loyalty_ams"):
        if c in out.columns:
            out[c] = out[c].fillna(0.0)

    # Merge redemption KPIs
    for d in (ytd_cashback, ytd_redemp, mtd_redemp):
        out = out.merge(d, left_on="msr_number", right_on="mobile_no", how="left")
        out.drop(columns=["mobile_no"], inplace=True, errors="ignore")
    out["ytd_cashback_earned"] = out["ytd_cashback_earned"].fillna(0.0)
    out["ytd_redemption"] = out["ytd_redemption"].fillna(0.0)
    out["mtd_redemption"] = out["mtd_redemption"].fillna(0.0)

    # ----- Derived fields ----------------------------------------------
    out["shopper_behaviour"] = np.where(out["bill_value"] > 0, "Shopped", "Not Shopped")
    out["bill_slab"] = out["bill_value"].apply(_bill_slab)

    out["mtd_cashback_earned"] = out["eligible_bill_value"].apply(_mtd_cashback)

    # Ratio fields with safe division (avoid eager numerator/0 in np.where)
    def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
        n = pd.to_numeric(num, errors="coerce")
        d = pd.to_numeric(den, errors="coerce")
        d_safe = d.where(d > 0, np.nan)
        return n / d_safe

    out["current_asp"] = _safe_div(out["bill_value"], out["current_qty"]).fillna(0.0)
    out["past_asp"] = _safe_div(out["past_ams"], out["past_qty"]).fillna(0.0)
    out["current_qpb"] = _safe_div(out["current_qty"], out["current_nob"]).fillna(0.0)
    out["past_qpb"] = _safe_div(out["past_qty"], out["past_nob"]).fillna(0.0)

    # Past / current AMS slabs
    out["past_ams_slab"] = out["past_ams"].apply(_ams_slab)
    # Current AMS slab is based on current month bill value
    out["current_ams_slab"] = out["bill_value"].apply(_ams_slab)

    # Incremental / lost
    delta_sales = out["bill_value"] - out["past_ams"].fillna(0)
    out["incremental_sales"] = np.where(out["shopper_behaviour"] == "Shopped", delta_sales, 0.0)
    out["lost_sales"] = np.where(out["shopper_behaviour"] == "Not Shopped", delta_sales, 0.0)

    delta_nob = out["current_nob"] - out["past_nob"].fillna(0)
    out["incremental_nob"] = np.where(out["shopper_behaviour"] == "Shopped", delta_nob, 0.0)
    out["lost_nob"] = np.where(out["shopper_behaviour"] == "Not Shopped", delta_nob, 0.0)

    # Fill missing store-master fields with empty strings
    for c in ("store_name", "region", "cluster", "city", "format"):
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].fillna("")

    # ----- Order columns -----------------------------------------------
    out_cols = [
        "store_code", "store_name", "msr_number", "customer_name",
        "enroll_month", "enroll_month_label", "channel",
        "shopper_behaviour", "bill_slab", "region", "cluster", "city", "format",
        "bill_value", "eligible_bill_value",
        "past_ams", "past_qty", "current_qty",
        "current_asp", "past_asp", "current_qpb", "past_qpb",
        "post_loyalty_ams", "past_ams_slab", "current_ams_slab",
        "mtd_cashback_earned", "mtd_redemption",
        "ytd_cashback_earned", "ytd_redemption",
        "incremental_sales", "lost_sales",
        "past_nob", "current_nob",
        "incremental_nob", "lost_nob",
        "plan_tier", "start_date", "end_date",
    ]
    out = out[out_cols].copy()

    # ----- Persist cache -----------------------------------------------
    db.replace_table("ams_report_cache", out)

    return out


# ---------------------------------------------------------------------------
# Display formatting
# ---------------------------------------------------------------------------
DISPLAY_COLUMN_MAP = {
    "store_code": "Store Code",
    "store_name": "Store Name",
    "msr_number": "MSR Number",
    "customer_name": "Customer Name",
    "enroll_month_label": "Month of Enrollment",
    "channel": "Channel of Enrollment",
    "shopper_behaviour": "Shopper Behaviour",
    "bill_slab": "Bill Slab",
    "region": "Region",
    "cluster": "Cluster",
    "city": "City",
    "format": "Format",
    "bill_value": "Bill Value",
    "eligible_bill_value": "Eligible Bill Value",
    "past_ams": "Past AMS",
    "past_qty": "Past Qty",
    "current_qty": "Current Qty",
    "current_asp": "Current ASP",
    "past_asp": "Past ASP",
    "current_qpb": "Current QPB",
    "past_qpb": "Past QPB",
    "post_loyalty_ams": "Post Loyalty AMS",
    "past_ams_slab": "Past AMS Slab",
    "current_ams_slab": "Current AMS Slab",
    "mtd_cashback_earned": "MTD Cashback Earned",
    "mtd_redemption": "MTD Redemption",
    "ytd_cashback_earned": "YTD Cashback Earned",
    "ytd_redemption": "YTD Redemption",
    "incremental_sales": "Incremental Sales",
    "lost_sales": "Lost Sales",
    "past_nob": "Past NOB",
    "current_nob": "Current NOB",
    "incremental_nob": "Incremental NOB",
    "lost_nob": "Lost NOB",
    "plan_tier": "Plan Tier",
    "start_date": "Start Date",
    "end_date": "End Date",
}


def format_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Rename internal columns to user-facing names and format dates dd-mm-yyyy."""
    if df.empty:
        return df
    show = df.copy()
    # Drop the raw enroll_month YYYY-MM-01 column from the user view —
    # we surface enroll_month_label (mmm-yy) instead.
    if "enroll_month" in show.columns and "enroll_month_label" in show.columns:
        show = show.drop(columns=["enroll_month"])
    # dd-mm-yyyy for date columns
    for c in ("start_date", "end_date"):
        if c in show.columns:
            show[c] = pd.to_datetime(show[c], errors="coerce").dt.strftime("%d-%m-%Y")
    show = show.rename(columns=DISPLAY_COLUMN_MAP)
    return show
