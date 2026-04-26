"""Renewal & New-Acquisition report.

Logic
-----
- Group all membership rows by mobile_no.
- Sort each group by start_date.
- The first record per mobile is a "new acquisition" in its enrollment month.
- For every subsequent record, if its `start_date` is **after** the previous
  record's `end_date`, treat it as a renewal in its enrollment month.
  (If start_date <= previous end_date it is treated as overlapping / re-issue,
  not a renewal.)
- "Previously Registered" for store/month = how many memberships expired in
  that store and that month — i.e. how many were eligible to renew.
- Renewal % = renewals / previously_registered  (per store-month).
"""
from __future__ import annotations

import pandas as pd
import numpy as np

import db


def build_renewal_report() -> pd.DataFrame:
    membership = db.fetch_df("SELECT * FROM membership")
    stores = db.fetch_df(
        "SELECT store_code, store_name, region, cluster, city, format FROM stores"
    )
    if membership.empty:
        return pd.DataFrame()

    membership = membership.copy()
    membership["start_dt"] = pd.to_datetime(membership["start_date"], errors="coerce")
    membership["end_dt"] = pd.to_datetime(membership["end_date"], errors="coerce")
    membership = membership.dropna(subset=["start_dt"])
    membership = membership.sort_values(["mobile_no", "start_dt"]).reset_index(drop=True)

    # Tag each row as: New | Renewal | Overlap
    membership["prev_end_dt"] = membership.groupby("mobile_no")["end_dt"].shift(1)
    membership["prev_seen"] = membership.groupby("mobile_no").cumcount()  # 0 = first

    is_first = membership["prev_seen"] == 0
    is_renewal = (~is_first) & (membership["start_dt"] > membership["prev_end_dt"])
    is_overlap = (~is_first) & (~is_renewal)

    membership["event_type"] = np.where(
        is_first, "New",
        np.where(is_renewal, "Renewal", "Overlap"),
    )

    # Activity month = enrollment month for new + renewal events
    membership["activity_month"] = pd.to_datetime(
        membership["enroll_month"], errors="coerce"
    )

    # New acquisitions per store/month
    new_df = membership[membership["event_type"] == "New"]
    new_agg = (
        new_df.groupby(["registered_store_code", "activity_month"])
        .size().reset_index(name="new_acquisitions")
    )

    # Renewals per store/month + tier breakdown
    ren_df = membership[membership["event_type"] == "Renewal"].copy()
    ren_df["plan_tier"] = ren_df["plan_tier"].fillna("Other")
    ren_total = (
        ren_df.groupby(["registered_store_code", "activity_month"])
        .size().reset_index(name="renewals")
    )
    ren_tier = (
        ren_df.groupby(["registered_store_code", "activity_month", "plan_tier"])
        .size().unstack(fill_value=0).reset_index()
    )
    for col in ("Gold", "Black", "Platinum"):
        if col not in ren_tier.columns:
            ren_tier[col] = 0
    ren_tier = ren_tier.rename(columns={
        "Gold": "gold_renewals",
        "Black": "black_renewals",
        "Platinum": "platinum_renewals",
    })

    # Previously Registered: memberships that EXPIRED in (store, month).
    # For renewals we want to compare against previous memberships at *any*
    # store, but the user asked for store-wise reporting, so we attribute the
    # expiry to the store on the prior membership.
    #
    # IMPORTANT: only count memberships whose end_date has actually passed by
    # the current data period. A plan that ends in July 2026 is NOT yet
    # expired in April 2026 — it shouldn't inflate the "previously registered"
    # count or distort the renewal denominator.
    membership["expiry_month"] = pd.to_datetime(
        membership["end_dt"].dt.to_period("M").dt.to_timestamp(), errors="coerce"
    )

    # Cutoff = end of the current data period month (or end of "today" if no
    # period set yet).
    current_period_str = db.get_meta("current_period", None)
    if current_period_str:
        cutoff = pd.to_datetime(current_period_str) + pd.offsets.MonthEnd(0)
    else:
        cutoff = pd.Timestamp.now().normalize() + pd.offsets.MonthEnd(0)

    prev_df = membership.dropna(subset=["expiry_month"]).copy()
    prev_df = prev_df[prev_df["end_dt"] <= cutoff]   # only past expirations
    prev_agg = (
        prev_df.groupby(["registered_store_code", "expiry_month"])
        .size().reset_index(name="previously_registered")
    )
    prev_agg = prev_agg.rename(columns={"expiry_month": "activity_month"})

    # Combine
    out = (
        new_agg.merge(ren_total,
                      on=["registered_store_code", "activity_month"], how="outer")
        .merge(prev_agg,
               on=["registered_store_code", "activity_month"], how="outer")
        .merge(ren_tier,
               on=["registered_store_code", "activity_month"], how="left")
    )
    for col in ("new_acquisitions", "renewals", "previously_registered",
                "gold_renewals", "black_renewals", "platinum_renewals"):
        if col not in out.columns:
            out[col] = 0
        out[col] = out[col].fillna(0).astype(int)

    out["renewal_pct"] = np.where(
        out["previously_registered"] > 0,
        (out["renewals"] / out["previously_registered"]) * 100.0,
        0.0,
    )
    out["renewal_pct"] = out["renewal_pct"].round(2)

    out["period"] = out["activity_month"].dt.strftime("%Y-%m-%d")
    out["period_label"] = out["activity_month"].dt.strftime("%b-%y")

    out = out.merge(stores, left_on="registered_store_code", right_on="store_code", how="left")
    out = out.rename(columns={"registered_store_code": "store_code_in"})
    out["store_code"] = out["store_code"].fillna(out["store_code_in"])
    out = out.drop(columns=["store_code_in", "activity_month"])

    out = out[[
        "store_code", "store_name", "region", "cluster", "city", "format",
        "period", "period_label",
        "new_acquisitions", "renewals", "previously_registered",
        "renewal_pct",
        "gold_renewals", "black_renewals", "platinum_renewals",
    ]].fillna({"store_name": "", "region": "", "cluster": "", "city": "", "format": ""})

    out = out.sort_values(["store_code", "period"]).reset_index(drop=True)

    db.replace_table("renewal_cache", out)
    return out


DISPLAY_RENEWAL_MAP = {
    "store_code": "Store Code",
    "store_name": "Store Name",
    "region": "Region",
    "cluster": "Cluster",
    "city": "City",
    "format": "Format",
    "period_label": "Month",
    "new_acquisitions": "New Acquisitions",
    "renewals": "Renewals",
    "previously_registered": "Previously Registered",
    "renewal_pct": "Renewal %",
    "gold_renewals": "Gold Renewals",
    "black_renewals": "Black Renewals",
    "platinum_renewals": "Platinum Renewals",
}


def format_renewal_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    show = df.copy()
    if "period" in show.columns:
        show = show.drop(columns=["period"])
    return show.rename(columns=DISPLAY_RENEWAL_MAP)
