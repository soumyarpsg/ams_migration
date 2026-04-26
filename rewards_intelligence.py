"""Rewards Intelligence integration — adapts the AMS Migration Report into the
schema expected by the embedded rewards dashboard, and injects it into the
HTML template at render time.

Field mapping (Python AMS Report → Rewards Dashboard JS field):
    store_code               → store_code
    store_name               → store_name
    customer_name            → customer_name
    city                     → city_name
    cluster                  → cluster_name
    region                   → region_name
    format                   → format_type
    shopper_behaviour        → shopper_behaviour
    enroll_month_label       → enroll_month   (Mmm-yy)
    bill_value               → current_bill_value
    mtd_cashback_earned      → cashback_earned_current_month
    mtd_redemption           → redemed_amount_current_month
    incremental_sales        → incremental_sales
    current_nob              → current_nob
    past_nob                 → past_six_months_average_nob
    past_ams                 → past_six_months_average_ams
    past_ams_slab            → past_six_months_ams_slab
    current_ams_slab         → current_ams_slab
    current_asp              → current_asp
    bill_slab                → bill_slab
    msr_number               → msr_number       (kept for explorer rows)

`customer_type` is derived live in JavaScript using the reporting month.

The reporting month sent to the template is the dashboard's "current period"
in Mmm-yy form (e.g. "Apr-26"). The dashboard then defines:
    Existing Customer = enrolled in any month BEFORE the reporting month
    New Customer      = enrolled in the reporting month
    Return Rate       = (Existing Customers who shopped) / (Existing Customers)
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Path to the embeddable HTML template (sits next to this module)
# ---------------------------------------------------------------------------
TEMPLATE_PATH = Path(__file__).parent / "rewards_template.html"


# ---------------------------------------------------------------------------
# Column rename map (AMS report internal name → rewards dashboard field)
# ---------------------------------------------------------------------------
RENAME_MAP = {
    "store_code": "store_code",
    "store_name": "store_name",
    "customer_name": "customer_name",
    "city": "city_name",
    "cluster": "cluster_name",
    "region": "region_name",
    "format": "format_type",
    "shopper_behaviour": "shopper_behaviour",
    "enroll_month_label": "enroll_month",
    "bill_value": "current_bill_value",
    "mtd_cashback_earned": "cashback_earned_current_month",
    "mtd_redemption": "redemed_amount_current_month",
    "incremental_sales": "incremental_sales",
    "current_nob": "current_nob",
    "past_nob": "past_six_months_average_nob",
    "past_ams": "past_six_months_average_ams",
    "past_ams_slab": "past_six_months_ams_slab",
    "current_ams_slab": "current_ams_slab",
    "current_asp": "current_asp",
    "bill_slab": "bill_slab",
    "msr_number": "msr_number",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _scrub_for_json(value: Any) -> Any:
    """Coerce values into JSON-safe form (None for NaN/Inf, str for unknowns)."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (np.floating,)):
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (int, str, bool)):
        return value
    if pd.isna(value):
        return None
    return str(value)


def _round_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Round large numeric columns to 2dp to keep the embedded JSON small."""
    money_cols = [
        "current_bill_value",
        "cashback_earned_current_month",
        "redemed_amount_current_month",
        "incremental_sales",
        "past_six_months_average_ams",
        "current_asp",
    ]
    nob_cols = ["current_nob", "past_six_months_average_nob"]
    for c in money_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    for c in nob_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def adapt_ams_to_rewards(ams_df: pd.DataFrame) -> pd.DataFrame:
    """Take the AMS report DataFrame (from `processing.build_ams_report`) and
    return a DataFrame whose column names match what the rewards dashboard JS
    expects.
    """
    if ams_df is None or ams_df.empty:
        return pd.DataFrame()

    df = ams_df.copy()

    # Keep only columns we know how to map
    keep = [c for c in RENAME_MAP if c in df.columns]
    df = df[keep].rename(columns={c: RENAME_MAP[c] for c in keep})

    # Drop rows without a store code — the dashboard's filter does the same
    if "store_code" in df.columns:
        df = df[df["store_code"].astype(str).str.strip() != ""]

    df = _round_numeric(df)
    return df.reset_index(drop=True)


def derive_reporting_month(ams_df: pd.DataFrame, fallback: str | None = None) -> str:
    """Pick the dashboard's "current month" — the latest enroll_month_label that
    actually appears in the data, falling back to the supplied label.

    Note: by the time this is called the column has already been renamed from
    ``enroll_month_label`` to ``enroll_month`` (Mmm-yy form, e.g. "Apr-26"),
    which is what the JS dashboard expects on the wire. We must therefore parse
    with an explicit ``%b-%y`` format — otherwise dateutil mis-reads "Apr-26"
    as April 26th of the current year.
    """
    if ams_df is None or ams_df.empty or "enroll_month" not in ams_df.columns:
        return fallback or ""
    months = pd.to_datetime(
        ams_df["enroll_month"].astype(str),
        format="%b-%y",
        errors="coerce",
    ).dropna()
    if months.empty:
        return fallback or ""
    latest = months.max()
    return latest.strftime("%b-%y")


def to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame → list of JSON-safe dicts. Faster than df.to_dict for large frames."""
    if df.empty:
        return []
    records: list[dict] = []
    cols = list(df.columns)
    # Avoid using df.itertuples for sub-second perf with ~100k rows; df.to_dict is fine.
    for raw in df.to_dict(orient="records"):
        records.append({k: _scrub_for_json(raw.get(k)) for k in cols})
    return records


def render_html(records: list[dict], reporting_month: str) -> str:
    """Inject the records (as JSON) and reporting month into the template."""
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"Rewards template not found at {TEMPLATE_PATH}. "
            "Run build_template.py to regenerate it."
        )

    template = TEMPLATE_PATH.read_text()
    # json.dumps with allow_nan=False would error on NaN/Inf, but we've scrubbed
    # those already in to_records.
    payload = json.dumps(records, ensure_ascii=False, separators=(",", ":"))
    rm = reporting_month or ""

    html = (
        template
        .replace("__INJECT_DATA__", payload)
        .replace("__INJECT_REPORTING_MONTH__", rm.replace('"', '\\"'))
    )
    return html


def build_for_streamlit(ams_df: pd.DataFrame, current_period_label: str | None = None) -> tuple[str, int, str]:
    """One-shot helper used by the Streamlit page.

    Returns (html_string, record_count, reporting_month).
    """
    adapted = adapt_ams_to_rewards(ams_df)
    if adapted.empty:
        return ("", 0, "")
    rm = derive_reporting_month(adapted, fallback=current_period_label)
    records = to_records(adapted)
    html = render_html(records, rm)
    return html, len(records), rm
