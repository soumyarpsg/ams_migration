"""File-ingestion helpers.

Each `parse_*` function takes a path-like (or file-buffer) and returns a clean
DataFrame ready to load into SQLite via `db.replace_table`.
"""
from __future__ import annotations

import io
import re
from typing import Iterable

import numpy as np
import pandas as pd

from config import INVALID_MOBILES, PLAN_TIER

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
_MONTH_NAME_TO_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _clean_mobile_series(s: pd.Series) -> pd.Series:
    """Normalise a mobile column to clean string digits."""
    out = s.astype(str).str.strip()
    # strip trailing .0 from float-ised ints
    out = out.str.replace(r"\.0$", "", regex=True)
    # remove anything non-numeric
    out = out.str.replace(r"\D", "", regex=True)
    return out


def _is_valid_mobile(s: pd.Series) -> pd.Series:
    """True where mobile is a valid 10-digit-ish number not in the deny list."""
    digits = s.fillna("").astype(str)
    valid = (digits.str.len() >= 10) & (~digits.isin(INVALID_MOBILES))
    # Anything that parses to <= 0 is bad
    nums = pd.to_numeric(digits, errors="coerce")
    valid &= nums.fillna(0) > 0
    return valid


def _to_period_str(d: pd.Series) -> pd.Series:
    """Series of pandas datetimes -> 'YYYY-MM-01' strings."""
    return d.dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-%d")


def _parse_date(s: pd.Series, fmts: Iterable[str] = ()) -> pd.Series:
    """Parse dates robustly.

    Order of attempts:
      1. ISO 8601 (e.g. ``2025-07-01`` or ``2025-07-01T18:25``).
      2. Explicit Indian dd-mm-yyyy formats with optional time component
         (e.g. ``01-07-2025`` or ``01-07-2025 18:25``). Trying these first
         avoids ambiguity for values like ``01-07-2025`` which dateutil
         would otherwise mis-read as 7-Jan when ``dayfirst`` is wrong.
      3. Fallback to dateutil with ``dayfirst=True`` so any remaining
         day-month-year variants (``01/07/2025``, ``1-7-2025``, etc.) are
         interpreted as day-first, matching the source CSVs.
    """
    out = pd.to_datetime(s, errors="coerce", format="ISO8601")

    explicit_formats = (
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    )
    for fmt in explicit_formats:
        mask = out.isna()
        if not mask.any():
            break
        attempt = pd.to_datetime(s.where(mask), errors="coerce", format=fmt)
        out = out.fillna(attempt)

    mask = out.isna()
    if mask.any():
        out2 = pd.to_datetime(s.where(mask), errors="coerce", dayfirst=True)
        out = out.fillna(out2)
    return out


def _strip_quoted_number(s: pd.Series) -> pd.Series:
    """Remove quotes, commas and surrounding spaces, then to numeric."""
    cleaned = s.astype(str).str.replace(r'[",\s]', "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


# ---------------------------------------------------------------------------
# 1. Membership
# ---------------------------------------------------------------------------
def parse_membership(file) -> pd.DataFrame:
    df = pd.read_csv(file, dtype=str, keep_default_na=False, na_values=[""])
    df.columns = [re.sub(r"\s+", " ", c.strip()) for c in df.columns]

    rename = {
        "Plan Name": "plan_name",
        "Plan Cost": "plan_cost",
        "Plan Duration": "plan_duration",
        "Mobile No": "mobile_no",
        "Name": "name",
        "Membership Code": "membership_code",
        "Merchant Code": "merchant_code",
        "Start Date": "start_date",
        "End Date": "end_date",
        "Registered Store Code": "registered_store_code",
        "Purchased Date": "purchased_date",
        "Purchased Platform": "purchased_platform",
    }
    df = df.rename(columns=rename)

    # Trim string fields
    for c in ("plan_name", "name", "membership_code", "merchant_code",
              "registered_store_code", "purchased_platform"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()

    df["mobile_no"] = _clean_mobile_series(df["mobile_no"])
    df = df[_is_valid_mobile(df["mobile_no"])].copy()

    df["plan_cost"] = pd.to_numeric(df["plan_cost"], errors="coerce")
    df["plan_duration"] = pd.to_numeric(df["plan_duration"], errors="coerce").astype("Int64")
    df["plan_tier"] = df["plan_cost"].round().map(PLAN_TIER).fillna("Other")

    df["start_date_dt"] = _parse_date(df["start_date"])
    df["end_date_dt"] = _parse_date(df["end_date"])
    df["purchased_date_dt"] = _parse_date(df["purchased_date"])

    # Persist canonical YYYY-MM-DD strings for SQLite text storage
    df["start_date"] = df["start_date_dt"].dt.strftime("%Y-%m-%d")
    df["end_date"] = df["end_date_dt"].dt.strftime("%Y-%m-%d")
    df["purchased_date"] = df["purchased_date_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df["enroll_month"] = _to_period_str(df["start_date_dt"])

    keep = ["plan_name", "plan_cost", "plan_duration", "mobile_no", "name",
            "membership_code", "merchant_code", "start_date", "end_date",
            "registered_store_code", "purchased_date", "purchased_platform",
            "plan_tier", "enroll_month"]
    return df[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. Shopping summary
# ---------------------------------------------------------------------------
def parse_shopping(file) -> pd.DataFrame:
    df = pd.read_csv(file, dtype=str, keep_default_na=False, na_values=[""])
    df.columns = [re.sub(r"\s+", " ", c.strip()) for c in df.columns]

    rename = {
        "Mobile": "mobile_no",
        "Total Bill Value": "total_bill_value",
        "Eligable Bill Value": "eligible_bill_value",
        "Eligible Bill Value": "eligible_bill_value",
        "Month": "month_name",
        "Year": "year_num",
        "Created On": "created_on",
    }
    df = df.rename(columns=rename)

    df["mobile_no"] = _clean_mobile_series(df["mobile_no"])
    df = df[_is_valid_mobile(df["mobile_no"])].copy()

    df["total_bill_value"] = pd.to_numeric(df["total_bill_value"], errors="coerce")
    df["eligible_bill_value"] = pd.to_numeric(df["eligible_bill_value"], errors="coerce")

    df["month_name"] = df["month_name"].fillna("").astype(str).str.strip()
    df["year_num"] = pd.to_numeric(
        df["year_num"].astype(str).str.replace(r"\D", "", regex=True), errors="coerce"
    ).astype("Int64")

    month_num = df["month_name"].str.lower().map(_MONTH_NAME_TO_NUM)
    period = pd.to_datetime(
        {"year": df["year_num"], "month": month_num, "day": 1},
        errors="coerce",
    )
    df["period"] = period.dt.strftime("%Y-%m-%d")

    df["created_on"] = df["created_on"].fillna("").astype(str).str.strip()

    keep = ["mobile_no", "total_bill_value", "eligible_bill_value", "month_name",
            "year_num", "created_on", "period"]
    return df[keep].dropna(subset=["period"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Redemption
# ---------------------------------------------------------------------------
def parse_redemption(file) -> pd.DataFrame:
    df = pd.read_csv(file, dtype=str, keep_default_na=False, na_values=[""])
    df.columns = [re.sub(r"\s+", " ", c.strip()) for c in df.columns]

    rename = {
        "Sub ACK NO": "sub_ack_no",
        "Mobile No": "mobile_no",
        "Memebership Code": "membership_code",
        "Membership Code": "membership_code",
        "Sub Plan ID": "sub_plan_id",
        "Transaction Type": "transaction_type",
        "Transaction Amt": "transaction_amt",
        "Transaction Store Code": "transaction_store_code",
        "Bill Date": "bill_date",
        "Bill No": "bill_no",
        "Till No": "till_no",
        "Bill Amt": "bill_amt",
        "Transaction date": "transaction_date",
        "Transaction Date": "transaction_date",
    }
    df = df.rename(columns=rename)

    df["mobile_no"] = _clean_mobile_series(df["mobile_no"])
    df = df[_is_valid_mobile(df["mobile_no"])].copy()

    df["transaction_amt"] = pd.to_numeric(df["transaction_amt"], errors="coerce")
    df["bill_amt"] = pd.to_numeric(df["bill_amt"], errors="coerce")

    for c in ("transaction_type", "transaction_store_code", "sub_ack_no",
              "membership_code", "sub_plan_id", "bill_no", "till_no"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()

    txn_dt = _parse_date(df["transaction_date"])
    bill_dt = _parse_date(df["bill_date"])
    df["transaction_date"] = txn_dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    df["bill_date"] = bill_dt.dt.strftime("%Y-%m-%d")
    df["txn_period"] = _to_period_str(txn_dt)

    keep = ["sub_ack_no", "mobile_no", "membership_code", "sub_plan_id",
            "transaction_type", "transaction_amt", "transaction_store_code",
            "bill_date", "bill_no", "till_no", "bill_amt", "transaction_date",
            "txn_period"]
    return df[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Customer trend  (UTF-16 tab-separated, two-row header)
# ---------------------------------------------------------------------------
def _read_customer_trend_raw(file) -> pd.DataFrame:
    """Robust reader: tries utf-16 first, then utf-8 / latin1."""
    if hasattr(file, "read"):
        raw = file.read()
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        buf = io.BytesIO(raw)
        for enc in ("utf-16", "utf-16-le", "utf-8", "latin1"):
            try:
                buf.seek(0)
                return pd.read_csv(buf, sep="\t", header=None, dtype=str, encoding=enc)
            except (UnicodeError, UnicodeDecodeError):
                continue
        raise ValueError("Could not decode customer trend file with utf-16/utf-8/latin1.")
    else:
        for enc in ("utf-16", "utf-16-le", "utf-8", "latin1"):
            try:
                return pd.read_csv(file, sep="\t", header=None, dtype=str, encoding=enc)
            except (UnicodeError, UnicodeDecodeError):
                continue
        raise ValueError("Could not decode customer trend file.")


def parse_customer_trend(file) -> pd.DataFrame:
    """Reshape Customer_Trend.csv into long format.

    Source layout
    -------------
    Row 0: <blank>, 01-Jan-25, 01-Jan-25, 01-Jan-25, 01-Jan-25, 01-Jan-25, 01-Feb-25, ...
    Row 1: Phone Number, NOB, Sales, ABV, Qty, QPB, NOB, Sales, ABV, Qty, QPB, ...
    Row 2+: data

    Returns columns: mobile_no, month_start (YYYY-MM-01), nob, sales, abv, qty, qpb
    """
    raw = _read_customer_trend_raw(file)
    if len(raw) < 3:
        return pd.DataFrame(columns=["mobile_no", "month_start", "nob", "sales", "abv", "qty", "qpb"])

    header_dates = raw.iloc[0].fillna("").astype(str).str.strip().str.replace("\r", "", regex=False)
    header_metrics = raw.iloc[1].fillna("").astype(str).str.strip().str.lower().str.replace("\r", "", regex=False)
    body = raw.iloc[2:].reset_index(drop=True)

    # First column is phone number — confirm
    phone_col_idx = 0
    body.columns = list(range(body.shape[1]))

    mobiles = _clean_mobile_series(body[phone_col_idx])
    valid_mask = _is_valid_mobile(mobiles)
    body = body[valid_mask].copy()
    mobiles = mobiles[valid_mask].reset_index(drop=True)
    body = body.reset_index(drop=True)

    # Walk through each metric column and emit (mobile, month, metric, value)
    metric_aliases = {"nob": "nob", "sales": "sales", "abv": "abv",
                      "qty": "qty", "qpb": "qpb"}

    long_records = []
    for col in range(1, body.shape[1]):
        date_str = header_dates.iloc[col] if col < len(header_dates) else ""
        metric = header_metrics.iloc[col] if col < len(header_metrics) else ""
        metric = metric_aliases.get(metric)
        if not metric or not date_str:
            continue
        ts = pd.to_datetime(date_str, errors="coerce", dayfirst=True)
        if pd.isna(ts):
            continue
        month_start = ts.to_period("M").to_timestamp().strftime("%Y-%m-%d")
        values = _strip_quoted_number(body[col])
        long_records.append(
            pd.DataFrame({
                "mobile_no": mobiles,
                "month_start": month_start,
                "metric": metric,
                "value": values,
            })
        )

    if not long_records:
        return pd.DataFrame(columns=["mobile_no", "month_start", "nob", "sales", "abv", "qty", "qpb"])

    long_df = pd.concat(long_records, ignore_index=True)
    long_df = long_df.dropna(subset=["value"])

    pivoted = long_df.pivot_table(
        index=["mobile_no", "month_start"],
        columns="metric",
        values="value",
        aggfunc="sum",   # in case of duplicate mobiles in a single month
    ).reset_index()

    for c in ("nob", "sales", "abv", "qty", "qpb"):
        if c not in pivoted.columns:
            pivoted[c] = np.nan

    pivoted = pivoted[["mobile_no", "month_start", "nob", "sales", "abv", "qty", "qpb"]]
    return pivoted
