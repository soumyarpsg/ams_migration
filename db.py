"""SQLite persistence layer.

Tables
------
membership        : raw membership rows
shopping          : raw shopping summary rows
redemption        : raw redemption rows
customer_trend    : long format trend data (mobile_no, month_start, nob, sales, abv, qty, qpb)
stores            : editable store master (admin can add / remove)
admins            : admin users (username, password_hash)
meta              : key/value metadata (last_upload timestamps, current_month etc.)
ams_report_cache  : cached AMS Migration Report rows
renewal_cache     : cached renewal report rows
"""
from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import DB_PATH, DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD
from lookups import DEFAULT_STORES


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


@contextmanager
def get_conn():
    conn = _conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _hash_pw(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS membership (
    plan_name TEXT,
    plan_cost REAL,
    plan_duration INTEGER,
    mobile_no TEXT,
    name TEXT,
    membership_code TEXT,
    merchant_code TEXT,
    start_date TEXT,
    end_date TEXT,
    registered_store_code TEXT,
    purchased_date TEXT,
    purchased_platform TEXT,
    plan_tier TEXT,
    enroll_month TEXT  -- YYYY-MM-01
);

CREATE INDEX IF NOT EXISTS ix_membership_mobile ON membership(mobile_no);
CREATE INDEX IF NOT EXISTS ix_membership_store  ON membership(registered_store_code);
CREATE INDEX IF NOT EXISTS ix_membership_month  ON membership(enroll_month);

CREATE TABLE IF NOT EXISTS shopping (
    mobile_no TEXT,
    total_bill_value REAL,
    eligible_bill_value REAL,
    month_name TEXT,
    year_num INTEGER,
    created_on TEXT,
    period TEXT  -- YYYY-MM-01
);

CREATE INDEX IF NOT EXISTS ix_shopping_mobile ON shopping(mobile_no);
CREATE INDEX IF NOT EXISTS ix_shopping_period ON shopping(period);

CREATE TABLE IF NOT EXISTS redemption (
    sub_ack_no TEXT,
    mobile_no TEXT,
    membership_code TEXT,
    sub_plan_id TEXT,
    transaction_type TEXT,
    transaction_amt REAL,
    transaction_store_code TEXT,
    bill_date TEXT,
    bill_no TEXT,
    till_no TEXT,
    bill_amt REAL,
    transaction_date TEXT,
    txn_period TEXT  -- YYYY-MM-01 derived from transaction_date
);

CREATE INDEX IF NOT EXISTS ix_redemption_mobile ON redemption(mobile_no);
CREATE INDEX IF NOT EXISTS ix_redemption_period ON redemption(txn_period);
CREATE INDEX IF NOT EXISTS ix_redemption_type   ON redemption(transaction_type);

CREATE TABLE IF NOT EXISTS customer_trend (
    mobile_no TEXT,
    month_start TEXT,  -- YYYY-MM-01
    nob REAL,
    sales REAL,
    abv REAL,
    qty REAL,
    qpb REAL,
    PRIMARY KEY (mobile_no, month_start)
);

CREATE INDEX IF NOT EXISTS ix_ct_mobile ON customer_trend(mobile_no);
CREATE INDEX IF NOT EXISTS ix_ct_month  ON customer_trend(month_start);

CREATE TABLE IF NOT EXISTS stores (
    store_code TEXT PRIMARY KEY,
    store_name TEXT,
    region TEXT,
    city TEXT,
    cluster TEXT,
    format TEXT
);

CREATE TABLE IF NOT EXISTS admins (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_on TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS ams_report_cache (
    store_code TEXT,
    store_name TEXT,
    msr_number TEXT,
    customer_name TEXT,
    enroll_month TEXT,
    enroll_month_label TEXT,
    channel TEXT,
    shopper_behaviour TEXT,
    bill_slab TEXT,
    region TEXT,
    cluster TEXT,
    city TEXT,
    format TEXT,
    bill_value REAL,
    eligible_bill_value REAL,
    past_ams REAL,
    past_qty REAL,
    current_qty REAL,
    current_asp REAL,
    past_asp REAL,
    current_qpb REAL,
    past_qpb REAL,
    post_loyalty_ams REAL,
    past_ams_slab TEXT,
    current_ams_slab TEXT,
    mtd_cashback_earned REAL,
    mtd_redemption REAL,
    ytd_cashback_earned REAL,
    ytd_redemption REAL,
    incremental_sales REAL,
    lost_sales REAL,
    past_nob REAL,
    current_nob REAL,
    incremental_nob REAL,
    lost_nob REAL,
    plan_tier TEXT,
    start_date TEXT,
    end_date TEXT
);

CREATE INDEX IF NOT EXISTS ix_ams_store  ON ams_report_cache(store_code);
CREATE INDEX IF NOT EXISTS ix_ams_month  ON ams_report_cache(enroll_month);
CREATE INDEX IF NOT EXISTS ix_ams_region ON ams_report_cache(region);

CREATE TABLE IF NOT EXISTS renewal_cache (
    store_code TEXT,
    store_name TEXT,
    region TEXT,
    cluster TEXT,
    city TEXT,
    format TEXT,
    period TEXT,             -- YYYY-MM-01 of the activity month
    period_label TEXT,
    new_acquisitions INTEGER,
    renewals INTEGER,
    previously_registered INTEGER,
    renewal_pct REAL,
    gold_renewals INTEGER,
    black_renewals INTEGER,
    platinum_renewals INTEGER
);

CREATE INDEX IF NOT EXISTS ix_renewal_store ON renewal_cache(store_code);
CREATE INDEX IF NOT EXISTS ix_renewal_month ON renewal_cache(period);
"""


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------
def init_db() -> None:
    """Create tables and seed default stores / admin user if missing."""
    with get_conn() as c:
        c.executescript(SCHEMA)

        # Seed admin
        cur = c.execute("SELECT COUNT(*) FROM admins")
        if cur.fetchone()[0] == 0:
            c.execute(
                "INSERT INTO admins (username, password_hash, created_on) VALUES (?, ?, ?)",
                (
                    DEFAULT_ADMIN_USERNAME,
                    _hash_pw(DEFAULT_ADMIN_PASSWORD),
                    datetime.utcnow().isoformat(timespec="seconds"),
                ),
            )

        # Seed stores
        cur = c.execute("SELECT COUNT(*) FROM stores")
        if cur.fetchone()[0] == 0:
            rows = [
                (s["Store Code"], s["Store Name"], s["Region"], s["City"], s["Cluster"], s["Format"])
                for s in DEFAULT_STORES
            ]
            c.executemany(
                "INSERT OR REPLACE INTO stores (store_code, store_name, region, city, cluster, format) VALUES (?,?,?,?,?,?)",
                rows,
            )


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def verify_admin(username: str, password: str) -> bool:
    with get_conn() as c:
        row = c.execute(
            "SELECT password_hash FROM admins WHERE username = ?", (username,)
        ).fetchone()
    return bool(row) and row[0] == _hash_pw(password)


def change_admin_password(username: str, new_password: str) -> None:
    with get_conn() as c:
        c.execute(
            "UPDATE admins SET password_hash = ? WHERE username = ?",
            (_hash_pw(new_password), username),
        )


# ---------------------------------------------------------------------------
# Stores CRUD
# ---------------------------------------------------------------------------
def list_stores() -> pd.DataFrame:
    with get_conn() as c:
        return pd.read_sql_query(
            "SELECT store_code AS 'Store Code', store_name AS 'Store Name', region AS Region, "
            "cluster AS Cluster, city AS City, format AS Format FROM stores ORDER BY store_code",
            c,
        )


def upsert_store(store_code: str, store_name: str, region: str, city: str, cluster: str, fmt: str) -> None:
    with get_conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO stores (store_code, store_name, region, city, cluster, format) "
            "VALUES (?,?,?,?,?,?)",
            (store_code.strip(), store_name.strip(), region.strip(), city.strip(), cluster.strip(), fmt.strip()),
        )


def delete_store(store_code: str) -> None:
    with get_conn() as c:
        c.execute("DELETE FROM stores WHERE store_code = ?", (store_code,))


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
def set_meta(key: str, value: str) -> None:
    with get_conn() as c:
        c.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def get_meta(key: str, default: str | None = None) -> str | None:
    with get_conn() as c:
        row = c.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


# ---------------------------------------------------------------------------
# Bulk replace helpers
# ---------------------------------------------------------------------------
def replace_table(table: str, df: pd.DataFrame, chunksize: int = 10000) -> int:
    """Replace all rows of `table` with the rows in `df` (column names must match)."""
    conn = _conn()
    try:
        conn.execute(f"DELETE FROM {table}")
        conn.commit()
        df.to_sql(table, conn, if_exists="append", index=False, chunksize=chunksize, method=None)
        conn.commit()
    finally:
        conn.close()
    return len(df)


def append_table(table: str, df: pd.DataFrame, chunksize: int = 10000) -> int:
    conn = _conn()
    try:
        df.to_sql(table, conn, if_exists="append", index=False, chunksize=chunksize)
        conn.commit()
    finally:
        conn.close()
    return len(df)


def has_data(table: str) -> bool:
    with get_conn() as c:
        try:
            row = c.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            return row is not None
        except sqlite3.OperationalError:
            return False


def row_count(table: str) -> int:
    with get_conn() as c:
        return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def fetch_df(query: str, params: Iterable | None = None) -> pd.DataFrame:
    with get_conn() as c:
        return pd.read_sql_query(query, c, params=params)


def clear_all_data() -> None:
    """Wipe every data table (keep admins, stores, meta)."""
    with get_conn() as c:
        for t in ("membership", "shopping", "redemption", "customer_trend",
                  "ams_report_cache", "renewal_cache"):
            c.execute(f"DELETE FROM {t}")
