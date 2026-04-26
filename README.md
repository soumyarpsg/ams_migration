# Spencer's MSR Dashboard

A Streamlit dashboard for the Spencer's MyRewards loyalty programme — produces
the **AMS Migration Report**, a **Renewals Report**, and an interactive
overview, all backed by a SQLite database that persists data between sessions.

---

## Quick start

```bash
git clone <this-repo>          # or just unzip the folder
cd spencers_dashboard
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501 in your browser.

**Default admin login**

| Username | Password           |
|----------|--------------------|
| `admin`  | `spencers@2026`    |

After your first login, change the password under **Settings → Change admin password**.

---

## What the dashboard does

| Page | Audience | What you'll see |
|---|---|---|
| **Overview** | Everyone | KPI tiles for enrolled customers, MTD bill value, MTD/YTD cashback & redemption, plus charts for enrolment trend, store leaderboard, channel mix, bill slabs, past-AMS slabs, regional cashback, and incremental-vs-lost sales |
| **AMS Migration Report** | Everyone | The full customer-level table (38 columns) with filters on Region/Cluster/Format/Store/Enrolment Month/Shopper Behaviour, plus a CSV download |
| **Renewals** | Everyone | Store × month table of New Acquisitions, Renewals, Previously Registered, Renewal %, and Gold/Black/Platinum renewal counts |
| **Store Master** | Everyone (read-only); Admins can add/edit/delete | Mapping for store name, region, cluster, city, format |
| **Upload Data** | **Admin only** | File uploaders for Membership / Shopping / Redemption / Customer Trend, with a "process & rebuild" button and a danger-zone clear-all option |
| **Settings** | **Admin only** | Change admin password |

---

## Data model

The dashboard expects the four source files exactly as Spencer's MSR system exports them:

| File | Encoding | Format | Notes |
|---|---|---|---|
| Membership-*.csv | UTF-8 | Comma-separated | Headers may have stray spaces — handled automatically. Plan tier derived from cost: 500 → Gold, 750 → Black, 1000 → Platinum |
| ShoppingSummary-*.csv | UTF-8 | Comma-separated | Mobile, Total Bill Value, Eligible (Eligable) Bill Value, Month name, Year |
| Redemption-*.csv | UTF-8 | Comma-separated | Transaction Type ∈ { BALANCE CREDIT, REDEMPTION, REVERSAL } |
| Customer_Trend.csv | **UTF-16, tab-separated** | Two-row header | Row 1 = month dates (`01-Jan-25` etc.), Row 2 = metric (NOB / Sales / ABV / Qty / QPB). Numbers may be quoted with thousand separators (`"15,266"`) — handled automatically |

The **store lookup** (region, cluster, city, format) is **baked into the codebase** at `lookups.py` — you do not need to upload it. Admins can add/remove stores via the **Store Master** page; changes persist in SQLite.

Mobile numbers equal to `0`, `9999999999`, or anything that parses to ≤ 0 are dropped during ingest, per spec.

---

## Calculation rules implemented

These are the rules the dashboard applies — all driven by `processing.py` and `renewal.py`. The "current period" is the latest month for which any source file has data; this is auto-detected on every report build.

### Past-window KPIs (`Past AMS`, `Past Qty`, `Past NOB`)
For a customer enrolled in month *M*, the window is the **6 months immediately before M**. Example: enrolled Jul-25 → window is Jan-25 … Jun-25; enrolled Aug-25 → Feb-25 … Jul-25. The mean is taken using pandas `mean()`, which ignores blanks, so a customer with only 5 months of trend data divides by 5.

### `Post Loyalty AMS`
Mean of monthly sales from the enrolment month to the current month inclusive (blanks ignored).

### `Current NOB / Current Qty`
Pulled directly from the Customer Trend file for the current month.

### `Bill Value / Eligible Bill Value`
Sum of `Total Bill Value` / `Eligible Bill Value` from Shopping Summary for the current month.

### `Shopper Behaviour`
`Shopped` if Bill Value > 0, else `Not Shopped`.

### `MTD Cashback Earned` (computed from Eligible Bill Value)
| Eligible Bill Value | Cashback |
|---|---|
| ≤ 3,300 | ₹0 |
| 3,301 – 4,000 | ₹100 (flat) |
| 4,001 – 5,000 | 4% of bill |
| 5,001 – 10,000 | 6% of bill, capped at ₹600 |
| > 10,000 | ₹600 |

### `MTD Redemption`
Sum of `Transaction Amt` for `REDEMPTION` rows in the current month, sign-flipped to positive.

### `YTD Cashback Earned` / `YTD Redemption`
Same logic over the window **Jul-25 → current month** (loyalty programme launch).

### `Incremental Sales` / `Lost Sales`
`Bill Value − Past AMS` — credited to **Incremental** if Shopped, **Lost** if Not Shopped.

### `Incremental NOB` / `Lost NOB`
`Current NOB − Past NOB` — same Shopped/Not-Shopped split.

### Bill slabs
`<=25K`, `>25K_<50K`, `>50K_<75K`, `>75K_<1L`, `>1L`.

### AMS slabs (for `Past AMS Slab` and `Current AMS Slab`)
14 buckets from `0 to 500` up to `15000 & Above`, exactly per the spec.

### Renewal rules
For each mobile, sort enrollments by start date. The first record is a **New Acquisition**. Subsequent records are **Renewals** if the new `start_date` is *after* the previous record's `end_date`; otherwise they're treated as overlap and ignored. Tier of the renewal is read from the new record's plan cost.

`Previously Registered` for a (store, month) = number of memberships whose `end_date` falls in that store/month — i.e. eligible-to-renew. `Renewal % = Renewals / Previously Registered × 100`.

### Date formatting
All dates stored in the database use `YYYY-MM-DD` for sortability. The user-facing report shows `Start Date` and `End Date` in **dd-mm-yyyy** and `Month of Enrollment` in **mmm-yy** (e.g. `Jul-25`), as requested.

---

## Architecture

```
spencers_dashboard/
├── app.py            # Streamlit UI — sidebar nav + 6 pages
├── auth.py           # Session helpers (login / is_admin / logout)
├── config.py         # Brand colours, slab definitions, default admin creds
├── db.py             # SQLite schema + helpers (upsert, replace_table, etc.)
├── ingest.py         # 4 file parsers
├── processing.py     # AMS Migration Report builder
├── renewal.py        # Renewal & New-Acquisition report builder
├── lookups.py        # Embedded 92-store master (from LOOKUPS.xlsx)
├── requirements.txt
└── data/
    └── spencers.db   # Created on first run
```

### Tables in `data/spencers.db`

- `membership`, `shopping`, `redemption`, `customer_trend` — raw uploads (replaced wholesale on each upload)
- `customer_trend` is stored in **long format**: `(mobile_no, month_start, nob, sales, abv, qty, qpb)`. This means the file can grow indefinitely as new months arrive — adding more months simply appends more rows.
- `ams_report_cache`, `renewal_cache` — pre-computed reports, rebuilt every time you click *Process uploaded files & rebuild reports*
- `stores` — editable store master (seeded from `lookups.DEFAULT_STORES` on first run)
- `admins` — admin users (SHA-256 password hashes)
- `meta` — small key/value store (`current_period`, `report_built_at`, etc.)

### How re-uploads work

Each table is **replaced wholesale** when its corresponding file is uploaded — there's no merge logic. Upload only the files that have changed. The reports are rebuilt at the end of each upload session and cached to SQLite, so the Overview / AMS / Renewals pages render instantly thereafter.

If you don't upload a particular file, that table keeps the rows from the last upload — useful when, for example, only `Customer_Trend.csv` has updated.

---

## Deployment to Streamlit Community Cloud

1. Push this folder to a GitHub repo.
2. On https://share.streamlit.io, click **New app** and point it at `app.py`.
3. The default admin password (`spencers@2026`) is intended only for local use — change it via **Settings** the first time you log in to a deployed instance.
4. **Persistence note:** Streamlit Cloud's container filesystem is ephemeral. The SQLite file in `data/spencers.db` will reset whenever the app restarts. For permanent storage on Streamlit Cloud, mount a persistent volume or move to a hosted database (Postgres / Supabase / Neon). For an internal company server with a stable disk, the default works fine.

---

## Performance

On a modest laptop with the sample files (105K members, 437K shopping, 533K redemption, 861K trend rows):

| Step | Time |
|---|---|
| Parse Membership | ~3 s |
| Parse Shopping | ~5 s |
| Parse Redemption | ~6 s |
| Parse Customer Trend (UTF-16) | ~16 s |
| Build AMS Migration Report | ~25 s |
| Build Renewal Report | ~1 s |
| **Total upload-to-ready** | **~55 s** |

Subsequent dashboard renders use the SQLite cache and are sub-second.

---

## Troubleshooting

- **`database is locked`** — happens if you close the app mid-write. Delete `data/spencers.db-shm` and `data/spencers.db-wal` and re-run.
- **Customer Trend file fails to parse** — confirm the file is **UTF-16** and **tab-separated** (the parser auto-tries UTF-16 → UTF-16-LE → UTF-8 → Latin-1, but if your export uses something exotic let me know).
- **Forgot admin password** — delete `data/spencers.db` and re-run; the default admin will be re-seeded. (You'll need to re-upload data.)
- **A store appears in the Membership file but not in the Store Master** — admins can add it via **Store Master → Add / update store**. Until then, the AMS Migration Report will show the store code with blank Region / Cluster / City / Format.
