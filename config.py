"""Central configuration for the Spencer's MSR Dashboard."""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "spencers.db"

# Loyalty programme start (used for YTD calculations)
LOYALTY_START_YEAR = 2025
LOYALTY_START_MONTH = 7  # July 2025

# Default admin credentials (change after first login via the UI)
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "spencers@2026"

# Mobile numbers to ignore
INVALID_MOBILES = {"0", "9999999999"}

# Plan tier mapping by plan cost
PLAN_TIER = {500: "Gold", 750: "Black", 1000: "Platinum"}

# Bill slab definitions (lower bound exclusive, upper bound inclusive)
# (label, min_exclusive, max_inclusive)  -- min_exclusive=None means no lower bound
BILL_SLABS = [
    ("<=25K", None, 25000),
    (">25K_<50K", 25000, 50000),
    (">50K_<75K", 50000, 75000),
    (">75K_<1L", 75000, 100000),
    (">1L", 100000, None),
]

# AMS slab definitions
AMS_SLABS = [
    ("0 to 500", 0, 500),
    ("501 to 1000", 500, 1000),
    ("1001 to 1500", 1000, 1500),
    ("1501 to 2000", 1500, 2000),
    ("2001 to 2500", 2000, 2500),
    ("2501 to 3000", 2500, 3000),
    ("3001 to 3300", 3000, 3300),
    ("3301 to 4000", 3300, 4000),
    ("4001 to 5000", 4000, 5000),
    ("5001 to 7500", 5000, 7500),
    ("7501 to 10000", 7500, 10000),
    ("10001 to 12500", 10000, 12500),
    ("12501 to 15000", 12500, 15000),
    ("15000 & Above", 15000, None),
]

# Spencer's brand colours
BRAND_RED = "#E5202E"
BRAND_DARK = "#1F1F2E"
BRAND_LIGHT = "#F5F5F7"
