"""
OTU Wheel v2.0 — Ticker universe (40 tickers, segmented)

Reduced from v1's 78-ticker dragnet to 40 quality names, segmented by role:
    CORE_WHEEL   — mega/large caps we'd happily be assigned on (20)
    LEAP_CAND    — higher-vol names for leap candidates (10)
    WATCHLIST    — momentum / speculative watch (5)
    SAFE_HAVEN   — defensive, low vol (5)

ENTRY-CSP (Morning Brief): scans CORE_WHEEL + SAFE_HAVEN
ENTRY-LEAP (Trade Alerts): scans ALL 40
MANAGE: only tickers currently in positions table
"""

CORE_WHEEL: tuple[str, ...] = (
    "AMZN", "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD",
    "AVGO", "TSM",  "JPM",  "AXP",   "CAT", "GE",   "RTX",
    "COST", "WMT",  "UBER", "SHOP",  "PLTR","CRDO",
)

LEAP_CAND: tuple[str, ...] = (
    "TSLA", "HOOD", "MU", "LRCX", "AMAT",
    "APH",  "CCJ",  "VRT","ANET", "DELL",
)

WATCHLIST: tuple[str, ...] = (
    "COIN", "MSTR", "SMCI", "IREN", "CIFR",
)

SAFE_HAVEN: tuple[str, ...] = (
    "LMT", "MCD", "PG", "XOM", "T",
)

ALL_40: tuple[str, ...]    = CORE_WHEEL + LEAP_CAND + WATCHLIST + SAFE_HAVEN
CSP_SCAN: tuple[str, ...]  = CORE_WHEEL + SAFE_HAVEN
LEAP_SCAN: tuple[str, ...] = ALL_40


assert len(ALL_40) == 40, f"Universe must be 40, got {len(ALL_40)}"
