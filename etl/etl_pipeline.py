"""
Marketing ROI – ETL Pipeline
=============================
Loads raw CSVs + JSON, cleans / deduplicates / standardizes,
and writes three curated fact tables:

  1. fact_sessions.csv      – one row per session
  2. fact_campaign_daily.csv – one row per (date, campaign_id)
  3. fact_channel_daily.csv  – one row per (date, channel)

Run:
    python3 etl_pipeline.py          # writes to ./output/
    python3 etl_pipeline.py --outdir /tmp/curated

Decisions & assumptions are documented inline with "DECISION:" comments.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

# ---------------------------------------------------------------------------
# 0.  Paths & CLI
# ---------------------------------------------------------------------------
RAW_DIR = Path(__file__).resolve().parent

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Marketing ROI ETL pipeline")
    ap.add_argument(
        "--outdir",
        type=Path,
        default=RAW_DIR / "output",
        help="Directory for curated CSVs (default: ./output/)",
    )
    return ap.parse_args()


# ===================================================================
# 1.  LOAD
# ===================================================================
def load_raw(raw: Path) -> dict[str, pd.DataFrame]:
    """Read every raw source into a dict of DataFrames."""
    log.info("Loading raw files from %s", raw)

    sessions = pd.read_csv(raw / "sessions.csv")
    orders = pd.read_csv(raw / "orders.csv")
    order_items = pd.read_csv(raw / "order_items.csv")
    ad_spend = pd.read_csv(raw / "ad_spend_daily.csv")
    users = pd.read_csv(raw / "users.csv")
    campaigns = pd.read_csv(raw / "campaigns.csv")

    with open(raw / "products.json") as f:
        products = pd.DataFrame(json.load(f))

    for name, df in [
        ("sessions", sessions),
        ("orders", orders),
        ("order_items", order_items),
        ("ad_spend", ad_spend),
        ("users", users),
        ("campaigns", campaigns),
        ("products", products),
    ]:
        log.info("  %-15s %s", name, df.shape)

    return dict(
        sessions=sessions,
        orders=orders,
        order_items=order_items,
        ad_spend=ad_spend,
        users=users,
        campaigns=campaigns,
        products=products,
    )


# ===================================================================
# 2.  CLEAN & STANDARDIZE
# ===================================================================

# -- 2a. Normalize channel casing ----------------------------------------
CHANNEL_MAP = {
    "search": "search",
    "paid_social": "paid_social",
    "email": "email",
    "referral": "referral",
    "organic": "organic",
}


def normalize_channel(series: pd.Series) -> pd.Series:
    """Map every casing variant (Search, SEARCH, …) to lowercase canonical."""
    return series.str.strip().str.lower().map(CHANNEL_MAP)


# -- 2b. Deduplication ---------------------------------------------------
def deduplicate(df: pd.DataFrame, key: str | list[str], label: str) -> pd.DataFrame:
    """
    Drop exact-duplicate rows on *key*, keeping the first occurrence.
    DECISION: keep first occurrence (earliest row order in the CSV).
    """
    n_before = len(df)
    df = df.drop_duplicates(subset=key, keep="first").reset_index(drop=True)
    n_dropped = n_before - len(df)
    if n_dropped:
        log.info("  Dedup %-20s  dropped %d rows", label, n_dropped)
    return df


# -- 2c. Handle missing spend / clicks / impressions ---------------------
def fill_media_nulls(ad: pd.DataFrame) -> pd.DataFrame:
    """
    DECISION: Missing spend → 0 (no money was spent).
    Missing clicks/impressions → 0 (conservative; avoids inflating KPIs).
    """
    for col in ("spend", "clicks", "impressions"):
        n_miss = ad[col].isna().sum()
        if n_miss:
            log.info("  Filling %d missing '%s' with 0", n_miss, col)
            ad[col] = ad[col].fillna(0)
    return ad


# -- 2d. Revenue outlier detection & capping -----------------------------
IQR_MULTIPLIER = 3.0  # DECISION: 3× IQR (generous) to avoid clipping legitimate large orders


def flag_and_cap_outliers(
    orders: pd.DataFrame,
    col: str = "net_amount",
) -> pd.DataFrame:
    """
    Flag revenue outliers with an `is_revenue_outlier` boolean column
    and cap them at the upper fence (Q3 + 3×IQR).
    DECISION: Use IQR method with 3× multiplier on net_amount.
    Capped values are stored in `net_amount_capped`; original is preserved.
    """
    q1 = orders[col].quantile(0.25)
    q3 = orders[col].quantile(0.75)
    iqr = q3 - q1
    upper_fence = q3 + IQR_MULTIPLIER * iqr

    orders["is_revenue_outlier"] = orders[col] > upper_fence
    orders["net_amount_capped"] = orders[col].clip(upper=upper_fence)

    n_outliers = orders["is_revenue_outlier"].sum()
    log.info(
        "  Revenue outliers (net_amount > %.0f): %d flagged & capped",
        upper_fence,
        n_outliers,
    )
    return orders


# -- 2e. Parse timestamps ------------------------------------------------
def parse_timestamps(data: dict[str, pd.DataFrame]) -> None:
    data["sessions"]["session_ts"] = pd.to_datetime(data["sessions"]["session_ts"])
    data["orders"]["order_ts"] = pd.to_datetime(data["orders"]["order_ts"])
    data["ad_spend"]["date"] = pd.to_datetime(data["ad_spend"]["date"])
    data["users"]["signup_date"] = pd.to_datetime(data["users"]["signup_date"])


def clean(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Run all cleaning steps; mutates the dict in place for convenience."""
    log.info("Cleaning & standardizing …")

    # Normalize channels
    data["sessions"]["channel"] = normalize_channel(data["sessions"]["channel"])
    data["ad_spend"]["channel"] = normalize_channel(data["ad_spend"]["channel"])
    data["campaigns"]["channel"] = normalize_channel(data["campaigns"]["channel"])

    # Dedup
    data["sessions"] = deduplicate(data["sessions"], "session_id", "sessions")
    data["orders"] = deduplicate(data["orders"], "order_id", "orders")
    data["ad_spend"] = deduplicate(data["ad_spend"], ["date", "campaign_id"], "ad_spend")

    # Fill missing media metrics
    data["ad_spend"] = fill_media_nulls(data["ad_spend"])

    # Outliers
    data["orders"] = flag_and_cap_outliers(data["orders"])

    # Timestamps
    parse_timestamps(data)

    return data


# ===================================================================
# 3.  BUILD FACT TABLES
# ===================================================================

# -- 3a. fact_sessions ---------------------------------------------------
def build_fact_sessions(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    One row per session.
    Attribution model: LAST-TOUCH (the campaign/channel on the session record).
    """
    log.info("Building fact_sessions …")
    sessions = data["sessions"].copy()
    orders = data["orders"]
    users = data["users"]

    # Join orders onto sessions (left join – most sessions have no order)
    order_cols = [
        "session_id",
        "order_id",
        "order_ts",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "net_amount_capped",
        "is_revenue_outlier",
    ]
    sessions = sessions.merge(orders[order_cols], on="session_id", how="left")

    # Purchase flag
    sessions["purchase_flag"] = sessions["order_id"].notna().astype(int)

    # Revenue fields → 0 when no purchase
    for col in ("gross_amount", "discount_amount", "net_amount", "net_amount_capped"):
        sessions[col] = sessions[col].fillna(0)
    sessions["is_revenue_outlier"] = sessions["is_revenue_outlier"].fillna(False).astype(bool)

    # Session-to-order time (seconds); NaN for non-purchase sessions
    sessions["session_to_order_sec"] = (
        (sessions["order_ts"] - sessions["session_ts"]).dt.total_seconds()
    )

    # is_new_user: session date == signup date
    # DECISION: A session is from a "new user" if the session falls on the
    # user's signup_date.  Anonymous sessions → False.
    sessions = sessions.merge(
        users[["user_id", "signup_date"]], on="user_id", how="left"
    )
    sessions["is_new_user"] = (
        sessions["session_ts"].dt.date == sessions["signup_date"].dt.date
    )
    sessions.loc[sessions["user_id"].isna(), "is_new_user"] = False

    # Session date (useful for downstream aggregations)
    sessions["session_date"] = sessions["session_ts"].dt.date

    # Final column selection & ordering
    fact = sessions[
        [
            "session_id",
            "user_id",
            "session_ts",
            "session_date",
            "device",
            "channel",
            "campaign_id",
            "is_new_user",
            "purchase_flag",
            "order_id",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "net_amount_capped",
            "is_revenue_outlier",
            "session_to_order_sec",
        ]
    ].copy()

    log.info("  fact_sessions: %s", fact.shape)
    return fact


# -- 3b. fact_campaign_daily ---------------------------------------------
def build_fact_campaign_daily(
    data: dict[str, pd.DataFrame],
    fact_sessions: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per (date, campaign_id).
    Attribution: last-touch via session → campaign_id.
    """
    log.info("Building fact_campaign_daily …")
    ad = data["ad_spend"][
        ["date", "campaign_id", "channel", "spend", "impressions", "clicks"]
    ].copy()
    ad["date"] = ad["date"].dt.date

    # Aggregate session-level attribution metrics per (date, campaign_id)
    sess = fact_sessions.copy()
    sess_agg = (
        sess.groupby(["session_date", "campaign_id"])
        .agg(
            attributed_sessions=("session_id", "count"),
            attributed_orders=("purchase_flag", "sum"),
            attributed_revenue=("net_amount_capped", "sum"),
        )
        .reset_index()
        .rename(columns={"session_date": "date"})
    )

    # Merge spend with attributed metrics (outer keeps days with spend but no
    # sessions AND days with sessions but no recorded spend)
    fact = ad.merge(sess_agg, on=["date", "campaign_id"], how="outer")

    # Fill NaN attribution metrics for spend-only rows
    for col in ("attributed_sessions", "attributed_orders", "attributed_revenue"):
        fact[col] = fact[col].fillna(0)
    for col in ("spend", "impressions", "clicks"):
        fact[col] = fact[col].fillna(0)

    # Resolve channel from campaigns table when missing after outer join
    camp_map = data["campaigns"].set_index("campaign_id")["channel"]
    fact["channel"] = fact["channel"].fillna(fact["campaign_id"].map(camp_map))

    # Derived KPIs  (guard against division by zero)
    fact["cpc"] = np.where(fact["clicks"] > 0, fact["spend"] / fact["clicks"], 0)
    fact["ctr"] = np.where(
        fact["impressions"] > 0, fact["clicks"] / fact["impressions"], 0
    )
    fact["cvr"] = np.where(
        fact["attributed_sessions"] > 0,
        fact["attributed_orders"] / fact["attributed_sessions"],
        0,
    )
    fact["roas"] = np.where(
        fact["spend"] > 0, fact["attributed_revenue"] / fact["spend"], 0
    )
    # CAC proxy = spend / orders (only when orders > 0)
    fact["cac_proxy"] = np.where(
        fact["attributed_orders"] > 0,
        fact["spend"] / fact["attributed_orders"],
        np.nan,
    )

    fact["date"] = pd.to_datetime(fact["date"])
    fact = fact.sort_values(["date", "campaign_id"]).reset_index(drop=True)

    log.info("  fact_campaign_daily: %s", fact.shape)
    return fact


# -- 3c. fact_channel_daily ----------------------------------------------
def build_fact_channel_daily(
    data: dict[str, pd.DataFrame],
    fact_campaign_daily: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per (date, channel).
    Includes control variables for regression / MMM modelling.
    """
    log.info("Building fact_channel_daily …")

    chan = (
        fact_campaign_daily.groupby(["date", "channel"])
        .agg(
            total_spend=("spend", "sum"),
            total_impressions=("impressions", "sum"),
            total_clicks=("clicks", "sum"),
            attributed_sessions=("attributed_sessions", "sum"),
            attributed_orders=("attributed_orders", "sum"),
            attributed_revenue=("attributed_revenue", "sum"),
        )
        .reset_index()
    )

    # --- Control variables ---

    # Day-of-week (0 = Monday … 6 = Sunday)
    chan["day_of_week"] = pd.to_datetime(chan["date"]).dt.dayofweek

    # promo_flag: take the max promo_flag from ad_spend for that date
    # DECISION: If *any* campaign ran a promo on a date, the whole date
    # is flagged as promo for the channel (conservative approach).
    ad = data["ad_spend"].copy()
    ad["date_dt"] = ad["date"].dt.date
    promo_by_date_channel = (
        ad.groupby(["date_dt", "channel"])["promo_flag"]
        .max()
        .reset_index()
        .rename(columns={"date_dt": "date"})
    )
    chan["date_key"] = pd.to_datetime(chan["date"]).dt.date
    chan = chan.merge(
        promo_by_date_channel,
        left_on=["date_key", "channel"],
        right_on=["date", "channel"],
        how="left",
        suffixes=("", "_promo"),
    )
    chan["promo_flag"] = chan["promo_flag"].fillna(0).astype(int)
    chan.drop(columns=["date_promo", "date_key"], inplace=True, errors="ignore")

    # Week index (weeks since earliest date – continuous trend feature)
    min_date = pd.to_datetime(chan["date"]).min()
    chan["week_index"] = ((pd.to_datetime(chan["date"]) - min_date).dt.days // 7).astype(int)

    chan = chan.sort_values(["date", "channel"]).reset_index(drop=True)

    log.info("  fact_channel_daily: %s", chan.shape)
    return chan


# ===================================================================
# 4.  WRITE
# ===================================================================
def write_outputs(
    outdir: Path,
    fact_sessions: pd.DataFrame,
    fact_campaign_daily: pd.DataFrame,
    fact_channel_daily: pd.DataFrame,
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    for name, df in [
        ("fact_sessions", fact_sessions),
        ("fact_campaign_daily", fact_campaign_daily),
        ("fact_channel_daily", fact_channel_daily),
    ]:
        path = outdir / f"{name}.csv"
        df.to_csv(path, index=False)
        log.info("Wrote %-28s  %s rows × %s cols", path.name, *df.shape)


# ===================================================================
# MAIN
# ===================================================================
def main() -> None:
    args = parse_args()

    # 1. Load
    data = load_raw(RAW_DIR)

    # 2. Clean
    data = clean(data)

    # 3. Build fact tables
    fs = build_fact_sessions(data)
    fcd = build_fact_campaign_daily(data, fs)
    fch = build_fact_channel_daily(data, fcd)

    # 4. Write
    write_outputs(args.outdir, fs, fcd, fch)

    log.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()
