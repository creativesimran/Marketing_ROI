"""
Budget Reallocation Plan + 30-Day Impact Estimate
==================================================
Uses curated ETL outputs and product cost data to:
  - Compute historical ROAS and margin proxy by channel
  - Propose a constrained spend allocation for a fixed next-month budget
  - Estimate base / best / worst-case incremental revenue and margin

Run after ETL:  python3 etl_pipeline.py && python3 budget_reallocation.py
Outputs:        allocation table, impact summary, and inputs for BUDGET_REALLOCATION_PLAN.md
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG (documented in BUDGET_REALLOCATION_PLAN.md)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_DIR = BASE_DIR

# Fixed next-month budget (30 days). Chosen to match ~recent run-rate while rounding.
BUDGET_TOTAL = 10_000_000.0  # $10M

# Min/max spend per channel as share of total budget (justified in plan).
MIN_PCT = 0.05   # 5%  – maintain presence in every channel
MAX_PCT = 0.45   # 45% – avoid over-concentration in one channel

# Lookback for historical efficiency (days). Use 90 for stability.
LOOKBACK_DAYS = 90

# Scenario multipliers on effective ROAS (base=1.0, best=1.2, worst=0.8).
ROAS_BEST_MULTIPLIER = 1.2
ROAS_WORST_MULTIPLIER = 0.8


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load curated outputs and raw order_items + products for margin."""
    fch = pd.read_csv(OUTPUT_DIR / "fact_channel_daily.csv")
    fch["date"] = pd.to_datetime(fch["date"])

    fs = pd.read_csv(OUTPUT_DIR / "fact_sessions.csv")
    fs["session_ts"] = pd.to_datetime(fs["session_ts"])
    fs["session_date"] = pd.to_datetime(fs["session_date"])

    orders = pd.read_csv(DATA_DIR / "orders.csv")
    order_items = pd.read_csv(DATA_DIR / "order_items.csv")
    with open(DATA_DIR / "products.json") as f:
        products = pd.DataFrame(json.load(f))

    return fch, fs, orders, order_items, products


def attributed_margin_by_channel(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    products: pd.DataFrame,
    fact_sessions: pd.DataFrame,
) -> pd.Series:
    """
    Order-level margin = revenue (from order_items) - cost (from products).
    Attribute to channel via session_id -> fact_sessions (last-touch).
    """
    # Revenue and cost per order from order_items × products
    oi = order_items.merge(products[["product_id", "cost"]], on="product_id")
    oi["revenue"] = oi["quantity"] * oi["unit_price"]
    oi["cost"] = oi["quantity"] * oi["cost"]
    order_margin = oi.groupby("order_id").agg(revenue=("revenue", "sum"), cost=("cost", "sum")).assign(
        margin=lambda x: x["revenue"] - x["cost"]
    )["margin"]

    # Map order_id -> channel from sessions that have a purchase
    sess_with_order = fact_sessions.loc[fact_sessions["purchase_flag"] == 1, ["order_id", "channel"]].drop_duplicates("order_id")
    order_to_channel = sess_with_order.set_index("order_id")["channel"]

    margin_by_channel = order_margin.reindex(order_to_channel.index).groupby(order_to_channel).sum()
    return margin_by_channel


def channel_metrics(
    fch: pd.DataFrame,
    margin_by_channel: pd.Series,
    lookback_days: int,
) -> pd.DataFrame:
    """Aggregate spend, attributed revenue, ROAS, and margin proxy over lookback window."""
    cutoff = fch["date"].max() - pd.Timedelta(days=lookback_days)
    recent = fch.loc[fch["date"] >= cutoff]

    agg = recent.groupby("channel").agg(
        total_spend=("total_spend", "sum"),
        attributed_revenue=("attributed_revenue", "sum"),
        attributed_orders=("attributed_orders", "sum"),
    ).reset_index()

    agg["roas"] = np.where(agg["total_spend"] > 0, agg["attributed_revenue"] / agg["total_spend"], 0)
    agg["attributed_margin"] = agg["channel"].map(margin_by_channel).fillna(0)
    agg["margin_per_dollar"] = np.where(agg["total_spend"] > 0, agg["attributed_margin"] / agg["total_spend"], 0)

    return agg


def allocate_budget(
    metrics: pd.DataFrame,
    budget_total: float,
    min_pct: float,
    max_pct: float,
) -> pd.DataFrame:
    """
    Allocate budget across channels: favor higher ROAS subject to min/max share.
    Rule: assign min_spend to each, distribute remainder by ROAS, hard-cap at max_spend;
    if any hit cap, redistribute their overflow to others (by ROAS) until feasible.
    """
    channels = metrics["channel"].tolist()
    roas = metrics.set_index("channel")["roas"].reindex(channels).fillna(0).values
    n = len(channels)

    min_spend = budget_total * min_pct
    max_spend = budget_total * max_pct
    if n * min_spend > budget_total:
        min_spend = budget_total / n

    alloc = np.full(n, min_spend)
    remainder = budget_total - n * min_spend
    roas_sum = roas.sum()
    if roas_sum <= 0:
        alloc = np.ones(n) / n * budget_total
    else:
        # Distribute remainder by ROAS; cap at max_spend and redistribute overflow
        while remainder > 1e-6:
            can_take = np.maximum(0, max_spend - alloc)
            if can_take.sum() <= 0:
                break
            add = remainder * (roas * can_take) / (roas * can_take).sum()
            add = np.minimum(add, can_take)
            alloc += add
            remainder -= add.sum()
        alloc = np.clip(alloc, min_spend, max_spend)
        alloc = alloc / alloc.sum() * budget_total

    out = pd.DataFrame({"channel": channels, "allocated_spend": alloc})
    out["allocated_pct"] = out["allocated_spend"] / budget_total
    return out


def impact_estimates(
    allocation: pd.DataFrame,
    metrics: pd.DataFrame,
    roas_best_mult: float,
    roas_worst_mult: float,
) -> pd.DataFrame:
    """Base / best / worst incremental revenue and margin for the allocated spend."""
    df = allocation.merge(metrics[["channel", "roas", "margin_per_dollar"]], on="channel")
    df["base_revenue"] = df["allocated_spend"] * df["roas"]
    df["best_revenue"] = df["allocated_spend"] * df["roas"] * roas_best_mult
    df["worst_revenue"] = df["allocated_spend"] * df["roas"] * roas_worst_mult
    df["base_margin"] = df["allocated_spend"] * df["margin_per_dollar"]
    df["best_margin"] = df["allocated_spend"] * df["margin_per_dollar"] * roas_best_mult  # scale margin with revenue scenario
    df["worst_margin"] = df["allocated_spend"] * df["margin_per_dollar"] * roas_worst_mult
    return df


def run_sensitivity(
    allocation: pd.DataFrame,
    metrics: pd.DataFrame,
    budget_total: float,
) -> pd.DataFrame:
    """Sensitivity: total revenue when ROAS shifts ±10% and budget ±10%."""
    base_rev = (allocation["allocated_spend"] * allocation["channel"].map(metrics.set_index("channel")["roas"])).sum()
    rows = []
    for roas_pct in [-10, 0, 10]:
        for budget_pct in [-10, 0, 10]:
            mult = 1 + roas_pct / 100
            bud = budget_total * (1 + budget_pct / 100)
            spend_scale = bud / budget_total
            rev = (allocation["allocated_spend"] * spend_scale * allocation["channel"].map(metrics.set_index("channel")["roas"]) * mult).sum()
            rows.append({"roas_change_pct": roas_pct, "budget_change_pct": budget_pct, "estimated_revenue": rev})
    return pd.DataFrame(rows)


def main() -> None:
    print("Loading inputs…")
    fch, fs, orders, order_items, products = load_inputs()

    print("Computing attributed margin by channel (order_items × products → session channel)…")
    margin_by_channel = attributed_margin_by_channel(orders, order_items, products, fs)

    print("Channel metrics (last {} days)…".format(LOOKBACK_DAYS))
    metrics = channel_metrics(fch, margin_by_channel, LOOKBACK_DAYS)
    print(metrics.to_string(index=False))

    print("\nAllocating budget ${:,.0f} with min {:.0%} / max {:.0%} per channel…".format(BUDGET_TOTAL, MIN_PCT, MAX_PCT))
    allocation = allocate_budget(metrics, BUDGET_TOTAL, MIN_PCT, MAX_PCT)
    allocation = allocation.merge(metrics[["channel", "roas", "margin_per_dollar"]], on="channel")
    print(allocation[["channel", "allocated_spend", "allocated_pct", "roas"]].to_string(index=False))

    impact = impact_estimates(
        allocation[["channel", "allocated_spend", "allocated_pct"]],
        metrics,
        ROAS_BEST_MULTIPLIER,
        ROAS_WORST_MULTIPLIER,
    )
    print("\n--- 30-day impact (incremental revenue & margin proxy) ---")
    print(impact.to_string(index=False))

    tot_base = impact["base_revenue"].sum()
    tot_best = impact["best_revenue"].sum()
    tot_worst = impact["worst_revenue"].sum()
    tot_base_margin = impact["base_margin"].sum()
    tot_best_margin = impact["best_margin"].sum()
    tot_worst_margin = impact["worst_margin"].sum()
    print("\nTotal estimated revenue (30 days):  Base {:.0f}  |  Best {:.0f}  |  Worst {:.0f}".format(tot_base, tot_best, tot_worst))
    print("Total estimated margin proxy:      Base {:.0f}  |  Best {:.0f}  |  Worst {:.0f}".format(tot_base_margin, tot_best_margin, tot_worst_margin))

    sens = run_sensitivity(allocation[["channel", "allocated_spend"]], metrics, BUDGET_TOTAL)
    print("\nSensitivity (revenue vs ROAS and budget change):")
    print(sens.to_string(index=False))

    # Write outputs for reporting
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    allocation.round(2).to_csv(OUTPUT_DIR / "budget_allocation.csv", index=False)
    impact.round(2).to_csv(OUTPUT_DIR / "budget_impact_by_channel.csv", index=False)
    sens.round(2).to_csv(OUTPUT_DIR / "budget_sensitivity.csv", index=False)
    metrics.round(4).to_csv(OUTPUT_DIR / "channel_metrics.csv", index=False)
    print("\nWrote output/budget_allocation.csv, budget_impact_by_channel.csv, budget_sensitivity.csv, channel_metrics.csv")


if __name__ == "__main__":
    main()
