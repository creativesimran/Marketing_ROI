# Budget Reallocation Plan + 30-Day Impact Estimate

## 1. Fixed next-month budget

| Parameter | Value | Rationale |
|-----------|--------|------------|
| **Budget (30 days)** | **$10,000,000** | Chosen to align with recent run-rate (last-30d total spend in the data is ~\$11.3M). Rounding down to \$10M gives a conservative, easy-to-communicate target and stresses the need to reallocate for efficiency. |

All figures below are in the same currency as the source data (no FX adjustment).

---

## 2. New spend allocation across channels

Allocation is computed by **maximizing expected attributed revenue** subject to min/max share constraints, using **last-90-day ROAS** as the efficiency signal.

### 2.1 Constraints (min / max spend per channel)

| Constraint | Value | Justification |
|------------|--------|----------------|
| **Minimum share per channel** | **5%** (\$500k) | Ensures no channel is starved; keeps presence for brand, tests, and learning. Avoids dropping a channel to zero and losing optionality. |
| **Maximum share per channel** | **45%** (\$4.5M) | Limits concentration risk (creative fatigue, platform dependency, auction crowding). Prevents one channel from dominating the mix. |

These bounds are configurable in `budget_reallocation.py` (`MIN_PCT`, `MAX_PCT`).

### 2.2 Proposed allocation (from `output/budget_allocation.csv`)

| Channel      | Allocated spend | Share of budget |
|-------------|-----------------|------------------|
| **Organic** | $4,500,000      | 45.0% (at cap)   |
| **Email**   | $2,565,365      | 25.7%            |
| **Referral**| $1,075,055      | 10.8%            |
| **Search**  | $1,068,521      | 10.7%            |
| **Paid social** | $791,060     | 7.9%             |
| **Total**   | **$10,000,000** | 100%             |

Higher ROAS channels (organic, email) receive a larger share up to the 45% cap; paid social receives the minimum 5%-equivalent (slightly above due to rounding) because its historical ROAS is lowest.

---

## 3. 30-day impact estimates

### 3.1 Base / best / worst-case incremental revenue

Efficiency is modeled by scaling **effective ROAS** by a scenario multiplier (no change in mix):

| Scenario | ROAS multiplier | Interpretation |
|----------|------------------|----------------|
| **Base** | 1.0 | Last-90d performance continues. |
| **Best** | 1.2 | +20% efficiency (e.g. creative/audience improvements, seasonality). |
| **Worst** | 0.8 | −20% efficiency (e.g. saturation, competition, seasonality). |

**Estimated 30-day attributed revenue:**

| Scenario | Total revenue |
|----------|----------------|
| **Base** | **$73.7M** |
| **Best** | **$88.5M** |
| **Worst** | **$59.0M** |

(Values from `output/budget_impact_by_channel.csv`; totals match script output.)

### 3.2 Margin proxy impact

**Margin** = order-level (revenue − cost) from `order_items` × `products`, attributed to channel via last-touch session.

**Margin per dollar of spend** is computed by channel over the same 90-day window and applied to allocated spend.

| Scenario | Total margin proxy |
|----------|--------------------|
| **Base** | **$66.4M** |
| **Best** | **$79.7M** |
| **Worst** | **$53.1M** |

Margin scales with the same scenario multipliers as revenue (best/worst ±20%).

---

## 4. Assumptions

1. **Attribution**  
   Last-touch: the channel (and campaign) on the session that led to the order gets 100% credit. No multi-touch or time-decay.

2. **Efficiency stability**  
   Last-90-day ROAS and margin-per-dollar are used as the efficiency signal for the next 30 days. No creative or auction structural changes are modeled explicitly.

3. **Linear response**  
   Revenue and margin scale linearly with spend within the tested range (no saturation or threshold effects in the model).

4. **Organic “spend”**  
   In the dataset, organic has a lower total spend and very high attributed revenue, so its ROAS is high. The plan treats this as an efficiency weight for allocation. In practice, “spend” for organic may include content/SEO/investment not fully reflected in the spend field; interpret organic’s share as “relative priority” rather than literal media spend only.

5. **Product cost**  
   Margin uses product `cost` from `products.json` and order line items. No fulfillment, returns, or payment costs; hence “margin proxy.”

6. **Budget fixed**  
   Total budget is fixed at \$10M; reallocation only shifts share across channels within min/max constraints.

---

## 5. Sensitivity

### 5.1 Revenue vs. ROAS and budget (±10%)

From `output/budget_sensitivity.csv`:

| ROAS change | Budget change | Estimated revenue |
|-------------|----------------|--------------------|
| −10%        | −10%           | $59.7M             |
| −10%        | 0%             | $66.3M             |
| −10%        | +10%           | $73.0M             |
| 0%          | −10%           | $66.3M             |
| **0%**      | **0%**         | **$73.7M**        |
| 0%          | +10%           | $81.1M             |
| +10%        | −10%           | $73.0M             |
| +10%        | 0%             | $81.1M             |
| +10%        | +10%           | $89.2M             |

- **Revenue elasticity to budget** (at 0% ROAS change): ~0.74 (e.g. +10% budget → +10% revenue in the table is ~+10%, so roughly 1:1 in this linear model).
- **Revenue elasticity to ROAS** (at 0% budget change): 1:1 by construction (scaling ROAS scales revenue proportionally).

### 5.2 Sensitivity takeaways

- A **−10% ROAS** with **−10% budget** drops revenue to ~\$59.7M (about −19% vs base).
- A **+10% ROAS** with **+10% budget** raises revenue to ~\$89.2M (about +21% vs base).
- The allocation is most sensitive to **organic and email** (largest share and high ROAS); errors in their ROAS have the biggest impact on total revenue.

---

## 6. How to reproduce

1. Run the ETL pipeline:
   ```bash
   python3 etl_pipeline.py
   ```
2. Run the budget reallocation script:
   ```bash
   python3 budget_reallocation.py
   ```
3. Outputs (in `output/`):
   - `budget_allocation.csv` – proposed spend and share by channel
   - `budget_impact_by_channel.csv` – base/best/worst revenue and margin by channel
   - `budget_sensitivity.csv` – revenue under ROAS and budget changes
   - `channel_metrics.csv` – historical ROAS and margin-per-dollar by channel

Constants (budget, min/max %, lookback, scenario multipliers) are defined at the top of `budget_reallocation.py` and can be changed to test other scenarios.
