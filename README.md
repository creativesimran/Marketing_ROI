# Marketing ROI & Budget Reallocation (Attribution + Regression)

## 1. Business Background

You are a Business Analytics Associate at an e-commerce company. Leadership needs a decision-grade recommendation on how to allocate next month’s marketing budget across multiple channels (Search, Paid Social, Email, Referral, Organic) and campaigns.

### Goal
Provide a clear, data-driven answer to:

1. **Which channels/campaigns are actually driving revenue and orders?**
2. **How should we reallocate next month’s budget to maximize revenue (or a profit proxy)?**
3. **How confident are we in the recommendation?** (assumptions + sensitivity)

---

## 2. Data Inputs (Raw + Derived)

### Raw Data Sources
These are the files feeding the ETL process in `etl/etl_pipeline.py`:

- `sessions.csv` — session-level traffic data (channel, campaign, session_id, etc.)
- `orders.csv` — order-level sales data
- `order_items.csv` — line-item detail for orders
- `ad_spend_daily.csv` — daily spend by campaign/channel
- `users.csv` — customer profile data
- `campaigns.csv` — campaign metadata
- `products.json` — product catalog

### Analytics Layer (Generated)
The ETL produces cleaned, curated tables such as:

- `fact_sessions` — session + order attribution (last-touch)
- `campaign_daily` — campaign-level daily performance (spend, orders, revenue, ROAS)
- `fact_channel_daily` — channel-level daily performance summary

---

## 3. Approach (How We Answer the Questions)

### 3.1 Attribution: Which Channels/Campaigns Drive Revenue?

We apply **last-touch attribution** to map each order to the channel + campaign of the session that led to the purchase. This allows us to compute:

- **ROAS** (Return on Ad Spend)
- **CAC** (Customer Acquisition Cost)
- **CVR** (Conversion Rate)

Key outputs:
- Top-performing campaigns by revenue / ROAS
- Channels that deliver the most orders per dollar spent
- “Wasted spend” where high investment yields low or negative ROI

### 3.2 Regression: How Much Revenue Does Additional Spend Generate?

To make a budget reallocation recommendation, we estimate **marginal impact** of spend using regression modeling.

Typical steps:

1. Aggregate data to a consistent cadence (weekly or daily) by channel/campaign.
2. Fit a regression model where `revenue ~ spend + controls` (e.g., seasonality, promotions, day-of-week).
3. Extract **marginal ROAS** (revenue per additional dollar spent) by channel.

This provides an estimate of **which channels still have “room to grow”** and which are likely saturated.

### 3.3 Budget Reallocation Recommendation

Using the regression-derived marginal returns, we generate a recommended budget plan that:

- Keeps total spend fixed (or within a target range)
- Shifts dollars toward channels/campaigns with higher marginal ROAS
- Respects practical constraints (minimum/maximum channel floor/ceiling)

Deliverables:
- Recommended spend allocation by channel/campaign for next month
- Expected incremental revenue (and/or profit proxy)

### 3.4 Confidence & Sensitivity (How Confident Are We?)

Key assumptions and sensitivity checks:

- **Attribution assumption:** last-touch is an approximation; multi-touch would change allocations.
- **Modeling assumptions:** linear spend-to-revenue relationship; no strong cross-channel interactions.
- **Data quality:** accuracy of campaign tagging, session attribution, and spend data.

Sensitivity analysis:

- Vary model coefficients (e.g., ±10–20%) to see how the recommendation changes
- Test alternate spend ceilings and floor constraints
- Highlight where small changes in assumptions produce large changes in recommended budget

---

## 4. How to Run (Quick Start)

### 4.1 Setup

Install requirements (example):

```bash
python -m pip install -r requirements.txt
```

> If a `requirements.txt` does not exist, install at least: `pandas`, `numpy`, `statsmodels`, `matplotlib`.

### 4.2 Run ETL

From the repo root:

```bash
python etl/etl_pipeline.py
```

This generates the cleaned analytics tables used in analysis.

### 4.3 Run Analysis

From the repo root:

```bash
python analysis/analysis.py
```

This will:
- Compute channel/campaign ROAS, CAC, CVR
- Identify top performers and potential wasted spend
- (Optionally) generate charts and summary outputs

---

## 5. Outputs & Artifacts

- `data/fact_channel_daily.csv` — channel-level performance summary
- `data/fact_campaign_daily.csv` — campaign-level performance summary (if generated)
- `Capston_Project.pbix` — Power BI dashboard for visualization
- `Final Story.pdf` — narrative summary of findings and recommendations

---

## 6. Next Steps (Optional Enhancements)

- Add multi-touch attribution (e.g., linear, time-decay)
- Use advanced budget optimization (e.g., solver/linear programming)
- Include external variables (holidays, macro events, competitive shifts)
- Convert regression modeling into a reusable function or pipeline

---

## 7. Decision Notes (Quick Answer)

### 1. Which channels/campaigns are actually driving revenue and orders?
Use the `fact_channel_daily` and `campaign_daily` outputs to rank by **ROAS** and **attributed revenue**.

### 2. How should we reallocate next month’s budget?
Allocate more spend to channels/campaigns with higher **marginal ROAS** (as estimated by regression) and reduce spend on low-ROAS / high CAC channels.

### 3. How confident are we?
Confidence depends on:
- the stability of channel performance over time
- quality of campaign tagging (accurate `channel` + `campaign_id`)
- the extent to which spend effects are linear and independent

Provide a sensitivity table (±X% ROI) to quantify how recommendation changes.
