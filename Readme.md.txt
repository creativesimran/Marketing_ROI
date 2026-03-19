# Marketing ROI Analytics & Budget Optimization

## Project Overview

This project builds an end-to-end Marketing ROI analytics pipeline to measure advertising effectiveness and optimize next-month marketing budget allocation.

The workflow includes:

- Loading raw marketing and transaction data
- Cleaning and standardizing datasets
- Building curated fact tables
- Applying last-touch attribution logic
- Calculating performance KPIs (ROAS, CAC, CVR)
- Recommending optimized budget allocation
- Visualizing insights using Power BI

The objective is to enable data-driven marketing decisions and maximize profitable revenue growth.

---

## North Star Metric

**North Star: Profitable Revenue Growth**

Primary KPI:

**ROAS (Return on Ad Spend)**  
ROAS = Attributed Revenue / Spend

Supporting KPIs:
- Attributed Revenue
- Attributed Orders
- CAC (Customer Acquisition Cost)
- CVR (Conversion Rate)
- Margin per Dollar

The goal is to maximize revenue and margin under a fixed marketing budget constraint.

---

## Attribution Method Used

**Model: Last-Touch Attribution**

Definition:
Revenue is attributed to the campaign and channel associated with the session that generated the order.

Logic:
- If `purchase_flag = 1`
- The session’s `campaign_id` and `channel` receive full revenue credit
- Attribution flow:

Session → Campaign → Channel

Reason for selection:
- Simple and explainable
- Industry-standard for performance marketing
- Suitable for channel efficiency comparison

---

## How to Run ETL End-to-End

### Step 1 — Ensure Raw Files Exist

The following files must be in the same directory as `etl_pipeline.py`:

- sessions.csv
- orders.csv
- order_items.csv
- ad_spend_daily.csv
- users.csv
- campaigns.csv
- products.json

---

### Step 2 — Run ETL Pipeline

Run:

```bash
python3 etl_pipeline.py