# -*- coding: utf-8 -*-
"""Simran_Jaiswal_capston_marketing_ROI.ipynb

# Task
Calculate and present the Return on Ad Spend (ROAS) for each marketing channel using the `fact_channel_daily` DataFrame.

## Calculate ROAS by Channel

### Subtask:
Calculate and present the Return on Ad Spend (ROAS) for each marketing channel using the `fact_channel_daily` DataFrame.

**Reasoning**:
To calculate the Return on Ad Spend (ROAS) for each marketing channel, I need to group the `fact_channel_daily` DataFrame by 'channel' and then sum the 'attributed_revenue' and 'spend' columns. After that, I will calculate ROAS, ensuring to handle cases where 'spend' is zero to prevent division by zero errors.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _load_data(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


fact_channel_daily = _load_data("fact_channel_daily.csv")
fact_campaign_daily = _load_data("fact_campaign_daily.csv")
fact_sessions = _load_data("fact_sessions.csv")
ad_spend = _load_data("ad_spend.csv")
users = _load_data("users.csv")
order_items = _load_data("order_items.csv")

for df, date_col in [
    (fact_channel_daily, "date"),
    (fact_campaign_daily, "date"),
    (fact_sessions, "session_date"),
]:
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

if "session_ts" in fact_sessions.columns and "date" not in fact_sessions.columns:
    fact_sessions["date"] = pd.to_datetime(fact_sessions["session_ts"], errors="coerce").dt.date

channel_roas = fact_channel_daily.groupby("channel").agg({
    "attributed_revenue": "sum",
    "spend": "sum"
}).reset_index()

channel_roas["ROAS"] = np.where(
    channel_roas["spend"] > 0,
    channel_roas["attributed_revenue"] / channel_roas["spend"],
    0
)

print("Return on Ad Spend (ROAS) by Channel:\n")
print(channel_roas[["channel", "ROAS"]])

"""**Reasoning**:
The previous ROAS calculation resulted in all zero values because the 'channel' column in `campaign_daily` and consequently `fact_channel_daily` was incorrectly populated (filled with `None`) after a merge operation. This led to `attributed_revenue` being zero in `fact_channel_daily`. I need to regenerate `campaign_daily` and `fact_channel_daily` by explicitly assigning the correct 'channel' from the ad spend data to ensure `attributed_revenue` and `spend` are correctly associated, then recalculate ROAS.


"""

campaign_daily = fact_sessions.groupby(
    ["date", "campaign_id", "channel"]
).agg({
    "session_id": "count",
    "purchase_flag": "sum",
    "gross_revenue": "sum"
}).reset_index()

campaign_daily.rename(columns={
    "session_id": "attributed_sessions",
    "purchase_flag": "attributed_orders",
    "gross_revenue": "attributed_revenue"
}, inplace=True)

campaign_daily = campaign_daily.merge(
    ad_spend,
    on=["date", "campaign_id"],
    how="left"
)

campaign_daily["channel"] = campaign_daily["channel_y"]

campaign_daily["spend"] = campaign_daily["spend"].fillna(0)
campaign_daily["impressions"] = campaign_daily["impressions"].fillna(0)
campaign_daily["clicks"] = campaign_daily["clicks"].fillna(0)

campaign_daily["CPC"] = np.where(
    campaign_daily["clicks"] > 0,
    campaign_daily["spend"] / campaign_daily["clicks"],
    0
)

campaign_daily["CTR"] = np.where(
    campaign_daily["impressions"] > 0,
    campaign_daily["clicks"] / campaign_daily["impressions"],
    0
)

campaign_daily["CVR"] = np.where(
    campaign_daily["attributed_sessions"] > 0,
    campaign_daily["attributed_orders"] / campaign_daily["attributed_sessions"],
    0
)

campaign_daily["ROAS"] = np.where(
    campaign_daily["spend"] > 0,
    campaign_daily["attributed_revenue"] / campaign_daily["spend"],
    0
)

campaign_daily["CAC"] = np.where(
    campaign_daily["attributed_orders"] > 0,
    campaign_daily["spend"] / campaign_daily["attributed_orders"],
    0
)

campaign_daily = campaign_daily[
    [
        "date",
        "campaign_id",
        "channel",
        "spend",
        "impressions",
        "clicks",
        "attributed_sessions",
        "attributed_orders",
        "attributed_revenue",
        "CPC",
        "CTR",
        "CVR",
        "ROAS",
        "CAC"
    ]
]

ad_spend_agg = ad_spend.groupby(["date", "channel"]).agg({
    "spend": "sum",
    "impressions": "sum",
    "clicks": "sum",
    "promo_flag": "max"
}).reset_index()

channel_attrib = campaign_daily.groupby(["date", "channel"]).agg({
    "attributed_sessions": "sum",
    "attributed_orders": "sum",
    "attributed_revenue": "sum"
}).reset_index()

fact_channel_daily = ad_spend_agg.merge(
    channel_attrib,
    on=["date", "channel"],
    how="left"
)

fact_channel_daily["attributed_sessions"] = fact_channel_daily["attributed_sessions"].fillna(0).astype(int)
fact_channel_daily["attributed_orders"] = fact_channel_daily["attributed_orders"].fillna(0).astype(int)
fact_channel_daily["attributed_revenue"] = fact_channel_daily["attributed_revenue"].fillna(0)

fact_channel_daily["date_dt"] = pd.to_datetime(fact_channel_daily["date"])
fact_channel_daily["day_of_week"] = fact_channel_daily["date_dt"].dt.weekday

min_date = fact_channel_daily["date_dt"].min()
fact_channel_daily["week_index"] = ((fact_channel_daily["date_dt"] - min_date).dt.days // 7).astype(int)

fact_channel_daily["promo_flag"] = fact_channel_daily["promo_flag"].fillna(0).astype(int)

fact_channel_daily = fact_channel_daily[
    [
        "date",
        "channel",
        "spend",
        "impressions",
        "clicks",
        "attributed_sessions",
        "attributed_orders",
        "attributed_revenue",
        "day_of_week",
        "promo_flag",
        "week_index"
    ]
]

channel_roas = fact_channel_daily.groupby("channel").agg({
    "attributed_revenue": "sum",
    "spend": "sum"
}).reset_index()

channel_roas["ROAS"] = np.where(
    channel_roas["spend"] > 0,
    channel_roas["attributed_revenue"] / channel_roas["spend"],
    0
)

print("Return on Ad Spend (ROAS) by Channel:\n")
print(channel_roas[["channel", "ROAS"]])

"""## Identify Top 5 Campaigns by Spend and Revenue

### Subtask:
Determine and list the top 5 campaigns based on 'spend' and 'attributed_revenue' from the `campaign_daily` DataFrame. Provide a bar chart for both.

**Reasoning**:
To identify and visualize the top 5 campaigns by spend, I will first sort the `campaign_daily` DataFrame by the 'spend' column in descending order and select the top 5 rows. Then, I will print the relevant columns ('campaign_id' and 'spend') and create a bar chart to visualize this data.
"""

import matplotlib.pyplot as plt

top_5_spend_campaigns = campaign_daily.sort_values("spend", ascending=False).head(5)
print("Top 5 Campaigns by Spend:\n")
print(top_5_spend_campaigns[["campaign_id", "spend"]])

plt.figure(figsize=(10, 6))
plt.bar(top_5_spend_campaigns["campaign_id"], top_5_spend_campaigns["spend"], color='skyblue')
plt.xlabel("Campaign ID")
plt.ylabel("Spend")
plt.title("Top 5 Campaigns by Spend")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

"""**Reasoning**:
To complete the subtask, I will now identify and visualize the top 5 campaigns based on 'attributed_revenue'. This involves sorting the `campaign_daily` DataFrame, printing the relevant columns, and creating a bar chart for visualization.


"""

top_5_revenue_campaigns = campaign_daily.sort_values("attributed_revenue", ascending=False).head(5)
print("\nTop 5 Campaigns by Attributed Revenue:\n")
print(top_5_revenue_campaigns[["campaign_id", "attributed_revenue"]])

plt.figure(figsize=(10, 6))
plt.bar(top_5_revenue_campaigns["campaign_id"], top_5_revenue_campaigns["attributed_revenue"], color='lightcoral')
plt.xlabel("Campaign ID")
plt.ylabel("Attributed Revenue")
plt.title("Top 5 Campaigns by Attributed Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

"""## Find Wasted Spend Areas

### Subtask:
Analyze campaign performance in `campaign_daily` to identify areas of 'wasted spend' where high expenditure does not translate to proportional revenue or ROAS. This could involve looking at campaigns with low ROAS despite high spend.

**Reasoning**:
To identify wasted spend, I will first calculate the average ROAS across all campaigns in the `campaign_daily` DataFrame.
"""

avg_roas = campaign_daily["ROAS"].mean()
print(f"Average ROAS across all campaigns: {avg_roas:.2f}")

"""**Reasoning**:
To identify campaigns with 'wasted spend', I will calculate the average spend across all campaigns and then filter the `campaign_daily` DataFrame to find campaigns with spend above this average and ROAS below the overall average ROAS. Then, I will sort them by spend and print the relevant information.


"""

avg_spend = campaign_daily["spend"].mean()
wasted_spend_campaigns = campaign_daily[
    (campaign_daily["spend"] > avg_spend) &
    (campaign_daily["ROAS"] < avg_roas)
].sort_values("spend", ascending=False)

print("\nCampaigns with Potential Wasted Spend (High Spend, Low ROAS):\n")
print(wasted_spend_campaigns[["campaign_id", "spend", "attributed_revenue", "ROAS"]])

"""## Analyze Daily/Weekly KPI Trends

### Subtask:
Calculate and visualize daily and weekly trends for Conversion Rate (CVR), ROAS, and Revenue using `fact_channel_daily`. Plot these trends over time.

**Reasoning**:
To analyze daily trends, I will first aggregate the `fact_channel_daily` DataFrame by 'date', summing the necessary metrics. Then, I will calculate daily Conversion Rate (CVR), Return on Ad Spend (ROAS), and total revenue, ensuring to handle potential division by zero errors. Finally, I will display the first few rows of the resulting DataFrame to verify the calculations.
"""

daily_trends = fact_channel_daily.groupby("date").agg({
    "attributed_sessions": "sum",
    "attributed_orders": "sum",
    "attributed_revenue": "sum",
    "spend": "sum"
}).reset_index()

daily_trends["daily_cvr"] = np.where(
    daily_trends["attributed_sessions"] > 0,
    daily_trends["attributed_orders"] / daily_trends["attributed_sessions"],
    0
)

daily_trends["daily_roas"] = np.where(
    daily_trends["spend"] > 0,
    daily_trends["attributed_revenue"] / daily_trends["spend"],
    0
)

daily_trends.rename(columns={
    "attributed_revenue": "daily_revenue"
}, inplace=True)

print("Daily KPI Trends:")
print(daily_trends.head())

"""**Reasoning**:
Now that daily KPI trends have been calculated, I will create three separate line plots to visualize the daily trends for CVR, ROAS, and revenue over time, as requested by the instructions.


"""

plt.figure(figsize=(15, 5))

plt.subplot(1, 3, 1)
plt.plot(daily_trends["date"], daily_trends["daily_cvr"], color='skyblue')
plt.xlabel("Date")
plt.ylabel("Daily CVR")
plt.title("Daily Conversion Rate Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.subplot(1, 3, 2)
plt.plot(daily_trends["date"], daily_trends["daily_roas"], color='lightcoral')
plt.xlabel("Date")
plt.ylabel("Daily ROAS")
plt.title("Daily Return on Ad Spend Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.subplot(1, 3, 3)
plt.plot(daily_trends["date"], daily_trends["daily_revenue"], color='lightgreen')
plt.xlabel("Date")
plt.ylabel("Daily Revenue")
plt.title("Daily Revenue Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.tight_layout()
plt.show()

"""**Reasoning**:
To calculate weekly trends, I will group the `fact_channel_daily` DataFrame by 'week_index' and sum the relevant metrics: 'attributed_sessions', 'attributed_orders', 'attributed_revenue', and 'spend'. Then, I will calculate the weekly CVR, ROAS, and rename 'attributed_revenue' to 'weekly_revenue', ensuring to handle division by zero. Finally, I will display the first few rows of the resulting DataFrame to verify the calculations.


"""

weekly_trends = fact_channel_daily.groupby("week_index").agg({
    "attributed_sessions": "sum",
    "attributed_orders": "sum",
    "attributed_revenue": "sum",
    "spend": "sum"
}).reset_index()

weekly_trends["weekly_cvr"] = np.where(
    weekly_trends["attributed_sessions"] > 0,
    weekly_trends["attributed_orders"] / weekly_trends["attributed_sessions"],
    0
)

weekly_trends["weekly_roas"] = np.where(
    weekly_trends["spend"] > 0,
    weekly_trends["attributed_revenue"] / weekly_trends["spend"],
    0
)

weekly_trends.rename(columns={
    "attributed_revenue": "weekly_revenue"
}, inplace=True)

print("Weekly KPI Trends:")
print(weekly_trends.head())

"""**Reasoning**:
Now that weekly KPI trends have been calculated, I will create three separate line plots to visualize the weekly trends for CVR, ROAS, and revenue over time, as requested by the instructions.


"""

plt.figure(figsize=(15, 5))

plt.subplot(1, 3, 1)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_cvr"], color='skyblue')
plt.xlabel("Week Index")
plt.ylabel("Weekly CVR")
plt.title("Weekly Conversion Rate Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.subplot(1, 3, 2)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_roas"], color='lightcoral')
plt.xlabel("Week Index")
plt.ylabel("Weekly ROAS")
plt.title("Weekly Return on Ad Spend Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.subplot(1, 3, 3)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_revenue"], color='lightgreen')
plt.xlabel("Week Index")
plt.ylabel("Weekly Revenue")
plt.title("Weekly Revenue Trend")
plt.xticks(rotation=45)
plt.grid(True)

plt.tight_layout()
plt.show()

"""## Explain Trend Spikes with Promotions

### Subtask:
Overlay the `promo_flag` onto the daily/weekly KPI trend visualizations to identify if promotional activities correlate with significant spikes or dips in performance.

**Reasoning**:
To overlay promotional flags onto the daily KPI trends, I first need to aggregate the `fact_channel_daily` DataFrame by date to determine if a promotion was active on any given day. Then, I will merge this promotional information with the `daily_trends` DataFrame, which contains the daily KPI calculations.
"""

daily_promo_flags = fact_channel_daily.groupby("date")["promo_flag"].max().reset_index()
daily_promo_flags.rename(columns={"promo_flag": "has_promo"}, inplace=True)

daily_trends = daily_trends.merge(daily_promo_flags, on="date", how="left")

print("Daily KPI Trends with Promo Flags:")
print(daily_trends.head())

"""**Reasoning**:
To visualize the correlation between promotional activities and daily KPI trends, I will create three subplots for daily CVR, ROAS, and Revenue. On each subplot, I will plot the respective daily KPI trend and mark the dates where `has_promo` is 1, using vertical lines to indicate promotion days.


"""

plt.figure(figsize=(18, 6))

# Plot Daily CVR Trend with Promo Flags
plt.subplot(1, 3, 1)
plt.plot(daily_trends["date"], daily_trends["daily_cvr"], color='skyblue', label='Daily CVR')
promo_dates_cvr = daily_trends[daily_trends["has_promo"] == 1]["date"]
for date in promo_dates_cvr:
    plt.axvline(x=date, color='red', linestyle='--', alpha=0.7, label='Promo Day' if date == promo_dates_cvr.iloc[0] else "")
plt.xlabel("Date")
plt.ylabel("Daily CVR")
plt.title("Daily Conversion Rate Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

# Plot Daily ROAS Trend with Promo Flags
plt.subplot(1, 3, 2)
plt.plot(daily_trends["date"], daily_trends["daily_roas"], color='lightcoral', label='Daily ROAS')
promo_dates_roas = daily_trends[daily_trends["has_promo"] == 1]["date"]
for date in promo_dates_roas:
    plt.axvline(x=date, color='red', linestyle='--', alpha=0.7, label='Promo Day' if date == promo_dates_roas.iloc[0] else "")
plt.xlabel("Date")
plt.ylabel("Daily ROAS")
plt.title("Daily Return on Ad Spend Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

# Plot Daily Revenue Trend with Promo Flags
plt.subplot(1, 3, 3)
plt.plot(daily_trends["date"], daily_trends["daily_revenue"], color='lightgreen', label='Daily Revenue')
promo_dates_revenue = daily_trends[daily_trends["has_promo"] == 1]["date"]
for date in promo_dates_revenue:
    plt.axvline(x=date, color='red', linestyle='--', alpha=0.7, label='Promo Day' if date == promo_dates_revenue.iloc[0] else "")
plt.xlabel("Date")
plt.ylabel("Daily Revenue")
plt.title("Daily Revenue Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.show()

"""**Reasoning**:
To overlay promotional flags onto the weekly KPI trends, I first need to aggregate the `fact_channel_daily` DataFrame by `week_index` to determine if a promotion was active during any given week. Then, I will merge this promotional information with the `weekly_trends` DataFrame, which contains the weekly KPI calculations.


"""

weekly_promo_flags = fact_channel_daily.groupby("week_index")["promo_flag"].max().reset_index()
weekly_promo_flags.rename(columns={"promo_flag": "has_promo"}, inplace=True)

weekly_trends = weekly_trends.merge(weekly_promo_flags, on="week_index", how="left")

print("Weekly KPI Trends with Promo Flags:")
print(weekly_trends.head())

"""**Reasoning**:
Now that weekly KPI trends have been calculated and merged with promotional flags, I will create three separate line plots to visualize the weekly trends for CVR, ROAS, and revenue over time, and mark the weeks with promotions using vertical lines, as requested by the instructions.


"""

plt.figure(figsize=(18, 6))

# Plot Weekly CVR Trend with Promo Flags
plt.subplot(1, 3, 1)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_cvr"], color='skyblue', label='Weekly CVR')
promo_weeks_cvr = weekly_trends[weekly_trends["has_promo"] == 1]["week_index"]
for week in promo_weeks_cvr:
    plt.axvline(x=week, color='red', linestyle='--', alpha=0.7, label='Promo Week' if week == promo_weeks_cvr.iloc[0] else "")
plt.xlabel("Week Index")
plt.ylabel("Weekly CVR")
plt.title("Weekly Conversion Rate Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

# Plot Weekly ROAS Trend with Promo Flags
plt.subplot(1, 3, 2)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_roas"], color='lightcoral', label='Weekly ROAS')
promo_weeks_roas = weekly_trends[weekly_trends["has_promo"] == 1]["week_index"]
for week in promo_weeks_roas:
    plt.axvline(x=week, color='red', linestyle='--', alpha=0.7, label='Promo Week' if week == promo_weeks_roas.iloc[0] else "")
plt.xlabel("Week Index")
plt.ylabel("Weekly ROAS")
plt.title("Weekly Return on Ad Spend Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

# Plot Weekly Revenue Trend with Promo Flags
plt.subplot(1, 3, 3)
plt.plot(weekly_trends["week_index"], weekly_trends["weekly_revenue"], color='lightgreen', label='Weekly Revenue')
promo_weeks_revenue = weekly_trends[weekly_trends["has_promo"] == 1]["week_index"]
for week in promo_weeks_revenue:
    plt.axvline(x=week, color='red', linestyle='--', alpha=0.7, label='Promo Week' if week == promo_weeks_revenue.iloc[0] else "")
plt.xlabel("Week Index")
plt.ylabel("Weekly Revenue")
plt.title("Weekly Revenue Trend with Promotions")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.show()

"""## Segment Analysis: Channel x Device

### Subtask:
Analyze and compare conversion rates and ROAS across different combinations of marketing 'channel' and 'device' using the `fact_sessions` DataFrame.

**Reasoning**:
To analyze conversion rates and ROAS by 'channel' and 'device', I need to first attribute advertising spend from the `ad_spend` DataFrame to individual sessions in `fact_sessions`. This involves calculating the spend per session for each unique combination of 'date', 'campaign_id', and 'channel', and then merging this value back into the `fact_sessions` DataFrame. Once `spend` is attributed to each session, I can group the `fact_sessions` DataFrame by 'channel' and 'device', sum the relevant metrics, and then calculate CVR and ROAS.
"""

import numpy as np

# Ensure 'date' column is present in fact_sessions
# This step is already performed in previous cells, but we ensure it here.
if 'date' not in fact_sessions.columns:
    fact_sessions["date"] = pd.to_datetime(fact_sessions["session_ts"], errors="coerce").dt.date

# 1. Calculate total sessions per date, campaign_id, channel from fact_sessions
# This is used to distribute the daily campaign spend proportionally across sessions.
sessions_for_spend_attribution = fact_sessions.groupby(["date", "campaign_id", "channel"]).agg(
    total_sessions_in_group=("session_id", "count")
).reset_index()

# 2. Merge sessions_for_spend_attribution with ad_spend to calculate spend_per_session
spend_attribution_data = ad_spend.merge(
    sessions_for_spend_attribution,
    on=["date", "campaign_id", "channel"],
    how="left"
)

# Handle cases where there might be ad_spend but no sessions recorded for that specific group, or vice-versa.
spend_attribution_data["total_sessions_in_group"] = spend_attribution_data["total_sessions_in_group"].fillna(0)

# Calculate spend per session for each date-campaign-channel group
spend_attribution_data["spend_per_session"] = np.where(
    spend_attribution_data["total_sessions_in_group"] > 0,
    spend_attribution_data["spend"] / spend_attribution_data["total_sessions_in_group"],
    0
)

# 3. Merge spend_per_session back to fact_sessions
# We create a copy to avoid modifying the original fact_sessions DataFrame directly with attributed spend values
fact_sessions_with_attributed_spend = fact_sessions.copy()
fact_sessions_with_attributed_spend = fact_sessions_with_attributed_spend.merge(
    spend_attribution_data[["date", "campaign_id", "channel", "spend_per_session"]],
    on=["date", "campaign_id", "channel"],
    how="left"
)

# Fill any NaNs resulting from the merge (e.g., sessions not linked to an ad_spend entry) with 0
fact_sessions_with_attributed_spend["spend_per_session"] = fact_sessions_with_attributed_spend["spend_per_session"].fillna(0)

# 4. Group fact_sessions_with_attributed_spend by 'channel' and 'device' and aggregate metrics
channel_device_analysis = fact_sessions_with_attributed_spend.groupby(["channel", "device"]).agg(
    attributed_sessions=("session_id", "count"),
    attributed_orders=("purchase_flag", "sum"),
    attributed_revenue=("gross_revenue", "sum"),
    attributed_spend=("spend_per_session", "sum") # Summing up the attributed spend per session for the group
).reset_index()

# 5. Calculate CVR (Conversion Rate)
channel_device_analysis["CVR"] = np.where(
    channel_device_analysis["attributed_sessions"] > 0,
    channel_device_analysis["attributed_orders"] / channel_device_analysis["attributed_sessions"],
    0
)

# 6. Calculate ROAS (Return on Ad Spend)
channel_device_analysis["ROAS"] = np.where(
    channel_device_analysis["attributed_spend"] > 0,
    channel_device_analysis["attributed_revenue"] / channel_device_analysis["attributed_spend"],
    0
)

print("Channel and Device Segment Analysis:")
print(channel_device_analysis.sort_values(by=['channel', 'device']))

"""## Segment Analysis: New vs Returning Users

### Subtask:
Compare the performance metrics (conversion rate, ROAS) between 'new' and 'returning' users based on the `is_new_user` column in `fact_sessions`.

**Reasoning**:
To compare performance metrics between new and returning users, I will group the `fact_sessions_with_attributed_spend` DataFrame by the `is_new_user` column, aggregate the necessary metrics (sessions, orders, revenue, spend), and then calculate CVR and ROAS, handling division by zero.
"""

user_segment_analysis = fact_sessions_with_attributed_spend.groupby("is_new_user").agg(
    total_sessions=("session_id", "count"),
    total_orders=("purchase_flag", "sum"),
    total_revenue=("gross_revenue", "sum"),
    total_spend=("spend_per_session", "sum")
).reset_index()

user_segment_analysis["CVR"] = np.where(
    user_segment_analysis["total_sessions"] > 0,
    user_segment_analysis["total_orders"] / user_segment_analysis["total_sessions"],
    0
)

user_segment_analysis["ROAS"] = np.where(
    user_segment_analysis["total_spend"] > 0,
    user_segment_analysis["total_revenue"] / user_segment_analysis["total_spend"],
    0
)

print("Performance Metrics by User Segment (New vs. Returning):")
print(user_segment_analysis)

"""## Segment Analysis: City Tier

### Subtask:
Analyze and compare conversion rates and ROAS across different 'city_tier' segments by merging `fact_sessions` with the `users` DataFrame.

**Reasoning**:
To analyze conversion rates and ROAS by 'city_tier', I will first merge `fact_sessions_with_attributed_spend` with the `users` DataFrame on 'user_id' to incorporate the 'city_tier' information. Then, I will group the merged data by 'city_tier', aggregate the necessary metrics (attributed sessions, orders, revenue, and spend), and calculate the CVR and ROAS for each segment, ensuring to handle potential division by zero.
"""

fact_sessions_with_city_tier = fact_sessions_with_attributed_spend.merge(
    users[["user_id", "city_tier"]],
    on="user_id",
    how="left"
)

city_tier_analysis = fact_sessions_with_city_tier.groupby("city_tier").agg(
    attributed_sessions=("session_id", "count"),
    attributed_orders=("purchase_flag", "sum"),
    attributed_revenue=("gross_revenue", "sum"),
    attributed_spend=("spend_per_session", "sum")
).reset_index()

city_tier_analysis["CVR"] = np.where(
    city_tier_analysis["attributed_sessions"] > 0,
    city_tier_analysis["attributed_orders"] / city_tier_analysis["attributed_sessions"],
    0
)

city_tier_analysis["ROAS"] = np.where(
    city_tier_analysis["attributed_spend"] > 0,
    city_tier_analysis["attributed_revenue"] / city_tier_analysis["attributed_spend"],
    0
)

print("Performance Metrics by City Tier:")
print(city_tier_analysis.sort_values(by="city_tier"))

"""## Segment Analysis: Product Category

### Subtask:
Evaluate revenue and margin performance across different 'product category' by utilizing the `order_items` DataFrame and summarizing its impact.

**Reasoning**:
To evaluate revenue and margin performance across different product categories, I will group the `order_items` DataFrame by 'category' and sum the 'gross_revenue' and 'margin' columns. Then, I will reset the index and print the resulting DataFrame.
"""

category_performance = order_items.groupby("category").agg({
    "gross_revenue": "sum",
    "margin": "sum"
}).reset_index()

print("Revenue and Margin Performance by Product Category:")
print(category_performance)

"""## Identify Key Segment Differences & Suggest Actions

### Subtask:
Based on the segment analyses, identify three segments with the largest conversion rate differences and three with the largest ROAS differences. For each identified segment, provide a brief suggestion for potential actions.

**Reasoning**:
To identify segments with the largest conversion rate differences, I will first calculate the CVR difference for the 'channel' and 'device' segments from the `channel_device_analysis` DataFrame.
"""

print("Channel x Device Analysis CVR and ROAS:")
print(channel_device_analysis[['channel', 'device', 'CVR', 'ROAS']].sort_values(by=['channel', 'device']))

# Calculate CVR differences for Channel x Device
channel_device_cvr_diff = channel_device_analysis.groupby('channel')['CVR'].agg(['max', 'min']).reset_index()
channel_device_cvr_diff['CVR_diff'] = channel_device_cvr_diff['max'] - channel_device_cvr_diff['min']

# Calculate ROAS differences for Channel x Device
channel_device_roas_diff = channel_device_analysis.groupby('channel')['ROAS'].agg(['max', 'min']).reset_index()
channel_device_roas_diff['ROAS_diff'] = channel_device_roas_diff['max'] - channel_device_roas_diff['min']

print("\nChannel x Device CVR Differences:\n")
print(channel_device_cvr_diff.sort_values(by='CVR_diff', ascending=False))

print("\nChannel x Device ROAS Differences:\n")
print(channel_device_roas_diff.sort_values(by='ROAS_diff', ascending=False))

"""**Reasoning**:
To continue identifying segments with the largest CVR and ROAS differences, I will now calculate these differences for the 'New vs Returning Users' segment using the `user_segment_analysis` DataFrame.


"""

print("\nNew vs Returning Users Analysis CVR and ROAS:")
print(user_segment_analysis[['is_new_user', 'CVR', 'ROAS']])

# Calculate CVR differences for New vs Returning Users
user_segment_cvr_diff = pd.DataFrame({
    'segment': ['New vs Returning Users'],
    'CVR_diff': [user_segment_analysis['CVR'].max() - user_segment_analysis['CVR'].min()]
})

# Calculate ROAS differences for New vs Returning Users
user_segment_roas_diff = pd.DataFrame({
    'segment': ['New vs Returning Users'],
    'ROAS_diff': [user_segment_analysis['ROAS'].max() - user_segment_analysis['ROAS'].min()]
})

print("\nNew vs Returning Users CVR Differences:\n")
print(user_segment_cvr_diff)

print("\nNew vs Returning Users ROAS Differences:\n")
print(user_segment_roas_diff)

"""**Reasoning**:
To complete the identification of segments with the largest CVR and ROAS differences, I will now calculate these differences for the 'City Tier' segment using the `city_tier_analysis` DataFrame.


"""

print(
    "\nCity Tier Analysis CVR and ROAS:"
)
print(city_tier_analysis[["city_tier", "CVR", "ROAS"]].sort_values(by="city_tier"))

# Calculate CVR differences for City Tier
city_tier_cvr_diff = pd.DataFrame({
    "segment": ["City Tier"],
    "CVR_diff": [
        city_tier_analysis["CVR"].max()
        - city_tier_analysis["CVR"].min()
    ],
})

# Calculate ROAS differences for City Tier
city_tier_roas_diff = pd.DataFrame({
    "segment": ["City Tier"],
    "ROAS_diff": [
        city_tier_analysis["ROAS"].max()
        - city_tier_analysis["ROAS"].min()
    ],
})

print("\nCity Tier CVR Differences:\n")
print(city_tier_cvr_diff)

print("\nCity Tier ROAS Differences:\n")
print(city_tier_roas_diff)

"""**Reasoning**:
To identify the top three segments with the largest CVR and ROAS differences, I will consolidate all calculated difference DataFrames into two main DataFrames (one for CVR and one for ROAS). Then, I will sort them in descending order and select the top three for each metric. Finally, I will provide actionable recommendations based on these identified segments.


"""

all_cvr_diffs = pd.concat([
    channel_device_cvr_diff[['channel', 'CVR_diff']].rename(columns={'channel': 'segment'}),
    user_segment_cvr_diff,
    city_tier_cvr_diff
]).sort_values(by='CVR_diff', ascending=False)

all_roas_diffs = pd.concat([
    channel_device_roas_diff[['channel', 'ROAS_diff']].rename(columns={'channel': 'segment'}),
    user_segment_roas_diff,
    city_tier_roas_diff
]).sort_values(by='ROAS_diff', ascending=False)

print("\n--- Top 3 Segments with Largest CVR Differences ---")
for index, row in all_cvr_diffs.head(3).iterrows():
    segment_name = row['segment']
    cvr_diff = row['CVR_diff']
    print(f"\nSegment: {segment_name}")
    print(f"CVR Difference: {cvr_diff:.4f}")

    if segment_name in channel_device_analysis['channel'].values:
        channel_data = channel_device_analysis[channel_device_analysis['channel'] == segment_name][['device', 'CVR']]
        max_cvr_device = channel_data.loc[channel_data['CVR'].idxmax()]
        min_cvr_device = channel_data.loc[channel_data['CVR'].idxmin()]
        print(f"Observation: {segment_name} shows a CVR difference between {max_cvr_device['device']} (CVR: {max_cvr_device['CVR']:.4f}) and {min_cvr_device['device']} (CVR: {min_cvr_device['CVR']:.4f}).")
        print(f"Action: Investigate user experience on {min_cvr_device['device']} for {segment_name} to identify friction points and optimize conversion funnel. Consider reallocating budget from lower performing device to higher performing one within this channel.")
    elif segment_name == 'New vs Returning Users':
        new_user_data = user_segment_analysis[user_segment_analysis['is_new_user'] == 1]
        returning_user_data = user_segment_analysis[user_segment_analysis['is_new_user'] == 0]
        print(f"Observation: New users have a CVR of {new_user_data['CVR'].iloc[0]:.4f} while returning users have {returning_user_data['CVR'].iloc[0]:.4f}.")
        print("Action: Tailor marketing messages and landing pages specifically for new users to improve their initial conversion journey. For returning users, focus on loyalty programs or personalized recommendations.")
    elif segment_name == 'City Tier':
        city_tier_data = city_tier_analysis[['city_tier', 'CVR']]
        max_cvr_tier = city_tier_data.loc[city_tier_data['CVR'].idxmax()]
        min_cvr_tier = city_tier_data.loc[city_tier_data['CVR'].idxmin()]
        print(f"Observation: City Tier {max_cvr_tier['city_tier']} has the highest CVR of {max_cvr_tier['CVR']:.4f}, while City Tier {min_cvr_tier['city_tier']} has the lowest at {min_cvr_tier['CVR']:.4f}.")
        print(f"Action: Study successful campaigns and user behavior in City Tier {max_cvr_tier['city_tier']} and apply learnings to optimize strategies for City Tier {min_cvr_tier['city_tier']}. Consider localized promotions.")

print("\n--- Top 3 Segments with Largest ROAS Differences ---")
for index, row in all_roas_diffs.head(3).iterrows():
    segment_name = row['segment']
    roas_diff = row['ROAS_diff']
    print(f"\nSegment: {segment_name}")
    print(f"ROAS Difference: {roas_diff:.4f}")

    if segment_name in channel_device_analysis['channel'].values:
        channel_data = channel_device_analysis[channel_device_analysis['channel'] == segment_name][['device', 'ROAS']]
        max_roas_device = channel_data.loc[channel_data['ROAS'].idxmax()]
        min_roas_device = channel_data.loc[channel_data['ROAS'].idxmin()]
        print(f"Observation: {segment_name} shows a ROAS difference between {max_roas_device['device']} (ROAS: {max_roas_device['ROAS']:.4f}) and {min_roas_device['device']} (ROAS: {min_roas_device['ROAS']:.4f}).")
        print(f"Action: Reallocate ad spend from {segment_name} on {min_roas_device['device']} to {segment_name} on {max_roas_device['device']} or other high-performing segments to maximize return.")
    elif segment_name == 'New vs Returning Users':
        new_user_data = user_segment_analysis[user_segment_analysis['is_new_user'] == 1]
        returning_user_data = user_segment_analysis[user_segment_analysis['is_new_user'] == 0]
        print(f"Observation: New users have a ROAS of {new_user_data['ROAS'].iloc[0]:.4f} while returning users have {returning_user_data['ROAS'].iloc[0]:.4f}.")
        print("Action: Investigate why ROAS differs between new and returning users. If new user ROAS is lower, consider optimizing initial ad creatives and targeting to attract more profitable new customers. If returning user ROAS is lower, focus on remarketing strategies.")
    elif segment_name == 'City Tier':
        city_tier_data = city_tier_analysis[['city_tier', 'ROAS']]
        max_roas_tier = city_tier_data.loc[city_tier_data['ROAS'].idxmax()]
        min_roas_tier = city_tier_data.loc[city_tier_data['ROAS'].idxmin()]
        print(f"Observation: City Tier {max_roas_tier['city_tier']} has the highest ROAS of {max_roas_tier['ROAS']:.4f}, while City Tier {min_roas_tier['city_tier']} has the lowest at {min_roas_tier['ROAS']:.4f}.")
        print(f"Action: Evaluate ad spend efficiency in lower performing City Tier {min_roas_tier['city_tier']}. Perhaps re-evaluate campaign goals or adjust bidding strategies in these areas, or explore alternative channels for better ROAS.")

"""## Create Spend Inefficiency Table

### Subtask:
Construct a table highlighting two specific areas of spend inefficiency identified in the analyses (e.g., high spend, low revenue/ROAS). For each area, propose possible reasons for the inefficiency and suggest concrete experiments to run to address it.

**Reasoning**:
To construct the spend inefficiency table, I need to identify two specific areas of inefficiency based on prior analysis, define their descriptions, brainstorm potential reasons, and suggest actionable experiments. I will use the `wasted_spend_campaigns` and `channel_device_analysis` DataFrames to identify these areas.
"""

spend_inefficiencies = []

# --- Inefficiency Area 1: Specific campaign with high spend and low ROAS ---
# Using the top campaign from wasted_spend_campaigns (C006)
inefficiency_campaign = wasted_spend_campaigns.iloc[0]
campaign_id = inefficiency_campaign["campaign_id"]
channel = inefficiency_campaign["channel"]
spend = inefficiency_campaign["spend"]
roas = inefficiency_campaign["ROAS"]

spend_inefficiencies = []

# --- Inefficiency Area 1: Specific campaign with high spend and low ROAS ---
# Using the top campaign from wasted_spend_campaigns (C006)
inefficiency_campaign = wasted_spend_campaigns.iloc[0]
campaign_id = inefficiency_campaign["campaign_id"]
channel = inefficiency_campaign["channel"]
spend = inefficiency_campaign["spend"]
roas = inefficiency_campaign["ROAS"]

spend_inefficiencies.append({
    "Inefficiency Area": f"Campaign {campaign_id} ({channel})",
    "Description": (
        f"Campaign {campaign_id} on the {channel} channel has a high spend of "
        f"{spend:.2f} but a very low ROAS of {roas:.2f}, indicating that for "
        f"every dollar spent, only {roas:.0f} cents are generated in revenue."
    ),
    "Possible Reasons": [
        "Poor targeting: Ads reaching irrelevant audience.",
        "Irrelevant ad copy/creative: Ads not resonating with the audience or not matching landing page content.",
        "High competition/CPC: Bidding too high on competitive keywords, driving up spend without proportional returns.",
        "Poor landing page experience: Users clicking but not converting due to bad landing page design, slow load times, or confusing content.",
        "Product/offer mismatch: The product or offer being promoted is not appealing to the audience attracted by the campaign."
    ],
    "Suggested Experiments": [
        "A/B Test Ad Creatives/Copy: Test different ad headlines, descriptions, and call-to-actions to see which ones drive higher conversion rates and ROAS.",
        "Audience Segmentation Refinement: Experiment with more granular audience targeting based on demographics, interests, and behaviors to reach more qualified leads.",
        "Landing Page Optimization: Test different landing page designs, content layouts, and offer placements to improve on-page conversion rates.",
        "Keyword Optimization (for search campaigns): Review and prune underperforming keywords, reallocate budget to high-performing keywords, and test long-tail keywords."
    ]
})

# --- Inefficiency Area 2: Channel-device segment with low ROAS ---
# Identify the channel-device segment with the lowest ROAS from channel_device_analysis
lowest_roas_segment = channel_device_analysis.sort_values(by='ROAS', ascending=True).iloc[0]
lowest_roas_channel = lowest_roas_segment['channel']
lowest_roas_device = lowest_roas_segment['device']
lowest_roas_value = lowest_roas_segment['ROAS']

spend_inefficiencies.append({
    "Inefficiency Area": f"Channel: {lowest_roas_channel}, Device: {lowest_roas_device}",
    "Description": (
        f"The {lowest_roas_channel} channel on {lowest_roas_device} devices exhibits the "
        f"lowest ROAS ({lowest_roas_value:.2f}), indicating significant inefficiency "
        f"in ad spend for this segment."
    ),
    "Possible Reasons": [
        "Mobile user experience issues: Website/app not optimized for mobile, leading to high bounce rates and low conversions.",
        "Ad creative not suited for mobile: Ads are not visually appealing or engaging on smaller screens.",
        "Targeting mismatch: The mobile audience targeted by paid social campaigns may not be as conversion-ready as other segments.",
        "Slow mobile page load times: Users abandoning before content loads due to poor performance.",
        "Difficulty completing purchases on mobile: Complex checkout processes or forms on mobile."
    ],
    "Suggested Experiments": [
        "Mobile-Specific Creative Testing: Design and test ad creatives specifically optimized for mobile platforms (e.g., short-form videos, interactive ads).",
        "Improve Mobile Landing Page Experience: Optimize landing pages for faster load times, intuitive navigation, and simplified forms on mobile devices.",
        "A/B Test Mobile CTA and Layout: Experiment with different call-to-action placements and overall layout specific to mobile screens.",
        "Refine Mobile Targeting: Adjust audience targeting parameters to focus on mobile users who are more likely to convert, potentially excluding segments with consistently low ROAS."
    ]
})

print("Spend Inefficiencies Identified:")
for inefficiency in spend_inefficiencies:
    print(f"\nInefficiency Area: {inefficiency['Inefficiency Area']}")
    print(f"Description: {inefficiency['Description']}")
    print("Possible Reasons:")
    for reason in inefficiency['Possible Reasons']:
        print(f"- {reason}")
    print("Suggested Experiments:")
    for experiment in inefficiency['Suggested Experiments']:
        print(f"- {experiment}")

"""## Final Task

### Subtask:
Provide a summary of all findings, including key performance insights, segment differences, and actionable recommendations for optimizing spend and campaign performance.

## Summary:

### Q&A
The main task was to summarize all findings, including key performance insights, segment differences, and actionable recommendations for optimizing spend and campaign performance. The analysis provided a comprehensive overview of these aspects.

### Data Analysis Key Findings
*   **Return on Ad Spend (ROAS) by Channel**:
    *   `organic` leads significantly with a ROAS of 13.82.
    *   `email` also performs strongly with a ROAS of 5.25.
    *   `referral` and `search` channels show positive but lower ROAS at 1.33 and 1.31, respectively.
    *   `paid_social` has a concerningly low ROAS of 0.73, indicating a net loss for every dollar spent.
*   **Top Campaign Performance**:
    *   **Top 5 Campaigns by Spend**: Campaigns C006 (\$42,715), C005 (\$40,036), C007 (\$39,982), C007 (\$38,254), and C008 (\$37,501) incurred the highest expenditures.
    *   **Top 5 Campaigns by Attributed Revenue**: Campaigns C020 (\$106,764), C022 (\$99,635), C003 (\$86,452), C019 (\$85,306), and C001 (\$83,353) generated the most revenue.
*   **Wasted Spend Areas**:
    *   The average ROAS across all campaigns was 4.62.
    *   3,327 campaigns were identified with potential "wasted spend," characterized by spending above the average while yielding below-average ROAS. For example, Campaign C006 on the 'search' channel spent \$42,715 but only achieved a ROAS of 0.75.
*   **KPI Trends (Daily/Weekly)**: Daily and weekly trends for Conversion Rate (CVR), ROAS, and Revenue were calculated and visualized, revealing fluctuations over time. Promotional flags were successfully overlaid, showing potential correlations between promotional activities and spikes in performance metrics.
*   **Channel x Device Segment Performance**:
    *   `organic` traffic on `web` devices exhibits the highest ROAS (16.58).
    *   `paid_social` campaigns on `mobile` devices show the lowest ROAS (0.59), indicating significant inefficiency.
    *   Generally, `web` devices outperform `mobile` devices in both CVR and ROAS across most channels.
    *   `email` on `web` has a high CVR of 4.53%.
*   **New vs. Returning User Performance**:
    *   Returning users (ROAS 1.45, CVR 2.46%) contribute the vast majority of sessions, orders, and revenue.
    *   New users (ROAS 1.38, CVR 2.72%) have a slightly higher CVR but a marginally lower ROAS and much lower overall volume compared to returning users.
*   **City Tier Performance**: Performance metrics (CVR and ROAS) are relatively consistent across all three city tiers, with City Tier 1 showing a slightly higher CVR (2.53%) and ROAS (1.49).
*   **Product Category Performance**: Revenue and margin performance were evaluated for various categories (Beauty, Books, Electronics, Fashion, Home, Sports), showing their individual contributions.
*   **Key Segment Differences for Optimization**:
    *   **Largest CVR Differences**: Observed in Channel x Device segments, specifically `referral` (0.0077 difference between web and mobile), `search` (0.0069), and `paid_social` (0.0064), where mobile consistently underperforms web.
    *   **Largest ROAS Differences**: Also in Channel x Device segments: `organic` (4.9436 difference between web and mobile), `email` (0.4594), and `referral` (0.3800), with mobile again showing lower ROAS.
*   **Identified Spend Inefficiency Areas**:
    *   **Campaign-level inefficiency**: High-spending campaigns like C006 on 'search' channel with very low ROAS (0.75). Possible reasons include poor targeting or irrelevant ad creatives.
    *   **Channel-device segment inefficiency**: The 'paid\_social' channel on 'mobile' devices, exhibiting the lowest ROAS (0.59). Potential causes include mobile user experience issues or unoptimized mobile ad creatives.

### Insights or Next Steps
*   **Prioritize Mobile Optimization for Key Channels**: Given the consistent underperformance of mobile devices in both CVR and ROAS across various channels (especially `paid_social`, `referral`, `search`, `organic`, and `email`), invest in improving mobile user experience, tailoring ad creatives, and refining targeting strategies specifically for mobile users.
*   **Reallocate Budget from Underperforming Campaigns/Segments**: Systematically reduce or reallocate spend from campaigns identified as having "wasted spend" (high spend, low ROAS) and from inefficient channel-device segments (e.g., `paid_social` on `mobile`). Redirect these funds towards high-performing segments like `organic` and `email` on `web` to maximize overall ROI.
*   **Develop Differentiated Strategies for User Segments**: Leverage the understanding of new versus returning users. While returning users drive the bulk of revenue, the slightly higher CVR for new users suggests potential if acquisition efforts are optimized for efficiency. Implement targeted strategies for each group, focusing on initial conversion for new users and loyalty/retention for returning users.
*   **Experiment to Address Inefficiencies**: Conduct targeted A/B tests on ad creatives, landing pages, and audience segmentation for identified inefficient areas (e.g., Campaign C006, paid\_social mobile). This data-driven approach will help uncover the root causes of underperformance and guide optimization efforts.

# Task
The task is complete.

## Implement Last-Touch Attribution

### Subtask:
Implement last-touch attribution by identifying the channel of the last session preceding each order. Calculate the total attributed revenue and attributed orders for each channel.

**Reasoning**:
To implement last-touch attribution, I will first filter the `fact_sessions` DataFrame to isolate sessions that resulted in a purchase. Then, I will group these purchase-sessions by 'channel' and sum their 'gross_revenue' and count 'order_id' to determine the attributed revenue and orders for each channel.
"""

last_touch_attribution = fact_sessions[fact_sessions["purchase_flag"] == 1].groupby("channel").agg(
    attributed_revenue=("gross_revenue", "sum"),
    attributed_orders=("order_id", "count")
).reset_index()

print("Last-Touch Attribution by Channel:")
print(last_touch_attribution)

"""## Calculate Attributed ROAS by Channel

### Subtask:
Calculate the Return on Ad Spend (ROAS) for each channel based on the last-touch attributed revenue and the total spend for each channel. Display the attributed ROAS.

**Reasoning**:
To calculate the attributed ROAS, I will first calculate the total spend for each channel from the `fact_channel_daily` DataFrame as instructed. This will prepare the necessary spend data for merging with the last-touch attribution results.
"""

channel_spend = fact_channel_daily.groupby("channel")["spend"].sum().reset_index()

print("Total Spend by Channel:")
print(channel_spend)

"""**Reasoning**:
To calculate the attributed ROAS, I will merge the `last_touch_attribution` DataFrame with the `channel_spend` DataFrame. Then, I will calculate the `attributed_ROAS` for each channel, handling potential division by zero, and print the results.


"""

attributed_roas_by_channel = last_touch_attribution.merge(
    channel_spend,
    on="channel",
    how="left"
)

attributed_roas_by_channel["attributed_ROAS"] = np.where(
    attributed_roas_by_channel["spend"] > 0,
    attributed_roas_by_channel["attributed_revenue"] / attributed_roas_by_channel["spend"],
    0
)

print("Attributed ROAS by Channel (Last-Touch Attribution):")
print(attributed_roas_by_channel[["channel", "attributed_revenue", "spend", "attributed_ROAS"]])

"""## Compare Attribution vs. Simple ROAS

### Subtask:
Compare the newly calculated attributed ROAS with the simple ROAS (calculated previously) for each channel and explain any observed differences.

**Reasoning**:
To compare the two ROAS calculations, I will first merge the `channel_roas` DataFrame with the `attributed_roas_by_channel` DataFrame on the 'channel' column and rename the ROAS columns for clarity, as instructed.
"""

roas_comparison = channel_roas.merge(
    attributed_roas_by_channel[['channel', 'attributed_ROAS']],
    on="channel",
    how="left"
)

roas_comparison.rename(columns={
    "ROAS": "simple_ROAS",
    "attributed_ROAS": "last_touch_ROAS"
}, inplace=True)

print("ROAS Comparison (Simple vs. Last-Touch):")
print(roas_comparison.head())

"""**Reasoning**:
To complete the comparison, I will calculate the absolute difference between 'simple_ROAS' and 'last_touch_ROAS' and then print the comparison table, as per the instructions. Afterward, I will explain the observed differences.


"""

roas_comparison["roas_difference"] = abs(roas_comparison["simple_ROAS"] - roas_comparison["last_touch_ROAS"])

print("ROAS Comparison (Simple vs. Last-Touch):")
print(roas_comparison[["channel", "simple_ROAS", "last_touch_ROAS", "roas_difference"]])

"""### Explanation of ROAS Differences (or lack thereof)

The comparison between `simple_ROAS` and `last_touch_ROAS` reveals that there is no difference between the two metrics for any of the marketing channels (`roas_difference` is 0.0 for all channels).

This outcome is due to the way `attributed_revenue` was defined and calculated throughout the previous steps:

1.  **`attributed_revenue` in `fact_channel_daily`**: In the initial setup, the `attributed_revenue` used to construct `fact_channel_daily` (and subsequently `channel_roas` for `simple_ROAS`) was derived by summing `gross_revenue` from `fact_sessions` where `purchase_flag` was 1. The `fact_sessions` DataFrame explicitly links sessions to a `channel` and `gross_revenue` from an `order_id` if a purchase occurred during that session.
2.  **`attributed_revenue` in `last_touch_attribution`**: For the `last_touch_attribution`, we explicitly filtered `fact_sessions` for `purchase_flag == 1` and then aggregated `gross_revenue` by `channel`.

Because both methods essentially sum the `gross_revenue` of *purchasing sessions* and attribute it to the `channel` of that session, they both inherently implement a last-touch attribution logic. The `simple_ROAS` calculation was not a general 'first-touch' or 'multi-touch' model but rather an implicit last-touch model from its inception, using the revenue already attributed to the session's channel. Similarly, the `spend` used in both calculations was the aggregated spend per channel.

## Prepare Daily Data for Regression Model

### Subtask:
Aggregate data to a daily level, ensuring to include daily revenue, total spend, spend disaggregated by channel (e.g., 'spend_email', 'spend_organic'), day of the week, and promo flag. This DataFrame will be used for the regression analysis.

**Reasoning**:
To prepare the daily data for regression, I will first aggregate the `fact_channel_daily` DataFrame by 'date' to get the daily revenue, total spend, day of the week, and promo flag. Concurrently, I will pivot the `fact_channel_daily` DataFrame to create columns for spend disaggregated by each channel. Then, I will merge these two DataFrames and fill any missing values in the spend columns with 0, convert the 'date' column to datetime objects and display the head and info of the resulting `daily_regression_data` DataFrame.
"""

daily_regression_data = fact_channel_daily.groupby("date").agg(
    daily_revenue=("attributed_revenue", "sum"),
    total_spend=("spend", "sum"),
    day_of_week=("day_of_week", "max"),
    promo_flag=("promo_flag", "max")
).reset_index()

spend_by_channel_pivot = fact_channel_daily.pivot_table(
    index="date",
    columns="channel",
    values="spend",
    fill_value=0
).reset_index()

spend_by_channel_pivot.columns.name = None

# Rename columns to 'spend_channelname'
spend_by_channel_pivot = spend_by_channel_pivot.rename(columns={col: f"spend_{col}" for col in spend_by_channel_pivot.columns if col != "date"})

daily_regression_data = daily_regression_data.merge(
    spend_by_channel_pivot,
    on="date",
    how="left"
)

# Fill any remaining NaN values in the newly merged spend columns with 0
for col in spend_by_channel_pivot.columns:
    if col != "date":
        daily_regression_data[col] = daily_regression_data[col].fillna(0)

# Convert 'date' column to datetime objects
daily_regression_data["date"] = pd.to_datetime(daily_regression_data["date"])

print("Daily Regression Data Head:")
print(daily_regression_data.head())

print("\nDaily Regression Data Info:")
daily_regression_data.info()

"""## Split Data and Train Regression Model

### Subtask:
Split the prepared daily data into training and testing sets. Build and train a regression model (e.g., Linear Regression) to understand how spend, day of week, and promo flag affect daily revenue.

**Reasoning**:
To begin the regression analysis, I will import the necessary modules, define the features and target variable based on the `daily_regression_data` DataFrame, and then split the data into training and testing sets. After splitting, I will instantiate and train a Linear Regression model.
"""

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# Define features (X) and target variable (y)
X = daily_regression_data[['total_spend', 'day_of_week', 'promo_flag',
                           'spend_email', 'spend_organic', 'spend_paid_social',
                           'spend_referral', 'spend_search']]
y = daily_regression_data['daily_revenue']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Instantiate the Linear Regression model
model = LinearRegression()

# Train the model
model.fit(X_train, y_train)

print("Linear Regression model trained successfully.")

"""## Evaluate Regression Model

### Subtask:
Evaluate the performance of the trained regression model using metrics such as R-squared, Mean Absolute Error (MAE), and Mean Absolute Percentage Error (MAPE) on the test set.

**Reasoning**:
To evaluate the performance of the trained regression model, I will first predict the daily revenue on the test set. Then, I will import the necessary metrics from `sklearn.metrics` and `numpy` to calculate R-squared, Mean Absolute Error (MAE), and Mean Absolute Percentage Error (MAPE), ensuring to handle potential division by zero for MAPE. Finally, I will print these metrics.
"""

from sklearn.metrics import r2_score, mean_absolute_error
import numpy as np

# 1. Predict the daily revenue on the test set
y_pred = model.predict(X_test)

# 3. Calculate R-squared
r_squared = r2_score(y_test, y_pred)

# 4. Calculate Mean Absolute Error (MAE)
mae = mean_absolute_error(y_test, y_pred)

# 5. Calculate Mean Absolute Percentage Error (MAPE)
# Filter out cases where y_test is zero to avoid division by zero
y_test_non_zero = y_test[y_test != 0]
y_pred_non_zero = y_pred[y_test != 0]

if len(y_test_non_zero) > 0:
    mape = np.mean(np.abs((y_test_non_zero - y_pred_non_zero) / y_test_non_zero)) * 100
else:
    mape = np.nan # Or handle as appropriate if all y_test values could be zero

# 6. Print the calculated metrics
print("\nModel Evaluation on Test Set:")
print(f"R-squared: {r_squared:.4f}")
print(f"Mean Absolute Error (MAE): {mae:.2f}")
print(f"Mean Absolute Percentage Error (MAPE): {mape:.2f}%")

"""## Interpret Regression Model Impact

### Subtask:
Interpret the coefficients (or feature importances) of the regression model to identify which channels or factors have the highest impact on daily revenue. Discuss the implications, including the distinction between correlation and causation.

**Reasoning**:
To interpret the regression model, I will extract the coefficients from the trained model and associate them with their corresponding feature names. Then, I will sort them by absolute value to identify the most impactful factors on daily revenue and print them.
"""

coefficients = model.coef_
feature_names = X_train.columns

coefficients_df = pd.DataFrame({
    'Feature': feature_names,
    'Coefficient': coefficients
})

coefficients_df['Absolute_Coefficient'] = np.abs(coefficients_df['Coefficient'])
coefficients_df = coefficients_df.sort_values(by='Absolute_Coefficient', ascending=False)

print("Regression Model Coefficients (Sorted by Absolute Value):\n")
print(coefficients_df[['Feature', 'Coefficient']])

"""### Interpretation of Regression Model Coefficients and Implications

The regression model's coefficients indicate the estimated change in daily revenue for a one-unit change in the corresponding feature, holding all other features constant.

**Most Impactful Factors on Daily Revenue (Based on Absolute Coefficient Value):**

1.  **`promo_flag` (Coefficient: 139381.84)**: This is by far the most impactful factor. A positive coefficient indicates that on days when a promotion is active (`promo_flag` = 1), daily revenue is estimated to increase by approximately $139,382 compared to non-promotion days. This highlights the significant positive effect of promotional activities.

2.  **`day_of_week` (Coefficient: -202.97)**: This coefficient suggests a slight negative relationship with daily revenue. While its absolute value is much smaller than `promo_flag`, it implies that for each increment in the day of the week index (e.g., from Monday=0 to Tuesday=1), daily revenue slightly decreases. This might indicate that revenue tends to be higher earlier in the week or during weekends (if Sunday is at the end of the index).

3.  **`spend_organic` (Coefficient: 7.37)**: This channel's spend has a strong positive impact. For every dollar spent on organic marketing, daily revenue is estimated to increase by $7.37. This aligns with the high ROAS observed for the organic channel in previous analyses, suggesting it's a highly efficient spending area.

4.  **`total_spend` (Coefficient: 2.64)**: This coefficient represents the overall impact of aggregated spend across all channels (excluding the individual channel spends, which are also in the model to capture channel-specific effects). It suggests that, broadly, increased total spending tends to increase daily revenue, albeit to a lesser extent than organic spend alone.

5.  **`spend_search` (Coefficient: -2.06)**, **`spend_paid_social` (Coefficient: -1.97)**, **`spend_referral` (Coefficient: -1.16)**: Surprisingly, these channels show negative coefficients for their individual spend. This implies that, when controlling for other factors, an increase in spend specifically on these channels might be associated with a decrease in daily revenue. This could be indicative of diminishing returns, inefficient spending, or competitive bidding driving costs up without proportional revenue gains. This aligns with the lower ROAS values observed for `paid_social` and `search` in earlier analyses.

6.  **`spend_email` (Coefficient: 0.46)**: Email spend shows a small positive coefficient, meaning every dollar spent on email marketing is associated with a $0.46 increase in daily revenue. While positive, it's less impactful than organic spend.

**Implications:**

*   **Promotions are Key**: Promotional activities are a major driver of daily revenue. Businesses should strategically plan and execute promotions, potentially increasing their frequency or scale, provided they remain profitable.
*   **Organic Channel Efficiency**: The high coefficient for organic spend reinforces its importance. Efforts to improve SEO, content marketing, and other organic strategies are likely to yield high returns.
*   **Review Inefficient Channels**: The negative coefficients for `search`, `paid_social`, and `referral` spend are concerning. This suggests that simply increasing spend in these areas may not be effective. A deeper dive into campaign specifics (targeting, creatives, bids, landing pages) is needed to convert these negative impacts into positive ones.

**Correlation vs. Causation:**

It is crucial to understand that these coefficients represent **correlation**, not necessarily **causation**. The regression model identifies statistical relationships and how changes in independent variables are associated with changes in the dependent variable (daily revenue). For example:

*   A high positive coefficient for `promo_flag` correlates with higher revenue on promotion days. While it's intuitively causal (promotions cause more sales), the model doesn't *prove* this causation; it merely quantifies the observed association.
*   The negative coefficients for `spend_search`, `spend_paid_social`, and `spend_referral` suggest a negative correlation. This doesn't inherently mean that spending *less* will automatically increase revenue. It could be that these channels are used in ways that are currently inefficient, or there might be underlying factors not captured by the model that influence both increased spend and decreased revenue (e.g., spending more in a saturated market).

**Importance in Business Context:**

In a business context, recognizing this distinction is vital to avoid drawing incorrect conclusions and making suboptimal decisions. While the model provides valuable insights into which factors are statistically associated with revenue, it should be used as a guide for further investigation and experimentation. For example, instead of immediately cutting spend in negatively correlated channels, one should investigate *why* this correlation exists through A/B testing, deeper qualitative analysis, and controlled experiments to establish true causal links and optimize strategies effectively.

## Propose Budget Allocation Strategy

### Subtask:
Propose a new monthly budget allocation across channels, considering a hypothetical total monthly budget and specific minimum/maximum spend constraints for each channel.

**Reasoning**:
To propose a new budget allocation strategy, I will first define a hypothetical total monthly budget and set minimum and maximum spend constraints for each channel. This provides the framework within which the budget optimization will occur. I will also consolidate the relevant ROAS and regression coefficient data for easier access in the next steps.
"""

total_monthly_budget = 40000000 # Example hypothetical total monthly budget, adjust as needed

min_spend_constraints = {
    'email': 500000,
    'organic': 100000,
    'paid_social': 5000000,
    'referral': 1000000,
    'search': 10000000
}

max_spend_constraints = {
    'email': 5000000,
    'organic': 1500000,
    'paid_social': 20000000,
    'referral': 5000000,
    'search': 25000000
}

# Consolidate ROAS and regression coefficients for easier access
channel_performance_data = attributed_roas_by_channel[['channel', 'attributed_ROAS']].copy()

# Extract spend coefficients from the regression model
spend_coefficients = coefficients_df[coefficients_df['Feature'].str.startswith('spend_')]
spend_coefficients['channel'] = spend_coefficients['Feature'].str.replace('spend_', '')

channel_performance_data = channel_performance_data.merge(
    spend_coefficients[['channel', 'Coefficient']],
    on='channel',
    how='left'
)
channel_performance_data.rename(columns={'Coefficient': 'regression_coefficient'}, inplace=True)

print(f"Hypothetical Total Monthly Budget: ${total_monthly_budget:,.2f}")
print("\nMinimum Spend Constraints:")
for channel, amount in min_spend_constraints.items():
    print(f"- {channel}: ${amount:,.2f}")

print("\nMaximum Spend Constraints:")
for channel, amount in max_spend_constraints.items():
    print(f"- {channel}: ${amount:,.2f}")

print("\nChannel Performance Data (ROAS and Regression Coefficients):")
print(channel_performance_data)

"""**Reasoning**:
The previous step successfully defined budget constraints and consolidated performance data, but produced a `SettingWithCopyWarning`. I will fix this by explicitly creating a copy of the DataFrame before modifying it. Then, I will propose a new budget allocation based on the defined constraints, ROAS, and regression coefficients, prioritizing channels with positive revenue impact. Finally, I will print the proposed allocation with a clear rationale.


"""

total_monthly_budget = 40000000 # Example hypothetical total monthly budget, adjust as needed

min_spend_constraints = {
    'email': 500000,
    'organic': 100000,
    'paid_social': 5000000,
    'referral': 1000000,
    'search': 10000000
}

max_spend_constraints = {
    'email': 5000000,
    'organic': 1500000,
    'paid_social': 20000000,
    'referral': 5000000,
    'search': 25000000
}

# Consolidate ROAS and regression coefficients for easier access
channel_performance_data = attributed_roas_by_channel[['channel', 'attributed_ROAS']].copy()

# Extract spend coefficients from the regression model, and create a copy to avoid SettingWithCopyWarning
spend_coefficients = coefficients_df[coefficients_df['Feature'].str.startswith('spend_')].copy()
spend_coefficients['channel'] = spend_coefficients['Feature'].str.replace('spend_', '')

channel_performance_data = channel_performance_data.merge(
    spend_coefficients[['channel', 'Coefficient']],
    on='channel',
    how='left'
)
channel_performance_data.rename(columns={'Coefficient': 'regression_coefficient'}, inplace=True)

print(f"Hypothetical Total Monthly Budget: ${total_monthly_budget:,.2f}")
print("\nMinimum Spend Constraints:")
for channel, amount in min_spend_constraints.items():
    print(f"- {channel}: ${amount:,.2f}")

print("\nMaximum Spend Constraints:")
for channel, amount in max_spend_constraints.items():
    print(f"- {channel}: ${amount:,.2f}")

print("\nChannel Performance Data (ROAS and Regression Coefficients):")
print(channel_performance_data)

proposed_budget_allocation = {channel: min_spend_constraints[channel] for channel in min_spend_constraints}
current_total_spend = sum(proposed_budget_allocation.values())
remaining_budget = total_monthly_budget - current_total_spend

# Create a DataFrame for channels with positive regression coefficients, sorted by coefficient
channels_to_increase_df = channel_performance_data[channel_performance_data['regression_coefficient'] > 0].sort_values(
    by='regression_coefficient', ascending=False
).copy()

while remaining_budget > 0 and not channels_to_increase_df.empty:
    # Calculate how much each eligible channel can still take
    channels_to_increase_df['current_spend'] = channels_to_increase_df['channel'].map(proposed_budget_allocation)
    channels_to_increase_df['capacity'] = channels_to_increase_df['channel'].map(max_spend_constraints) - channels_to_increase_df['current_spend']

    # Filter out channels that are already at max capacity or have negative capacity
    channels_to_increase_df = channels_to_increase_df[channels_to_increase_df['capacity'] > 0]

    if channels_to_increase_df.empty:
        break # No more channels can take budget

    # Sum of positive coefficients for available channels
    total_positive_coeff = channels_to_increase_df['regression_coefficient'].sum()

    if total_positive_coeff == 0: # Avoid division by zero if all remaining have 0 or negative coefficients
        break

    # Distribute remaining budget proportionally
    distributed_this_round = 0
    for index, row in channels_to_increase_df.iterrows():
        channel_name = row['channel']
        coeff = row['regression_coefficient']
        capacity = row['capacity']

        if coeff > 0: # Only distribute to channels with positive coefficients
            share_of_budget = (coeff / total_positive_coeff) * remaining_budget
            amount_to_add = min(share_of_budget, capacity)

            proposed_budget_allocation[channel_name] += amount_to_add
            distributed_this_round += amount_to_add

    remaining_budget -= distributed_this_round

    # After distribution, remove channels that hit their max for the next round
    channels_to_increase_df = channels_to_increase_df[
        channels_to_increase_df['channel'].map(lambda c: proposed_budget_allocation[c]) < channels_to_increase_df['channel'].map(max_spend_constraints)
    ]

# Ensure total budget is used if possible (may not be if max constraints are hit early)
# Distribute any final remaining budget to the highest coeff channel that still has capacity
if remaining_budget > 0 and not channels_to_increase_df.empty:
    channels_to_increase_df['current_spend'] = channels_to_increase_df['channel'].map(proposed_budget_allocation)
    channels_to_increase_df['capacity'] = channels_to_increase_df['channel'].map(max_spend_constraints) - channels_to_increase_df['current_spend']
    channels_to_increase_df = channels_to_increase_df[channels_to_increase_df['capacity'] > 0]

    if not channels_to_increase_df.empty:
        # Find the channel with the highest positive coefficient that still has capacity
        highest_coeff_channel = channels_to_increase_df.sort_values(by='regression_coefficient', ascending=False).iloc[0]
        channel_name = highest_coeff_channel['channel']
        amount_to_add = min(remaining_budget, highest_coeff_channel['capacity'])
        proposed_budget_allocation[channel_name] += amount_to_add
        remaining_budget -= amount_to_add

# Round to two decimal places for currency
for channel in proposed_budget_allocation:
    proposed_budget_allocation[channel] = round(proposed_budget_allocation[channel], 2)

print("\nProposed Monthly Budget Allocation:")
total_allocated = 0
for channel, spend in proposed_budget_allocation.items():
    print(f"- {channel}: ${spend:,.2f}")
    total_allocated += spend
print(f"Total Allocated: ${total_allocated:,.2f} (Remaining: ${total_monthly_budget - total_allocated:,.2f})")

print("\nRationale for Proposed Allocation:")
print("- Organic and Email channels receive increased allocations due to their high ROAS and positive regression coefficients, indicating they are efficient in driving revenue. Organic, with the highest coefficient, is prioritized to its maximum spend capacity.")
print("- Paid Social, Search, and Referral channels are maintained at their minimum spend constraints. This decision is primarily driven by their lower ROAS and, critically, their negative regression coefficients, suggesting that additional spend in these areas may not yield positive returns and could even decrease overall revenue. Further investigation into optimizing these channels (e.g., targeting, creatives) is recommended before increasing their budgets.")
print("- The allocation respects all defined minimum and maximum spend constraints for each channel, ensuring a balanced approach within practical limits.")

"""## Estimate Budget Impact and Assumptions

### Subtask:
Estimate the expected revenue increase based on the proposed budget allocation, providing best and worst-case scenarios. Clearly state all assumptions made during the budget recommendation process.

**Reasoning**:
To estimate the baseline current revenue, I will use the features from the entire `daily_regression_data` DataFrame and predict the revenue using the trained linear regression model. This will give us the model's estimate of the total revenue over the period, serving as the baseline for comparison with the proposed budget allocation.
"""

current_features = daily_regression_data[X.columns] # Use the same feature columns as used for training
estimated_current_revenue = model.predict(current_features).sum()

print(f"Estimated Current Total Revenue (Model-predicted Baseline): ${estimated_current_revenue:,.2f}")

"""**Reasoning**:
To calculate the estimated new revenue based on the proposed budget, I will first convert the monthly proposed budget for each channel into a daily spend. Then, I will create a hypothetical feature set representing an 'average' day using these daily spend values, alongside the average historical values for 'day_of_week' and 'promo_flag'. This feature set will be used with the trained regression model to predict the average daily revenue, which will then be scaled up to a total estimated revenue for the entire period covered by the historical data (211 days) to allow for direct comparison with the baseline. Finally, I will calculate the best-case and worst-case scenarios for revenue increase by applying a +/- 10% variation and list the assumptions made during this process.


"""

num_historical_days = len(daily_regression_data)

# Calculate average values for day_of_week and promo_flag from historical data
avg_day_of_week = daily_regression_data['day_of_week'].mean()
avg_promo_flag = daily_regression_data['promo_flag'].mean()

# Convert proposed monthly budget to daily spend for each channel
daily_proposed_spend = {}
for channel, monthly_spend in proposed_budget_allocation.items():
    daily_proposed_spend[f'spend_{channel}'] = monthly_spend / num_historical_days

# Calculate total daily spend for the proposed budget
daily_total_proposed_spend = sum(daily_proposed_spend.values())

# Create a DataFrame for the new features (representing one 'average' day)
new_features_data = {
    'total_spend': [daily_total_proposed_spend],
    'day_of_week': [avg_day_of_week],
    'promo_flag': [avg_promo_flag]
}

for channel_col in daily_proposed_spend:
    new_features_data[channel_col] = [daily_proposed_spend[channel_col]]

# Ensure the order of columns matches the training features
new_features_df = pd.DataFrame(new_features_data, columns=X.columns)

# Predict the average daily revenue with the new budget allocation
estimated_avg_daily_new_revenue = model.predict(new_features_df)[0]

# Scale up to total revenue for the entire historical period (211 days)
estimated_new_total_revenue = estimated_avg_daily_new_revenue * num_historical_days

# Calculate estimated revenue increase
estimated_revenue_increase = estimated_new_total_revenue - estimated_current_revenue

# Define best-case and worst-case scenarios (e.g., +/- 10% variation)
variation_percentage = 0.10 # 10%
best_case_increase = estimated_revenue_increase * (1 + variation_percentage)
worst_case_increase = estimated_revenue_increase * (1 - variation_percentage)

print(f"\nEstimated New Total Revenue with Proposed Budget: ${estimated_new_total_revenue:,.2f}")
print(f"Estimated Revenue Increase: ${estimated_revenue_increase:,.2f}")
print(f"Best-Case Revenue Increase (+{variation_percentage*100:.0f}%): ${best_case_increase:,.2f}")
print(f"Worst-Case Revenue Increase (-{variation_percentage*100:.0f}%): ${worst_case_increase:,.2f}")

print("\n--- Assumptions Made During Budget Recommendation ---")
print(f"1. Total Hypothetical Monthly Budget: ${total_monthly_budget:,.2f}")
print("2. Minimum and Maximum Spend Constraints: Defined for each channel as specified previously.")
print("3. Model Accuracy: The linear regression model accurately captures the relationship between spend and revenue (R-squared: {r_squared:.2f}).")
print("4. Linearity: The relationship between spend and revenue is linear within the range of proposed spend changes.")
print("5. Ceteris Paribus: All other factors not included in the model, or included but held constant (e.g., average day_of_week, average promo_flag), remain constant.")
print("6. Channel Independence: The impact of spend on one channel is independent of spend on other channels (as per a basic linear model without interaction terms).")
print("7. Time Horizon: The proposed budget and its impact are considered over a period equivalent to the historical data (211 days in this case, scaled from a 'monthly' budget assumption).")
print("8. No Diminishing Returns Beyond Max: For channels where budget was increased to max, it's assumed that the linear relationship holds up to that max, though in reality, diminishing returns often occur.")
print("9. External Factors: No unforeseen external factors (e.g., economic downturns, competitor actions) will significantly impact performance.")
print(f"10. Promotional Activity: The average historical frequency of promotional activities (avg promo_flag: {avg_promo_flag:.2f}) is maintained.")
print(f"11. Day of Week Effect: The average historical distribution of day of week effects (avg day_of_week: {avg_day_of_week:.2f}) is maintained.")

"""## Final Task

### Subtask:
Provide a comprehensive summary of all findings from the attribution analysis, regression model, and budget recommendations, including key performance insights, segment differences, and actionable recommendations for optimizing spend and campaign performance.

## Summary:

### Q&A

1.  **What are the key findings from the attribution analysis?**
    The last-touch attribution analysis revealed that the `search` channel generated the highest attributed revenue at \$44,856,523.34 and the most orders (6,681). Conversely, `referral` had the lowest attributed revenue at \$8,900,372.31 and the fewest orders (1,307). When calculating Return on Ad Spend (ROAS), `organic` marketing demonstrated the highest attributed ROAS of 13.82, indicating high efficiency, while `paid_social` had the lowest attributed ROAS of 0.73. The comparison between "simple ROAS" and "last-touch ROAS" showed no differences, as the initial "simple ROAS" calculation implicitly utilized a last-touch attribution logic.

2.  **How did the regression model perform, and what factors have the highest impact on daily revenue?**
    The regression model achieved an R-squared value of 0.6071, explaining approximately 61% of the variance in daily revenue, with a Mean Absolute Error (MAE) of \$54,182.53 and a Mean Absolute Percentage Error (MAPE) of 11.16%.
    The most impactful factors on daily revenue, based on coefficient analysis, were:
    *   **`promo_flag`**: Estimated to increase daily revenue by approximately \$139,381.84 on promotion days.
    *   **`spend_organic`**: Associated with a strong positive return, estimating \$7.37 in revenue for every dollar spent.
    *   **`total_spend`**: Showed a positive impact, estimating a \$2.64 revenue increase per dollar of total spend.
    *   **`spend_search`**, **`spend_paid_social`**, and **`spend_referral`**: Surprisingly, these channels exhibited *negative* coefficients (e.g., `spend_search`: -2.06, `spend_paid_social`: -1.97), suggesting that increased individual spend in these areas might be associated with a *decrease* in daily revenue.
    *   **`spend_email`**: Had a small positive impact of \$0.46 in revenue per dollar spent.

3.  **What budget recommendations were made, and what was their estimated revenue impact?**
    A new monthly budget allocation strategy was proposed based on a hypothetical total budget of \$40,000,000, respecting defined minimum and maximum spend constraints. The proposed allocation was: `email`: \$5,000,000 (maxed out), `organic`: \$1,500,000 (maxed out), `paid_social`: \$5,000,000 (minimized), `referral`: \$1,000,000 (minimized), and `search`: \$10,000,000 (minimized).
    This allocation resulted in a total allocated budget of \$22,500,000, leaving \$17,500,000 unallocated.
    The estimated revenue impact of this proposed budget allocation was a *decrease* in total revenue of approximately \$29,057,689.31 compared to the baseline. Best-case and worst-case scenarios, assuming a +/- 10% variation, projected a potential revenue decrease ranging from \$26,151,920.38 to \$31,963,458.24.

### Data Analysis Key Findings

*   **Attribution Model Consistency**: Both implicitly "simple" and explicitly "last-touch" ROAS calculations yielded identical results, demonstrating that revenue attribution for ROAS was consistently last-touch.
*   **Organic Channel Efficiency**: `Organic` marketing consistently shows high efficiency with the highest attributed ROAS (13.82) and a strong positive regression coefficient (\$7.37 revenue increase per \$1 spend).
*   **Inefficiencies in Key Paid Channels**: `Paid_social` and `search` exhibit low attributed ROAS (0.73 and 1.31, respectively) and, critically, *negative* regression coefficients (-1.97 and -2.06), suggesting that increasing spend in these channels might lead to revenue decline. `Referral` also showed a negative regression coefficient (-1.16) despite a slightly better ROAS (1.33).
*   **Promotional Activities are High Impact**: The `promo_flag` was identified as the most significant driver of daily revenue, with an estimated increase of approximately \$139,382 on promotional days.
*   **Budget Allocation Paradox**: The proposed budget reallocation, which minimized spend in negatively correlated channels and maximized spend in positively correlated ones (within constraints), paradoxically led to an estimated overall *decrease* in total revenue by approximately \$29.06 million compared to the current baseline. This is partly due to a significant portion of the total budget (\$17.5 million) remaining unallocated because increasing spend in other channels was estimated to be detrimental.
*   **Model Performance**: The linear regression model, with an R-squared of 0.6071 and MAPE of 11.16%, provides a reasonable, but not perfect, fit for predicting daily revenue.

### Insights or Next Steps

*   **Re-evaluate Spend in Underperforming Channels**: The negative regression coefficients for `paid_social`, `search`, and `referral` are a critical finding. Instead of simply minimizing spend, a deeper investigation is required to understand *why* these channels are not performing, potentially through A/B testing, campaign audits, or exploring different targeting and creative strategies.
*   **Capitalize on Organic and Promotional Success**: Maximizing investment in `organic` strategies up to its effective capacity and strategically scaling promotional activities are clear opportunities to drive profitable revenue growth.
*   **Refine Budget Optimization Approach**: The fact that a significant portion of the hypothetical budget remained unallocated due to estimated negative returns highlights a need to reconsider the current budget optimization framework. This could involve exploring non-linear models, considering interaction effects between channels, or setting different constraints where negative impacts are detected.
*   **Acknowledge Correlation vs. Causation**: While the model identifies strong correlations, it doesn't prove causation. Business decisions, especially for channels with negative coefficients, should be validated through controlled experiments to establish true causal links and avoid potentially misleading interpretations.
"""