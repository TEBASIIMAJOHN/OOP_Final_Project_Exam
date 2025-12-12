# Automation script to refresh and export Plotly dashboards for TNDailySales dataset

"""
refresh_and_export.py
Automated ETL + Plotly Dashboard Export for TNDailySales.csv
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Resolve BASE_DIR 

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

DATA_PATH = os.path.join(BASE_DIR, "TNDailySales.csv")
CLEANED_PATH = os.path.join(BASE_DIR, "data", "TNDailySales_cleaned.csv")
EXPORT_DIR = os.path.join(BASE_DIR, "outputs")

os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

# 1. LOAD RAW DATA

df = pd.read_csv(DATA_PATH)

# 2. FIX COLUMN NAMES (critical to prevent zeros)

df.columns = (
    df.columns
    .str.strip()
    .str.replace("\uFEFF", "", regex=False)  # remove BOM
    .str.replace("\xa0", "", regex=False)    # remove non-breaking spaces
)

# 3. STANDARDIZE COLUMN NAMES

rename_map = {
    "channelname": "ChannelName",
    "productcategory": "ProductCategory",
    "date": "Date",
    "productname": "ProductName",
    "netweightkgs": "NetWeightKGs",
    "salescategory": "SalesCategory",
    "paymenttype": "PaymentType",
    "customername": "CustomerName",
}

clean_cols = {}
for col in df.columns:
    key = col.lower().replace(" ", "").replace("_", "")
    clean_cols[col] = rename_map.get(key, col)

df = df.rename(columns=clean_cols)


#CLEANING PIPELINE

# Ensure mandatory columns exist
required = ["ChannelName", "ProductName", "Date", "NetWeightKGs",
            "ProductCategory", "SalesCategory"]
for col in required:
    if col not in df.columns:
        df[col] = np.nan

# Fix date
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# Fix numeric columns
df["NetWeightKGs"] = pd.to_numeric(df["NetWeightKGs"], errors="coerce").fillna(0)


# Fix text
for col in ["ChannelName", "ProductCategory", "ProductName", "SalesCategory"]:
    df[col] = df[col].astype(str).fillna("").str.strip()

# Drop invalid rows
df = df.dropna(subset=["Date"])
df = df[df["ProductName"] != ""]
df = df[df["ChannelName"] != ""]

# Remove duplicates
df = df.drop_duplicates()

# Derived fields
df["Month"] = df["Date"].dt.to_period("M").astype(str)
df["Day"] = df["Date"].dt.date

# Save cleaned dataset
df.to_csv(CLEANED_PATH, index=False)


# DASHBOARDS (PLOTLY EXPORTS)

#KPI calculations 
total_sales = df["NetWeightKGs"].sum()
total_txn = len(df)

# BAR: Channel vs NetWeightKGs

fig_channel = px.bar(
    df.groupby("ChannelName")["NetWeightKGs"].sum().sort_values(ascending=False).reset_index(),
    x="ChannelName",
    y="NetWeightKGs",
    title="Sales Volume by Channel (KGs)",
)
fig_channel.write_html(os.path.join(EXPORT_DIR, "channel_sales.html"))


# LINE: Monthly Trend

month_order = sorted(df["Month"].unique())
fig_month = px.line(
    df.groupby("Month")["NetWeightKGs"].sum().reindex(month_order).reset_index(),
    x="Month",
    y="NetWeightKGs",
    title="Monthly Sales Trend (KGs)",
    markers=True
)
fig_month.write_html(os.path.join(EXPORT_DIR, "month_trend.html"))


# BAR: SalesCategory Breakdown

fig_salescat = px.bar(
    df.groupby("SalesCategory")["NetWeightKGs"].sum().reset_index(),
    x="SalesCategory",
    y="NetWeightKGs",
    title="Sales Category Breakdown (KGs)"
)
fig_salescat.write_html(os.path.join(EXPORT_DIR, "sales_category.html"))


# BAR: ProductName Breakdown

fig_product = px.bar(
    df.groupby("ProductName")["NetWeightKGs"].sum().reset_index(),
    x="ProductName",
    y="NetWeightKGs",
    title="Products Ranked by Sales Weight"
)
fig_product.write_html(os.path.join(EXPORT_DIR, "product_sales.html"))

# INTERACTIVE DASHBOARD WITH MONTH FILTER


from plotly.subplots import make_subplots

unique_months = sorted(df["Month"].unique())

fig_filtered = make_subplots(
    rows=2, cols=2,
    subplot_titles=(
        "Channel Sales",
        "Sales Category Performance",
        "Product Sales",
        "Monthly Sales Variation"
    )
)



def filter_month(m):
    return df[df["Month"] == m]

# initial month
initial = filter_month(unique_months[0])

# Channel
fig_filtered.add_trace(
    go.Bar(
        x=initial.groupby("ChannelName")["NetWeightKGs"].sum().index,
        y=initial.groupby("ChannelName")["NetWeightKGs"].sum().sort_values(ascending=False).values,
        name="Channel"
    ),
    row=1, col=1
)

#Sales category
fig_filtered.add_trace(
    go.Bar(
        x=initial.groupby("SalesCategory")["NetWeightKGs"].sum().index,
        y=initial.groupby("SalesCategory")["NetWeightKGs"].sum().sort_values(ascending=False).values,
        name="SalesCategory"
    ),
    row=1, col=2
)



# Product
fig_filtered.add_trace(
    go.Bar(
        x=initial.groupby("ProductName")["NetWeightKGs"].sum().index,
        y=initial.groupby("ProductName")["NetWeightKGs"].sum().sort_values(ascending=False).values,
        name="Product"
    ),
    row=2, col=1
)

# Monthly trend (full)
fig_filtered.add_trace(
    go.Scatter(
        x=df.groupby("Month")["NetWeightKGs"].sum().index,
        y=df.groupby("Month")["NetWeightKGs"].sum().values,
        mode="lines+markers",
        name="Trend"
    ),
    row=2, col=2
)

# Buttons for dropdown
buttons = []
for m in unique_months:
    dff = filter_month(m)

    buttons.append(
        dict(
            label=m,
            method="update",
            args=[
                {
                    "x": [
                        dff.groupby("ChannelName")["NetWeightKGs"].sum().index,
                        dff.groupby("SalesCategory")["NetWeightKGs"].sum().index,
                        dff.groupby("ProductName")["NetWeightKGs"].sum().index,
                        df.groupby("Month")["NetWeightKGs"].sum().index,
                    ],
                    "y": [
                        dff.groupby("ChannelName")["NetWeightKGs"].sum().sort_values(ascending=False).values,
                        dff.groupby("SalesCategory")["NetWeightKGs"].sum().sort_values(ascending=False).values,
                        dff.groupby("ProductName")["NetWeightKGs"].sum().sort_values(ascending=False).values,
                        df.groupby("Month")["NetWeightKGs"].sum().values,
                    ]
                }
            ]
        )
    )


fig_filtered.update_layout(
    title="Interactive Sales Dashboard (Month Filter)",
    updatemenus=[dict(
        buttons=buttons,
        direction="down",
        x=1.05,
        y=1.15
    )],
    height=900
)

fig_filtered.write_html(os.path.join(EXPORT_DIR, "interactive_dashboard.html"))


# appending all plotly charts in one html file

master_path = os.path.join(EXPORT_DIR, "master_dashboard.html")

with open(master_path, "w", encoding="utf-8") as f:
    f.write("<html><head><title>Poultry Sales Dashboard</title></head><body>")
    f.write("<h1><center>TNU Sales Dashboard</center></h1>")
    f.write("<p>Last Updated: " + str(datetime.now()) + "</p>")

    f.write("<h2><center>Sales Volume by Channel</center></h2>")
    f.write(fig_channel.to_html(full_html=False, include_plotlyjs='cdn'))

    f.write("<h2><center>Monthly Sales Trend</center></h2>")
    f.write(fig_month.to_html(full_html=False, include_plotlyjs=False))

    f.write("<h2><center>Sales Category Breakdown</center></h2>")
    f.write(fig_salescat.to_html(full_html=False, include_plotlyjs=False))

    f.write("<h2><center>Product Sales Ranking</center></h2>")
    f.write(fig_product.to_html(full_html=False, include_plotlyjs=False))

    f.write("<h2><center>Interactive Dashboard with Month Slicer</center></h2>")
    f.write(fig_filtered.to_html(full_html=False, include_plotlyjs=False))

    f.write("</body></html>")

print("Master Dashboard saved to:", master_path)


#DONE

print("Refresh completed successfully at:", datetime.now())
print("Dashboards updated in 'outputs/' and cleaned data saved in 'data/'.")
