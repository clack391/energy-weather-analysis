import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import os
import sys
from datetime import datetime

# Allow importing from src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load weather & energy data
@st.cache_data
def load_data():
    weather_path = "data/weather.csv"
    energy_path = "data/energy.csv"

    if not os.path.exists(weather_path) or not os.path.exists(energy_path):
        st.error("Missing data files. Please run the pipeline first.")
        st.stop()

    weather = pd.read_csv(weather_path, parse_dates=["date"])
    energy = pd.read_csv(energy_path, parse_dates=["date"])
    return weather, energy

weather, energy = load_data()

# Check latest dates
latest_weather_date = weather["date"].max().date()
latest_energy_date = energy["date"].max().date()
today = datetime.utcnow().date()

# Sidebar Filters
cities = sorted(weather["city"].unique())
selected_cities = st.sidebar.multiselect("Select cities:", cities, default=cities)
date_range = st.sidebar.date_input("Date range:", [weather.date.min(), weather.date.max()])

# Filter data
weather = weather[weather["city"].isin(selected_cities)]
energy = energy[energy["city"].isin(selected_cities)]
weather = weather[(weather.date >= pd.to_datetime(date_range[0])) & (weather.date <= pd.to_datetime(date_range[1]))]
energy = energy[(energy.date >= pd.to_datetime(date_range[0])) & (energy.date <= pd.to_datetime(date_range[1]))]

# Page title & freshness info
st.title("US Weather + Energy Analysis Dashboard")
st.markdown(f"**Last Refreshed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.info(f"📅 **Latest weather data:** {latest_weather_date}")
st.info(f"⚡ **Latest energy data:** {latest_energy_date}")

if (today - latest_weather_date).days > 2:
    st.warning("⚠️ Weather data may be outdated.")
if (today - latest_energy_date).days > 2:
    st.warning("⚠️ Energy data may be outdated.")

# ----------------------------------------
# Visualization 1: Geographic Overview
# ----------------------------------------
st.header("1. Geographic Overview")

coords = {
    "New York": (40.7128, -74.0060),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Seattle": (47.6062, -122.3321),
}

map_data = weather.merge(energy, on=["date", "city"], how="inner")
latest = map_data.groupby("city").last().reset_index()

latest["lat"] = latest["city"].map(lambda c: coords[c][0])
latest["lon"] = latest["city"].map(lambda c: coords[c][1])
latest["color"] = pd.qcut(latest["value"], q=2, labels=["green", "red"])

# Safe sizing
latest["size"] = latest["value"].clip(lower=0.1)
latest["size"] = (latest["size"] - latest["size"].min()) / (latest["size"].max() - latest["size"].min() + 1e-6) * 50 + 10

fig1 = px.scatter_map(
    latest,
    lat="lat",
    lon="lon",
    color="color",
    size="size",
    hover_name="city",
    hover_data=["tmax", "tmin", "value"],
    zoom=3,
)
st.plotly_chart(fig1, use_container_width=True)

# ----------------------------------------
# Visualization 2: Time Series Analysis
# ----------------------------------------
st.header("2. Time Series Analysis")
selected_city = st.selectbox("Select city:", ["All Cities"] + cities)

if selected_city == "All Cities":
    temp = weather.groupby("date")[["tmax", "tmin"]].mean()
    eng = energy.groupby("date")["value"].mean()
else:
    temp = weather[weather.city == selected_city].set_index("date")[["tmax", "tmin"]]
    eng = energy[energy.city == selected_city].set_index("date")["value"]

fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=temp.index, y=temp["tmax"], name="TMAX", line=dict(color="firebrick")))
fig2.add_trace(go.Scatter(x=temp.index, y=temp["tmin"], name="TMIN", line=dict(color="royalblue")))
fig2.add_trace(go.Scatter(x=eng.index, y=eng, name="Energy Usage", yaxis="y2", line=dict(color="green", dash="dot")))

fig2.update_layout(
    yaxis=dict(title="Temperature (°F)"),
    yaxis2=dict(title="Energy", overlaying="y", side="right"),
    legend=dict(orientation="h"),
    xaxis=dict(title="Date")
)
st.plotly_chart(fig2, use_container_width=True)

# ----------------------------------------
# Visualization 3: Correlation Analysis
# ----------------------------------------
st.header("3. Correlation Analysis")
merged = pd.merge(weather, energy, on=["city", "date"])
fig3 = px.scatter(
    merged,
    x="tmax",
    y="value",
    color="city",
    hover_data=["date"],
    trendline="ols"
)
st.plotly_chart(fig3, use_container_width=True)

# ----------------------------------------
# ----------------------------------------
# Visualization 4: Usage Patterns Heatmap
# ----------------------------------------

st.header("4. Usage Patterns Heatmap")

heat_df = merged.copy()
heat_df["temp_bin"] = pd.cut(
    heat_df["tmax"],
    bins=[-50, 50, 60, 70, 80, 90, 200],
    labels=["<50°F", "50-60°F", "60-70°F", "70-80°F", "80-90°F", ">90°F"]
)

# Correctly ordered day names
from pandas.api.types import CategoricalDtype
weekday_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
heat_df["day"] = pd.to_datetime(heat_df["date"]).dt.day_name()
heat_df["day"] = heat_df["day"].astype(CategoricalDtype(categories=weekday_order, ordered=True))

# Create pivot table
pivot = heat_df.pivot_table(
    index="temp_bin",
    columns="day",
    values="value",
    aggfunc="mean",
    observed=False
)

fig4 = px.imshow(
    pivot,
    text_auto=True,
    color_continuous_scale="RdBu",
    aspect="auto"
)
fig4.update_layout(
    title="Avg Energy Usage by Temperature & Day",
    xaxis_title="Day of Week",
    yaxis_title="Temperature Range"
)
st.plotly_chart(fig4, use_container_width=True)

