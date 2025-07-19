import streamlit as st, pandas as pd
from datetime import date

st.title("Data Quality Report")

dfw = pd.read_csv("data/weather.csv", parse_dates=["date"])
dfe = pd.read_csv("data/energy.csv", parse_dates=["date"])

# Missing
mv = dfw.isnull().sum().sum() + dfe.isnull().sum().sum()
st.subheader("Total Missing Values")
st.write(mv)

# Outliers
dfw["temp_outlier"]   = (dfw.TMAX>130)|(dfw.TMIN<-50)
dfe["energy_outlier"] = dfe.demand<0
st.subheader("Outliers")
st.write(f"{dfw.temp_outlier.sum()} temperature outliers")
st.write(f"{dfe.energy_outlier.sum()} energy outliers")

# Freshness
fw = dfw.groupby("city")["date"].max().reset_index()
fe = dfe.groupby("region")["date"].max().reset_index()
fw["stale"] = (date.today() - fw.date.dt.date).dt.days > 1
fe["stale"] = (date.today() - fe.date.dt.date).dt.days > 1
st.subheader("Data Freshness")
st.dataframe(fw)
st.dataframe(fe)
