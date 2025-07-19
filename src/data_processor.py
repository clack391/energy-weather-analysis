import pandas as pd

def merge_and_clean(city, weather_df, energy_df):
    """
    Merge weather & energy, flag missing/outliers.
    """
    df = pd.merge(weather_df, energy_df, on="date", how="outer")
    df["city"] = city
    df["missing"] = df.isnull().sum(axis=1)
    df["temp_outlier"] = df["TMAX"].gt(130) | df["TMIN"].lt(-50)
    df["energy_outlier"] = df["demand"].lt(0)
    return df

def detect_staleness(df, threshold_days=1):
    """Return True if latest date is older than threshold_days."""
    last = df["date"].max()
    return (pd.Timestamp.today().date() - last).days > threshold_days
