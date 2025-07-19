import pandas as pd
import statsmodels.api as sm

def compute_correlation(df):
    """
    Return dict with r, r2, slope, intercept for demand ~ TMAX.
    """
    clean = df.dropna(subset=["TMAX","demand"])
    r = clean["TMAX"].corr(clean["demand"])
    model = sm.OLS(clean["demand"], sm.add_constant(clean["TMAX"])).fit()
    return {
        "r": r,
        "r2": model.rsquared,
        "slope": model.params["TMAX"],
        "intercept": model.params["const"]
    }

def prepare_heatmap(df):
    """
    Bin TMAX and compute avg demand by city/temp_range/weekday.
    """
    bins = [-float("inf"),50,60,70,80,90,float("inf")]
    labels = ["<50","50-60","60-70","70-80","80-90",">90"]
    df["temp_range"] = pd.cut(df["TMAX"], bins=bins, labels=labels)
    df["weekday"] = pd.to_datetime(df["date"]).dt.weekday
    return (
        df.groupby(["city","temp_range","weekday"])["demand"]
          .mean().reset_index(name="avg_energy")
    )
