import os
import requests
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import yaml
import json

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

def fetch_with_retry(url, params, headers, max_retries=3, backoff_factor=1):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            wait = backoff_factor * (2 ** attempt)
            logging.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s")
            time.sleep(wait)
    logging.error(f"Failed after {max_retries} attempts: {url}")
    return None

def fetch_noaa_data(station_id, start_date, end_date):
    token = os.getenv("NOAA_API_KEY")
    if not token:
        logging.error("NOAA_API_KEY not set")
        return None
    headers = {"token": token}
    params = {
        "datasetid": "GHCND",
        "stationid": station_id,
        "startdate": start_date,
        "enddate": end_date,
        "datatypeid": "TMAX,TMIN",
        "limit": 1000
    }
    cfg = load_config()
    return fetch_with_retry(cfg["data_sources"]["noaa_base_url"], params, headers)

def fetch_eia_v2(region_code, start_date, end_date):
    key = os.getenv("EIA_API_KEY")
    if not key:
        logging.error("EIA_API_KEY not set")
        return None
    params = {
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": region_code,
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 5000
    }
    cfg = load_config()
    headers = {"X-Api-Key": key}
    return fetch_with_retry(cfg["data_sources"]["eia_base_url"], params, headers)

def fetch_eia_fallback(region_code, start_date, end_date):
    key = os.getenv("EIA_API_KEY")
    if not key:
        logging.error("EIA_API_KEY not set")
        return pd.DataFrame()
    series_map = {
        "NYIS": "EBA.NYIS-ALL.D.H",
        "PJM": "EBA.PJM-ALL.D.H",
        "ERCO": "EBA.ERCO.D.H",
        "AZPS": "EBA.AZPS.D.H",
        "SCL": "EBA.SCL.D.H"
    }
    series_id = series_map.get(region_code)
    if not series_id:
        logging.warning(f"No fallback series for {region_code}")
        return pd.DataFrame()
    url = "https://api.eia.gov/series/"
    params = {"api_key": key, "series_id": series_id}
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        series = resp.json().get("series", [])[0]
        df = pd.DataFrame(series["data"], columns=["date", "value"])
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d%H")
        df = df.set_index("date").resample("D").mean().reset_index()
        df["region"] = region_code
        df["value_units"] = series.get("units", "")
        return df[["date", "region", "value", "value_units"]]
    except Exception as e:
        logging.error(f"EIA fallback failed for {region_code}: {e}")
        return pd.DataFrame()

def fetch_historical_data(days=90):
    cfg = load_config()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    weather_records = []
    energy_frames = []

    for city in cfg["cities"]:
        logging.info(f"Fetching NOAA for {city['name']}")
        w_json = fetch_noaa_data(city["station_id"], start_date, end_date)
        if w_json and "results" in w_json:
            df = pd.DataFrame(w_json["results"])
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["city"] = city["name"]
            df = df.pivot_table(index=["date", "city"], columns="datatype", values="value").reset_index()
            df.rename(columns={"TMAX": "tmax", "TMIN": "tmin"}, inplace=True)
            df["tmax"] = df["tmax"] * 0.1 * 9/5 + 32
            df["tmin"] = df["tmin"] * 0.1 * 9/5 + 32
            weather_records.append(df)
            logging.info(f"Weather: {len(df)} rows for {city['name']}")
        else:
            logging.warning(f"No weather data for {city['name']}")

        logging.info(f"Fetching EIA v2 for {city['name']}")
        e_json = fetch_eia_v2(city["region_code"], start_date, end_date)
        if e_json and e_json.get("response", {}).get("data"):
            df = pd.DataFrame(e_json["response"]["data"])
            df["city"] = city["name"]
            df["date"] = pd.to_datetime(df["period"]).dt.date
            energy_frames.append(df)
            logging.info(f"EIA v2: {len(df)} rows for {city['name']}")
        else:
            logging.warning(f"EIA v2 failed for {city['name']} — trying fallback")
            fallback = fetch_eia_fallback(city["region_code"], start_date, end_date)
            if not fallback.empty:
                fallback["city"] = city["name"]
                energy_frames.append(fallback)
                logging.info(f"EIA fallback: {len(fallback)} rows for {city['name']}")
            else:
                logging.warning(f"No fallback data for {city['name']}")

    # Save CSVs
    os.makedirs("data", exist_ok=True)
    if weather_records:
        pd.concat(weather_records).to_csv("data/weather.csv", index=False)
    if energy_frames:
        pd.concat(energy_frames).to_csv("data/energy.csv", index=False)

    logging.info("Saved data/weather.csv and data/energy.csv")
