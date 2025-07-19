"""
Main entry point for the energy + weather data pipeline.

This script imports the `fetch_historical_data` function from the `data_fetcher` module,
parses command-line arguments, and triggers the data pipeline.

Run with:
    python -m src.pipeline --days 90
"""

import argparse
from src.data_fetcher import fetch_historical_data

def main():
    parser = argparse.ArgumentParser(description="Fetch historical weather and energy data")
    parser.add_argument("--days", type=int, default=90, help="Number of days to fetch (default: 90)")
    args = parser.parse_args()

    print(f"Fetching {args.days} days of data...")
    fetch_historical_data(days=args.days)
    print("✅ Pipeline finished. Check data/ folder and logs/pipeline.log.")

if __name__ == "__main__":
    main()
