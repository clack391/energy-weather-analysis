import pytest, pandas as pd
from src.pipeline import run

class Dummy:
    def fetch(self,*args,**kwargs):
        return pd.DataFrame({
            "date": pd.date_range("2025-01-01", periods=3),
            "TMAX":[60,65,70],"TMIN":[50,55,60],
            "region":["X"]*3,"region_name":["X"]*3,
            "type":["A"]*3,"type_name":["A"]*3,
            "timezone":["Z"]*3,"timezone_description":["Z"]*3,
            "demand":[100,110,105],"value_units":["MW"]*3
        })

@pytest.fixture(autouse=True)
def patch_fetchers(monkeypatch):
    monkeypatch.setattr("src.pipeline.NOAAFetcher", lambda: Dummy())
    monkeypatch.setattr("src.pipeline.EIAFetcher", lambda: Dummy())

def test_days(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run(days=90)
    assert (tmp_path/"data"/"weather.csv").exists()
    assert (tmp_path/"data"/"energy.csv").exists()
