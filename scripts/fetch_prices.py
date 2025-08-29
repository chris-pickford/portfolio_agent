import os, csv, yaml
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

with open("config/tickers.yml") as f:
    TICKERS = yaml.safe_load(f)["tickers"]

Path("data").mkdir(exist_ok=True)
out_path = Path("data/prices_daily.csv")
fieldnames = ["date","ticker","close"]

# ensure header exists
if not out_path.exists():
    with out_path.open("w", newline="") as f:
        csv.DictWriter(f, fieldnames).writeheader()

# load existing keys to avoid duplicates
existing = set()
with out_path.open() as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        existing.add((r["date"], r["ticker"]))

new_rows = []

for t in TICKERS:
    # grab the last few days and take the latest trading day
    df = yf.download(t, period="10d", interval="1d", auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"No data for {t}")
    close_series = df["Close"].dropna()
    last_ts = close_series.index[-1]           # pandas Timestamp (UTC)
    last_date = last_ts.date().isoformat()
    close = float(close_series.iloc[-1])

    key = (last_date, t)
    if key not in existing:
        new_rows.append({"date": last_date, "ticker": t, "close": close})

# append any new rows
if new_rows:
    with out_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames)
        w.writerows(new_rows)

print(f"Wrote {len(new_rows)} new rows on {datetime.utcnow().date()}.")
