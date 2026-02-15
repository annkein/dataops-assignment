import pandas as pd
from pathlib import Path

# Read all bronze batches
bronze_path = Path("data/bronze")
files = sorted(bronze_path.glob("batch_*.csv"))

dfs = []
for f in files:
    df = pd.read_csv(f)
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)

# Convert date to datetime and sort
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# Continuity check
expected_dates = pd.date_range(df["date"].min(), df["date"].max())
if len(expected_dates) != len(df):
    raise ValueError("Time index is not continuous.")

# Duplicate check
if df["date"].duplicated().any():
    raise ValueError("Duplicate dates found.")

# Value range check (reasonable temperature range)
if not df["meantemp"].between(-50, 60).all():
    raise ValueError("Temperature values out of expected range.")

print("All data quality checks passed.")

# Now save the silver dataset
silver_path = Path("data/silver")
silver_path.mkdir(parents=True, exist_ok=True)

df.to_csv(silver_path / "climate_silver.csv", index=False)

print("Silver dataset created.")