import pandas as pd

# Read silver dataset
silver = pd.read_csv("data/silver/climate_silver.csv")

# Only select the needed features
gold = silver[["date", "meantemp", "humidity", "wind_speed", "meanpressure"]].copy()

# Create lagged feature for next-day prediction
gold["meantemp_lag1"] = gold["meantemp"].shift(1)

# Drop the first row which contains NaN due to lag
gold = gold.dropna()

# Save the gold dataset
gold.to_csv("data/gold/climate_gold.csv", index=False)

print("Gold dataset created.")