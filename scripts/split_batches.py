import pandas as pd
from pathlib import Path

# Paths
INPUT_PATH = Path("data/bronze/DailyDelhiClimateTrain.csv")
OUTPUT_DIR = Path("data/bronze")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load data
df = pd.read_csv(INPUT_PATH)

# Ensure datetime and temporal order
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# Calculate batch size
n_batches = 5
batch_size = len(df) // n_batches

# Split into 5 time-ordered batches
for i in range(n_batches):
    start = i * batch_size
    end = None if i == n_batches - 1 else (i + 1) * batch_size

    batch = df.iloc[start:end]
    output_path = OUTPUT_DIR / f"batch_{i+1}.csv"
    batch.to_csv(output_path, index=False)

    print(f"Saved {output_path}")