import pandas as pd
import os

OUT_PATH = os.path.join("data", "raw", "games.parquet")

df = pd.read_parquet('https://github.com/nflverse/nflverse-data/releases/download/schedules/games.parquet')
os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df.to_parquet(OUT_PATH, index=False)