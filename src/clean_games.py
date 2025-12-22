import pandas as pd
import os

OUT_PATH = os.path.join("data", "intermediate", "clean_games.parquet")
IN_PATH = os.path.join("data", "raw", "games.parquet")

columns_to_include = [
    "season",
    "game_type",
    "game_id",
    "gameday",
    "week",
    "away_team",
    "away_score",
    "home_team",
    "home_score"
]

df = pd.read_parquet(IN_PATH)

keep = [c for c in columns_to_include if c in df.columns]
missing = [c for c in columns_to_include if c not in df.columns]
if missing:
    print("Missing expected columns:", missing)
df = df[keep].copy()

if "gameday" in df.columns:
    df["gameday"] = pd.to_datetime(df["gameday"], errors="coerce")

if "game_type" in df.columns:
    df = df[df["game_type"] == "REG"].copy()
else:
    raise KeyError("Expected column game_type not found in raw dataset")

df = df.dropna(subset=["home_score", "away_score"]).copy()

df["home_score"] = df["home_score"].astype(int)
df["away_score"] = df["away_score"].astype(int)

df = df[df["home_score"] != df["away_score"]].copy()

df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)

sort_cols = [c for c in ["season", "week", "gameday"] if c in df.columns]
df = df.sort_values(sort_cols).reset_index(drop=True)

total_rows = len(df)
unique_game_ids = df["game_id"].nunique()

if (total_rows != unique_game_ids):
    raise ValueError("Duplicate games found, please check raw data")

print("Home win rate is", round(df["home_win"].mean(), 4))

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df.to_parquet(OUT_PATH, index=False)
