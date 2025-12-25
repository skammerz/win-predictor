import pandas as pd
import os

CLEAN_PATH = os.path.join("data", "intermediate", "clean_games.parquet")
ROLLING_PATH = os.path.join("data", "processed", "rolling_features.parquet")
ELO_PATH = os.path.join("data", "processed", "elo_features.parquet")
OUT_PATH = os.path.join("data", "processed", "model_table.parquet")

df_clean = pd.read_parquet(CLEAN_PATH)
df_rolling = pd.read_parquet(ROLLING_PATH)
df_elo = pd.read_parquet(ELO_PATH)

columns_to_keep = [
    "game_id",
    "home_win",
    "season",
    "week",
    "gameday",
    "home_team",
    "away_team"
]

df_clean = df_clean[columns_to_keep].copy()
if df_rolling["game_id"].nunique() != len(df_rolling):
    raise ValueError("df_rolling has duplicate game_ids")
if df_elo["game_id"].nunique() != len(df_elo):
    raise ValueError("df_elo has duplicate game_ids")

result = df_clean.merge(df_rolling, on="game_id", how="left")
if len(result) != len(df_clean):
    raise ValueError("merge with rolling changed row counts")
if result["game_id"].nunique() != len(result):
    raise ValueError("result has duplicate game_ids after merge with rolling")

result = result.merge(df_elo, on="game_id", how="left")
if len(result) != len(df_clean):
    raise ValueError("merge with elo changed row counts")
if result["game_id"].nunique() != len(result):
    raise ValueError("result has duplicate game_ids after merge with elo")

result["pf_last5_diff"] = result.home_pf_last5 - result.away_pf_last5
result["pa_last5_diff"] = result.home_pa_last5 - result.away_pa_last5
result["pd_last5_diff"] = result.home_pd_last5 - result.away_pd_last5
result["wr_last5_diff"] = result.home_wr_last5 - result.away_wr_last5

result["pf_last5_diff"] = result["pf_last5_diff"].fillna(0)
result["pa_last5_diff"] = result["pa_last5_diff"].fillna(0)
result["pd_last5_diff"] = result["pd_last5_diff"].fillna(0)
result["wr_last5_diff"] = result["wr_last5_diff"].fillna(0)

columns = [
    "game_id",
    "home_win",
    "elo_diff",
    "elo_home_win_prob",
    "pf_last5_diff",
    "pa_last5_diff",
    "pd_last5_diff",
    "wr_last5_diff"
]

result = result[columns].copy()

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
result.to_parquet(OUT_PATH, index=False)