import pandas as pd
import numpy as np
import os

IN_PATH = os.path.join("data", "intermediate", "clean_games.parquet")
OUT_PATH = os.path.join("data", "processed", "rolling_features.parquet")

def compute_stats(hist): 
    if len(hist) == 0:
        return (np.nan, np.nan, np.nan, np.nan)
    pf = np.mean([x[0] for x in hist])
    pa = np.mean([x[1] for x in hist])
    pd = np.mean([x[2] for x in hist])
    wr = np.mean([x[3] for x in hist])
    return (pf, pa, pd, wr)

df = pd.read_parquet(IN_PATH)

sort_cols = [c for c in ["season", "week", "gameday"] if c in df.columns]
df = df.sort_values(sort_cols).reset_index(drop=True)

team_hist = {}
home_pf_last5 = []
home_pa_last5 = []
home_pd_last5 = []
home_wr_last5 = []
away_pf_last5 = []
away_pa_last5 = []
away_pd_last5 = []
away_wr_last5 = []

for row in df.itertuples(index=False):
    home_team = row.home_team
    away_team = row.away_team
    home_hist = team_hist.get(home_team, [])
    away_hist = team_hist.get(away_team, [])
    home_pf, home_pa, home_pd, home_wr = compute_stats(home_hist)
    away_pf, away_pa, away_pd, away_wr = compute_stats(away_hist)
    home_pf_last5.append(home_pf)
    home_pa_last5.append(home_pa)
    home_pd_last5.append(home_pd)
    home_wr_last5.append(home_wr)
    away_pf_last5.append(away_pf)
    away_pa_last5.append(away_pa)
    away_pd_last5.append(away_pd)
    away_wr_last5.append(away_wr)
    if home_team not in team_hist:
        team_hist[home_team] = []
    if away_team not in team_hist:
        team_hist[away_team] = []
    team_hist[home_team].append((row.home_score, row.away_score, row.home_score - row.away_score, row.home_win))
    team_hist[away_team].append((row.away_score, row.home_score, row.away_score - row.home_score, 1 - row.home_win))
    team_hist[home_team] = team_hist[home_team][-5:]
    team_hist[away_team] = team_hist[away_team][-5:]

if len(home_pf_last5) != len(df):
    raise ValueError("Alignment was incorrect")
if len(away_pf_last5) != len(df):
    raise ValueError("Alignment was incorrect")

df["home_pf_last5"] = home_pf_last5
df["home_pa_last5"] = home_pa_last5
df["home_pd_last5"] = home_pd_last5
df["home_wr_last5"] = home_wr_last5
df["away_pf_last5"] = away_pf_last5
df["away_pa_last5"] = away_pa_last5
df["away_pd_last5"] = away_pd_last5
df["away_wr_last5"] = away_wr_last5

columns_to_include = [
    "game_id",
    "home_pf_last5",
    "home_pa_last5",
    "home_pd_last5",
    "home_wr_last5",
    "away_pf_last5",
    "away_pa_last5",
    "away_pd_last5",
    "away_wr_last5"
]

df = df[columns_to_include].copy()

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df.to_parquet(OUT_PATH, index=False)