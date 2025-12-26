import pandas as pd
import os

ELO_INIT = 1500
K = 20

IN_PATH = os.path.join("data", "intermediate", "clean_games.parquet")
OUT_PATH = os.path.join("data", "processed", "elo_features.parquet")

df = pd.read_parquet(IN_PATH)

sort_cols = [c for c in ["season", "week", "gameday"] if c in df.columns]
df = df.sort_values(sort_cols).reset_index(drop=True)

team_elo = {}
game_id = []
home_elo_pre = []
away_elo_pre = []
elo_diff = []
elo_home_win_prob = []

for row in df.itertuples(index=False):
    home_team = row.home_team
    away_team = row.away_team
    home_win = row.home_win
    home_elo = team_elo.get(home_team, ELO_INIT)
    away_elo = team_elo.get(away_team, ELO_INIT)
    home_elo_pre.append(home_elo)
    away_elo_pre.append(away_elo)
    elo_diff.append(home_elo - away_elo)
    p_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
    delta = K * (home_win - p_home)
    home_elo += delta
    away_elo -= delta
    team_elo[home_team] = home_elo
    team_elo[away_team] = away_elo
    game_id.append(row.game_id)
    elo_home_win_prob.append(p_home)

if len(game_id) != len(df):
    raise ValueError("Alignment was incorrect")
if len(home_elo_pre) != len(df):
    raise ValueError("Alignment was incorrect")
if len(away_elo_pre) != len(df):
    raise ValueError("Alignment was incorrect")
if len(elo_diff) != len(df):
    raise ValueError("Alignment was incorrect")
if len(elo_home_win_prob) != len(df):
    raise ValueError("Alignment was incorrect")

output_df = pd.DataFrame()

output_df["game_id"] = game_id
output_df["home_elo_pre"] = home_elo_pre
output_df["away_elo_pre"] = away_elo_pre
output_df["elo_diff"] = elo_diff
output_df["elo_home_win_prob"] = elo_home_win_prob

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
output_df.to_parquet(OUT_PATH, index=False)