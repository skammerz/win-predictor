import streamlit as st
from data_live import load_schedules_live, load_model_table, load_joblib_model, load_joblib_scaler
import pandas as pd
import os

st.title("NFL Win Predictor")

df = load_schedules_live()

df = df[df["game_type"] == "REG"].copy()

seasons = sorted(df["season"].dropna().unique())

season = st.selectbox('Select a season', seasons)

season_df = df[df["season"] == season]
weeks = sorted(season_df["week"].dropna().unique())

week = st.selectbox('Select a week', weeks)

week_df = season_df[season_df["week"] == week].copy()
week_df["label"] = (week_df["away_team"] + " @ " + week_df["home_team"] + " (" + week_df["gameday"].astype(str) + ")")
label_to_game = dict(zip(week_df["label"], week_df["game_id"]))
game_label = st.selectbox('Select a game', week_df["label"])
game_id = label_to_game[game_label]

st.write('You have picked', game_label, 'from week', week, 'in season', season)

game_row = df[df["game_id"] == game_id].copy()
home_score = game_row["home_score"].iat[0]
home_team = game_row["home_team"].iat[0]
away_team = game_row["away_team"].iat[0]
away_score = game_row["away_score"].iat[0]
if pd.isna(home_score) or pd.isna(away_score):
    st.write("Final score: Game not yet completed")
else:
    st.write("Final score:", away_team, int(away_score), "-", home_team, int(home_score))

IN_PATH = os.path.join("data", "processed", "model_table.parquet")

model_table = load_model_table(IN_PATH)

game_data = model_table[model_table["game_id"] == game_id].copy()
if game_data.empty:
    st.write('Future game, feature not implemented yet')
else:
    model = load_joblib_model()
    scaler = load_joblib_scaler()

    feature_cols = [
        "elo_diff",
        "elo_home_win_prob",
        "pf_last5_diff",
        "pa_last5_diff",
        "pd_last5_diff",
        "wr_last5_diff"
    ]

    X = game_data[feature_cols]
    X_scaled = scaler.transform(X)

    result = model.predict_proba(X_scaled)
    p_home = result[0, 1]
    st.write('The model predicted ', home_team if p_home >= .5 else away_team, 
             ' with home win probability of ', round(p_home, 2))