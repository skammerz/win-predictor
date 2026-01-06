import numpy as np
import pandas as pd

def build_features_for_game(schedules_df, game_id, n=5):
    ELO_INIT = 1500
    K = 20

    row = schedules_df[schedules_df["game_id"] == game_id].iloc[0]
    home_team = row["home_team"]
    away_team = row["away_team"]
    gameday = row["gameday"]
    T = gameday
    history_df = schedules_df[schedules_df["game_type"] == "REG"]
    history_df = history_df[history_df["gameday"] < T]
    history_df = history_df.dropna(subset=["home_score", "away_score"]).copy()
    history_df = history_df[history_df["home_score"] != history_df["away_score"]]
    sort_cols = ["season", "week", "gameday"]
    history_df = history_df.sort_values(sort_cols).reset_index(drop=True)

    team_elo = {}

    for row in history_df.itertuples(index=False):
        team_home = row.home_team
        team_away = row.away_team
        home_win = 1 if row.home_score > row.away_score else 0
        home_elo = team_elo.get(team_home, ELO_INIT)
        away_elo = team_elo.get(team_away, ELO_INIT)
        p_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
        delta = K * (home_win - p_home)
        home_elo += delta
        away_elo -= delta
        team_elo[team_home] = home_elo
        team_elo[team_away] = away_elo

    home_elo = team_elo.get(home_team, ELO_INIT)
    away_elo = team_elo.get(away_team, ELO_INIT)
    elo_diff = home_elo - away_elo
    p_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))

    team_hist = {}

    for row in history_df.itertuples(index=False):
        team_home = row.home_team
        team_away = row.away_team
        home_pf = row.home_score
        home_pa = row.away_score
        home_pd = home_pf - home_pa
        home_win = 1 if home_pf > home_pa else 0
        away_pf = row.away_score
        away_pa = row.home_score
        away_pd = away_pf - away_pa
        away_win = 1 - home_win
        if team_home not in team_hist:
            team_hist[team_home] = []
        if team_away not in team_hist:
            team_hist[team_away] = []
        team_hist[team_home].append((home_pf, home_pa, home_pd, home_win))
        team_hist[team_away].append((away_pf, away_pa, away_pd, away_win))
        team_hist[team_home] = team_hist[team_home][-n:]
        team_hist[team_away] = team_hist[team_away][-n:]

    def compute_stats(hist): 
        if len(hist) == 0:
            return (np.nan, np.nan, np.nan, np.nan)
        pf = np.mean([x[0] for x in hist])
        pa = np.mean([x[1] for x in hist])
        pd = np.mean([x[2] for x in hist])
        wr = np.mean([x[3] for x in hist])
        return (pf, pa, pd, wr)
    
    home_hist = team_hist.get(home_team, [])
    away_hist = team_hist.get(away_team, [])

    home_pf, home_pa, home_pd, home_wr = compute_stats(home_hist)
    away_pf, away_pa, away_pd, away_wr = compute_stats(away_hist)

    pf_last5_diff = home_pf - away_pf
    pa_last5_diff = home_pa - away_pa
    pd_last5_diff = home_pd - away_pd
    wr_last5_diff = home_wr - away_wr

    data = {
        'elo_diff': elo_diff,
        'elo_home_win_prob': p_home,
        'pf_last5_diff': pf_last5_diff,
        'pa_last5_diff': pa_last5_diff,
        'pd_last5_diff': pd_last5_diff,
        'wr_last5_diff': wr_last5_diff
    }

    return_df = pd.DataFrame([data])

    return_df = return_df.fillna(0)

    return return_df