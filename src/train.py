import pandas as pd
import os
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score

IN_PATH = os.path.join("data", "processed", "model_table.parquet")

df = pd.read_parquet(IN_PATH)
scaler = StandardScaler()

max_season = max(df["season"])

test_seasons = {max_season - 1, max_season}

test_df = df[df["season"].isin(test_seasons)]
train_df = df[~df["season"].isin(test_seasons)]
test_df = test_df.reset_index(drop=True)
train_df = train_df.reset_index(drop=True)

feature_cols = [
    "elo_diff",
    "elo_home_win_prob",
    "pf_last5_diff",
    "pa_last5_diff",
    "pd_last5_diff",
    "wr_last5_diff"
]

X_train = train_df[feature_cols]
y_train = train_df["home_win"]
X_test = test_df[feature_cols]
y_test = test_df["home_win"]

scaler.fit(X_train)

X_train_scaled = scaler.transform(X_train)
X_test_scaled = scaler.transform(X_test)

clf = LogisticRegression()
clf.fit(X_train_scaled, y_train)

elo_pred = (test_df["elo_home_win_prob"] >= 0.5).astype(int)

probabilities = clf.predict_proba(X_test_scaled)
predictions = clf.predict(X_test_scaled)

score_model = log_loss(y_test, probabilities[:, 1])
accuracy_model = accuracy_score(y_test, predictions)

score_elo = log_loss(y_test, test_df["elo_home_win_prob"])
accuracy_elo = accuracy_score(y_test, elo_pred)

score_always_home = log_loss(y_test, [1.0] * len(y_test))
accuracy_always_home = accuracy_score(y_test, [1] * len(y_test))

# save models and reports

os.makedirs("models", exist_ok=True)
os.makedirs("reports", exist_ok=True)

import joblib

joblib.dump(clf, "models/logreg_v1.joblib")
joblib.dump(scaler, "models/scaler_v1.joblib")

import json

metrics = {
    "always_home": {
        "accuracy": accuracy_always_home,
        "log_loss": score_always_home
    },
    "elo_only": {
        "accuracy": accuracy_elo,
        "log_loss": score_elo
    },
    "logistic_regression": {
        "accuracy": accuracy_model,
        "log_loss": score_model
    }
}

with open("reports/metrics_v1.json", "w") as f:
    json.dump(metrics, f, indent=4)

pred_df = test_df[["game_id", "home_win"]].copy()
pred_df["elo_prob"] = test_df["elo_home_win_prob"]
pred_df["model_prob"] = probabilities[:, 1]
pred_df["model_pred"] = predictions

pred_df.to_parquet("reports/test_predictions_v1.parquet", index=False)