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

print("=== Baseline: Always Pick Home ===")
print(f"Accuracy: {accuracy_always_home:.4f}")
print(f"Log Loss: {score_always_home:.4f}\n")

print("=== Baseline: Elo Only ===")
print(f"Accuracy: {accuracy_elo:.4f}")
print(f"Log Loss: {score_elo:.4f}\n")

print("=== Logistic Regression Model ===")
print(f"Accuracy: {accuracy_model:.4f}")
print(f"Log Loss: {score_model:.4f}")