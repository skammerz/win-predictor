# NFL Win Probability Predictor

This project predicts the **probability that the home team wins an NFL game**, using only **pre-game information** such as recent team performance and Elo ratings.

The goal of this project is to build a **leakage-safe, reproducible machine learning pipeline** using real NFL data, and to evaluate models using proper probabilistic metrics (not just accuracy).

This repository is under active development.

---

## Project Goals
- Predict **win probabilities**, not just winners
- Avoid data leakage by using only information available **before kickoff**
- Compare machine learning models against strong baselines (e.g. Elo)
- Emphasize **calibration**, interpretability, and reproducibility

---

## Data
- Source: **nflverse** (game-level NFL data)
- Scope: Regular season games only
- Each row represents a single NFL game

Raw data is not committed to this repository and can be regenerated.

---

## Approach (High Level)
- Clean and normalize game-level data
- Engineer pre-game features:
  - Rolling team performance (last 5 / last 10 games)
  - Elo ratings
- Train time-aware models (no random splits)
- Evaluate using:
  - Accuracy
  - Log loss
  - Brier score
  - Calibration curves

---

## Current Status
- [ ] Data ingestion
- [ ] Feature engineering
- [ ] Baselines (home team, Elo)
- [ ] Machine learning models
- [ ] Evaluation and calibration
- [ ] Interactive demo

---

## Planned Demo
An interactive app that allows users to select a matchup and view:
- Predicted win probability
- Predicted winner
- Key factors influencing the prediction

---

## Future Work
- Incorporate additional pre-game features (rest days, travel, QB info)
- Compare predictions to betting market lines
- Extend to playoff games
- Explore live, in-game win probability updates
