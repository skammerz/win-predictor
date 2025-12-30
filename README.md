# NFL Win Probability Predictor

This project creates a model that predicts the probability that the home team wins an NFL game, using pre-game information such as the team's recent performance and Elo ratings.

---

## Project Goals
- Predict win probabilities based on the data we have
- Make the predictions using only information available before kickoff
- Compare machine learning models against strong baselines (e.g. Elo)

---

## Data
- Source: nflverse's game-level NFL data
- Scope: Regular season games only

---

## Approach (High Level)
- Clean and normalize game-level data from nflverse
- Create the pre-game features of the teams' rolling performance (using their last 5 games) and their Elo rating
- Train models on the data
- Evaluate the data, currently using both log loss and accuracy scores

---

## Planned Demo
An interactive app that allows users to select any matchup, including future scheduled matchups, and view:
- Model's predicted win probability
- Model's predicted winner
- Key factors influencing the model's prediction
- If the game is complete, it will show the actual result