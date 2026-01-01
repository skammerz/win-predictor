import pandas as pd
import streamlit as st
import joblib

@st.cache_data(ttl=6 * 60 * 60)
def load_schedules_live():
    return pd.read_parquet('https://github.com/nflverse/nflverse-data/releases/download/schedules/games.parquet')

@st.cache_data(ttl=6 * 60 * 60)
def load_model_table(path):
    return pd.read_parquet(path)

@st.cache_resource(ttl=6 * 60 * 60)
def load_joblib_model():
    return joblib.load('models/logreg_v1.joblib')

@st.cache_resource(ttl=6 * 60 * 60)
def load_joblib_scaler():
    return joblib.load('models/scaler_v1.joblib')