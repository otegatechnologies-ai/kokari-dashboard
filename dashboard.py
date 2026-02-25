"""
╔══════════════════════════════════════════════════════════════════╗
║         KOKARI CAFE — FINANCIAL DASHBOARD                        ║
║         Built with Streamlit + SQLite                            ║
...
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
from datetime import date, datetime, timedelta
from contextlib import contextmanager

# ─── CONFIG ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kokari Cafe · Financial Dashboard",
    page_icon="☕",
    layout="wide",
    initial_sidebar_state="expanded",
)
...