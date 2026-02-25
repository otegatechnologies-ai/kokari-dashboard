"""
╔══════════════════════════════════════════════════════════════════╗
║         KOKARI CAFE — FINANCIAL DASHBOARD                        ║
║         Built with Streamlit + SQLite                            ║
║                                                                  ║
║  INSTALL:                                                        ║
║    pip install streamlit pandas plotly                           ║
║                                                                  ║
║  RUN LOCALLY:                                                    ║
║    streamlit run dashboard.py                                    ║
║                                                                  ║
║  DEPLOY (Streamlit Cloud):                                       ║
║    1. Push to GitHub                                             ║
║    2. Go to share.streamlit.io → Deploy                          ║
║    3. Add requirements.txt (see bottom of this file)             ║
║                                                                  ║
║  DEPLOY (VPS / your own server):                                 ║
║    pip install streamlit pandas plotly                           ║
║    streamlit run dashboard.py --server.port 8501                 ║
║    # Use nginx as reverse proxy for production                   ║
╚══════════════════════════════════════════════════════════════════╝
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

DB_PATH = "kokari_cafe.db"

# ─── STYLES ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main .block-container { padding-top: 1.5rem; }
    .kpi-card {
        background: white; border-radius: 12px; padding: 16px 20px;
        border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,.06);
        text-align: center;
    }
    .kpi-label { font-size: 11px; color: #9ca3af; text-transform: uppercase;
                 letter-spacing: .05em; margin-bottom: 4px; }
    .kpi-value { font-size: 22px; font-weight: 700; margin-bottom: 2px; }
    .kpi-sub   { font-size: 11px; color: #9ca3af; }
    .kpi-blue  { color: #2563eb; }
    .kpi-green { color: #16a34a; }
    .kpi-red   { color: #dc2626; }
    .kpi-orange{ color: #ea580c; }
    .kpi-purple{ color: #7c3aed; }
    .note-box {
        background: #fffbeb; border: 1px solid #fde68a;
        border-radius: 10px; padding: 12px 16px; font-size: 13px; color: #92400e;
    }
    div[data-testid="stMetric"] {
        background: white; border-radius: 10px; padding: 14px;
        border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,.05);
    }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0; padding: 8px 18px;
        font-size: 13px; font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# ─── DATABASE ─────────────────────────────────────────────────────────────────
@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create tables and seed with real Kokari Cafe data if empty."""
    with get_conn() as conn:
        cur = conn.cursor()

        # ── Products table ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            category    TEXT NOT NULL,
            cost_ratio  REAL NOT NULL DEFAULT 0.40
        )""")

        # ── Transactions table ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT    NOT NULL,
            type        TEXT    NOT NULL CHECK(type IN ('sale','expense')),
            product_id  TEXT,
            name        TEXT,
            category    TEXT    NOT NULL,
            amount      REAL    NOT NULL,
            cogs        REAL    NOT NULL DEFAULT 0,
            note        TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )""")

        # ── Users table (for future auth) ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT    UNIQUE NOT NULL,
            role     TEXT    NOT NULL DEFAULT 'accountant'
        )""")

        # ── Seed products if empty ──
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            products = [
                ("p01","Pancakes",             "Cafe",     0.38),
                ("p02","Fruit Smoothie",        "Cafe",     0.40),
                ("p03","Books",                 "Retail",   0.55),
                ("p04","Puff Puff",             "Cafe",     0.30),
                ("p05","Spicy Chicken Wrap",    "Cafe",     0.42),
                ("p06","Chicken Wings",         "Cafe",     0.45),
                ("p07","Tapioca",               "Cafe",     0.35),
                ("p08","Coffee",                "Cafe",     0.28),
                ("p09","Ice Coffee",            "Cafe",     0.30),
                ("p10","Zobo",                  "Cafe",     0.25),
                ("p11","Parfait & Wings Combo", "Cafe",     0.45),
                ("p12","Parfait Café",          "Cafe",     0.40),
                ("p13","Granola 500g",          "Packaged", 0.50),
                ("p14","Spicy Coconut Flakes",  "Packaged", 0.48),
                ("p15","Honey Coconut Cashew",  "Packaged", 0.50),
                ("p16","CCB",                   "Packaged", 0.50),
                ("p17","Wholesale (B2B)",       "B2B",      0.55),
                ("p18","Take Away",             "Cafe",     0.40),
                ("p19","Water",                 "Cafe",     0.20),
                ("p20","Space Rental",          "Other",    0.05),
            ]
            cur.executemany(
                "INSERT INTO products VALUES (?,?,?,?)", products)

        # ── Seed transactions if empty (real Feb 9-14 data) ──
        cur.execute("SELECT COUNT(*) FROM transactions")
        if cur.fetchone()[0] == 0:
            _seed_transactions(cur)


def _seed_transactions(cur):
    """Insert real Kokari Cafe Feb 9-14 2026 data."""

    # Helper: fetch cost_ratio for a product
    def cogs(pid, amt):
        cur.execute("SELECT cost_ratio FROM products WHERE id=?", (pid,))
        r = cur.fetchone()
        ratio = r[0] if r else 0.40
        return round(amt * ratio)

    # ── SALES ──
    sales = [
        # Feb 9
        ("2026-02-09","p01","Pancakes",            "Cafe",     15060),
        ("2026-02-09","p08","Coffee",               "Cafe",     8600),
        ("2026-02-09","p09","Ice Coffee",           "Cafe",     18215),
        ("2026-02-09","p11","Parfait & Wings Combo","Cafe",     26355),
        ("2026-02-09","p12","Parfait Café",         "Cafe",     21500),
        # Feb 10
        ("2026-02-10","p01","Pancakes",             "Cafe",     11295),
        ("2026-02-10","p08","Coffee",               "Cafe",     11370),
        ("2026-02-10","p09","Ice Coffee",           "Cafe",     4840),
        ("2026-02-10","p10","Zobo",                 "Cafe",     3765),
        ("2026-02-10","p11","Parfait & Wings Combo","Cafe",     8600),
        ("2026-02-10","p14","Spicy Coconut Flakes", "Packaged", 3765),
        # Feb 11
        ("2026-02-11","p01","Pancakes",             "Cafe",     11295),
        ("2026-02-11","p04","Puff Puff",            "Cafe",     3765),
        ("2026-02-11","p08","Coffee",               "Cafe",     11265),
        ("2026-02-11","p09","Ice Coffee",           "Cafe",     24200),
        ("2026-02-11","p10","Zobo",                 "Cafe",     3765),
        ("2026-02-11","p11","Parfait & Wings Combo","Cafe",     8600),
        ("2026-02-11","p12","Parfait Café",         "Cafe",     10750),
        # Feb 12
        ("2026-02-12","p01","Pancakes",             "Cafe",     22815),
        ("2026-02-12","p02","Fruit Smoothie",       "Cafe",     14520),
        ("2026-02-12","p05","Spicy Chicken Wrap",   "Cafe",     10750),
        ("2026-02-12","p06","Chicken Wings",        "Cafe",     33260),
        ("2026-02-12","p07","Tapioca",              "Cafe",     4300),
        ("2026-02-12","p08","Coffee",               "Cafe",     9140),
        ("2026-02-12","p09","Ice Coffee",           "Cafe",     29050),
        ("2026-02-12","p10","Zobo",                 "Cafe",     11295),
        ("2026-02-12","p13","Granola 500g",         "Packaged", 6757),
        # Feb 13
        ("2026-02-13","p01","Pancakes",             "Cafe",     18825),
        ("2026-02-13","p02","Fruit Smoothie",       "Cafe",     24200),
        ("2026-02-13","p03","Books",                "Retail",   21950),
        ("2026-02-13","p04","Puff Puff",            "Cafe",     3765),
        ("2026-02-13","p05","Spicy Chicken Wrap",   "Cafe",     10750),
        ("2026-02-13","p06","Chicken Wings",        "Cafe",     8065),
        ("2026-02-13","p08","Coffee",               "Cafe",     3765),
        ("2026-02-13","p09","Ice Coffee",           "Cafe",     4840),
        ("2026-02-13","p12","Parfait Café",         "Cafe",     5375),
        ("2026-02-13","p14","Spicy Coconut Flakes", "Packaged", 3765),
        ("2026-02-13","p17","Wholesale (B2B)",      "B2B",      504513),
        # Feb 14
        ("2026-02-14","p01","Pancakes",             "Cafe",     26355),
        ("2026-02-14","p02","Fruit Smoothie",       "Cafe",     4840),
        ("2026-02-14","p05","Spicy Chicken Wrap",   "Cafe",     10750),
        ("2026-02-14","p06","Chicken Wings",        "Cafe",     8065),
        ("2026-02-14","p07","Tapioca",              "Cafe",     21500),
        ("2026-02-14","p08","Coffee",               "Cafe",     29575),
        ("2026-02-14","p09","Ice Coffee",           "Cafe",     9680),
        ("2026-02-14","p10","Zobo",                 "Cafe",     11295),
        ("2026-02-14","p11","Parfait & Wings Combo","Cafe",     25800),
    ]
    for s in sales:
        amt = s[4]
        cur.execute("""
            INSERT INTO transactions (date,type,product_id,name,category,amount,cogs,note)
            VALUES (?,?,?,?,?,?,?,'')
        """, (s[0], "sale", s[1], s[2], s[3], amt, cogs(s[1], amt)))

    # ── EXPENSES ──
    expenses = [
        # Feb 9
        ("2026-02-09","Ingredients","Sugar",             9000),
        ("2026-02-09","Ingredients","Flour",             5400),
        ("2026-02-09","Ingredients","Chicken Wings",     20178),
        ("2026-02-09","Ingredients","Chicken",           11000),
        ("2026-02-09","Ingredients","Bread",             1200),
        ("2026-02-09","Ingredients","Mayonnaise",        4000),
        ("2026-02-09","Ingredients","Banana",            2000),
        ("2026-02-09","Ingredients","Oil",               4400),
        ("2026-02-09","Ingredients","Carrot & Cabbage",  3000),
        ("2026-02-09","Ingredients","Eggs",              5900),
        ("2026-02-09","Ingredients","Groundnut",         2000),
        ("2026-02-09","Ingredients","Powder Milk",       44000),
        ("2026-02-09","Ingredients","Pineapple",         3000),
        ("2026-02-09","Ingredients","Ginger",            2000),
        ("2026-02-09","Ingredients","Cinnamon",          2000),
        ("2026-02-09","Ingredients","Cloves",            1000),
        ("2026-02-09","Ingredients","Grapes",            6000),
        ("2026-02-09","Ingredients","Honey",             6000),
        ("2026-02-09","Ingredients","Liquid Milk",       10200),
        ("2026-02-09","Utilities",  "NEPA (Electricity)",10000),
        ("2026-02-09","Utilities",  "Data",              3500),
        ("2026-02-09","Utilities",  "Recharge Card",     2000),
        ("2026-02-09","Utilities",  "Water (CWay)",      3400),
        ("2026-02-09","Packaging",  "Zobo Bottles",      4400),
        ("2026-02-09","Packaging",  "Foil",              3000),
        ("2026-02-09","Packaging",  "Serviettes",        2000),
        ("2026-02-09","Packaging",  "Spoons",            3000),
        ("2026-02-09","Packaging",  "Water (retail)",    2500),
        ("2026-02-09","Transport",  "Transport",         1500),
        ("2026-02-09","Miscellaneous","Printing",        400),
        ("2026-02-09","Miscellaneous","Bank Charges",    400),
        ("2026-02-09","Miscellaneous","Phone Repair",    500),
        ("2026-02-09","Logistics",  "Bucket",            3000),
        ("2026-02-09","Logistics",  "Item Delivery x7", 2000),
        ("2026-02-09","Utilities",  "NEPA (Imprest)",    3000),
        ("2026-02-09","Stationery", "Battery & Book",    1000),
        ("2026-02-09","Packaging",  "Serviettes (Imprest)",1000),
        # Feb 14
        ("2026-02-14","Ingredients","Chicken Wings",     36321),
        ("2026-02-14","Ingredients","Flour",             6400),
        ("2026-02-14","Ingredients","Eggs",              6000),
        ("2026-02-14","Packaging",  "Straws",            3700),
        ("2026-02-14","Packaging",  "Water (retail)",    4400),
        ("2026-02-14","Transport",  "Transport",         500),
    ]
    for e in expenses:
        cur.execute("""
            INSERT INTO transactions (date,type,product_id,name,category,amount,cogs,note)
            VALUES (?,?,NULL,?,?,?,?,?)
        """, (e[0], "expense", e[2], e[1], e[3], e[3], ""))


# ─── DATA HELPERS ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_transactions(from_date: str, to_date: str) -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query("""
            SELECT * FROM transactions
            WHERE date BETWEEN ? AND ?
            ORDER BY date DESC, id DESC
        """, conn, params=(from_date, to_date))
    return df


@st.cache_data(ttl=60)
def load_products() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("SELECT * FROM products ORDER BY category, name", conn)


def invalidate_cache():
    load_transactions.clear()
    load_products.clear()


def fmt(n: float) -> str:
    return f"₦{int(n):,}"


def add_transaction(date_val, tx_type, product_id, name, category, amount, note=""):
    with get_conn() as conn:
        if tx_type == "sale":
            conn.execute("SELECT cost_ratio FROM products WHERE id=?", (product_id,))
            row = conn.execute("SELECT cost_ratio FROM products WHERE id=?", (product_id,)).fetchone()
            ratio = row[0] if row else 0.40
            cogs_val = round(amount * ratio)
        else:
            cogs_val = amount
        conn.execute("""
            INSERT INTO transactions (date,type,product_id,name,category,amount,cogs,note)
            VALUES (?,?,?,?,?,?,?,?)
        """, (str(date_val), tx_type, product_id if tx_type == "sale" else None,
              name, category, amount, cogs_val, note))
    invalidate_cache()


def update_transaction(tx_id, date_val, tx_type, product_id, name, category, amount, note):
    with get_conn() as conn:
        if tx_type == "sale":
            row = conn.execute("SELECT cost_ratio FROM products WHERE id=?", (product_id,)).fetchone()
            ratio = row[0] if row else 0.40
            cogs_val = round(amount * ratio)
        else:
            cogs_val = amount
        conn.execute("""
            UPDATE transactions
            SET date=?, type=?, product_id=?, name=?, category=?, amount=?, cogs=?, note=?
            WHERE id=?
        """, (str(date_val), tx_type,
              product_id if tx_type == "sale" else None,
              name, category, amount, cogs_val, note, tx_id))
    invalidate_cache()


def delete_transaction(tx_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM transactions WHERE id=?", (tx_id,))
    invalidate_cache()


def compute_kpis(df: pd.DataFrame) -> dict:
    sales = df[df["type"] == "sale"]
    exps  = df[df["type"] == "expense"]
    revenue  = sales["amount"].sum()
    cogs     = sales["cogs"].sum()
    expenses = exps["amount"].sum()
    gp = revenue - cogs
    np_ = gp - expenses
    b2b  = sales[sales["category"] == "B2B"]["amount"].sum()
    cafe = sales[sales["category"] == "Cafe"]["amount"].sum()
    pkg  = sales[sales["category"] == "Packaged"]["amount"].sum()
    days = sales["date"].nunique()
    return dict(
        revenue=revenue, cogs=cogs, expenses=expenses,
        gp=gp, np=np_,
        gp_mar=revenue and (gp / revenue * 100),
        np_mar=revenue and (np_ / revenue * 100),
        b2b=b2b, cafe=cafe, pkg=pkg, days=days,
        avg_daily=days and revenue / days,
    )


# ─── PLOTLY THEME ─────────────────────────────────────────────────────────────
PALETTE = [
    "#2563eb","#16a34a","#dc2626","#d97706","#7c3aed",
    "#0891b2","#be185d","#059669","#ea580c","#6366f1",
    "#0d9488","#b45309","#9333ea","#0284c7","#65a30d","#64748b",
]

def fig_layout(fig, height=320, legend=True):
    fig.update_layout(
        height=height, margin=dict(l=10, r=10, t=30, b=10),
        plot_bgcolor="white", paper_bgcolor="white",
        font=dict(family="Inter, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1) if legend else dict(visible=False),
        xaxis=dict(showgrid=False, showline=True, linecolor="#e5e7eb"),
        yaxis=dict(gridcolor="#f3f4f6", showline=False),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ══════════════════════════════════════════════════════════════════════════════
def main():
    init_db()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.image("https://via.placeholder.com/280x80/2563eb/ffffff?text=☕+Kokari+Cafe",
                 use_container_width=True)
        st.markdown("### 📅 Date Range")
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("From", value=date(2026, 2, 9), label_visibility="visible")
        with col2:
            to_date   = st.date_input("To",   value=date(2026, 2, 14), label_visibility="visible")

        # Quick range buttons
        st.markdown("**Quick ranges:**")
        qc = st.columns(3)
        if qc[0].button("This week", use_container_width=True):
            today = date.today()
            from_date = today - timedelta(days=today.weekday())
            to_date = today
        if qc[1].button("Last 30d", use_container_width=True):
            to_date   = date.today()
            from_date = to_date - timedelta(days=30)
        if qc[2].button("All time", use_container_width=True):
            from_date = date(2020, 1, 1)
            to_date   = date.today()

        st.divider()
        st.markdown("### ⚡ Quick Add")
        with st.form("quick_add", clear_on_submit=True):
            products_df = load_products()
            tx_type_q   = st.selectbox("Type", ["sale","expense"], key="q_type")
            if tx_type_q == "sale":
                prod_names = products_df["name"].tolist()
                prod_ids   = products_df["id"].tolist()
                sel_idx    = st.selectbox("Product", range(len(prod_names)),
                                          format_func=lambda i: prod_names[i], key="q_prod")
                q_name     = prod_names[sel_idx]
                q_pid      = prod_ids[sel_idx]
                q_cat      = products_df.iloc[sel_idx]["category"]
            else:
                exp_cats = ["Ingredients","Utilities","Staff/Wages","Packaging",
                            "Rent","Transport","Logistics","Stationery","Marketing",
                            "Maintenance","Miscellaneous"]
                q_cat  = st.selectbox("Category", exp_cats, key="q_cat")
                q_name = st.text_input("Description", key="q_desc")
                q_pid  = None
            q_amt  = st.number_input("Amount (₦)", min_value=0, step=100, key="q_amt")
            q_date = st.date_input("Date", value=date.today(), key="q_date")
            q_note = st.text_input("Note (optional)", key="q_note")
            if st.form_submit_button("✅ Save", use_container_width=True, type="primary"):
                if q_amt > 0:
                    add_transaction(q_date, tx_type_q, q_pid, q_name, q_cat, q_amt, q_note)
                    st.success("Saved!")
                else:
                    st.error("Enter an amount > 0")

        st.divider()
        st.caption("Kokari Cafe v1.0 · Built with Streamlit")

    # ── Load data ─────────────────────────────────────────────────────────────
    df  = load_transactions(str(from_date), str(to_date))
    kpi = compute_kpis(df)

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("## ☕ Kokari Cafe · Financial Dashboard")
    st.caption(f"Period: **{from_date}** → **{to_date}**  ·  {kpi['days']} trading days  ·  {len(df)} transactions")

    # ── TABS ──────────────────────────────────────────────────────────────────
    tabs = st.tabs(["📊 Dashboard","➕ Entry","📋 Ledger","🛍 Products","🧾 Expenses","📈 P&L Report"])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — DASHBOARD
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[0]:
        # ── KPI Row ──
        c1,c2,c3,c4,c5,c6,c7,c8 = st.columns(8)
        metrics = [
            (c1,"💰 Revenue",        fmt(kpi["revenue"]),   f"{kpi['days']} days"),
            (c2,"📈 Avg Daily",      fmt(kpi["avg_daily"]), "per day"),
            (c3,"✅ Gross Profit",   fmt(kpi["gp"]),        f"Margin {kpi['gp_mar']:.1f}%"),
            (c4,"💵 Net Profit",     fmt(kpi["np"]),        f"Margin {kpi['np_mar']:.1f}%"),
            (c5,"🧾 Expenses",       fmt(kpi["expenses"]),  "operating costs"),
            (c6,"📦 COGS",           fmt(kpi["cogs"]),      "est. cost of sales"),
            (c7,"🏢 B2B Sales",      fmt(kpi["b2b"]),
             f"{(kpi['b2b']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}% of rev"),
            (c8,"☕ Cafe Sales",     fmt(kpi["cafe"]),
             f"{(kpi['cafe']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}% of rev"),
        ]
        for col, label, val, sub in metrics:
            col.metric(label, val, sub)

        st.divider()

        # ── Daily Revenue Chart ──
        sales_df = df[df["type"] == "sale"].copy()
        exp_df   = df[df["type"] == "expense"].copy()

        if not sales_df.empty:
            daily_s = sales_df.groupby("date").agg(
                revenue=("amount","sum"), cogs=("cogs","sum")).reset_index()
            daily_e = exp_df.groupby("date").agg(
                expenses=("amount","sum")).reset_index() if not exp_df.empty else pd.DataFrame(columns=["date","expenses"])
            daily = daily_s.merge(daily_e, on="date", how="left").fillna(0)
            daily["gross_profit"] = daily["revenue"] - daily["cogs"]
            daily["net_profit"]   = daily["gross_profit"] - daily["expenses"]
            daily = daily.sort_values("date")

            col_left, col_right = st.columns([2, 1])

            with col_left:
                st.markdown("#### Daily Revenue · COGS · Expenses · Net Profit")
                fig = go.Figure()
                for col_name, label, color in [
                    ("revenue",      "Revenue",      PALETTE[0]),
                    ("gross_profit", "Gross Profit", PALETTE[1]),
                    ("expenses",     "Expenses",     PALETTE[3]),
                    ("net_profit",   "Net Profit",   PALETTE[4]),
                ]:
                    fig.add_trace(go.Bar(
                        x=daily["date"], y=daily[col_name],
                        name=label, marker_color=color,
                        hovertemplate=f"<b>{label}</b>: ₦%{{y:,.0f}}<extra></extra>"
                    ))
                fig_layout(fig, height=330)
                fig.update_layout(barmode="group")
                st.plotly_chart(fig, use_container_width=True)

            with col_right:
                st.markdown("#### Revenue by Channel")
                channel = sales_df.groupby("category")["amount"].sum().reset_index()
                fig_pie = px.pie(channel, values="amount", names="category",
                                 color_discrete_sequence=PALETTE, hole=0.4)
                fig_pie.update_traces(
                    textposition="inside", textinfo="percent+label",
                    hovertemplate="<b>%{label}</b>: ₦%{value:,.0f}<extra></extra>"
                )
                fig_layout(fig_pie, height=330, legend=False)
                st.plotly_chart(fig_pie, use_container_width=True)

            # ── Trend line ──
            st.markdown("#### Revenue vs Net Profit Trend")
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=daily["date"], y=daily["revenue"], name="Revenue",
                line=dict(color=PALETTE[0], width=2), mode="lines+markers",
                hovertemplate="Revenue: ₦%{y:,.0f}<extra></extra>"
            ))
            fig_line.add_trace(go.Scatter(
                x=daily["date"], y=daily["net_profit"], name="Net Profit",
                line=dict(color=PALETTE[4], width=2.5), mode="lines+markers",
                marker=dict(size=7),
                hovertemplate="Net Profit: ₦%{y:,.0f}<extra></extra>"
            ))
            fig_layout(fig_line, height=270)
            st.plotly_chart(fig_line, use_container_width=True)

            # ── Top products ──
            st.markdown("#### Top Products by Revenue")
            prod_perf = (sales_df.groupby(["name","category"])
                         .agg(revenue=("amount","sum"), cogs=("cogs","sum")).reset_index())
            prod_perf["gross_profit"] = prod_perf["revenue"] - prod_perf["cogs"]
            prod_perf["margin"] = (prod_perf["gross_profit"] / prod_perf["revenue"] * 100).round(1)
            prod_perf = prod_perf.sort_values("revenue", ascending=False).head(10)
            fig_h = px.bar(prod_perf, x="revenue", y="name", orientation="h",
                           color="gross_profit",
                           color_continuous_scale=["#fca5a5","#86efac","#166534"],
                           labels={"revenue":"Revenue (₦)","name":"","gross_profit":"Gross Profit"},
                           hover_data={"margin":True})
            fig_h.update_traces(
                hovertemplate="<b>%{y}</b><br>Revenue: ₦%{x:,.0f}<br>Margin: %{customdata[0]:.1f}%<extra></extra>"
            )
            fig_layout(fig_h, height=350, legend=False)
            st.plotly_chart(fig_h, use_container_width=True)

            # ── Insight cards ──
            best_rev_day  = daily.loc[daily["revenue"].idxmax()]
            best_prof_day = daily.loc[daily["net_profit"].idxmax()]
            top_product   = prod_perf.iloc[0]
            ic1,ic2,ic3,ic4 = st.columns(4)
            ic1.success(f"🏆 **Best Revenue Day**\n\n{best_rev_day['date']}  \n{fmt(best_rev_day['revenue'])}")
            ic2.success(f"💰 **Best Profit Day**\n\n{best_prof_day['date']}  \n{fmt(best_prof_day['net_profit'])}")
            ic3.info(   f"📦 **Top Product**\n\n{top_product['name']}  \n{fmt(top_product['revenue'])}")
            ic4.warning(f"🏢 **B2B Share**\n\n{(kpi['b2b']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}% of revenue  \n{fmt(kpi['b2b'])}")
        else:
            st.info("No sales data in selected range. Use the Entry tab to add transactions.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — ENTRY
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("### ➕ Add / Edit Transaction")
        products_df = load_products()
        EXP_CATS = ["Ingredients","Utilities","Staff/Wages","Packaging","Rent",
                    "Transport","Logistics","Stationery","Marketing","Maintenance","Miscellaneous"]

        edit_id = st.session_state.get("edit_id")
        edit_row = None
        if edit_id:
            matches = df[df["id"] == edit_id]
            if not matches.empty:
                edit_row = matches.iloc[0]
            st.info(f"✏️ Editing transaction #{edit_id}  —  [Cancel edit](javascript:void(0))")
            if st.button("❌ Cancel Edit"):
                st.session_state.pop("edit_id", None)
                st.rerun()

        with st.form("tx_form", clear_on_submit=not edit_id):
            fc1, fc2 = st.columns(2)
            with fc1:
                f_date = st.date_input("Date",
                    value=pd.to_datetime(edit_row["date"]).date() if edit_row is not None else date.today())
            with fc2:
                type_opts = ["sale","expense"]
                f_type = st.selectbox("Transaction Type",
                    type_opts,
                    index=type_opts.index(edit_row["type"]) if edit_row is not None else 0)

            if f_type == "sale":
                prod_names = products_df["name"].tolist()
                prod_ids   = products_df["id"].tolist()
                default_idx = 0
                if edit_row is not None and edit_row["product_id"] in prod_ids:
                    default_idx = prod_ids.index(edit_row["product_id"])
                sel_idx = st.selectbox("Product / Service",
                    range(len(prod_names)), format_func=lambda i: f"{prod_names[i]}  [{products_df.iloc[i]['category']}]",
                    index=default_idx)
                f_pid  = prod_ids[sel_idx]
                f_name = prod_names[sel_idx]
                f_cat  = products_df.iloc[sel_idx]["category"]
                ratio  = products_df.iloc[sel_idx]["cost_ratio"]
                f_amt  = st.number_input("Sales Amount (₦)", min_value=0.0, step=100.0,
                    value=float(edit_row["amount"]) if edit_row is not None else 0.0)
                if f_amt > 0:
                    est_cogs = f_amt * ratio
                    est_gp   = f_amt - est_cogs
                    pc1,pc2,pc3 = st.columns(3)
                    pc1.metric("Revenue",      fmt(f_amt))
                    pc2.metric("Est. COGS",    fmt(est_cogs))
                    pc3.metric("Gross Profit", fmt(est_gp))
            else:
                fc3, fc4 = st.columns(2)
                with fc3:
                    f_cat = st.selectbox("Category", EXP_CATS,
                        index=EXP_CATS.index(edit_row["category"]) if edit_row is not None
                              and edit_row["category"] in EXP_CATS else 0)
                with fc4:
                    f_name = st.text_input("Item Description",
                        value=edit_row["name"] if edit_row is not None else "",
                        placeholder="e.g. Chicken Wings, NEPA Bill")
                f_pid = None
                f_amt = st.number_input("Amount (₦)", min_value=0.0, step=100.0,
                    value=float(edit_row["amount"]) if edit_row is not None else 0.0)

            f_note = st.text_input("Note (optional)",
                value=edit_row["note"] if edit_row is not None else "",
                placeholder="e.g. Valentine's Day event, Zenith Bank order…")

            btn_label = "💾 Update Transaction" if edit_id else "✅ Save Transaction"
            submitted = st.form_submit_button(btn_label, type="primary", use_container_width=True)

            if submitted:
                if f_amt <= 0:
                    st.error("Amount must be greater than 0.")
                else:
                    if edit_id:
                        update_transaction(edit_id, f_date, f_type, f_pid, f_name, f_cat, f_amt, f_note)
                        st.session_state.pop("edit_id", None)
                        st.success(f"Transaction #{edit_id} updated!")
                    else:
                        add_transaction(f_date, f_type, f_pid, f_name, f_cat, f_amt, f_note)
                        st.success("Transaction saved!")
                    st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LEDGER
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("### 📋 Transaction Ledger")
        lc1, lc2, lc3 = st.columns([3,1,1])
        with lc1:
            search_q = st.text_input("🔍 Search", placeholder="Search by name, category…", label_visibility="collapsed")
        with lc2:
            type_filter = st.selectbox("Filter", ["All","Sales","Expenses"], label_visibility="collapsed")
        with lc3:
            st.metric("Total records", len(df))

        ledger = df.copy()
        if search_q:
            mask = (ledger["name"].str.contains(search_q, case=False, na=False) |
                    ledger["category"].str.contains(search_q, case=False, na=False) |
                    ledger["date"].str.contains(search_q, na=False))
            ledger = ledger[mask]
        if type_filter == "Sales":
            ledger = ledger[ledger["type"] == "sale"]
        elif type_filter == "Expenses":
            ledger = ledger[ledger["type"] == "expense"]

        # Display & action columns
        st.markdown(f"Showing **{len(ledger)}** records")

        # Colour-coded table
        def style_type(val):
            return "color: #16a34a; font-weight:600" if val == "sale" else "color: #dc2626; font-weight:600"

        display_cols = ["id","date","type","name","category","amount","cogs","note"]
        styled = (ledger[display_cols]
                  .rename(columns={"id":"ID","date":"Date","type":"Type","name":"Description",
                                   "category":"Category","amount":"Amount (₦)","cogs":"COGS (₦)","note":"Note"})
                  .style
                  .format({"Amount (₦)":"{:,.0f}", "COGS (₦)":"{:,.0f}"})
                  .applymap(style_type, subset=["Type"]))
        st.dataframe(styled, use_container_width=True, height=380)

        # Totals footer
        tot_s = ledger[ledger["type"]=="sale"]
        tc1,tc2,tc3,tc4 = st.columns(4)
        tc1.metric("Total Sales",      fmt(tot_s["amount"].sum()))
        tc2.metric("Total COGS",       fmt(tot_s["cogs"].sum()))
        tc3.metric("Gross Profit",     fmt(tot_s["amount"].sum() - tot_s["cogs"].sum()))
        tc4.metric("Total Expenses",   fmt(ledger[ledger["type"]=="expense"]["amount"].sum()))

        # Edit / Delete
        st.divider()
        st.markdown("**Edit or Delete a Transaction**")
        ec1, ec2, ec3 = st.columns(3)
        with ec1:
            action_id = st.number_input("Transaction ID", min_value=1, step=1, value=1)
        with ec2:
            if st.button("✏️ Edit this transaction", use_container_width=True):
                if action_id in df["id"].values:
                    st.session_state["edit_id"] = int(action_id)
                    st.rerun()
                else:
                    st.error("ID not found in current range.")
        with ec3:
            if st.button("🗑️ Delete this transaction", use_container_width=True, type="secondary"):
                if action_id in df["id"].values:
                    delete_transaction(int(action_id))
                    st.success(f"Transaction #{action_id} deleted.")
                    st.rerun()
                else:
                    st.error("ID not found in current range.")

        # CSV export
        st.download_button(
            "⬇️ Export to CSV",
            data=ledger.to_csv(index=False).encode("utf-8"),
            file_name=f"kokari_transactions_{from_date}_{to_date}.csv",
            mime="text/csv",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — PRODUCTS
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("### 🛍 Product Performance")
        sales_df = df[df["type"] == "sale"].copy()

        if not sales_df.empty:
            prod_perf = (sales_df.groupby(["name","category"])
                         .agg(revenue=("amount","sum"), cogs=("cogs","sum")).reset_index())
            prod_perf["gross_profit"] = prod_perf["revenue"] - prod_perf["cogs"]
            prod_perf["margin_pct"]   = (prod_perf["gross_profit"] / prod_perf["revenue"] * 100).round(1)
            prod_perf["rev_share"]    = (prod_perf["revenue"] / kpi["revenue"] * 100).round(1)
            prod_perf = prod_perf.sort_values("revenue", ascending=False)

            # Table
            display = prod_perf.copy()
            display.columns = ["Product","Channel","Revenue (₦)","COGS (₦)","Gross Profit (₦)","Margin %","Rev Share %"]
            display["Revenue (₦)"]      = display["Revenue (₦)"].map("{:,.0f}".format)
            display["COGS (₦)"]         = display["COGS (₦)"].map("{:,.0f}".format)
            display["Gross Profit (₦)"] = display["Gross Profit (₦)"].map("{:,.0f}".format)
            st.dataframe(display, use_container_width=True, hide_index=True)

            # Charts
            pc_l, pc_r = st.columns(2)
            with pc_l:
                st.markdown("#### Revenue vs Gross Profit")
                fig_bp = go.Figure()
                fig_bp.add_trace(go.Bar(
                    y=prod_perf["name"], x=prod_perf["revenue"],
                    name="Revenue", orientation="h", marker_color=PALETTE[0],
                    hovertemplate="<b>%{y}</b><br>Revenue: ₦%{x:,.0f}<extra></extra>"
                ))
                fig_bp.add_trace(go.Bar(
                    y=prod_perf["name"], x=prod_perf["gross_profit"],
                    name="Gross Profit", orientation="h", marker_color=PALETTE[1],
                    hovertemplate="<b>%{y}</b><br>Gross Profit: ₦%{x:,.0f}<extra></extra>"
                ))
                fig_bp.update_layout(barmode="group")
                fig_layout(fig_bp, height=400)
                st.plotly_chart(fig_bp, use_container_width=True)

            with pc_r:
                st.markdown("#### Gross Margin by Product")
                fig_margin = px.bar(
                    prod_perf.sort_values("margin_pct"),
                    x="margin_pct", y="name", orientation="h",
                    color="margin_pct",
                    color_continuous_scale=["#fca5a5","#fde68a","#86efac","#166534"],
                    labels={"margin_pct":"Margin %","name":""},
                )
                fig_margin.update_traces(
                    hovertemplate="<b>%{y}</b><br>Margin: %{x:.1f}%<extra></extra>")
                fig_layout(fig_margin, height=400, legend=False)
                st.plotly_chart(fig_margin, use_container_width=True)
        else:
            st.info("No sales data in selected range.")

        # ── Manage Products ──
        with st.expander("⚙️ Manage Products / Update Cost Ratios"):
            prods_df = load_products()
            st.info("Update cost ratios (0.0–1.0) to reflect your actual supplier costs. "
                    "Example: 0.40 means the item costs 40% of selling price to make.")
            edited = st.data_editor(
                prods_df, use_container_width=True, num_rows="fixed",
                column_config={
                    "cost_ratio": st.column_config.NumberColumn(
                        "Cost Ratio", min_value=0.0, max_value=1.0,
                        step=0.01, format="%.2f",
                        help="Cost as a fraction of selling price"
                    )
                },
                disabled=["id","name","category"],
            )
            if st.button("💾 Save Cost Ratios", type="primary"):
                with get_conn() as conn:
                    for _, row in edited.iterrows():
                        conn.execute("UPDATE products SET cost_ratio=? WHERE id=?",
                                     (row["cost_ratio"], row["id"]))
                invalidate_cache()
                st.success("Cost ratios updated!")
                st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 — EXPENSES
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("### 🧾 Expense Analysis")
        exp_df = df[df["type"] == "expense"].copy()

        if not exp_df.empty:
            by_cat  = exp_df.groupby("category")["amount"].sum().reset_index().sort_values("amount", ascending=False)
            by_item = exp_df.groupby(["name","category"])["amount"].sum().reset_index().sort_values("amount", ascending=False)
            by_day  = exp_df.groupby("date")["amount"].sum().reset_index().sort_values("date")

            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("#### Expenses by Category")
                fig_ep = px.pie(by_cat, values="amount", names="category",
                                color_discrete_sequence=PALETTE, hole=0.35)
                fig_ep.update_traces(
                    textposition="inside", textinfo="percent+label",
                    hovertemplate="<b>%{label}</b>: ₦%{value:,.0f}<extra></extra>"
                )
                fig_layout(fig_ep, height=310, legend=False)
                st.plotly_chart(fig_ep, use_container_width=True)

            with ec2:
                st.markdown("#### Daily Spend")
                fig_ed = px.bar(by_day, x="date", y="amount",
                                color_discrete_sequence=[PALETTE[3]],
                                labels={"amount":"Amount (₦)","date":"Date"})
                fig_ed.update_traces(
                    hovertemplate="<b>%{x}</b>: ₦%{y:,.0f}<extra></extra>")
                fig_layout(fig_ed, height=310)
                st.plotly_chart(fig_ed, use_container_width=True)

            # Category totals
            st.markdown("#### Category Breakdown")
            total_exp = exp_df["amount"].sum()
            by_cat["% of Expenses"] = (by_cat["amount"] / total_exp * 100).round(1)
            by_cat.columns = ["Category","Total (₦)","% of Expenses"]
            by_cat["Total (₦)"] = by_cat["Total (₦)"].map("{:,.0f}".format)
            st.dataframe(by_cat, use_container_width=True, hide_index=True)

            # Itemised
            st.markdown("#### Itemised Purchases")
            by_item["% of Expenses"] = (by_item["amount"] / total_exp * 100).round(1)
            by_item.columns = ["Item","Category","Amount (₦)","% of Expenses"]
            by_item["Amount (₦)"] = by_item["Amount (₦)"].map("{:,.0f}".format)
            st.dataframe(by_item, use_container_width=True, hide_index=True, height=400)

            # Export
            st.download_button(
                "⬇️ Export Expenses CSV",
                data=exp_df.to_csv(index=False).encode("utf-8"),
                file_name=f"kokari_expenses_{from_date}_{to_date}.csv",
                mime="text/csv",
            )
        else:
            st.info("No expense data in selected range.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 6 — P&L REPORT
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown("### 📈 Profit & Loss Report")
        st.caption(f"Period: {from_date} → {to_date}")

        col_pl, col_ratio = st.columns([1, 1])

        with col_pl:
            st.markdown("#### Income Statement")
            exp_df = df[df["type"] == "expense"]
            by_exp_cat = exp_df.groupby("category")["amount"].sum().to_dict() if not exp_df.empty else {}
            sales_df2  = df[df["type"] == "sale"]

            pl_rows = [
                ("REVENUE", "", True),
                ("  Cafe Sales",       fmt(kpi["cafe"]),     False),
                ("  Packaged Goods",   fmt(kpi["pkg"]),      False),
                ("  B2B / Wholesale",  fmt(kpi["b2b"]),      False),
                ("── Total Revenue",   fmt(kpi["revenue"]),  True),
                ("", "", False),
                ("COST OF GOODS SOLD", "", True),
                ("  Est. COGS",        f"({fmt(kpi['cogs'])})", False),
                ("── Gross Profit",    fmt(kpi["gp"]),       True),
                (f"  Gross Margin",    f"{kpi['gp_mar']:.1f}%", False),
                ("", "", False),
                ("OPERATING EXPENSES", "", True),
                *[(f"  {cat}", f"({fmt(amt)})", False) for cat, amt in sorted(by_exp_cat.items())],
                ("── Total Expenses",  f"({fmt(kpi['expenses'])})", True),
                ("", "", False),
                ("══ NET PROFIT / LOSS", fmt(kpi["np"]),     True),
                ("  Net Margin",        f"{kpi['np_mar']:.1f}%", False),
            ]

            for label, value, is_bold in pl_rows:
                if label == "":
                    st.markdown("---")
                    continue
                prefix = "**" if is_bold else ""
                suffix = "**" if is_bold else ""
                color = ""
                if "NET PROFIT" in label:
                    color = "color:green;" if kpi["np"] >= 0 else "color:red;"
                lc, vc = st.columns([3, 1])
                lc.markdown(f"{prefix}{label}{suffix}")
                vc.markdown(f"<span style='float:right;{color}'>{prefix}{value}{suffix}</span>",
                             unsafe_allow_html=True)

        with col_ratio:
            st.markdown("#### Financial Ratios")
            ratios = [
                ("Gross Margin",            f"{kpi['gp_mar']:.1f}%",
                 "normal" if kpi["gp_mar"] > 40 else "off"),
                ("Net Margin",              f"{kpi['np_mar']:.1f}%",
                 "normal" if kpi["np_mar"] > 10 else "off"),
                ("Expense Ratio",
                 f"{(kpi['expenses']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}%", "normal"),
                ("COGS Ratio",
                 f"{(kpi['cogs']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}%", "normal"),
                ("B2B Share",
                 f"{(kpi['b2b']/kpi['revenue']*100 if kpi['revenue'] else 0):.1f}%", "normal"),
                ("Avg Daily Revenue",        fmt(kpi["avg_daily"]), "normal"),
                ("Revenue per ₦1 Expense",
                 f"₦{(kpi['revenue']/kpi['expenses'] if kpi['expenses'] else 0):.2f}", "normal"),
            ]
            for label, val, delta_type in ratios:
                st.metric(label, val)

            st.markdown("---")
            st.markdown("""
<div class='note-box'>
<b>⚠️ Accountant Notes</b><br><br>
• <b>COGS</b> are estimated from product cost ratios — update ratios in the Products tab with actual supplier invoices.<br>
• <b>Powder Milk ₦44,000</b> is the single largest purchase — monitor usage vs sales output.<br>
• <b>B2B Wholesale ₦504,513</b> was one transaction on Feb 13 — verify delivery & payment confirmation.<br>
• <b>No expenses recorded Feb 10–13</b> — confirm if purchases were missed or genuinely none.<br>
• <b>Imprest items</b> (Bucket, Delivery x7) should be reconciled with petty cash records.
</div>
""", unsafe_allow_html=True)

        # ── Waterfall / Cost structure chart ──
        st.markdown("#### Revenue & Cost Structure")
        waterfall_data = pd.DataFrame([
            {"Item": "Revenue",      "Amount": kpi["revenue"],   "color": PALETTE[0]},
            {"Item": "Gross Profit", "Amount": kpi["gp"],        "color": PALETTE[1]},
            {"Item": "Expenses",     "Amount": kpi["expenses"],  "color": PALETTE[3]},
            {"Item": "COGS",         "Amount": kpi["cogs"],      "color": PALETTE[7]},
            {"Item": "Net Profit",   "Amount": kpi["np"],
             "color": PALETTE[1] if kpi["np"] >= 0 else PALETTE[2]},
        ])
        fig_wf = px.bar(waterfall_data, x="Item", y="Amount",
                        color="Item", color_discrete_sequence=PALETTE,
                        labels={"Amount":"₦"})
        fig_wf.update_traces(
            hovertemplate="<b>%{x}</b>: ₦%{y:,.0f}<extra></extra>")
        fig_layout(fig_wf, height=280, legend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

        # Export full P&L
        pl_export = pd.DataFrame({
            "Metric": ["Revenue","Cafe Sales","B2B Sales","Packaged Sales",
                       "COGS","Gross Profit","Gross Margin %",
                       "Operating Expenses","Net Profit","Net Margin %"],
            "Value (₦ or %)": [
                kpi["revenue"], kpi["cafe"], kpi["b2b"], kpi["pkg"],
                kpi["cogs"], kpi["gp"], f"{kpi['gp_mar']:.1f}%",
                kpi["expenses"], kpi["np"], f"{kpi['np_mar']:.1f}%",
            ]
        })
        st.download_button(
            "⬇️ Export P&L to CSV",
            data=pl_export.to_csv(index=False).encode("utf-8"),
            file_name=f"kokari_PnL_{from_date}_{to_date}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()


# ══════════════════════════════════════════════════════════════════════════════
# requirements.txt  (create this file in the same folder)
# ══════════════════════════════════════════════════════════════════════════════
# streamlit>=1.32.0
# pandas>=2.0.0
# plotly>=5.18.0
#
# ══════════════════════════════════════════════════════════════════════════════
# .streamlit/config.toml  (optional — create this folder/file for deployment)
# ══════════════════════════════════════════════════════════════════════════════
# [theme]
# primaryColor = "#2563eb"
# backgroundColor = "#f9fafb"
# secondaryBackgroundColor = "#ffffff"
# textColor = "#111827"
# font = "sans serif"
#
# [server]
# headless = true
# port = 8501
# enableCORS = false
#
# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE PATH — PostgreSQL for production
# ══════════════════════════════════════════════════════════════════════════════
# Replace the get_conn() context manager with:
#
#   import psycopg2
#   from contextlib import contextmanager
#
#   @contextmanager
#   def get_conn():
#       conn = psycopg2.connect(
#           host=os.environ["DB_HOST"],
#           database=os.environ["DB_NAME"],
#           user=os.environ["DB_USER"],
#           password=os.environ["DB_PASSWORD"],
#           port=os.environ.get("DB_PORT", 5432)
#       )
#       try:
#           yield conn
#           conn.commit()
#       finally:
#           conn.close()
#
#   Store credentials in .env or Streamlit Cloud secrets.toml:
#   [secrets]
#   DB_HOST = "your-host"
#   DB_NAME = "kokari"
#   DB_USER = "postgres"
#   DB_PASSWORD = "your-password"
