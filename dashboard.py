"""
KOKARI CAFE FINANCIAL DASHBOARD
================================
Requirements:  streamlit  pandas  (that is ALL)
Run:           streamlit run dashboard.py
"""

# ── Standard library only ─────────────────────────────────────────
import sqlite3
from datetime import date, timedelta
from contextlib import contextmanager

# ── Third party — ONLY these two, nothing else ────────────────────
import streamlit as st
import pandas as pd

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kokari Cafe",
    page_icon="☕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────
DB_PATH  = "kokari_cafe.db"
EXP_CATS = [
    "Ingredients", "Utilities", "Staff/Wages", "Packaging", "Rent",
    "Transport", "Logistics", "Stationery", "Marketing",
    "Maintenance", "Miscellaneous",
]

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────
def fmt(n):
    try:
        return f"\u20a6{int(float(n)):,}"
    except Exception:
        return "\u20a60"

def safe_pct(num, denom):
    try:
        return round(num / denom * 100, 1) if denom else 0.0
    except Exception:
        return 0.0

# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────
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
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                category   TEXT NOT NULL,
                cost_ratio REAL NOT NULL DEFAULT 0.40
            )""")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                date       TEXT NOT NULL,
                type       TEXT NOT NULL,
                product_id TEXT,
                name       TEXT,
                category   TEXT NOT NULL,
                amount     REAL NOT NULL,
                cogs       REAL NOT NULL DEFAULT 0,
                note       TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now'))
            )""")

        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            cur.executemany("INSERT INTO products VALUES (?,?,?,?)", [
                ("p01", "Pancakes",              "Cafe",     0.38),
                ("p02", "Fruit Smoothie",         "Cafe",     0.40),
                ("p03", "Books",                  "Retail",   0.55),
                ("p04", "Puff Puff",              "Cafe",     0.30),
                ("p05", "Spicy Chicken Wrap",     "Cafe",     0.42),
                ("p06", "Chicken Wings",          "Cafe",     0.45),
                ("p07", "Tapioca",                "Cafe",     0.35),
                ("p08", "Coffee",                 "Cafe",     0.28),
                ("p09", "Ice Coffee",             "Cafe",     0.30),
                ("p10", "Zobo",                   "Cafe",     0.25),
                ("p11", "Parfait & Wings Combo",  "Cafe",     0.45),
                ("p12", "Parfait Cafe",           "Cafe",     0.40),
                ("p13", "Granola 500g",           "Packaged", 0.50),
                ("p14", "Spicy Coconut Flakes",   "Packaged", 0.48),
                ("p15", "Honey Coconut Cashew",   "Packaged", 0.50),
                ("p16", "CCB",                    "Packaged", 0.50),
                ("p17", "Wholesale (B2B)",        "B2B",      0.55),
                ("p18", "Take Away",              "Cafe",     0.40),
                ("p19", "Water",                  "Cafe",     0.20),
                ("p20", "Space Rental",           "Other",    0.05),
            ])

        cur.execute("SELECT COUNT(*) FROM transactions")
        if cur.fetchone()[0] == 0:
            _seed(cur)


def _get_ratio(cur, pid):
    row = cur.execute(
        "SELECT cost_ratio FROM products WHERE id=?", (pid,)).fetchone()
    return row[0] if row else 0.40


def _seed(cur):
    sales = [
        ("2026-02-09","p01","Pancakes",            "Cafe",     15060),
        ("2026-02-09","p08","Coffee",              "Cafe",     8600),
        ("2026-02-09","p09","Ice Coffee",          "Cafe",     18215),
        ("2026-02-09","p11","Parfait & Wings Combo","Cafe",    26355),
        ("2026-02-09","p12","Parfait Cafe",        "Cafe",     21500),
        ("2026-02-10","p01","Pancakes",            "Cafe",     11295),
        ("2026-02-10","p08","Coffee",              "Cafe",     11370),
        ("2026-02-10","p09","Ice Coffee",          "Cafe",     4840),
        ("2026-02-10","p10","Zobo",                "Cafe",     3765),
        ("2026-02-10","p11","Parfait & Wings Combo","Cafe",    8600),
        ("2026-02-10","p14","Spicy Coconut Flakes","Packaged", 3765),
        ("2026-02-11","p01","Pancakes",            "Cafe",     11295),
        ("2026-02-11","p04","Puff Puff",           "Cafe",     3765),
        ("2026-02-11","p08","Coffee",              "Cafe",     11265),
        ("2026-02-11","p09","Ice Coffee",          "Cafe",     24200),
        ("2026-02-11","p10","Zobo",                "Cafe",     3765),
        ("2026-02-11","p11","Parfait & Wings Combo","Cafe",    8600),
        ("2026-02-11","p12","Parfait Cafe",        "Cafe",     10750),
        ("2026-02-12","p01","Pancakes",            "Cafe",     22815),
        ("2026-02-12","p02","Fruit Smoothie",      "Cafe",     14520),
        ("2026-02-12","p05","Spicy Chicken Wrap",  "Cafe",     10750),
        ("2026-02-12","p06","Chicken Wings",       "Cafe",     33260),
        ("2026-02-12","p07","Tapioca",             "Cafe",     4300),
        ("2026-02-12","p08","Coffee",              "Cafe",     9140),
        ("2026-02-12","p09","Ice Coffee",          "Cafe",     29050),
        ("2026-02-12","p10","Zobo",                "Cafe",     11295),
        ("2026-02-12","p13","Granola 500g",        "Packaged", 6757),
        ("2026-02-13","p01","Pancakes",            "Cafe",     18825),
        ("2026-02-13","p02","Fruit Smoothie",      "Cafe",     24200),
        ("2026-02-13","p03","Books",               "Retail",   21950),
        ("2026-02-13","p04","Puff Puff",           "Cafe",     3765),
        ("2026-02-13","p05","Spicy Chicken Wrap",  "Cafe",     10750),
        ("2026-02-13","p06","Chicken Wings",       "Cafe",     8065),
        ("2026-02-13","p08","Coffee",              "Cafe",     3765),
        ("2026-02-13","p09","Ice Coffee",          "Cafe",     4840),
        ("2026-02-13","p12","Parfait Cafe",        "Cafe",     5375),
        ("2026-02-13","p14","Spicy Coconut Flakes","Packaged", 3765),
        ("2026-02-13","p17","Wholesale (B2B)",     "B2B",      504513),
        ("2026-02-14","p01","Pancakes",            "Cafe",     26355),
        ("2026-02-14","p02","Fruit Smoothie",      "Cafe",     4840),
        ("2026-02-14","p05","Spicy Chicken Wrap",  "Cafe",     10750),
        ("2026-02-14","p06","Chicken Wings",       "Cafe",     8065),
        ("2026-02-14","p07","Tapioca",             "Cafe",     21500),
        ("2026-02-14","p08","Coffee",              "Cafe",     29575),
        ("2026-02-14","p09","Ice Coffee",          "Cafe",     9680),
        ("2026-02-14","p10","Zobo",                "Cafe",     11295),
        ("2026-02-14","p11","Parfait & Wings Combo","Cafe",    25800),
    ]
    for s in sales:
        amt = s[4]
        c   = round(amt * _get_ratio(cur, s[1]))
        cur.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,?,?,?,?,?,'')",
            (s[0], "sale", s[1], s[2], s[3], amt, c))

    expenses = [
        ("2026-02-09","Ingredients","Sugar",               9000),
        ("2026-02-09","Ingredients","Flour",               5400),
        ("2026-02-09","Ingredients","Chicken Wings",       20178),
        ("2026-02-09","Ingredients","Chicken",             11000),
        ("2026-02-09","Ingredients","Bread",               1200),
        ("2026-02-09","Ingredients","Mayonnaise",          4000),
        ("2026-02-09","Ingredients","Banana",              2000),
        ("2026-02-09","Ingredients","Oil",                 4400),
        ("2026-02-09","Ingredients","Carrot and Cabbage",  3000),
        ("2026-02-09","Ingredients","Eggs",                5900),
        ("2026-02-09","Ingredients","Groundnut",           2000),
        ("2026-02-09","Ingredients","Powder Milk",         44000),
        ("2026-02-09","Ingredients","Pineapple",           3000),
        ("2026-02-09","Ingredients","Ginger",              2000),
        ("2026-02-09","Ingredients","Cinnamon",            2000),
        ("2026-02-09","Ingredients","Cloves",              1000),
        ("2026-02-09","Ingredients","Grapes",              6000),
        ("2026-02-09","Ingredients","Honey",               6000),
        ("2026-02-09","Ingredients","Liquid Milk",         10200),
        ("2026-02-09","Utilities",  "NEPA Electricity",    10000),
        ("2026-02-09","Utilities",  "Data",                3500),
        ("2026-02-09","Utilities",  "Recharge Card",       2000),
        ("2026-02-09","Utilities",  "Water CWay",          3400),
        ("2026-02-09","Packaging",  "Zobo Bottles",        4400),
        ("2026-02-09","Packaging",  "Foil",                3000),
        ("2026-02-09","Packaging",  "Serviettes",          2000),
        ("2026-02-09","Packaging",  "Spoons",              3000),
        ("2026-02-09","Packaging",  "Water retail",        2500),
        ("2026-02-09","Transport",  "Transport",           1500),
        ("2026-02-09","Miscellaneous","Printing",          400),
        ("2026-02-09","Miscellaneous","Bank Charges",      400),
        ("2026-02-09","Miscellaneous","Phone Repair",      500),
        ("2026-02-09","Logistics",  "Bucket",              3000),
        ("2026-02-09","Logistics",  "Item Delivery x7",   2000),
        ("2026-02-09","Utilities",  "NEPA Imprest",        3000),
        ("2026-02-09","Stationery", "Battery and Book",    1000),
        ("2026-02-09","Packaging",  "Serviettes Imprest",  1000),
        ("2026-02-14","Ingredients","Chicken Wings",       36321),
        ("2026-02-14","Ingredients","Flour",               6400),
        ("2026-02-14","Ingredients","Eggs",                6000),
        ("2026-02-14","Packaging",  "Straws",              3700),
        ("2026-02-14","Packaging",  "Water retail",        4400),
        ("2026-02-14","Transport",  "Transport",           500),
    ]
    for e in expenses:
        cur.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,'expense',?,?,?,?,'')",
            (e[0], "expense", e[2], e[1], e[3], e[3]))


# ─────────────────────────────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_tx(from_d, to_d):
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM transactions "
            "WHERE date BETWEEN ? AND ? "
            "ORDER BY date DESC, id DESC",
            conn, params=(str(from_d), str(to_d)))

@st.cache_data(ttl=60)
def load_products():
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM products ORDER BY category, name", conn)

def bust():
    load_tx.clear()
    load_products.clear()

def compute_kpis(df):
    s   = df[df["type"] == "sale"]
    e   = df[df["type"] == "expense"]
    rev = s["amount"].sum()
    cogs= s["cogs"].sum()
    exp = e["amount"].sum()
    gp  = rev - cogs
    np_ = gp - exp
    days= s["date"].nunique()
    return dict(
        revenue=rev, cogs=cogs, expenses=exp,
        gp=gp, np=np_,
        gp_mar=safe_pct(gp, rev),
        np_mar=safe_pct(np_, rev),
        b2b =s[s["category"]=="B2B"]["amount"].sum(),
        cafe=s[s["category"]=="Cafe"]["amount"].sum(),
        pkg =s[s["category"]=="Packaged"]["amount"].sum(),
        days=days,
        avg_daily=rev / days if days else 0,
    )

# ─────────────────────────────────────────────────────────────────
# CRUD
# ─────────────────────────────────────────────────────────────────
def add_tx(date_val, tx_type, pid, name, cat, amount, note=""):
    with get_conn() as conn:
        if tx_type == "sale":
            r    = conn.execute(
                "SELECT cost_ratio FROM products WHERE id=?", (pid,)).fetchone()
            cogs = round(amount * (r[0] if r else 0.40))
        else:
            cogs = amount
        conn.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (str(date_val), tx_type,
             pid if tx_type == "sale" else None,
             name, cat, amount, cogs, note))
    bust()

def update_tx(tid, date_val, tx_type, pid, name, cat, amount, note):
    with get_conn() as conn:
        if tx_type == "sale":
            r    = conn.execute(
                "SELECT cost_ratio FROM products WHERE id=?", (pid,)).fetchone()
            cogs = round(amount * (r[0] if r else 0.40))
        else:
            cogs = amount
        conn.execute(
            "UPDATE transactions "
            "SET date=?,type=?,product_id=?,name=?,category=?,"
            "amount=?,cogs=?,note=? WHERE id=?",
            (str(date_val), tx_type,
             pid if tx_type == "sale" else None,
             name, cat, amount, cogs, note, tid))
    bust()

def delete_tx(tid):
    with get_conn() as conn:
        conn.execute("DELETE FROM transactions WHERE id=?", (tid,))
    bust()

# ─────────────────────────────────────────────────────────────────
# CHART BUILDERS  (Streamlit native — no extra libraries)
# ─────────────────────────────────────────────────────────────────
def daily_bar(daily):
    """Revenue, Gross Profit, Expenses, Net Profit grouped by date."""
    chart_df = daily.set_index("date")[
        ["revenue", "gross_profit", "expenses", "net_profit"]]
    chart_df.columns = ["Revenue", "Gross Profit", "Expenses", "Net Profit"]
    st.bar_chart(chart_df, height=320)

def trend_line(daily):
    chart_df = daily.set_index("date")[["revenue", "net_profit"]]
    chart_df.columns = ["Revenue", "Net Profit"]
    st.line_chart(chart_df, height=260)

def product_bar(pp):
    chart_df = pp.set_index("name")[["revenue", "gross_profit"]]
    chart_df.columns = ["Revenue", "Gross Profit"]
    chart_df = chart_df.sort_values("Revenue", ascending=True)
    st.bar_chart(chart_df, height=340)

def expense_bar(by_cat):
    chart_df = by_cat.set_index("category")[["amount"]]
    chart_df.columns = ["Amount"]
    st.bar_chart(chart_df, height=300)

def daily_expense_bar(by_day):
    chart_df = by_day.set_index("date")[["amount"]]
    chart_df.columns = ["Daily Spend"]
    st.bar_chart(chart_df, height=300)

# ─────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────
def main():
    init_db()

    # ── Sidebar ──────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## ☕ Kokari Cafe")
        st.markdown("### Date Range")
        from_date = st.date_input("From", value=date(2026, 2, 9))
        to_date   = st.date_input("To",   value=date(2026, 2, 14))

        st.markdown("**Quick ranges**")
        q1, q2, q3 = st.columns(3)
        if q1.button("Week"):
            t         = date.today()
            from_date = t - timedelta(days=t.weekday())
            to_date   = t
        if q2.button("30d"):
            to_date   = date.today()
            from_date = to_date - timedelta(days=30)
        if q3.button("All"):
            from_date = date(2020, 1, 1)
            to_date   = date.today()

        st.divider()
        st.markdown("### Quick Add")
        products_df = load_products()

        with st.form("quick", clear_on_submit=True):
            qt = st.selectbox("Type", ["sale", "expense"])
            if qt == "sale":
                names = products_df["name"].tolist()
                ids   = products_df["id"].tolist()
                cats  = products_df["category"].tolist()
                idx   = st.selectbox("Product", range(len(names)),
                                     format_func=lambda i: names[i])
                q_pid  = ids[idx]
                q_name = names[idx]
                q_cat  = cats[idx]
            else:
                q_cat  = st.selectbox("Category", EXP_CATS)
                q_name = st.text_input("Description")
                q_pid  = None
            q_amt  = st.number_input("Amount", min_value=0, step=100)
            q_date = st.date_input("Date", value=date.today())
            q_note = st.text_input("Note")
            if st.form_submit_button("Save", use_container_width=True,
                                     type="primary"):
                if q_amt > 0:
                    add_tx(q_date, qt, q_pid, q_name, q_cat, q_amt, q_note)
                    st.success("Saved!")
                else:
                    st.error("Amount must be > 0")

    # ── Load & compute ────────────────────────────────────────────
    df  = load_tx(from_date, to_date)
    kpi = compute_kpis(df)

    # ── Header ────────────────────────────────────────────────────
    st.title("☕ Kokari Cafe · Financial Dashboard")
    st.caption(
        f"Period: {from_date} to {to_date}  |  "
        f"{kpi['days']} trading days  |  {len(df)} transactions")

    tabs = st.tabs([
        "Dashboard", "Entry", "Ledger",
        "Products",  "Expenses", "P&L Report",
    ])

    # ══════════════════════════════════════════════════════════════
    # DASHBOARD
    # ══════════════════════════════════════════════════════════════
    with tabs[0]:
        r1 = st.columns(4)
        r1[0].metric("Revenue",      fmt(kpi["revenue"]),
                     f"{kpi['days']} days")
        r1[1].metric("Gross Profit", fmt(kpi["gp"]),
                     f"Margin {kpi['gp_mar']}%")
        r1[2].metric("Net Profit",   fmt(kpi["np"]),
                     f"Margin {kpi['np_mar']}%")
        r1[3].metric("Expenses",     fmt(kpi["expenses"]),
                     f"COGS {fmt(kpi['cogs'])}")

        r2 = st.columns(4)
        r2[0].metric("Avg Daily",    fmt(kpi["avg_daily"]))
        r2[1].metric("B2B Sales",    fmt(kpi["b2b"]),
                     f"{safe_pct(kpi['b2b'],kpi['revenue'])}% of rev")
        r2[2].metric("Cafe Sales",   fmt(kpi["cafe"]),
                     f"{safe_pct(kpi['cafe'],kpi['revenue'])}% of rev")
        r2[3].metric("Packaged",     fmt(kpi["pkg"]),
                     f"{safe_pct(kpi['pkg'],kpi['revenue'])}% of rev")

        st.divider()

        s_df = df[df["type"] == "sale"].copy()
        e_df = df[df["type"] == "expense"].copy()

        if not s_df.empty:
            daily_s = (s_df.groupby("date")
                       .agg(revenue=("amount","sum"), cogs=("cogs","sum"))
                       .reset_index())
            daily_e = (e_df.groupby("date")["amount"].sum()
                       .reset_index(name="expenses")
                       if not e_df.empty
                       else pd.DataFrame(columns=["date","expenses"]))
            daily = (daily_s.merge(daily_e, on="date", how="left")
                     .fillna(0).sort_values("date"))
            daily["gross_profit"] = daily["revenue"] - daily["cogs"]
            daily["net_profit"]   = daily["gross_profit"] - daily["expenses"]

            st.markdown("#### Daily Revenue · Gross Profit · Expenses · Net Profit")
            daily_bar(daily)

            st.markdown("#### Revenue vs Net Profit Trend")
            trend_line(daily)

            # Top products
            pp = (s_df.groupby("name")
                  .agg(revenue=("amount","sum"), cogs=("cogs","sum"))
                  .reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp["margin"] = (pp["gross_profit"] / pp["revenue"] * 100).round(1)
            pp = pp.sort_values("revenue", ascending=False)

            st.markdown("#### Top Products by Revenue")
            product_bar(pp)

            # Insight cards
            bd  = daily.loc[daily["revenue"].idxmax()]
            bpd = daily.loc[daily["net_profit"].idxmax()]
            tp  = pp.iloc[0]["name"] if not pp.empty else "—"
            ic  = st.columns(4)
            ic[0].success(
                f"**Best Revenue Day**\n\n{bd['date']}\n\n{fmt(bd['revenue'])}")
            ic[1].success(
                f"**Best Profit Day**\n\n{bpd['date']}\n\n{fmt(bpd['net_profit'])}")
            ic[2].info(
                f"**Top Product**\n\n{tp}")
            ic[3].warning(
                f"**B2B Share**\n\n"
                f"{safe_pct(kpi['b2b'],kpi['revenue'])}%\n\n{fmt(kpi['b2b'])}")
        else:
            st.info("No sales data for this period.")

    # ══════════════════════════════════════════════════════════════
    # ENTRY
    # ══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("### Add / Edit Transaction")
        products_df = load_products()
        edit_id  = st.session_state.get("edit_id")
        edit_row = None

        if edit_id:
            st.info(f"Editing transaction #{edit_id}")
            if st.button("Cancel Edit"):
                st.session_state.pop("edit_id", None)
                st.rerun()
            m = df[df["id"] == edit_id]
            if not m.empty:
                edit_row = m.iloc[0]

        with st.form("tx_form", clear_on_submit=not bool(edit_id)):
            fc1, fc2 = st.columns(2)
            with fc1:
                f_date = st.date_input(
                    "Date",
                    value=(pd.to_datetime(edit_row["date"]).date()
                           if edit_row is not None else date.today()))
            with fc2:
                type_opts = ["sale", "expense"]
                f_type = st.selectbox(
                    "Type", type_opts,
                    index=(type_opts.index(edit_row["type"])
                           if edit_row is not None else 0))

            names  = products_df["name"].tolist()
            ids    = products_df["id"].tolist()
            cats   = products_df["category"].tolist()
            ratios = products_df["cost_ratio"].tolist()

            if f_type == "sale":
                def_i = 0
                if edit_row is not None and edit_row["product_id"] in ids:
                    def_i = ids.index(edit_row["product_id"])
                sel = st.selectbox(
                    "Product",
                    range(len(names)),
                    format_func=lambda i: f"{names[i]}  [{cats[i]}]",
                    index=def_i)
                f_pid  = ids[sel]
                f_name = names[sel]
                f_cat  = cats[sel]
                ratio  = ratios[sel]
                f_amt  = st.number_input(
                    "Sales Amount (N)",
                    min_value=0.0, step=100.0,
                    value=(float(edit_row["amount"])
                           if edit_row is not None else 0.0))
                if f_amt > 0:
                    pc = st.columns(3)
                    pc[0].metric("Revenue",      fmt(f_amt))
                    pc[1].metric("Est. COGS",    fmt(f_amt * ratio))
                    pc[2].metric("Gross Profit", fmt(f_amt * (1 - ratio)))
            else:
                fc3, fc4 = st.columns(2)
                with fc3:
                    def_cat = (edit_row["category"]
                               if edit_row is not None
                               and edit_row["category"] in EXP_CATS
                               else EXP_CATS[0])
                    f_cat = st.selectbox(
                        "Category", EXP_CATS,
                        index=EXP_CATS.index(def_cat))
                with fc4:
                    f_name = st.text_input(
                        "Item Description",
                        value=(edit_row["name"]
                               if edit_row is not None else ""))
                f_pid = None
                f_amt = st.number_input(
                    "Amount (N)", min_value=0.0, step=100.0,
                    value=(float(edit_row["amount"])
                           if edit_row is not None else 0.0))

            f_note = st.text_input(
                "Note",
                value=(edit_row["note"] if edit_row is not None else ""))

            if st.form_submit_button(
                    "Update" if edit_id else "Save",
                    type="primary", use_container_width=True):
                if f_amt <= 0:
                    st.error("Amount must be > 0")
                else:
                    if edit_id:
                        update_tx(edit_id, f_date, f_type,
                                  f_pid, f_name, f_cat, f_amt, f_note)
                        st.session_state.pop("edit_id", None)
                        st.success("Updated!")
                    else:
                        add_tx(f_date, f_type, f_pid,
                               f_name, f_cat, f_amt, f_note)
                        st.success("Saved!")
                    st.rerun()

    # ══════════════════════════════════════════════════════════════
    # LEDGER
    # ══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("### Transaction Ledger")
        lc1, lc2 = st.columns([3, 1])
        with lc1:
            sq = st.text_input("Search",
                               placeholder="Name, category, date…",
                               label_visibility="collapsed")
        with lc2:
            tf = st.selectbox("Filter", ["All","Sales","Expenses"],
                              label_visibility="collapsed")

        ledger = df.copy()
        if sq:
            ledger = ledger[
                ledger["name"].str.contains(sq, case=False, na=False) |
                ledger["category"].str.contains(sq, case=False, na=False) |
                ledger["date"].str.contains(sq, na=False)]
        if tf == "Sales":
            ledger = ledger[ledger["type"] == "sale"]
        elif tf == "Expenses":
            ledger = ledger[ledger["type"] == "expense"]

        st.caption(f"{len(ledger)} records")

        show = (ledger[["id","date","type","name","category",
                         "amount","cogs","note"]]
                .rename(columns={
                    "id":"ID","date":"Date","type":"Type",
                    "name":"Description","category":"Category",
                    "amount":"Amount","cogs":"COGS","note":"Note"}))
        st.dataframe(
            show.style.format({"Amount":"{:,.0f}","COGS":"{:,.0f}"}),
            use_container_width=True, height=380, hide_index=True)

        s2 = ledger[ledger["type"] == "sale"]
        tc = st.columns(4)
        tc[0].metric("Sales",        fmt(s2["amount"].sum()))
        tc[1].metric("COGS",         fmt(s2["cogs"].sum()))
        tc[2].metric("Gross Profit", fmt(s2["amount"].sum() - s2["cogs"].sum()))
        tc[3].metric("Expenses",     fmt(
            ledger[ledger["type"]=="expense"]["amount"].sum()))

        st.divider()
        ec1, ec2, ec3 = st.columns(3)
        with ec1:
            act_id = st.number_input("Transaction ID",
                                     min_value=1, step=1, value=1)
        with ec2:
            if st.button("Edit", use_container_width=True):
                if act_id in df["id"].values:
                    st.session_state["edit_id"] = int(act_id)
                    st.rerun()
                else:
                    st.error("ID not found.")
        with ec3:
            if st.button("Delete", use_container_width=True):
                if act_id in df["id"].values:
                    delete_tx(int(act_id))
                    st.success(f"Deleted #{act_id}")
                    st.rerun()
                else:
                    st.error("ID not found.")

        st.download_button(
            "Export CSV",
            data=ledger.to_csv(index=False).encode(),
            file_name=f"kokari_{from_date}_{to_date}.csv",
            mime="text/csv")

    # ══════════════════════════════════════════════════════════════
    # PRODUCTS
    # ══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("### Product Performance")
        sd = df[df["type"] == "sale"].copy()

        if not sd.empty:
            pp = (sd.groupby(["name","category"])
                  .agg(revenue=("amount","sum"), cogs=("cogs","sum"))
                  .reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp["margin"]       = (pp["gross_profit"]/pp["revenue"]*100).round(1)
            pp["rev_share"]    = (pp["revenue"]/kpi["revenue"]*100).round(1)
            pp = pp.sort_values("revenue", ascending=False)

            show_pp = pp.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_pp[c] = show_pp[c].map("{:,.0f}".format)
            show_pp.columns = ["Product","Channel","Revenue",
                               "COGS","Gross Profit","Margin %","Rev Share %"]
            st.dataframe(show_pp, use_container_width=True, hide_index=True)

            st.markdown("#### Revenue vs Gross Profit")
            product_bar(pp)

            st.markdown("#### Gross Margin % by Product")
            margin_df = (pp[["name","margin"]]
                         .set_index("name")
                         .sort_values("margin", ascending=True))
            margin_df.columns = ["Margin %"]
            st.bar_chart(margin_df, height=320)
        else:
            st.info("No sales data in this range.")

        with st.expander("Update Cost Ratios"):
            prods = load_products()
            st.info("Cost ratio = ingredient cost divided by selling price. "
                    "Example: 0.40 means it costs 40% of the price to make.")
            edited = st.data_editor(
                prods, use_container_width=True, num_rows="fixed",
                column_config={
                    "cost_ratio": st.column_config.NumberColumn(
                        "Cost Ratio", min_value=0.0, max_value=1.0,
                        step=0.01, format="%.2f")},
                disabled=["id","name","category"])
            if st.button("Save Cost Ratios", type="primary"):
                with get_conn() as conn:
                    for _, row in edited.iterrows():
                        conn.execute(
                            "UPDATE products SET cost_ratio=? WHERE id=?",
                            (row["cost_ratio"], row["id"]))
                bust()
                st.success("Saved!")
                st.rerun()

    # ══════════════════════════════════════════════════════════════
    # EXPENSES
    # ══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("### Expense Analysis")
        ed = df[df["type"] == "expense"].copy()

        if not ed.empty:
            total_exp = ed["amount"].sum()
            by_cat  = (ed.groupby("category")["amount"].sum()
                       .reset_index().sort_values("amount", ascending=False))
            by_item = (ed.groupby(["name","category"])["amount"].sum()
                       .reset_index().sort_values("amount", ascending=False))
            by_day  = (ed.groupby("date")["amount"].sum()
                       .reset_index().sort_values("date"))

            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("#### Spend by Category")
                expense_bar(by_cat)
            with ec2:
                st.markdown("#### Daily Spend")
                daily_expense_bar(by_day)

            st.markdown("#### Category Summary")
            by_cat["pct"] = (by_cat["amount"]/total_exp*100).round(1)
            by_cat["amount_fmt"] = by_cat["amount"].map("{:,.0f}".format)
            by_cat.columns = ["Category","Amount (raw)","% of Total","Amount"]
            st.dataframe(
                by_cat[["Category","Amount","% of Total"]],
                use_container_width=True, hide_index=True)

            st.markdown("#### Itemised Purchases")
            by_item["pct"] = (by_item["amount"]/total_exp*100).round(1)
            by_item["amount_fmt"] = by_item["amount"].map("{:,.0f}".format)
            by_item.columns = ["Item","Category","Amount (raw)","% of Total","Amount"]
            st.dataframe(
                by_item[["Item","Category","Amount","% of Total"]],
                use_container_width=True, hide_index=True, height=400)

            st.download_button(
                "Export Expenses CSV",
                data=ed.to_csv(index=False).encode(),
                file_name=f"kokari_expenses_{from_date}_{to_date}.csv",
                mime="text/csv")
        else:
            st.info("No expense data in this range.")

    # ══════════════════════════════════════════════════════════════
    # P&L REPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown("### Profit & Loss Report")
        st.caption(f"Period: {from_date} to {to_date}")

        pl_col, ratio_col = st.columns(2)

        ed3   = df[df["type"] == "expense"]
        by_ec = (ed3.groupby("category")["amount"].sum().to_dict()
                 if not ed3.empty else {})

        with pl_col:
            st.markdown("#### Income Statement")
            rows = [
                ("REVENUE",              "",                   True),
                ("  Cafe Sales",          fmt(kpi["cafe"]),    False),
                ("  Packaged Goods",      fmt(kpi["pkg"]),     False),
                ("  B2B / Wholesale",     fmt(kpi["b2b"]),     False),
                ("TOTAL REVENUE",         fmt(kpi["revenue"]), True),
                ("---","","---"),
                ("  Est. COGS",  f"({fmt(kpi['cogs'])})",     False),
                ("GROSS PROFIT",          fmt(kpi["gp"]),      True),
                ("  Gross Margin",f"{kpi['gp_mar']}%",        False),
                ("---","","---"),
                ("OPERATING EXPENSES",   "",                   True),
                *[(f"  {c}", f"({fmt(a)})", False)
                  for c, a in sorted(by_ec.items())],
                ("TOTAL EXPENSES", f"({fmt(kpi['expenses'])})",True),
                ("---","","---"),
                ("NET PROFIT / LOSS",     fmt(kpi["np"]),      True),
                ("  Net Margin",  f"{kpi['np_mar']}%",        False),
            ]
            for label, value, bold in rows:
                if label == "---":
                    st.markdown("---")
                    continue
                a, b = st.columns([3, 1])
                prefix = "**" if bold else ""
                a.markdown(f"{prefix}{label}{prefix}")
                b.markdown(
                    f"<div style='text-align:right'>{prefix}{value}{prefix}</div>",
                    unsafe_allow_html=True)

        with ratio_col:
            st.markdown("#### Financial Ratios")
            st.metric("Gross Margin",          f"{kpi['gp_mar']}%")
            st.metric("Net Margin",            f"{kpi['np_mar']}%")
            st.metric("Expense Ratio",
                      f"{safe_pct(kpi['expenses'],kpi['revenue'])}%")
            st.metric("COGS Ratio",
                      f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
            st.metric("B2B Revenue Share",
                      f"{safe_pct(kpi['b2b'],kpi['revenue'])}%")
            st.metric("Avg Daily Revenue",     fmt(kpi["avg_daily"]))
            st.metric("Revenue per N1 Expense",
                      f"N{(kpi['revenue']/kpi['expenses'] if kpi['expenses'] else 0):.2f}")

            st.markdown("---")
            st.warning(
                "**Accountant Notes**\n\n"
                "- COGS are estimates. Update cost ratios in Products tab "
                "with real supplier invoices.\n"
                "- Powder Milk N44,000 is the largest single purchase. "
                "Monitor usage vs output.\n"
                "- B2B Wholesale N504,513 is one transaction on Feb 13. "
                "Confirm payment received.\n"
                "- No expenses recorded Feb 10-13. Confirm if entries "
                "are missing.\n"
                "- Imprest items need petty cash reconciliation."
            )

        # Cost structure bar
        st.markdown("#### Revenue and Cost Structure")
        wf = pd.DataFrame({
            "Item":   ["Revenue","Gross Profit","Expenses",
                       "COGS","Net Profit"],
            "Amount": [kpi["revenue"], kpi["gp"], kpi["expenses"],
                       kpi["cogs"],    kpi["np"]],
        }).set_index("Item")
        st.bar_chart(wf, height=280)

        # Export
        pl_export = pd.DataFrame({
            "Metric": ["Revenue","Cafe Sales","B2B Sales","Packaged",
                       "COGS","Gross Profit","Gross Margin",
                       "Operating Expenses","Net Profit","Net Margin"],
            "Value":  [kpi["revenue"], kpi["cafe"], kpi["b2b"], kpi["pkg"],
                       kpi["cogs"],    kpi["gp"],   f"{kpi['gp_mar']}%",
                       kpi["expenses"],kpi["np"],   f"{kpi['np_mar']}%"],
        })
        st.download_button(
            "Export P&L CSV",
            data=pl_export.to_csv(index=False).encode(),
            file_name=f"kokari_PnL_{from_date}_{to_date}.csv",
            mime="text/csv")


if __name__ == "__main__":
    main()
