"""
KOKARI CAFE FINANCIAL DASHBOARD - ENHANCED
===========================================
Dependencies (requirements.txt):
    streamlit
    pandas

Run:  streamlit run dashboard.py

Features:
  - Password login
  - Add/rename/remove products from UI
  - Add/rename/remove expense categories from UI
  - Add/rename/remove sales channels from UI
  - Products assigned to channels
  - Channel performance analysis
  - PDF P&L export (pure HTML→download, no external libs)
"""

import sqlite3
import hashlib
import base64
from datetime import date, timedelta
from contextlib import contextmanager

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
DB_PATH = "kokari_cafe.db"

# Default credentials — change via Settings once logged in
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "kokari2026"

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

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

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

        # ── Auth ──────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role     TEXT NOT NULL DEFAULT 'accountant'
            )""")

        # ── Channels ──────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")

        # ── Expense categories ────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expense_categories (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")

        # ── Products ──────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                channel_id INTEGER,
                cost_ratio REAL NOT NULL DEFAULT 0.40,
                active     INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (channel_id) REFERENCES channels(id)
            )""")

        # ── Transactions ──────────────────────────────────────────
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

        # ── Seed users ────────────────────────────────────────────
        cur.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 0:
            cur.execute(
                "INSERT INTO users (username,password,role) VALUES (?,?,?)",
                (DEFAULT_USERNAME, hash_pw(DEFAULT_PASSWORD), "admin"))

        # ── Seed channels ─────────────────────────────────────────
        cur.execute("SELECT COUNT(*) FROM channels")
        if cur.fetchone()[0] == 0:
            for ch in ["Cafe","B2B","Packaged","Retail","Other"]:
                cur.execute(
                    "INSERT INTO channels (name) VALUES (?)", (ch,))

        # ── Seed expense categories ───────────────────────────────
        cur.execute("SELECT COUNT(*) FROM expense_categories")
        if cur.fetchone()[0] == 0:
            for ec in ["Ingredients","Utilities","Staff/Wages","Packaging",
                       "Rent","Transport","Logistics","Stationery",
                       "Marketing","Maintenance","Miscellaneous"]:
                cur.execute(
                    "INSERT INTO expense_categories (name) VALUES (?)", (ec,))

        # ── Seed products ─────────────────────────────────────────
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            # get channel ids
            ch = {r[0]: r[1] for r in cur.execute(
                "SELECT name,id FROM channels").fetchall()}
            cur.executemany(
                "INSERT INTO products (id,name,channel_id,cost_ratio) "
                "VALUES (?,?,?,?)", [
                ("p01","Pancakes",             ch["Cafe"],     0.38),
                ("p02","Fruit Smoothie",        ch["Cafe"],     0.40),
                ("p03","Books",                 ch["Retail"],   0.55),
                ("p04","Puff Puff",             ch["Cafe"],     0.30),
                ("p05","Spicy Chicken Wrap",    ch["Cafe"],     0.42),
                ("p06","Chicken Wings",         ch["Cafe"],     0.45),
                ("p07","Tapioca",               ch["Cafe"],     0.35),
                ("p08","Coffee",                ch["Cafe"],     0.28),
                ("p09","Ice Coffee",            ch["Cafe"],     0.30),
                ("p10","Zobo",                  ch["Cafe"],     0.25),
                ("p11","Parfait & Wings Combo", ch["Cafe"],     0.45),
                ("p12","Parfait Cafe",          ch["Cafe"],     0.40),
                ("p13","Granola 500g",          ch["Packaged"], 0.50),
                ("p14","Spicy Coconut Flakes",  ch["Packaged"], 0.48),
                ("p15","Honey Coconut Cashew",  ch["Packaged"], 0.50),
                ("p16","CCB",                   ch["Packaged"], 0.50),
                ("p17","Wholesale (B2B)",       ch["B2B"],      0.55),
                ("p18","Take Away",             ch["Cafe"],     0.40),
                ("p19","Water",                 ch["Cafe"],     0.20),
                ("p20","Space Rental",          ch["Other"],    0.05),
            ])

        # ── Seed transactions ─────────────────────────────────────
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
            (s[0],"sale",s[1],s[2],s[3],amt,c))

    expenses = [
        ("2026-02-09","Ingredients","Sugar",              9000),
        ("2026-02-09","Ingredients","Flour",              5400),
        ("2026-02-09","Ingredients","Chicken Wings",      20178),
        ("2026-02-09","Ingredients","Chicken",            11000),
        ("2026-02-09","Ingredients","Bread",              1200),
        ("2026-02-09","Ingredients","Mayonnaise",         4000),
        ("2026-02-09","Ingredients","Banana",             2000),
        ("2026-02-09","Ingredients","Oil",                4400),
        ("2026-02-09","Ingredients","Carrot and Cabbage", 3000),
        ("2026-02-09","Ingredients","Eggs",               5900),
        ("2026-02-09","Ingredients","Groundnut",          2000),
        ("2026-02-09","Ingredients","Powder Milk",        44000),
        ("2026-02-09","Ingredients","Pineapple",          3000),
        ("2026-02-09","Ingredients","Ginger",             2000),
        ("2026-02-09","Ingredients","Cinnamon",           2000),
        ("2026-02-09","Ingredients","Cloves",             1000),
        ("2026-02-09","Ingredients","Grapes",             6000),
        ("2026-02-09","Ingredients","Honey",              6000),
        ("2026-02-09","Ingredients","Liquid Milk",        10200),
        ("2026-02-09","Utilities",  "NEPA Electricity",   10000),
        ("2026-02-09","Utilities",  "Data",               3500),
        ("2026-02-09","Utilities",  "Recharge Card",      2000),
        ("2026-02-09","Utilities",  "Water CWay",         3400),
        ("2026-02-09","Packaging",  "Zobo Bottles",       4400),
        ("2026-02-09","Packaging",  "Foil",               3000),
        ("2026-02-09","Packaging",  "Serviettes",         2000),
        ("2026-02-09","Packaging",  "Spoons",             3000),
        ("2026-02-09","Packaging",  "Water retail",       2500),
        ("2026-02-09","Transport",  "Transport",          1500),
        ("2026-02-09","Miscellaneous","Printing",         400),
        ("2026-02-09","Miscellaneous","Bank Charges",     400),
        ("2026-02-09","Miscellaneous","Phone Repair",     500),
        ("2026-02-09","Logistics",  "Bucket",             3000),
        ("2026-02-09","Logistics",  "Item Delivery x7",  2000),
        ("2026-02-09","Utilities",  "NEPA Imprest",       3000),
        ("2026-02-09","Stationery", "Battery and Book",   1000),
        ("2026-02-09","Packaging",  "Serviettes Imprest", 1000),
        ("2026-02-14","Ingredients","Chicken Wings",      36321),
        ("2026-02-14","Ingredients","Flour",              6400),
        ("2026-02-14","Ingredients","Eggs",               6000),
        ("2026-02-14","Packaging",  "Straws",             3700),
        ("2026-02-14","Packaging",  "Water retail",       4400),
        ("2026-02-14","Transport",  "Transport",          500),
    ]
    for e in expenses:
        cur.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,'expense',?,?,?,?,'')",
            (e[0],"expense",e[2],e[1],e[3],e[3]))


# ─────────────────────────────────────────────────────────────────
# LOADERS  (cached)
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_tx(from_d, to_d):
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT t.*, c.name as channel "
            "FROM transactions t "
            "LEFT JOIN products p ON t.product_id = p.id "
            "LEFT JOIN channels c ON p.channel_id = c.id "
            "WHERE t.date BETWEEN ? AND ? "
            "ORDER BY t.date DESC, t.id DESC",
            conn, params=(str(from_d), str(to_d)))

@st.cache_data(ttl=30)
def load_products():
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT p.id, p.name, c.name as channel, "
            "p.channel_id, p.cost_ratio, p.active "
            "FROM products p "
            "LEFT JOIN channels c ON p.channel_id = c.id "
            "WHERE p.active = 1 "
            "ORDER BY c.name, p.name",
            conn)

@st.cache_data(ttl=30)
def load_channels():
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM channels ORDER BY name", conn)

@st.cache_data(ttl=30)
def load_exp_cats():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT name FROM expense_categories ORDER BY name"
        ).fetchall()
        return [r[0] for r in rows]

def bust():
    load_tx.clear()
    load_products.clear()
    load_channels.clear()
    load_exp_cats.clear()

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
        days=days,
        avg_daily=rev / days if days else 0,
    )

# ─────────────────────────────────────────────────────────────────
# CRUD — Transactions
# ─────────────────────────────────────────────────────────────────
def add_tx(date_val, tx_type, pid, name, cat, amount, note=""):
    with get_conn() as conn:
        if tx_type == "sale":
            r    = conn.execute(
                "SELECT cost_ratio FROM products WHERE id=?",
                (pid,)).fetchone()
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
                "SELECT cost_ratio FROM products WHERE id=?",
                (pid,)).fetchone()
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
# CRUD — Products
# ─────────────────────────────────────────────────────────────────
def add_product(name, channel_id, cost_ratio):
    pid = "u" + str(abs(hash(name + str(date.today()))))[:8]
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO products (id,name,channel_id,cost_ratio) "
            "VALUES (?,?,?,?)",
            (pid, name, channel_id, cost_ratio))
    bust()

def update_product(pid, name, channel_id, cost_ratio):
    with get_conn() as conn:
        conn.execute(
            "UPDATE products SET name=?,channel_id=?,cost_ratio=? "
            "WHERE id=?",
            (name, channel_id, cost_ratio, pid))
    bust()

def deactivate_product(pid):
    with get_conn() as conn:
        conn.execute(
            "UPDATE products SET active=0 WHERE id=?", (pid,))
    bust()

# ─────────────────────────────────────────────────────────────────
# CRUD — Channels
# ─────────────────────────────────────────────────────────────────
def add_channel(name):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO channels (name) VALUES (?)", (name,))
    bust()

def rename_channel(cid, new_name):
    with get_conn() as conn:
        conn.execute(
            "UPDATE channels SET name=? WHERE id=?", (new_name, cid))
    bust()

def delete_channel(cid):
    with get_conn() as conn:
        used = conn.execute(
            "SELECT COUNT(*) FROM products WHERE channel_id=?",
            (cid,)).fetchone()[0]
        if used > 0:
            return False
        conn.execute("DELETE FROM channels WHERE id=?", (cid,))
    bust()
    return True

# ─────────────────────────────────────────────────────────────────
# CRUD — Expense categories
# ─────────────────────────────────────────────────────────────────
def add_exp_cat(name):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO expense_categories (name) VALUES (?)",
            (name,))
    bust()

def rename_exp_cat(old, new):
    with get_conn() as conn:
        conn.execute(
            "UPDATE expense_categories SET name=? WHERE name=?",
            (new, old))
        conn.execute(
            "UPDATE transactions SET category=? WHERE category=?",
            (new, old))
    bust()

def delete_exp_cat(name):
    with get_conn() as conn:
        used = conn.execute(
            "SELECT COUNT(*) FROM transactions "
            "WHERE category=? AND type='expense'",
            (name,)).fetchone()[0]
        if used > 0:
            return False
        conn.execute(
            "DELETE FROM expense_categories WHERE name=?", (name,))
    bust()
    return True

# ─────────────────────────────────────────────────────────────────
# CRUD — Auth
# ─────────────────────────────────────────────────────────────────
def verify_login(username, password):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT role FROM users WHERE username=? AND password=?",
            (username, hash_pw(password))).fetchone()
        return row[0] if row else None

def change_password(username, new_password):
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET password=? WHERE username=?",
            (hash_pw(new_password), username))

# ─────────────────────────────────────────────────────────────────
# PDF GENERATOR  (pure HTML — no external libs)
# ─────────────────────────────────────────────────────────────────
def generate_pdf_html(kpi, from_date, to_date, by_ec,
                       channel_data, prod_data):
    """Returns an HTML string styled for print/PDF save."""
    rows_pl = ""
    pl_items = [
        ("Revenue",            kpi["revenue"],   False),
        ("  Cost of Goods",   -kpi["cogs"],      False),
        ("Gross Profit",       kpi["gp"],         True),
        ("",                   None,              False),
    ]
    for cat, amt in sorted(by_ec.items()):
        pl_items.append((f"  {cat}", -amt, False))
    pl_items += [
        ("Total Expenses",    -kpi["expenses"],   True),
        ("",                   None,              False),
        ("NET PROFIT / LOSS",  kpi["np"],         True),
    ]
    for label, val, bold in pl_items:
        if val is None:
            rows_pl += "<tr><td colspan='2'><hr></td></tr>"
            continue
        style  = "font-weight:bold;" if bold else ""
        color  = ("color:#16a34a;" if val >= 0 else "color:#dc2626;") if bold else ""
        disp   = f"({fmt(abs(val))})" if val < 0 else fmt(val)
        rows_pl += (
            f"<tr>"
            f"<td style='{style}'>{label}</td>"
            f"<td style='text-align:right;{style}{color}'>{disp}</td>"
            f"</tr>")

    rows_ch = ""
    for _, row in channel_data.iterrows():
        pct = safe_pct(row["revenue"], kpi["revenue"])
        rows_ch += (
            f"<tr>"
            f"<td>{row['channel']}</td>"
            f"<td style='text-align:right'>{fmt(row['revenue'])}</td>"
            f"<td style='text-align:right'>{fmt(row['gross_profit'])}</td>"
            f"<td style='text-align:right'>{pct}%</td>"
            f"</tr>")

    rows_prod = ""
    for _, row in prod_data.head(10).iterrows():
        rows_prod += (
            f"<tr>"
            f"<td>{row['name']}</td>"
            f"<td>{row.get('channel','')}</td>"
            f"<td style='text-align:right'>{fmt(row['revenue'])}</td>"
            f"<td style='text-align:right'>{row['margin']}%</td>"
            f"</tr>")

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset='utf-8'>
<title>Kokari Cafe P&L Report</title>
<style>
  body {{ font-family: Arial, sans-serif; font-size: 13px;
          color: #111; margin: 40px; }}
  h1   {{ font-size: 22px; color: #2563eb; margin-bottom: 4px; }}
  h2   {{ font-size: 15px; color: #374151; border-bottom: 2px solid #2563eb;
          padding-bottom: 4px; margin-top: 30px; }}
  .sub {{ color: #6b7280; font-size: 12px; margin-bottom: 20px; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
  th   {{ background:#2563eb; color:white; padding:7px 10px;
          text-align:left; font-size:12px; }}
  td   {{ padding: 6px 10px; border-bottom: 1px solid #f0f0f0; }}
  tr:nth-child(even) td {{ background: #f9fafb; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr);
               gap:12px; margin:16px 0; }}
  .kpi  {{ background:#f3f4f6; border-radius:8px; padding:12px;
           text-align:center; }}
  .kpi-val {{ font-size:18px; font-weight:bold; color:#2563eb; }}
  .kpi-lbl {{ font-size:11px; color:#6b7280; margin-top:2px; }}
  .footer {{ margin-top:40px; font-size:11px; color:#9ca3af;
             border-top:1px solid #e5e7eb; padding-top:10px; }}
  @media print {{
    button {{ display:none; }}
  }}
</style>
</head>
<body>
<h1>☕ Kokari Cafe · Financial Report</h1>
<div class='sub'>
  Period: <strong>{from_date}</strong> to <strong>{to_date}</strong>
  &nbsp;|&nbsp; Generated: {date.today()}
</div>

<div class='kpi-grid'>
  <div class='kpi'>
    <div class='kpi-val'>{fmt(kpi['revenue'])}</div>
    <div class='kpi-lbl'>Total Revenue</div>
  </div>
  <div class='kpi'>
    <div class='kpi-val' style='color:#16a34a'>{fmt(kpi['gp'])}</div>
    <div class='kpi-lbl'>Gross Profit ({kpi['gp_mar']}%)</div>
  </div>
  <div class='kpi'>
    <div class='kpi-val' style='color:{"#16a34a" if kpi["np"]>=0 else "#dc2626"}'>
      {fmt(kpi['np'])}</div>
    <div class='kpi-lbl'>Net Profit ({kpi['np_mar']}%)</div>
  </div>
  <div class='kpi'>
    <div class='kpi-val' style='color:#ea580c'>{fmt(kpi['expenses'])}</div>
    <div class='kpi-lbl'>Total Expenses</div>
  </div>
</div>

<h2>Profit & Loss Statement</h2>
<table>
  <tr><th>Item</th><th style='text-align:right'>Amount</th></tr>
  {rows_pl}
</table>

<h2>Performance by Channel</h2>
<table>
  <tr>
    <th>Channel</th>
    <th style='text-align:right'>Revenue</th>
    <th style='text-align:right'>Gross Profit</th>
    <th style='text-align:right'>Rev Share</th>
  </tr>
  {rows_ch}
</table>

<h2>Top 10 Products</h2>
<table>
  <tr>
    <th>Product</th><th>Channel</th>
    <th style='text-align:right'>Revenue</th>
    <th style='text-align:right'>Margin</th>
  </tr>
  {rows_prod}
</table>

<div class='footer'>
  Kokari Cafe Financial Dashboard &nbsp;|&nbsp;
  This report was generated automatically.
  COGS figures are estimates based on product cost ratios.
</div>

<br>
<button onclick='window.print()' style='
  background:#2563eb;color:white;border:none;padding:10px 24px;
  border-radius:6px;font-size:14px;cursor:pointer;margin-top:10px'>
  Save as PDF / Print
</button>

</body>
</html>"""
    return html


def pdf_download_button(html_content, filename):
    """Encode HTML as base64 and render a download link."""
    b64 = base64.b64encode(html_content.encode()).decode()
    href = (
        f'<a href="data:text/html;base64,{b64}" '
        f'download="{filename}" '
        f'style="display:inline-block;background:#2563eb;color:white;'
        f'padding:8px 20px;border-radius:6px;text-decoration:none;'
        f'font-size:13px;font-weight:600;">'
        f'Download PDF Report</a>')
    st.markdown(href, unsafe_allow_html=True)
    st.caption(
        "Open the downloaded file in your browser, "
        "then press Ctrl+P (or Cmd+P on Mac) and choose Save as PDF.")


# ─────────────────────────────────────────────────────────────────
# LOGIN SCREEN
# ─────────────────────────────────────────────────────────────────
def login_screen():
    st.markdown("""
    <div style='max-width:380px;margin:80px auto;text-align:center'>
      <h1 style='color:#2563eb'>☕ Kokari Cafe</h1>
      <p style='color:#6b7280'>Financial Dashboard — Please sign in</p>
    </div>
    """, unsafe_allow_html=True)

    col = st.columns([1, 2, 1])[1]
    with col:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button(
                "Sign In", use_container_width=True, type="primary")
            if submitted:
                role = verify_login(username, password)
                if role:
                    st.session_state["logged_in"]  = True
                    st.session_state["username"]   = username
                    st.session_state["role"]       = role
                    st.rerun()
                else:
                    st.error("Incorrect username or password.")


# ─────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────
def main():
    init_db()

    # ── Auth gate ─────────────────────────────────────────────────
    if not st.session_state.get("logged_in"):
        login_screen()
        st.stop()

    username = st.session_state["username"]
    role     = st.session_state["role"]

    # ── Sidebar ───────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(f"## ☕ Kokari Cafe")
        st.caption(f"Signed in as **{username}**")
        if st.button("Sign Out", use_container_width=True):
            for k in ["logged_in","username","role","edit_id"]:
                st.session_state.pop(k, None)
            st.rerun()

        st.divider()
        st.markdown("### Date Range")
        from_date = st.date_input("From", value=date(2026, 2, 9))
        to_date   = st.date_input("To",   value=date(2026, 2, 14))
        q1, q2, q3 = st.columns(3)
        if q1.button("Week"):
            t = date.today()
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
        exp_cats    = load_exp_cats()

        with st.form("quick", clear_on_submit=True):
            qt = st.selectbox("Type", ["sale","expense"])
            if qt == "sale":
                names = products_df["name"].tolist()
                ids   = products_df["id"].tolist()
                cats  = products_df["channel"].tolist()
                idx   = st.selectbox("Product",
                    range(len(names)),
                    format_func=lambda i: names[i])
                q_pid  = ids[idx]
                q_name = names[idx]
                q_cat  = cats[idx]
            else:
                q_cat  = st.selectbox("Category", exp_cats)
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

    # ── Load data ─────────────────────────────────────────────────
    df  = load_tx(from_date, to_date)
    kpi = compute_kpis(df)

    # ── Header ────────────────────────────────────────────────────
    st.title("☕ Kokari Cafe · Financial Dashboard")
    st.caption(
        f"Period: {from_date} to {to_date}  |  "
        f"{kpi['days']} trading days  |  {len(df)} transactions")

    tabs = st.tabs([
        "Dashboard", "Entry", "Ledger",
        "Channels", "Products", "Expenses",
        "P&L Report", "Settings",
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
        r2[1].metric("Trading Days", kpi["days"])
        r2[2].metric("Transactions", len(df[df["type"]=="sale"]))
        r2[3].metric("Expense Lines",len(df[df["type"]=="expense"]))

        st.divider()
        s_df = df[df["type"] == "sale"].copy()
        e_df = df[df["type"] == "expense"].copy()

        if not s_df.empty:
            daily_s = (s_df.groupby("date")
                       .agg(revenue=("amount","sum"),
                            cogs=("cogs","sum")).reset_index())
            daily_e = (e_df.groupby("date")["amount"].sum()
                       .reset_index(name="expenses")
                       if not e_df.empty
                       else pd.DataFrame(columns=["date","expenses"]))
            daily = (daily_s.merge(daily_e, on="date", how="left")
                     .fillna(0).sort_values("date"))
            daily["gross_profit"] = daily["revenue"] - daily["cogs"]
            daily["net_profit"]   = daily["gross_profit"] - daily["expenses"]

            st.markdown("#### Daily Revenue · Gross Profit · Expenses · Net Profit")
            chart_d = daily.set_index("date")[
                ["revenue","gross_profit","expenses","net_profit"]]
            chart_d.columns = ["Revenue","Gross Profit","Expenses","Net Profit"]
            st.bar_chart(chart_d, height=300)

            st.markdown("#### Revenue vs Net Profit Trend")
            chart_t = daily.set_index("date")[["revenue","net_profit"]]
            chart_t.columns = ["Revenue","Net Profit"]
            st.line_chart(chart_t, height=240)

            st.markdown("#### Top Products by Revenue")
            pp = (s_df.groupby("name")
                  .agg(revenue=("amount","sum"),
                       cogs=("cogs","sum")).reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp = pp.sort_values("revenue", ascending=True).tail(10)
            chart_p = pp.set_index("name")[["revenue","gross_profit"]]
            chart_p.columns = ["Revenue","Gross Profit"]
            st.bar_chart(chart_p, height=300)

            bd  = daily.loc[daily["revenue"].idxmax()]
            bpd = daily.loc[daily["net_profit"].idxmax()]
            top = s_df.groupby("name")["amount"].sum().idxmax()
            ic  = st.columns(4)
            ic[0].success(
                f"**Best Revenue Day**\n\n{bd['date']}\n\n{fmt(bd['revenue'])}")
            ic[1].success(
                f"**Best Profit Day**\n\n"
                f"{bpd['date']}\n\n{fmt(bpd['net_profit'])}")
            ic[2].info(f"**Top Product**\n\n{top}")
            ic[3].warning(
                f"**Total Cost Deployed**\n\n"
                f"{fmt(kpi['cogs']+kpi['expenses'])}")
        else:
            st.info("No sales data for this period.")

    # ══════════════════════════════════════════════════════════════
    # ENTRY
    # ══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("### Add / Edit Transaction")
        products_df = load_products()
        exp_cats    = load_exp_cats()
        edit_id     = st.session_state.get("edit_id")
        edit_row    = None

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
                f_date = st.date_input("Date",
                    value=(pd.to_datetime(edit_row["date"]).date()
                           if edit_row is not None else date.today()))
            with fc2:
                type_opts = ["sale","expense"]
                f_type = st.selectbox("Type", type_opts,
                    index=(type_opts.index(edit_row["type"])
                           if edit_row is not None else 0))

            names  = products_df["name"].tolist()
            ids    = products_df["id"].tolist()
            chans  = products_df["channel"].tolist()
            ratios = products_df["cost_ratio"].tolist()

            if f_type == "sale":
                def_i = 0
                if edit_row is not None and edit_row["product_id"] in ids:
                    def_i = ids.index(edit_row["product_id"])
                sel = st.selectbox("Product",
                    range(len(names)),
                    format_func=lambda i: f"{names[i]}  [{chans[i]}]",
                    index=def_i)
                f_pid  = ids[sel]
                f_name = names[sel]
                f_cat  = chans[sel]
                ratio  = ratios[sel]
                f_amt  = st.number_input("Sales Amount",
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
                               and edit_row["category"] in exp_cats
                               else (exp_cats[0] if exp_cats else ""))
                    f_cat = st.selectbox("Category", exp_cats,
                        index=(exp_cats.index(def_cat)
                               if def_cat in exp_cats else 0))
                with fc4:
                    f_name = st.text_input("Item Description",
                        value=(edit_row["name"]
                               if edit_row is not None else ""))
                f_pid = None
                f_amt = st.number_input("Amount",
                    min_value=0.0, step=100.0,
                    value=(float(edit_row["amount"])
                           if edit_row is not None else 0.0))

            f_note = st.text_input("Note",
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
            tf = st.selectbox("Filter",
                ["All","Sales","Expenses"],
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
                         "channel","amount","cogs","note"]]
                .rename(columns={
                    "id":"ID","date":"Date","type":"Type",
                    "name":"Description","category":"Category",
                    "channel":"Channel",
                    "amount":"Amount","cogs":"COGS","note":"Note"}))
        st.dataframe(
            show.style.format({"Amount":"{:,.0f}","COGS":"{:,.0f}"}),
            use_container_width=True, height=360, hide_index=True)

        s2 = ledger[ledger["type"] == "sale"]
        tc = st.columns(4)
        tc[0].metric("Sales",        fmt(s2["amount"].sum()))
        tc[1].metric("COGS",         fmt(s2["cogs"].sum()))
        tc[2].metric("Gross Profit", fmt(
            s2["amount"].sum() - s2["cogs"].sum()))
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

        st.download_button("Export CSV",
            data=ledger.to_csv(index=False).encode(),
            file_name=f"kokari_{from_date}_{to_date}.csv",
            mime="text/csv")

    # ══════════════════════════════════════════════════════════════
    # CHANNELS
    # ══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("### Sales Channel Management")
        channels_df = load_channels()
        s_df2 = df[df["type"] == "sale"].copy()

        # Channel performance
        if not s_df2.empty and "channel" in s_df2.columns:
            st.markdown("#### Channel Performance")
            ch_perf = (s_df2.groupby("channel")
                       .agg(revenue=("amount","sum"),
                            cogs=("cogs","sum")).reset_index())
            ch_perf["gross_profit"] = ch_perf["revenue"] - ch_perf["cogs"]
            ch_perf["margin"]       = (
                ch_perf["gross_profit"] / ch_perf["revenue"] * 100
            ).round(1)
            ch_perf["rev_share"]    = (
                ch_perf["revenue"] / kpi["revenue"] * 100
            ).round(1)
            ch_perf = ch_perf.sort_values("revenue", ascending=False)
            ch_perf["channel"] = ch_perf["channel"].fillna("Unassigned")

            show_ch = ch_perf.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_ch[c] = show_ch[c].map("{:,.0f}".format)
            show_ch.columns = ["Channel","Revenue","COGS",
                               "Gross Profit","Margin %","Rev Share %"]
            st.dataframe(show_ch, use_container_width=True, hide_index=True)

            chart_ch = (ch_perf.set_index("channel")
                        [["revenue","gross_profit"]])
            chart_ch.columns = ["Revenue","Gross Profit"]
            st.bar_chart(chart_ch, height=280)

        st.divider()
        st.markdown("#### Manage Channels")

        # Add channel
        with st.form("add_channel"):
            nc = st.text_input("New Channel Name",
                placeholder="e.g. Online Orders, Catering, Event Sales")
            if st.form_submit_button("Add Channel", type="primary"):
                if nc.strip():
                    add_channel(nc.strip())
                    st.success(f"Channel '{nc}' added!")
                    st.rerun()
                else:
                    st.error("Enter a channel name.")

        st.markdown("**Existing Channels**")
        for _, row in channels_df.iterrows():
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                st.write(f"**{row['name']}**  (ID: {row['id']})")
            with col2:
                new_name = st.text_input(
                    "Rename", key=f"rn_ch_{row['id']}",
                    label_visibility="collapsed",
                    placeholder="New name…")
            with col3:
                if st.button("Rename", key=f"rb_ch_{row['id']}"):
                    if new_name.strip():
                        rename_channel(row["id"], new_name.strip())
                        st.success("Renamed!")
                        st.rerun()
                if st.button("Delete", key=f"db_ch_{row['id']}"):
                    ok = delete_channel(row["id"])
                    if ok:
                        st.success("Deleted!")
                        st.rerun()
                    else:
                        st.error(
                            "Cannot delete — products are assigned to this channel.")

    # ══════════════════════════════════════════════════════════════
    # PRODUCTS
    # ══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("### Product Management")
        channels_df = load_channels()
        ch_names    = channels_df["name"].tolist()
        ch_ids      = channels_df["id"].tolist()

        # ── Add new product ──
        st.markdown("#### Add New Product")
        with st.form("add_product"):
            ap1, ap2, ap3 = st.columns(3)
            with ap1:
                new_p_name = st.text_input("Product Name",
                    placeholder="e.g. Matcha Latte")
            with ap2:
                ch_sel = st.selectbox("Channel", range(len(ch_names)),
                    format_func=lambda i: ch_names[i])
            with ap3:
                new_p_ratio = st.number_input(
                    "Cost Ratio (0-1)", min_value=0.0,
                    max_value=1.0, step=0.01, value=0.40)
            if st.form_submit_button("Add Product", type="primary"):
                if new_p_name.strip():
                    add_product(
                        new_p_name.strip(),
                        ch_ids[ch_sel],
                        new_p_ratio)
                    st.success(f"Product '{new_p_name}' added!")
                    st.rerun()
                else:
                    st.error("Enter a product name.")

        st.divider()

        # ── Product performance ──
        s_df3 = df[df["type"] == "sale"].copy()
        if not s_df3.empty:
            st.markdown("#### Product Performance")
            pp = (s_df3.groupby(["name","channel"])
                  .agg(revenue=("amount","sum"),
                       cogs=("cogs","sum")).reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp["margin"]       = (
                pp["gross_profit"] / pp["revenue"] * 100).round(1)
            pp["rev_share"]    = (
                pp["revenue"] / kpi["revenue"] * 100).round(1)
            pp = pp.sort_values("revenue", ascending=False)
            pp["channel"]      = pp["channel"].fillna("Unassigned")

            show_pp = pp.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_pp[c] = show_pp[c].map("{:,.0f}".format)
            show_pp.columns = ["Product","Channel","Revenue","COGS",
                               "Gross Profit","Margin %","Rev Share %"]
            st.dataframe(show_pp, use_container_width=True, hide_index=True)

        # ── Edit existing products ──
        st.divider()
        st.markdown("#### Edit / Remove Products")
        all_prods = load_products()
        edited = st.data_editor(
            all_prods[["id","name","channel","cost_ratio"]],
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "id":         st.column_config.TextColumn("ID", disabled=True),
                "name":       st.column_config.TextColumn("Product Name"),
                "channel":    st.column_config.TextColumn(
                    "Channel", disabled=True,
                    help="Change channel via channel_id below"),
                "cost_ratio": st.column_config.NumberColumn(
                    "Cost Ratio", min_value=0.0, max_value=1.0,
                    step=0.01, format="%.2f"),
            },
            hide_index=True)

        sc1, sc2 = st.columns(2)
        with sc1:
            if st.button("Save Product Changes", type="primary",
                         use_container_width=True):
                for _, row in edited.iterrows():
                    orig = all_prods[all_prods["id"] == row["id"]].iloc[0]
                    update_product(
                        row["id"], row["name"],
                        int(orig["channel_id"]), row["cost_ratio"])
                bust()
                st.success("Products updated!")
                st.rerun()
        with sc2:
            rem_id = st.selectbox("Remove Product",
                all_prods["id"].tolist(),
                format_func=lambda i: (
                    all_prods[all_prods["id"]==i]["name"].values[0]))
            if st.button("Remove Product", use_container_width=True):
                deactivate_product(rem_id)
                st.success("Product removed from active list.")
                st.rerun()

    # ══════════════════════════════════════════════════════════════
    # EXPENSES
    # ══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown("### Expense Analysis")
        ed = df[df["type"] == "expense"].copy()

        if not ed.empty:
            total_exp = ed["amount"].sum()
            by_cat    = (ed.groupby("category")["amount"].sum()
                         .reset_index()
                         .sort_values("amount", ascending=False))
            by_item   = (ed.groupby(["name","category"])["amount"].sum()
                         .reset_index()
                         .sort_values("amount", ascending=False))
            by_day    = (ed.groupby("date")["amount"].sum()
                         .reset_index().sort_values("date"))

            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("#### Spend by Category")
                st.bar_chart(
                    by_cat.set_index("category")
                    .rename(columns={"amount":"Amount"}),
                    height=280)
            with ec2:
                st.markdown("#### Daily Spend")
                st.bar_chart(
                    by_day.set_index("date")
                    .rename(columns={"amount":"Daily Spend"}),
                    height=280)

            by_cat["pct"] = (by_cat["amount"]/total_exp*100).round(1)
            by_cat_show   = by_cat.copy()
            by_cat_show["amount"] = by_cat_show["amount"].map("{:,.0f}".format)
            by_cat_show.columns = ["Category","Amount","% of Total"]
            st.markdown("#### Category Summary")
            st.dataframe(by_cat_show, use_container_width=True,
                         hide_index=True)

            by_item["pct"] = (by_item["amount"]/total_exp*100).round(1)
            by_item_show   = by_item.copy()
            by_item_show["amount"] = by_item_show["amount"].map("{:,.0f}".format)
            by_item_show.columns = ["Item","Category","Amount","% of Total"]
            st.markdown("#### Itemised Purchases")
            st.dataframe(by_item_show, use_container_width=True,
                         hide_index=True, height=380)

            st.download_button("Export Expenses CSV",
                data=ed.to_csv(index=False).encode(),
                file_name=f"kokari_exp_{from_date}_{to_date}.csv",
                mime="text/csv")
        else:
            st.info("No expense data in this range.")

    # ══════════════════════════════════════════════════════════════
    # P&L REPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.markdown("### Profit & Loss Report")
        st.caption(f"Period: {from_date} to {to_date}")

        pl_col, ratio_col = st.columns(2)
        ed4   = df[df["type"] == "expense"]
        by_ec = (ed4.groupby("category")["amount"].sum().to_dict()
                 if not ed4.empty else {})

        with pl_col:
            st.markdown("#### Income Statement")
            s_ch = df[df["type"]=="sale"].groupby("channel")["amount"].sum()

            rows = [
                ("REVENUE", "", True),
                *[(f"  {ch}", fmt(v), False)
                  for ch, v in s_ch.items()],
                ("TOTAL REVENUE",      fmt(kpi["revenue"]), True),
                ("---","","---"),
                ("  Est. COGS",f"({fmt(kpi['cogs'])})",    False),
                ("GROSS PROFIT",       fmt(kpi["gp"]),      True),
                ("  Gross Margin",f"{kpi['gp_mar']}%",     False),
                ("---","","---"),
                ("OPERATING EXPENSES", "",                  True),
                *[(f"  {c}", f"({fmt(a)})", False)
                  for c, a in sorted(by_ec.items())],
                ("TOTAL EXPENSES",
                 f"({fmt(kpi['expenses'])})",               True),
                ("---","","---"),
                ("NET PROFIT / LOSS",  fmt(kpi["np"]),      True),
                ("  Net Margin", f"{kpi['np_mar']}%",       False),
            ]
            for label, value, bold in rows:
                if label == "---":
                    st.markdown("---")
                    continue
                a, b = st.columns([3, 1])
                prefix = "**" if bold else ""
                a.markdown(f"{prefix}{label}{prefix}")
                b.markdown(
                    f"<div style='text-align:right'>"
                    f"{prefix}{value}{prefix}</div>",
                    unsafe_allow_html=True)

        with ratio_col:
            st.markdown("#### Financial Ratios")
            st.metric("Gross Margin",    f"{kpi['gp_mar']}%")
            st.metric("Net Margin",      f"{kpi['np_mar']}%")
            st.metric("Expense Ratio",
                f"{safe_pct(kpi['expenses'],kpi['revenue'])}%")
            st.metric("COGS Ratio",
                f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
            st.metric("Avg Daily Rev",   fmt(kpi["avg_daily"]))
            st.metric("Rev per N1 Expense",
                f"N{(kpi['revenue']/kpi['expenses'] if kpi['expenses'] else 0):.2f}")

            st.markdown("---")
            st.warning(
                "**Accountant Notes**\n\n"
                "- COGS are estimates. Update cost ratios in Products tab.\n"
                "- Powder Milk N44k is the largest single purchase.\n"
                "- B2B N504k is one Feb 13 transaction — confirm payment.\n"
                "- No expenses recorded Feb 10-13 — verify records.\n"
                "- Imprest items need petty cash reconciliation.")

        st.markdown("#### Revenue and Cost Structure")
        wf = pd.DataFrame({
            "Item":   ["Revenue","Gross Profit","Expenses","COGS","Net Profit"],
            "Amount": [kpi["revenue"], kpi["gp"], kpi["expenses"],
                       kpi["cogs"], kpi["np"]],
        }).set_index("Item")
        st.bar_chart(wf, height=260)

        st.divider()
        st.markdown("#### Export Report as PDF")

        s_df4 = df[df["type"]=="sale"].copy()
        s_df4["channel"] = s_df4["channel"].fillna("Unassigned")
        ch_perf2 = (s_df4.groupby("channel")
                    .agg(revenue=("amount","sum"),
                         cogs=("cogs","sum")).reset_index())
        ch_perf2["gross_profit"] = ch_perf2["revenue"] - ch_perf2["cogs"]

        pp2 = (s_df4.groupby(["name","channel"])
               .agg(revenue=("amount","sum"),
                    cogs=("cogs","sum")).reset_index())
        pp2["gross_profit"] = pp2["revenue"] - pp2["cogs"]
        pp2["margin"]       = (pp2["gross_profit"]/pp2["revenue"]*100).round(1)
        pp2 = pp2.sort_values("revenue", ascending=False)

        html_report = generate_pdf_html(
            kpi, from_date, to_date, by_ec, ch_perf2, pp2)

        pdf_download_button(
            html_report,
            f"Kokari_PnL_{from_date}_{to_date}.html")

        st.download_button("Export P&L as CSV",
            data=pd.DataFrame({
                "Metric": ["Revenue","COGS","Gross Profit",
                           "Gross Margin","Expenses","Net Profit","Net Margin"],
                "Value":  [kpi["revenue"], kpi["cogs"], kpi["gp"],
                           f"{kpi['gp_mar']}%", kpi["expenses"],
                           kpi["np"], f"{kpi['np_mar']}%"],
            }).to_csv(index=False).encode(),
            file_name=f"kokari_PnL_{from_date}_{to_date}.csv",
            mime="text/csv")

    # ══════════════════════════════════════════════════════════════
    # SETTINGS
    # ══════════════════════════════════════════════════════════════
    with tabs[7]:
        st.markdown("### Settings")
        exp_cats = load_exp_cats()

        set1, set2 = st.columns(2)

        # ── Expense Categories ────────────────────────────────────
        with set1:
            st.markdown("#### Expense Categories")

            with st.form("add_exp_cat"):
                nec = st.text_input("New Category Name",
                    placeholder="e.g. Insurance, Equipment")
                if st.form_submit_button("Add Category", type="primary"):
                    if nec.strip():
                        add_exp_cat(nec.strip())
                        st.success(f"Category '{nec}' added!")
                        st.rerun()
                    else:
                        st.error("Enter a category name.")

            st.markdown("**Existing Categories**")
            for cat in exp_cats:
                col1, col2, col3 = st.columns([3, 2, 1])
                col1.write(cat)
                new_cat = col2.text_input("Rename",
                    key=f"rnc_{cat}",
                    label_visibility="collapsed",
                    placeholder="New name…")
                with col3:
                    if st.button("Rename", key=f"rcb_{cat}"):
                        if new_cat.strip():
                            rename_exp_cat(cat, new_cat.strip())
                            st.success("Renamed!")
                            st.rerun()
                    if st.button("Delete", key=f"dce_{cat}"):
                        ok = delete_exp_cat(cat)
                        if ok:
                            st.success("Deleted!")
                            st.rerun()
                        else:
                            st.error(
                                "Cannot delete — transactions exist "
                                "in this category.")

        # ── Password ──────────────────────────────────────────────
        with set2:
            st.markdown("#### Change Password")
            with st.form("change_pw"):
                cur_pw  = st.text_input("Current Password", type="password")
                new_pw  = st.text_input("New Password",     type="password")
                new_pw2 = st.text_input("Confirm New Password", type="password")
                if st.form_submit_button("Update Password", type="primary"):
                    role_check = verify_login(username, cur_pw)
                    if not role_check:
                        st.error("Current password is incorrect.")
                    elif new_pw != new_pw2:
                        st.error("New passwords do not match.")
                    elif len(new_pw) < 6:
                        st.error("Password must be at least 6 characters.")
                    else:
                        change_password(username, new_pw)
                        st.success("Password updated! Please sign in again.")
                        for k in ["logged_in","username","role"]:
                            st.session_state.pop(k, None)
                        st.rerun()

            st.markdown("---")
            st.markdown("#### App Info")
            st.info(
                f"**Logged in as:** {username}  \n"
                f"**Role:** {role}  \n"
                f"**Database:** {DB_PATH}  \n"
                f"**Default login:** admin / kokari2026  \n"
                "Change your password above after first login.")


if __name__ == "__main__":
    main()
