"""
KOKARI CAFE FINANCIAL DASHBOARD
================================
requirements.txt:
    streamlit
    pandas

Run:  streamlit run dashboard.py
"""

import sqlite3
import hashlib
import base64
import re
from datetime import date, timedelta

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Kokari Cafe",
    page_icon="☕",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH       = "kokari_cafe.db"
PAYMENT_METHODS = ["Opay", "Bank Transfer", "Cash", "POS", "Other"]

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
def get_write_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_read_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_write_conn()
    try:
        cur = conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role     TEXT NOT NULL DEFAULT 'accountant'
        );
        CREATE TABLE IF NOT EXISTS channels (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS expense_categories (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            id           TEXT PRIMARY KEY,
            name         TEXT NOT NULL,
            channel_id   INTEGER,
            cost_ratio   REAL NOT NULL DEFAULT 0.40,
            default_price REAL NOT NULL DEFAULT 0,
            active       INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        );
        CREATE TABLE IF NOT EXISTS orders (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            customer_name  TEXT NOT NULL DEFAULT 'Walk-in',
            order_type     TEXT NOT NULL DEFAULT 'Dine-in',
            payment_method TEXT NOT NULL DEFAULT 'Cash',
            total_amount   REAL NOT NULL DEFAULT 0,
            note           TEXT DEFAULT '',
            created_at     TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    INTEGER NOT NULL,
            product_id  TEXT,
            product_name TEXT NOT NULL,
            channel     TEXT NOT NULL DEFAULT 'Cafe',
            qty         REAL NOT NULL DEFAULT 1,
            unit_price  REAL NOT NULL DEFAULT 0,
            total_price REAL NOT NULL DEFAULT 0,
            unit_cogs   REAL NOT NULL DEFAULT 0,
            total_cogs  REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
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
        );
        """)

        # ── seed users ──
        cur.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 0:
            cur.execute(
                "INSERT INTO users (username,password,role) VALUES (?,?,?)",
                ("admin", hash_pw("kokari2026"), "admin"))

        # ── seed channels ──
        cur.execute("SELECT COUNT(*) FROM channels")
        if cur.fetchone()[0] == 0:
            for ch in ["Cafe","B2B","Packaged","Retail","Other"]:
                cur.execute("INSERT INTO channels (name) VALUES (?)", (ch,))

        # ── seed expense categories ──
        cur.execute("SELECT COUNT(*) FROM expense_categories")
        if cur.fetchone()[0] == 0:
            for ec in ["Ingredients","Utilities","Staff/Wages","Packaging",
                       "Rent","Transport","Logistics","Stationery",
                       "Marketing","Maintenance","Miscellaneous"]:
                cur.execute(
                    "INSERT INTO expense_categories (name) VALUES (?)", (ec,))

        # ── seed products (with default_price) ──
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            ch = {r[0]: r[1] for r in
                  cur.execute("SELECT name,id FROM channels").fetchall()}
            cur.executemany(
                "INSERT INTO products "
                "(id,name,channel_id,cost_ratio,default_price) "
                "VALUES (?,?,?,?,?)", [
                ("p01","Pancakes",             ch["Cafe"],     0.38, 4300),
                ("p02","Fruit Smoothie",        ch["Cafe"],     0.40, 4840),
                ("p03","Books",                 ch["Retail"],   0.55, 10975),
                ("p04","Puff Puff",             ch["Cafe"],     0.30, 3765),
                ("p05","Spicy Chicken Wrap",    ch["Cafe"],     0.42, 4600),
                ("p06","Chicken Wings",         ch["Cafe"],     0.45, 8065),
                ("p07","Tapioca",               ch["Cafe"],     0.35, 4300),
                ("p08","Coffee",                ch["Cafe"],     0.28, 4300),
                ("p09","Iced Coffee",           ch["Cafe"],     0.30, 4840),
                ("p10","Zobo",                  ch["Cafe"],     0.25, 3765),
                ("p11","Parfait & Wings Combo", ch["Cafe"],     0.45, 8600),
                ("p12","Parfait",               ch["Cafe"],     0.40, 5375),
                ("p13","Granola 500g",          ch["Packaged"], 0.50, 6757),
                ("p14","Spicy Coconut Flakes",  ch["Packaged"], 0.48, 3765),
                ("p15","Honey Coconut Cashew",  ch["Packaged"], 0.50, 10750),
                ("p16","CCB",                   ch["Packaged"], 0.50, 9675),
                ("p17","Wholesale (B2B)",       ch["B2B"],      0.55, 1000),
                ("p18","Iced Tea",              ch["Cafe"],     0.30, 4840),
                ("p19","Water",                 ch["Cafe"],     0.20, 500),
                ("p20","Space Rental",          ch["Other"],    0.05, 3500),
            ])

        # ── seed orders from Feb 9-14 historical data ──
        cur.execute("SELECT COUNT(*) FROM orders")
        if cur.fetchone()[0] == 0:
            _seed_orders(cur)

        # ── seed expenses ──
        cur.execute("SELECT COUNT(*) FROM transactions")
        if cur.fetchone()[0] == 0:
            _seed_expenses(cur)

        conn.commit()
    finally:
        conn.close()


def _get_product(cur, pid):
    return cur.execute(
        "SELECT cost_ratio, default_price FROM products WHERE id=?",
        (pid,)).fetchone()


def _seed_orders(cur):
    """Seed Feb 9-14 daily totals as single orders (one item each for simplicity)."""
    daily = [
        ("2026-02-09", [
            ("p01","Pancakes","Cafe",3,5020,15060),
            ("p08","Coffee","Cafe",2,4300,8600),
            ("p09","Iced Coffee","Cafe",4,4554,18215),
            ("p11","Parfait & Wings Combo","Cafe",3,8785,26355),
            ("p12","Parfait","Cafe",4,5375,21500),
        ]),
        ("2026-02-10", [
            ("p01","Pancakes","Cafe",3,3765,11295),
            ("p08","Coffee","Cafe",3,3790,11370),
            ("p09","Iced Coffee","Cafe",1,4840,4840),
            ("p10","Zobo","Cafe",1,3765,3765),
            ("p11","Parfait & Wings Combo","Cafe",1,8600,8600),
            ("p14","Spicy Coconut Flakes","Packaged",1,3765,3765),
        ]),
        ("2026-02-11", [
            ("p01","Pancakes","Cafe",3,3765,11295),
            ("p04","Puff Puff","Cafe",1,3765,3765),
            ("p08","Coffee","Cafe",3,3755,11265),
            ("p09","Iced Coffee","Cafe",5,4840,24200),
            ("p10","Zobo","Cafe",1,3765,3765),
            ("p11","Parfait & Wings Combo","Cafe",1,8600,8600),
            ("p12","Parfait","Cafe",2,5375,10750),
        ]),
        ("2026-02-12", [
            ("p01","Pancakes","Cafe",5,4563,22815),
            ("p02","Fruit Smoothie","Cafe",3,4840,14520),
            ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),
            ("p06","Chicken Wings","Cafe",4,8315,33260),
            ("p07","Tapioca","Cafe",1,4300,4300),
            ("p08","Coffee","Cafe",2,4570,9140),
            ("p09","Iced Coffee","Cafe",6,4842,29050),
            ("p10","Zobo","Cafe",3,3765,11295),
            ("p13","Granola 500g","Packaged",1,6757,6757),
        ]),
        ("2026-02-13", [
            ("p01","Pancakes","Cafe",5,3765,18825),
            ("p02","Fruit Smoothie","Cafe",5,4840,24200),
            ("p03","Books","Retail",2,10975,21950),
            ("p04","Puff Puff","Cafe",1,3765,3765),
            ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),
            ("p06","Chicken Wings","Cafe",1,8065,8065),
            ("p08","Coffee","Cafe",1,3765,3765),
            ("p09","Iced Coffee","Cafe",1,4840,4840),
            ("p12","Parfait","Cafe",1,5375,5375),
            ("p14","Spicy Coconut Flakes","Packaged",1,3765,3765),
            ("p17","Wholesale (B2B)","B2B",1,504513,504513),
        ]),
        ("2026-02-14", [
            ("p01","Pancakes","Cafe",6,4393,26355),
            ("p02","Fruit Smoothie","Cafe",1,4840,4840),
            ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),
            ("p06","Chicken Wings","Cafe",1,8065,8065),
            ("p07","Tapioca","Cafe",5,4300,21500),
            ("p08","Coffee","Cafe",7,4225,29575),
            ("p09","Iced Coffee","Cafe",2,4840,9680),
            ("p10","Zobo","Cafe",3,3765,11295),
            ("p11","Parfait & Wings Combo","Cafe",3,8600,25800),
        ]),
    ]
    for sale_date, items in daily:
        total = sum(i[5] for i in items)
        cur.execute(
            "INSERT INTO orders "
            "(date,customer_name,order_type,payment_method,total_amount,note) "
            "VALUES (?,?,?,?,?,?)",
            (sale_date,"Daily Batch","Dine-in","Bank Transfer",total,"Seeded data"))
        oid = cur.lastrowid
        for pid, pname, chan, qty, unit_p, tot_p in items:
            row = _get_product(cur, pid)
            ratio = row[0] if row else 0.40
            cur.execute(
                "INSERT INTO order_items "
                "(order_id,product_id,product_name,channel,qty,"
                "unit_price,total_price,unit_cogs,total_cogs) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (oid, pid, pname, chan, qty, unit_p, tot_p,
                 round(unit_p * ratio), round(tot_p * ratio)))


def _seed_expenses(cur):
    expenses = [
        ("2026-02-09","Ingredients","Sugar",9000),
        ("2026-02-09","Ingredients","Flour",5400),
        ("2026-02-09","Ingredients","Chicken Wings",20178),
        ("2026-02-09","Ingredients","Chicken",11000),
        ("2026-02-09","Ingredients","Bread",1200),
        ("2026-02-09","Ingredients","Mayonnaise",4000),
        ("2026-02-09","Ingredients","Banana",2000),
        ("2026-02-09","Ingredients","Oil",4400),
        ("2026-02-09","Ingredients","Carrot and Cabbage",3000),
        ("2026-02-09","Ingredients","Eggs",5900),
        ("2026-02-09","Ingredients","Groundnut",2000),
        ("2026-02-09","Ingredients","Powder Milk",44000),
        ("2026-02-09","Ingredients","Pineapple",3000),
        ("2026-02-09","Ingredients","Ginger",2000),
        ("2026-02-09","Ingredients","Cinnamon",2000),
        ("2026-02-09","Ingredients","Cloves",1000),
        ("2026-02-09","Ingredients","Grapes",6000),
        ("2026-02-09","Ingredients","Honey",6000),
        ("2026-02-09","Ingredients","Liquid Milk",10200),
        ("2026-02-09","Utilities","NEPA Electricity",10000),
        ("2026-02-09","Utilities","Data",3500),
        ("2026-02-09","Utilities","Recharge Card",2000),
        ("2026-02-09","Utilities","Water CWay",3400),
        ("2026-02-09","Packaging","Zobo Bottles",4400),
        ("2026-02-09","Packaging","Foil",3000),
        ("2026-02-09","Packaging","Serviettes",2000),
        ("2026-02-09","Packaging","Spoons",3000),
        ("2026-02-09","Packaging","Water retail",2500),
        ("2026-02-09","Transport","Transport",1500),
        ("2026-02-09","Miscellaneous","Printing",400),
        ("2026-02-09","Miscellaneous","Bank Charges",400),
        ("2026-02-09","Miscellaneous","Phone Repair",500),
        ("2026-02-09","Logistics","Bucket",3000),
        ("2026-02-09","Logistics","Item Delivery x7",2000),
        ("2026-02-09","Utilities","NEPA Imprest",3000),
        ("2026-02-09","Stationery","Battery and Book",1000),
        ("2026-02-09","Packaging","Serviettes Imprest",1000),
        ("2026-02-14","Ingredients","Chicken Wings",36321),
        ("2026-02-14","Ingredients","Flour",6400),
        ("2026-02-14","Ingredients","Eggs",6000),
        ("2026-02-14","Packaging","Straws",3700),
        ("2026-02-14","Packaging","Water retail",4400),
        ("2026-02-14","Transport","Transport",500),
    ]
    for e in expenses:
        cur.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,NULL,?,?,?,?,'')",
            (e[0],"expense",e[2],e[1],e[3],e[3]))


# ─────────────────────────────────────────────────────────────────
# WHATSAPP PARSER
# ─────────────────────────────────────────────────────────────────
def parse_whatsapp_sales(text, sale_date, products_df):
    """
    Parse Kokari Cafe WhatsApp daily sales report into structured orders.
    Returns list of dicts ready for review.
    """
    lines   = text.strip().split("\n")
    orders  = []
    prod_names = products_df["name"].str.lower().tolist()
    prod_ids   = products_df["id"].tolist()
    prod_prices= products_df["default_price"].tolist()
    prod_ratios= products_df["cost_ratio"].tolist()
    prod_chans = products_df["channel"].tolist()

    def best_match(item_text):
        """Fuzzy-match item text to a product."""
        item_lower = item_text.lower().strip()
        # aliases map
        aliases = {
            "iced coffee": "p09", "ice coffee": "p09", "iced tea": "p18",
            "coffee": "p08", "zobo": "p10", "pancake": "p01",
            "smoothie": "p02", "wrap": "p05", "wings": "p06",
            "tapioca": "p07", "parfait + wings": "p11",
            "parfait cafe": "p12", "parfait": "p12",
            "granola": "p13", "spicy coconut": "p14",
            "cashew": "p15", "ccb": "p16", "water": "p19",
            "space": "p20", "take away": "p01", "puff puff": "p04",
            "books": "p03", "book": "p03",
        }
        for alias, pid in aliases.items():
            if alias in item_lower:
                idx = prod_ids.index(pid)
                return {
                    "product_id":   pid,
                    "product_name": products_df.iloc[idx]["name"],
                    "channel":      prod_chans[idx],
                    "unit_price":   prod_prices[idx],
                    "cost_ratio":   prod_ratios[idx],
                }
        # fallback: partial match on product name
        for i, pname in enumerate(prod_names):
            if pname in item_lower or item_lower in pname:
                return {
                    "product_id":   prod_ids[i],
                    "product_name": products_df.iloc[i]["name"],
                    "channel":      prod_chans[i],
                    "unit_price":   prod_prices[i],
                    "cost_ratio":   prod_ratios[i],
                }
        # unmatched — return generic
        return {
            "product_id":   None,
            "product_name": item_text.strip().title(),
            "channel":      "Cafe",
            "unit_price":   0,
            "cost_ratio":   0.40,
        }

    def parse_items(items_text):
        """Parse item string like '2 iced coffee, 1 zobo, 1 pancake'."""
        items_text = items_text.lower()
        # split on comma or +
        parts  = re.split(r"[,+]", items_text)
        result = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # match qty at start: "2 iced coffee" or "1kg granola"
            m = re.match(r"^(\d+(?:\.\d+)?)\s*(?:kg|g|pcs|pc|x)?\s*(.+)$", part)
            if m:
                qty      = float(m.group(1))
                item_str = m.group(2).strip()
            else:
                qty      = 1
                item_str = part
            prod = best_match(item_str)
            # actual unit price = total / sum_qty if only one item
            result.append({
                **prod,
                "qty": qty,
            })
        return result

    # pattern: ✅️ Name --- #amount (items)  or  ✅ Name #amount (items)
    order_pattern = re.compile(
        r"[✅☑✓\*]\S*\s*"               # checkbox marker
        r"([^#\-\n]+?)"                  # customer name
        r"[-–—#\s]*#?([\d,]+)"          # amount
        r"(?:\s*\(([^)]+)\))?",         # optional items in parens
        re.UNICODE)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # skip total / transfer lines
        if re.search(r"total|transfer|opay|gtb|access|zenith", line.lower()):
            continue

        m = order_pattern.search(line)
        if not m:
            continue

        customer = m.group(1).strip().strip("-—").strip()
        customer = re.sub(r"\s+", " ", customer)
        if not customer or customer.lower() in ("", "walk-in", "walk in"):
            customer = "Walk-in"

        try:
            amount = float(m.group(2).replace(",",""))
        except Exception:
            continue

        items_str = m.group(3) or ""
        items     = parse_items(items_str) if items_str else []

        # detect payment method from line
        pay = "Bank Transfer"
        if "cash" in line.lower():
            pay = "Cash"
        elif "pos" in line.lower():
            pay = "POS"
        elif "opay" in line.lower():
            pay = "Opay"

        # detect order type
        order_type = "Take-out" if "take out" in line.lower() or "takeout" in line.lower() else "Dine-in"

        # distribute amount across items if possible
        if items:
            total_derived = sum(
                i["qty"] * i["unit_price"] for i in items
                if i["unit_price"] > 0)
            if total_derived == 0:
                # can't derive — divide equally
                per_item_share = amount / len(items)
                for it in items:
                    it["unit_price"]  = round(per_item_share / it["qty"])
                    it["total_price"] = round(per_item_share)
            else:
                # scale to match reported total
                scale = amount / total_derived if total_derived > 0 else 1
                for it in items:
                    it["unit_price"]  = round(it["unit_price"] * scale) if it["unit_price"] > 0 else round(amount / len(items) / it["qty"])
                    it["total_price"] = round(it["unit_price"] * it["qty"])

            for it in items:
                it["unit_cogs"]  = round(it["unit_price"] * it["cost_ratio"])
                it["total_cogs"] = round(it["unit_cogs"]  * it["qty"])
        else:
            # no items parsed — single line item with full amount
            items = [{
                "product_id":   None,
                "product_name": "Unknown Item",
                "channel":      "Cafe",
                "qty":          1,
                "unit_price":   amount,
                "total_price":  amount,
                "cost_ratio":   0.40,
                "unit_cogs":    round(amount * 0.40),
                "total_cogs":   round(amount * 0.40),
            }]

        orders.append({
            "date":           str(sale_date),
            "customer_name":  customer,
            "order_type":     order_type,
            "payment_method": pay,
            "total_amount":   amount,
            "note":           "",
            "items":          items,
        })

    return orders


def save_parsed_orders(orders):
    """Save a list of parsed order dicts to the database."""
    conn = get_write_conn()
    try:
        for o in orders:
            conn.execute(
                "INSERT INTO orders "
                "(date,customer_name,order_type,payment_method,"
                "total_amount,note) VALUES (?,?,?,?,?,?)",
                (o["date"], o["customer_name"], o["order_type"],
                 o["payment_method"], o["total_amount"], o["note"]))
            oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for it in o["items"]:
                conn.execute(
                    "INSERT INTO order_items "
                    "(order_id,product_id,product_name,channel,qty,"
                    "unit_price,total_price,unit_cogs,total_cogs) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (oid, it.get("product_id"), it["product_name"],
                     it["channel"], it["qty"], it["unit_price"],
                     it["total_price"], it["unit_cogs"], it["total_cogs"]))
        conn.commit()
    finally:
        conn.close()
    bust()


def save_single_order(order_date, customer, order_type,
                      payment, total, note, items):
    conn = get_write_conn()
    try:
        conn.execute(
            "INSERT INTO orders "
            "(date,customer_name,order_type,payment_method,total_amount,note) "
            "VALUES (?,?,?,?,?,?)",
            (str(order_date), customer, order_type, payment, total, note))
        oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for it in items:
            conn.execute(
                "INSERT INTO order_items "
                "(order_id,product_id,product_name,channel,qty,"
                "unit_price,total_price,unit_cogs,total_cogs) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (oid, it.get("product_id"), it["product_name"],
                 it["channel"], it["qty"], it["unit_price"],
                 it["total_price"], it["unit_cogs"], it["total_cogs"]))
        conn.commit()
    finally:
        conn.close()
    bust()


def add_expense(date_val, name, cat, amount, note=""):
    conn = get_write_conn()
    try:
        conn.execute(
            "INSERT INTO transactions "
            "(date,type,product_id,name,category,amount,cogs,note) "
            "VALUES (?,?,NULL,?,?,?,?,?)",
            (str(date_val), "expense", name, cat, amount, amount, note))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_order(oid):
    conn = get_write_conn()
    try:
        conn.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
        conn.execute("DELETE FROM orders WHERE id=?", (oid,))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_expense(tid):
    conn = get_write_conn()
    try:
        conn.execute("DELETE FROM transactions WHERE id=?", (tid,))
        conn.commit()
    finally:
        conn.close()
    bust()


# ─────────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_orders(from_d, to_d):
    conn = get_read_conn()
    try:
        return pd.read_sql_query(
            "SELECT * FROM orders "
            "WHERE date BETWEEN ? AND ? "
            "ORDER BY date DESC, id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=10)
def load_order_items(from_d, to_d):
    conn = get_read_conn()
    try:
        return pd.read_sql_query(
            "SELECT oi.*, o.date, o.customer_name, "
            "o.payment_method, o.order_type "
            "FROM order_items oi "
            "JOIN orders o ON oi.order_id = o.id "
            "WHERE o.date BETWEEN ? AND ? "
            "ORDER BY o.date DESC, o.id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=10)
def load_expenses(from_d, to_d):
    conn = get_read_conn()
    try:
        return pd.read_sql_query(
            "SELECT * FROM transactions "
            "WHERE type='expense' AND date BETWEEN ? AND ? "
            "ORDER BY date DESC, id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_products():
    conn = get_read_conn()
    try:
        return pd.read_sql_query(
            "SELECT p.id, p.name, "
            "COALESCE(c.name,'Unassigned') AS channel, "
            "p.channel_id, p.cost_ratio, p.default_price, p.active "
            "FROM products p "
            "LEFT JOIN channels c ON p.channel_id = c.id "
            "WHERE p.active = 1 "
            "ORDER BY c.name, p.name",
            conn)
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_channels():
    conn = get_read_conn()
    try:
        return pd.read_sql_query(
            "SELECT * FROM channels ORDER BY name", conn)
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_exp_cats():
    conn = get_read_conn()
    try:
        rows = conn.execute(
            "SELECT name FROM expense_categories ORDER BY name"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def bust():
    for fn in [load_orders, load_order_items, load_expenses,
               load_products, load_channels, load_exp_cats]:
        fn.clear()


def compute_kpis(orders_df, items_df, exp_df):
    revenue  = float(orders_df["total_amount"].sum()) if not orders_df.empty else 0
    cogs     = float(items_df["total_cogs"].sum())    if not items_df.empty  else 0
    expenses = float(exp_df["amount"].sum())          if not exp_df.empty    else 0
    gp       = revenue - cogs
    np_      = gp - expenses
    days     = int(orders_df["date"].nunique()) if not orders_df.empty else 0
    return dict(
        revenue=revenue, cogs=cogs, expenses=expenses,
        gp=gp, np=np_,
        gp_mar=safe_pct(gp, revenue),
        np_mar=safe_pct(np_, revenue),
        days=days,
        avg_daily=revenue / days if days else 0,
        orders=len(orders_df),
        avg_order=revenue / len(orders_df) if len(orders_df) > 0 else 0,
    )


# ─────────────────────────────────────────────────────────────────
# PRODUCT CRUD
# ─────────────────────────────────────────────────────────────────
def add_product(name, channel_id, cost_ratio, default_price):
    pid  = "u" + str(abs(hash(name + str(date.today()))))[:8]
    conn = get_write_conn()
    try:
        conn.execute(
            "INSERT INTO products "
            "(id,name,channel_id,cost_ratio,default_price) "
            "VALUES (?,?,?,?,?)",
            (pid, name, channel_id, cost_ratio, default_price))
        conn.commit()
    finally:
        conn.close()
    bust()

def update_product(pid, name, channel_id, cost_ratio, default_price):
    conn = get_write_conn()
    try:
        conn.execute(
            "UPDATE products SET name=?,channel_id=?,"
            "cost_ratio=?,default_price=? WHERE id=?",
            (name, channel_id, cost_ratio, default_price, pid))
        conn.commit()
    finally:
        conn.close()
    bust()

def deactivate_product(pid):
    conn = get_write_conn()
    try:
        conn.execute("UPDATE products SET active=0 WHERE id=?", (pid,))
        conn.commit()
    finally:
        conn.close()
    bust()

# ─────────────────────────────────────────────────────────────────
# CHANNEL / CATEGORY CRUD
# ─────────────────────────────────────────────────────────────────
def add_channel(name):
    conn = get_write_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO channels (name) VALUES (?)", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()

def rename_channel(cid, new_name):
    conn = get_write_conn()
    try:
        conn.execute("UPDATE channels SET name=? WHERE id=?", (new_name, cid))
        conn.commit()
    finally:
        conn.close()
    bust()

def delete_channel(cid):
    conn = get_write_conn()
    try:
        used = conn.execute(
            "SELECT COUNT(*) FROM products WHERE channel_id=?",
            (cid,)).fetchone()[0]
        if used > 0:
            return False
        conn.execute("DELETE FROM channels WHERE id=?", (cid,))
        conn.commit()
    finally:
        conn.close()
    bust()
    return True

def add_exp_cat(name):
    conn = get_write_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO expense_categories (name) VALUES (?)", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()

def rename_exp_cat(old, new):
    conn = get_write_conn()
    try:
        conn.execute(
            "UPDATE expense_categories SET name=? WHERE name=?", (new, old))
        conn.execute(
            "UPDATE transactions SET category=? WHERE category=?", (new, old))
        conn.commit()
    finally:
        conn.close()
    bust()

def delete_exp_cat(name):
    conn = get_write_conn()
    try:
        used = conn.execute(
            "SELECT COUNT(*) FROM transactions "
            "WHERE category=? AND type='expense'",
            (name,)).fetchone()[0]
        if used > 0:
            return False
        conn.execute(
            "DELETE FROM expense_categories WHERE name=?", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()
    return True

def verify_login(username, password):
    conn = get_read_conn()
    try:
        row = conn.execute(
            "SELECT role FROM users WHERE username=? AND password=?",
            (username, hash_pw(password))).fetchone()
        return row[0] if row else None
    finally:
        conn.close()

def change_password(username, new_password):
    conn = get_write_conn()
    try:
        conn.execute(
            "UPDATE users SET password=? WHERE username=?",
            (hash_pw(new_password), username))
        conn.commit()
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────
# PDF REPORT
# ─────────────────────────────────────────────────────────────────
def generate_pdf_html(kpi, from_date, to_date, by_ec, ch_data, prod_data):
    rows_pl = ""
    pl_items = [
        ("Revenue",           kpi["revenue"],  False),
        ("  Cost of Goods",  -kpi["cogs"],     False),
        ("Gross Profit",      kpi["gp"],        True),
        ("", None, False),
    ]
    for cat, amt in sorted(by_ec.items()):
        pl_items.append((f"  {cat}", -amt, False))
    pl_items += [
        ("Total Expenses", -kpi["expenses"], True),
        ("", None, False),
        ("NET PROFIT / LOSS", kpi["np"], True),
    ]
    for label, val, bold in pl_items:
        if val is None:
            rows_pl += "<tr><td colspan='2'><hr></td></tr>"
            continue
        s = "font-weight:bold;" if bold else ""
        c = ("color:#16a34a;" if val >= 0 else "color:#dc2626;") if bold else ""
        d = f"({fmt(abs(val))})" if val < 0 else fmt(val)
        rows_pl += f"<tr><td style='{s}'>{label}</td><td style='text-align:right;{s}{c}'>{d}</td></tr>"

    rows_ch = ""
    for _, row in ch_data.iterrows():
        rows_ch += (
            f"<tr><td>{row['channel']}</td>"
            f"<td style='text-align:right'>{fmt(row['revenue'])}</td>"
            f"<td style='text-align:right'>{fmt(row['gross_profit'])}</td>"
            f"<td style='text-align:right'>{safe_pct(row['revenue'],kpi['revenue'])}%</td></tr>")

    rows_prod = ""
    for _, row in prod_data.head(10).iterrows():
        rows_prod += (
            f"<tr><td>{row['product_name']}</td>"
            f"<td>{row.get('channel','')}</td>"
            f"<td style='text-align:right'>{fmt(row['total_price'])}</td>"
            f"<td style='text-align:right'>{row.get('margin',0)}%</td></tr>")

    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'>
<title>Kokari Cafe Report</title>
<style>
body{{font-family:Arial,sans-serif;font-size:13px;color:#111;margin:40px}}
h1{{font-size:22px;color:#2563eb}}h2{{font-size:15px;color:#374151;
border-bottom:2px solid #2563eb;padding-bottom:4px;margin-top:30px}}
.sub{{color:#6b7280;font-size:12px;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;margin-top:10px}}
th{{background:#2563eb;color:white;padding:7px 10px;text-align:left;font-size:12px}}
td{{padding:6px 10px;border-bottom:1px solid #f0f0f0}}
tr:nth-child(even) td{{background:#f9fafb}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}}
.kpi{{background:#f3f4f6;border-radius:8px;padding:12px;text-align:center}}
.kpi-val{{font-size:18px;font-weight:bold;color:#2563eb}}
.kpi-lbl{{font-size:11px;color:#6b7280;margin-top:2px}}
@media print{{button{{display:none}}}}
</style></head><body>
<h1>Kokari Cafe Financial Report</h1>
<div class='sub'>Period: <strong>{from_date}</strong> to <strong>{to_date}</strong>
 | Generated: {date.today()}</div>
<div class='kpi-grid'>
<div class='kpi'><div class='kpi-val'>{fmt(kpi['revenue'])}</div>
<div class='kpi-lbl'>Revenue ({kpi['orders']} orders)</div></div>
<div class='kpi'><div class='kpi-val' style='color:#16a34a'>{fmt(kpi['gp'])}</div>
<div class='kpi-lbl'>Gross Profit ({kpi['gp_mar']}%)</div></div>
<div class='kpi'><div class='kpi-val'
style='color:{"#16a34a" if kpi["np"]>=0 else "#dc2626"}'>{fmt(kpi['np'])}</div>
<div class='kpi-lbl'>Net Profit ({kpi['np_mar']}%)</div></div>
<div class='kpi'><div class='kpi-val' style='color:#ea580c'>{fmt(kpi['expenses'])}</div>
<div class='kpi-lbl'>Expenses</div></div></div>
<h2>Profit &amp; Loss</h2>
<table><tr><th>Item</th><th style='text-align:right'>Amount</th></tr>{rows_pl}</table>
<h2>By Channel</h2>
<table><tr><th>Channel</th><th style='text-align:right'>Revenue</th>
<th style='text-align:right'>Gross Profit</th>
<th style='text-align:right'>Share</th></tr>{rows_ch}</table>
<h2>Top 10 Products</h2>
<table><tr><th>Product</th><th>Channel</th>
<th style='text-align:right'>Revenue</th>
<th style='text-align:right'>Margin</th></tr>{rows_prod}</table>
<div style='margin-top:40px;font-size:11px;color:#9ca3af;border-top:1px solid #e5e7eb;padding-top:10px'>
Kokari Cafe Financial Dashboard | Auto-generated report</div>
<br><button onclick='window.print()' style='background:#2563eb;color:white;
border:none;padding:10px 24px;border-radius:6px;font-size:14px;cursor:pointer'>
Save as PDF / Print</button></body></html>"""

def pdf_download_button(html, filename):
    b64  = base64.b64encode(html.encode()).decode()
    href = (f'<a href="data:text/html;base64,{b64}" download="{filename}" '
            f'style="display:inline-block;background:#2563eb;color:white;'
            f'padding:8px 20px;border-radius:6px;text-decoration:none;'
            f'font-size:13px;font-weight:600;">Download PDF Report</a>')
    st.markdown(href, unsafe_allow_html=True)
    st.caption("Open downloaded file in browser → Ctrl+P → Save as PDF")

# ─────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────
def login_screen():
    st.markdown("""
    <div style='max-width:380px;margin:80px auto;text-align:center'>
    <h1 style='color:#2563eb'>Kokari Cafe</h1>
    <p style='color:#6b7280'>Financial Dashboard</p></div>""",
    unsafe_allow_html=True)
    col = st.columns([1, 2, 1])[1]
    with col:
        with st.form("login"):
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            if st.form_submit_button("Sign In", use_container_width=True,
                                     type="primary"):
                role = verify_login(u, p)
                if role:
                    st.session_state.update(
                        logged_in=True, username=u, role=role)
                    st.rerun()
                else:
                    st.error("Incorrect username or password.")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    init_db()

    if not st.session_state.get("logged_in"):
        login_screen()
        st.stop()

    username = st.session_state["username"]
    role     = st.session_state["role"]

    # ── Sidebar ───────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## Kokari Cafe")
        st.caption(f"**{username}** · {role}")
        if st.button("Sign Out", use_container_width=True):
            for k in ["logged_in","username","role",
                      "parsed_orders","edit_order_id"]:
                st.session_state.pop(k, None)
            st.rerun()
        st.divider()
        st.markdown("### Date Range")
        from_date = st.date_input("From", value=date(2026, 2, 9))
        to_date   = st.date_input("To",   value=date(2026, 2, 25))
        q1,q2,q3  = st.columns(3)
        if q1.button("Week"):
            t=date.today(); from_date=t-timedelta(days=t.weekday()); to_date=t
        if q2.button("30d"):
            to_date=date.today(); from_date=to_date-timedelta(days=30)
        if q3.button("All"):
            from_date=date(2020,1,1); to_date=date.today()
        st.divider()
        st.markdown("### Quick Expense")
        exp_cats = load_exp_cats()
        with st.form("qexp", clear_on_submit=True):
            qe_cat  = st.selectbox("Category",
                                   exp_cats if exp_cats else ["Miscellaneous"])
            qe_name = st.text_input("Description")
            qe_amt  = st.number_input("Amount", min_value=0, step=100)
            qe_date = st.date_input("Date", value=date.today())
            qe_note = st.text_input("Note")
            if st.form_submit_button("Save Expense", use_container_width=True,
                                     type="primary"):
                if qe_amt > 0:
                    add_expense(qe_date, qe_name, qe_cat, qe_amt, qe_note)
                    st.success("Saved!")
                else:
                    st.error("Amount > 0 required")

    # ── Load data ─────────────────────────────────────────────────
    orders_df = load_orders(from_date, to_date)
    items_df  = load_order_items(from_date, to_date)
    exp_df    = load_expenses(from_date, to_date)
    kpi       = compute_kpis(orders_df, items_df, exp_df)
    prods_df  = load_products()

    st.title("Kokari Cafe · Financial Dashboard")
    st.caption(
        f"Period: {from_date} to {to_date}  |  "
        f"{kpi['days']} days  |  {kpi['orders']} orders  |  "
        f"Avg order: {fmt(kpi['avg_order'])}")

    tabs = st.tabs([
        "Dashboard","WhatsApp Import","Manual Entry",
        "Orders","Expenses","Channels",
        "Products","P&L Report","Settings",
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
        r2[0].metric("Orders",       kpi["orders"])
        r2[1].metric("Avg Order",    fmt(kpi["avg_order"]))
        r2[2].metric("Avg Daily Rev",fmt(kpi["avg_daily"]))
        r2[3].metric("COGS Ratio",   f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
        st.divider()

        if not orders_df.empty:
            daily = (orders_df.groupby("date")["total_amount"].sum()
                     .reset_index(name="revenue"))
            daily_c = (items_df.groupby("date")["total_cogs"].sum()
                       .reset_index(name="cogs") if not items_df.empty
                       else pd.DataFrame(columns=["date","cogs"]))
            daily_e = (exp_df.groupby("date")["amount"].sum()
                       .reset_index(name="expenses") if not exp_df.empty
                       else pd.DataFrame(columns=["date","expenses"]))
            daily = (daily.merge(daily_c, on="date", how="left")
                     .merge(daily_e, on="date", how="left")
                     .fillna(0).sort_values("date"))
            daily["gross_profit"] = daily["revenue"] - daily["cogs"]
            daily["net_profit"]   = daily["gross_profit"] - daily["expenses"]

            st.markdown("#### Daily Revenue · Gross Profit · Expenses · Net Profit")
            dc = daily.set_index("date")[
                ["revenue","gross_profit","expenses","net_profit"]]
            dc.columns = ["Revenue","Gross Profit","Expenses","Net Profit"]
            st.bar_chart(dc, height=300)

            st.markdown("#### Trend")
            dl = daily.set_index("date")[["revenue","net_profit"]]
            dl.columns = ["Revenue","Net Profit"]
            st.line_chart(dl, height=240)

            if not items_df.empty:
                st.markdown("#### Top Products by Revenue")
                pp = (items_df.groupby("product_name")["total_price"]
                      .sum().reset_index()
                      .sort_values("total_price", ascending=True).tail(10))
                st.bar_chart(pp.set_index("product_name")
                             .rename(columns={"total_price":"Revenue"}),
                             height=280)

            bd  = daily.loc[daily["revenue"].idxmax()]
            bpd = daily.loc[daily["net_profit"].idxmax()]
            top_cust = (orders_df.groupby("customer_name")["total_amount"]
                        .sum().idxmax()
                        if len(orders_df) > 0 else "—")
            ic = st.columns(4)
            ic[0].success(f"**Best Day**\n\n{bd['date']}\n\n{fmt(bd['revenue'])}")
            ic[1].success(f"**Best Profit Day**\n\n{bpd['date']}\n\n{fmt(bpd['net_profit'])}")
            ic[2].info(   f"**Top Customer**\n\n{top_cust}")
            ic[3].warning(f"**Total Orders**\n\n{kpi['orders']} orders")
        else:
            st.info("No sales data for this period.")

    # ══════════════════════════════════════════════════════════════
    # WHATSAPP IMPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("### WhatsApp Sales Import")
        st.info(
            "Paste the daily WhatsApp sales message below. "
            "The system will parse each customer order automatically.")

        col1, col2 = st.columns([1, 1])
        with col1:
            wa_date = st.date_input("Sales Date", value=date.today())
        with col2:
            default_payment = st.selectbox(
                "Default Payment Method", PAYMENT_METHODS, index=0)

        wa_text = st.text_area(
            "Paste WhatsApp message here",
            height=220,
            placeholder=(
                "February 25th, 2026 Sales of the day\n"
                "✅️ Omoshelewa Temitope #4,300 (1 coffee, take out)\n"
                "✅️ Janet Johnson---#9,680(2 iced coffee)\n"
                "..."))

        if st.button("Parse Sales Report", type="primary",
                     use_container_width=True):
            if wa_text.strip():
                parsed = parse_whatsapp_sales(
                    wa_text, wa_date, prods_df)
                if parsed:
                    # apply default payment where not detected
                    for o in parsed:
                        if o["payment_method"] == "Bank Transfer":
                            o["payment_method"] = default_payment
                    st.session_state["parsed_orders"] = parsed
                    st.success(
                        f"Parsed {len(parsed)} orders. "
                        "Review below then click Save.")
                else:
                    st.warning(
                        "No orders found. Check the format — "
                        "each line should start with ✅ and contain #amount.")
            else:
                st.error("Paste the WhatsApp message first.")

        # ── Review parsed orders ──────────────────────────────────
        if "parsed_orders" in st.session_state:
            parsed = st.session_state["parsed_orders"]
            st.divider()
            st.markdown(f"#### Review {len(parsed)} Parsed Orders")
            st.caption("Check items, correct any mismatches, then save all.")

            total_parsed = sum(o["total_amount"] for o in parsed)
            st.metric("Total Parsed Amount", fmt(total_parsed))

            prod_options = ["— unmatched —"] + prods_df["name"].tolist()

            for oi, order in enumerate(parsed):
                with st.expander(
                    f"{'✅' if order['items'] else '⚠️'}  "
                    f"{order['customer_name']}  ·  "
                    f"{fmt(order['total_amount'])}  ·  "
                    f"{order['payment_method']}  ·  "
                    f"{order['order_type']}",
                    expanded=False):

                    oc1, oc2, oc3 = st.columns(3)
                    order["customer_name"] = oc1.text_input(
                        "Customer", value=order["customer_name"],
                        key=f"cust_{oi}")
                    order["payment_method"] = oc2.selectbox(
                        "Payment", PAYMENT_METHODS,
                        index=PAYMENT_METHODS.index(order["payment_method"])
                        if order["payment_method"] in PAYMENT_METHODS else 0,
                        key=f"pay_{oi}")
                    order["order_type"] = oc3.selectbox(
                        "Type", ["Dine-in","Take-out","Delivery"],
                        index=["Dine-in","Take-out","Delivery"].index(
                            order["order_type"])
                        if order["order_type"] in
                        ["Dine-in","Take-out","Delivery"] else 0,
                        key=f"otype_{oi}")

                    st.markdown("**Line Items:**")
                    for ii, item in enumerate(order["items"]):
                        ic1,ic2,ic3,ic4 = st.columns([3,1,1,1])
                        # product selector
                        cur_idx = 0
                        if item["product_name"] in prod_options:
                            cur_idx = prod_options.index(item["product_name"])
                        sel = ic1.selectbox(
                            "Product", prod_options,
                            index=cur_idx,
                            key=f"prod_{oi}_{ii}")
                        if sel != "— unmatched —":
                            row = prods_df[prods_df["name"] == sel]
                            if not row.empty:
                                item["product_name"] = sel
                                item["product_id"]   = row.iloc[0]["id"]
                                item["channel"]      = row.iloc[0]["channel"]
                                item["cost_ratio"]   = row.iloc[0]["cost_ratio"]

                        item["qty"] = ic2.number_input(
                            "Qty", min_value=0.0, step=0.5,
                            value=float(item["qty"]),
                            key=f"qty_{oi}_{ii}")
                        item["unit_price"] = ic3.number_input(
                            "Unit Price", min_value=0.0, step=100.0,
                            value=float(item["unit_price"]),
                            key=f"up_{oi}_{ii}")
                        item["total_price"] = round(
                            item["qty"] * item["unit_price"])
                        item["unit_cogs"]   = round(
                            item["unit_price"] * item["cost_ratio"])
                        item["total_cogs"]  = round(
                            item["unit_cogs"] * item["qty"])
                        ic4.metric("Total", fmt(item["total_price"]))

                    # reconciliation check
                    derived = sum(i["total_price"] for i in order["items"])
                    diff    = order["total_amount"] - derived
                    if abs(diff) > 1:
                        st.warning(
                            f"Reported: {fmt(order['total_amount'])}  |  "
                            f"Items sum: {fmt(derived)}  |  "
                            f"Difference: {fmt(abs(diff))} "
                            f"{'over' if diff < 0 else 'under'}")
                    else:
                        st.success(
                            f"Balanced: {fmt(order['total_amount'])}")

            st.divider()
            sc1, sc2 = st.columns(2)
            with sc1:
                if st.button("Save All Orders", type="primary",
                             use_container_width=True):
                    save_parsed_orders(
                        st.session_state["parsed_orders"])
                    st.session_state.pop("parsed_orders", None)
                    st.success("All orders saved!")
                    st.rerun()
            with sc2:
                if st.button("Discard", use_container_width=True):
                    st.session_state.pop("parsed_orders", None)
                    st.rerun()

    # ══════════════════════════════════════════════════════════════
    # MANUAL ENTRY
    # ══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("### Manual Order Entry")
        st.caption("Use this for single orders not in the WhatsApp report.")

        if "manual_items" not in st.session_state:
            st.session_state["manual_items"] = []

        with st.form("order_header"):
            mh1,mh2,mh3,mh4 = st.columns(4)
            m_date    = mh1.date_input("Date", value=date.today())
            m_cust    = mh2.text_input("Customer", value="Walk-in")
            m_otype   = mh3.selectbox(
                "Order Type", ["Dine-in","Take-out","Delivery"])
            m_pay     = mh4.selectbox("Payment", PAYMENT_METHODS)
            m_note    = st.text_input("Note")
            if st.form_submit_button("Set Order Header"):
                st.session_state["order_header"] = {
                    "date": m_date, "customer": m_cust,
                    "order_type": m_otype, "payment": m_pay,
                    "note": m_note}
                st.success("Header set. Add items below.")

        if "order_header" in st.session_state:
            hdr = st.session_state["order_header"]
            st.markdown(
                f"**Order:** {hdr['customer']}  ·  "
                f"{hdr['date']}  ·  {hdr['order_type']}  ·  "
                f"{hdr['payment']}")

            with st.form("add_item", clear_on_submit=True):
                ai1,ai2,ai3 = st.columns([3,1,1])
                names  = prods_df["name"].tolist()
                ids    = prods_df["id"].tolist()
                chans  = prods_df["channel"].tolist()
                ratios = prods_df["cost_ratio"].tolist()
                prices = prods_df["default_price"].tolist()
                sel_i  = ai1.selectbox("Product", range(len(names)),
                    format_func=lambda i: f"{names[i]}  [{chans[i]}]")
                ai_qty = ai2.number_input(
                    "Qty", min_value=0.0, step=0.5, value=1.0)
                ai_price = ai3.number_input(
                    "Unit Price", min_value=0.0, step=100.0,
                    value=float(prices[sel_i]))
                if st.form_submit_button("Add Item"):
                    ratio = ratios[sel_i]
                    st.session_state["manual_items"].append({
                        "product_id":   ids[sel_i],
                        "product_name": names[sel_i],
                        "channel":      chans[sel_i],
                        "qty":          ai_qty,
                        "unit_price":   ai_price,
                        "total_price":  round(ai_qty * ai_price),
                        "cost_ratio":   ratio,
                        "unit_cogs":    round(ai_price * ratio),
                        "total_cogs":   round(ai_qty * ai_price * ratio),
                    })

            items = st.session_state["manual_items"]
            if items:
                st.markdown("**Current Items:**")
                for idx, it in enumerate(items):
                    ic = st.columns([3,1,1,1,1])
                    ic[0].write(it["product_name"])
                    ic[1].write(f"x{it['qty']}")
                    ic[2].write(fmt(it["unit_price"]))
                    ic[3].write(fmt(it["total_price"]))
                    if ic[4].button("Remove", key=f"rm_{idx}"):
                        st.session_state["manual_items"].pop(idx)
                        st.rerun()

                total = sum(i["total_price"] for i in items)
                st.metric("Order Total", fmt(total))
                if st.button("Save Order", type="primary",
                             use_container_width=True):
                    save_single_order(
                        hdr["date"], hdr["customer"],
                        hdr["order_type"], hdr["payment"],
                        total, hdr["note"],
                        st.session_state["manual_items"])
                    st.session_state.pop("order_header", None)
                    st.session_state["manual_items"] = []
                    st.success("Order saved!")
                    st.rerun()
            else:
                st.info("No items yet. Add products above.")

    # ══════════════════════════════════════════════════════════════
    # ORDERS LEDGER
    # ══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("### Orders & Customer Ledger")
        if not orders_df.empty:
            oc1, oc2 = st.columns([3,1])
            sq = oc1.text_input("Search customer",
                label_visibility="collapsed",
                placeholder="Search customer name…")
            tf = oc2.selectbox("Filter",
                ["All","Dine-in","Take-out","Delivery"],
                label_visibility="collapsed")
            view = orders_df.copy()
            if sq:
                view = view[view["customer_name"].str.contains(
                    sq, case=False, na=False)]
            if tf != "All":
                view = view[view["order_type"] == tf]

            show_o = view[["id","date","customer_name","order_type",
                           "payment_method","total_amount","note"]].rename(
                columns={"id":"ID","date":"Date",
                         "customer_name":"Customer",
                         "order_type":"Type",
                         "payment_method":"Payment",
                         "total_amount":"Amount",
                         "note":"Note"})
            st.dataframe(
                show_o.style.format({"Amount":"{:,.0f}"}),
                use_container_width=True, height=320, hide_index=True)

            # Order items detail
            st.markdown("#### View Order Details")
            sel_oid = st.number_input(
                "Order ID", min_value=1, step=1, value=1)
            order_items_sel = items_df[items_df["order_id"] == sel_oid]
            if not order_items_sel.empty:
                st.dataframe(
                    order_items_sel[[
                        "product_name","channel","qty",
                        "unit_price","total_price",
                        "unit_cogs","total_cogs"]]
                    .style.format({
                        "unit_price":"{:,.0f}",
                        "total_price":"{:,.0f}",
                        "unit_cogs":"{:,.0f}",
                        "total_cogs":"{:,.0f}"}),
                    use_container_width=True, hide_index=True)
            if st.button("Delete Order", use_container_width=True):
                if sel_oid in orders_df["id"].values:
                    delete_order(int(sel_oid))
                    st.success(f"Order #{sel_oid} deleted.")
                    st.rerun()

            st.divider()
            st.markdown("#### Top Customers by Spend")
            top_c = (orders_df.groupby("customer_name")["total_amount"]
                     .agg(["sum","count"]).reset_index()
                     .rename(columns={"sum":"Total Spend",
                                      "count":"Visits",
                                      "customer_name":"Customer"})
                     .sort_values("Total Spend", ascending=False)
                     .head(15))
            top_c["Total Spend"] = top_c["Total Spend"].map("{:,.0f}".format)
            st.dataframe(top_c, use_container_width=True, hide_index=True)

            st.download_button("Export Orders CSV",
                data=orders_df.to_csv(index=False).encode(),
                file_name=f"kokari_orders_{from_date}_{to_date}.csv",
                mime="text/csv")
        else:
            st.info("No orders in this range.")

    # ══════════════════════════════════════════════════════════════
    # EXPENSES
    # ══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("### Expense Analysis")
        exp_cats = load_exp_cats()

        if not exp_df.empty:
            total_exp = exp_df["amount"].sum()
            by_cat    = (exp_df.groupby("category")["amount"].sum()
                         .reset_index().sort_values("amount",ascending=False))
            by_item   = (exp_df.groupby(["name","category"])["amount"].sum()
                         .reset_index().sort_values("amount",ascending=False))
            by_day    = (exp_df.groupby("date")["amount"].sum()
                         .reset_index().sort_values("date"))

            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("#### By Category")
                st.bar_chart(by_cat.set_index("category")
                             .rename(columns={"amount":"Amount"}), height=260)
            with ec2:
                st.markdown("#### Daily Spend")
                st.bar_chart(by_day.set_index("date")
                             .rename(columns={"amount":"Spend"}), height=260)

            bc = by_cat.copy()
            bc["pct"] = (bc["amount"]/total_exp*100).round(1)
            bc["amount"] = bc["amount"].map("{:,.0f}".format)
            bc.columns = ["Category","Amount","% of Total"]
            st.markdown("#### Category Summary")
            st.dataframe(bc, use_container_width=True, hide_index=True)

            bi = by_item.copy()
            bi["pct"] = (bi["amount"]/total_exp*100).round(1)
            bi["amount"] = bi["amount"].map("{:,.0f}".format)
            bi.columns = ["Item","Category","Amount","% of Total"]
            st.markdown("#### Itemised")
            st.dataframe(bi, use_container_width=True,
                         hide_index=True, height=360)

            st.divider()
            del_id = st.number_input(
                "Expense ID to delete", min_value=1, step=1, value=1)
            if st.button("Delete Expense"):
                if del_id in exp_df["id"].values:
                    delete_expense(int(del_id))
                    st.success(f"Deleted #{del_id}")
                    st.rerun()

            st.download_button("Export Expenses CSV",
                data=exp_df.to_csv(index=False).encode(),
                file_name=f"kokari_exp_{from_date}_{to_date}.csv",
                mime="text/csv")
        else:
            st.info("No expense data in this range.")

    # ══════════════════════════════════════════════════════════════
    # CHANNELS
    # ══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown("### Channel Performance")
        channels_df = load_channels()

        if not items_df.empty:
            ch_perf = (items_df.groupby("channel")
                       .agg(revenue=("total_price","sum"),
                            cogs=("total_cogs","sum")).reset_index())
            ch_perf["gross_profit"] = ch_perf["revenue"] - ch_perf["cogs"]
            ch_perf["margin"]       = (
                ch_perf["gross_profit"]/ch_perf["revenue"]*100).round(1)
            ch_perf["rev_share"]    = (
                ch_perf["revenue"]/kpi["revenue"]*100).round(1)
            ch_perf["channel"]      = ch_perf["channel"].fillna("Unassigned")
            ch_perf = ch_perf.sort_values("revenue", ascending=False)

            show_ch = ch_perf.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_ch[c] = show_ch[c].map("{:,.0f}".format)
            show_ch.columns = ["Channel","Revenue","COGS",
                               "Gross Profit","Margin %","Rev Share %"]
            st.dataframe(show_ch, use_container_width=True, hide_index=True)
            cch = ch_perf.set_index("channel")[["revenue","gross_profit"]]
            cch.columns = ["Revenue","Gross Profit"]
            st.bar_chart(cch, height=260)

        st.divider()
        st.markdown("#### Manage Channels")
        with st.form("add_ch"):
            nc = st.text_input("New Channel",
                placeholder="e.g. Online Orders, Catering, Event Sales")
            if st.form_submit_button("Add Channel", type="primary"):
                if nc.strip():
                    add_channel(nc.strip()); st.success("Added!"); st.rerun()
                else:
                    st.error("Enter a name.")
        for _, row in channels_df.iterrows():
            c1,c2,c3 = st.columns([3,2,1])
            c1.write(f"**{row['name']}**")
            nn = c2.text_input("Rename", key=f"rch_{row['id']}",
                               label_visibility="collapsed",
                               placeholder="New name…")
            with c3:
                if st.button("Rename", key=f"rcb_{row['id']}"):
                    if nn.strip():
                        rename_channel(row["id"],nn.strip())
                        st.success("Renamed!"); st.rerun()
                if st.button("Del", key=f"dch_{row['id']}"):
                    ok = delete_channel(row["id"])
                    if ok: st.success("Deleted!"); st.rerun()
                    else:  st.error("Products assigned — cannot delete.")

    # ══════════════════════════════════════════════════════════════
    # PRODUCTS
    # ══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.markdown("### Product Management")
        channels_df = load_channels()
        ch_names    = channels_df["name"].tolist()
        ch_ids      = channels_df["id"].tolist()

        st.markdown("#### Add New Product")
        with st.form("add_prod"):
            ap1,ap2,ap3,ap4 = st.columns(4)
            np_name  = ap1.text_input("Product Name")
            np_ch    = ap2.selectbox("Channel", range(len(ch_names)),
                                     format_func=lambda i: ch_names[i])
            np_price = ap3.number_input("Default Price", min_value=0.0,
                                        step=100.0)
            np_ratio = ap4.number_input("Cost Ratio", min_value=0.0,
                                        max_value=1.0, step=0.01, value=0.40)
            if st.form_submit_button("Add Product", type="primary"):
                if np_name.strip():
                    add_product(np_name.strip(), ch_ids[np_ch],
                                np_ratio, np_price)
                    st.success(f"'{np_name}' added!"); st.rerun()
                else:
                    st.error("Enter a product name.")

        st.divider()
        if not items_df.empty:
            st.markdown("#### Product Performance")
            pp = (items_df.groupby(["product_name","channel"])
                  .agg(revenue=("total_price","sum"),
                       qty=("qty","sum"),
                       cogs=("total_cogs","sum")).reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp["margin"]       = (
                pp["gross_profit"]/pp["revenue"]*100).round(1)
            pp = pp.sort_values("revenue", ascending=False)
            show_pp = pp.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_pp[c] = show_pp[c].map("{:,.0f}".format)
            show_pp.columns = ["Product","Channel","Revenue","Qty Sold",
                               "COGS","Gross Profit","Margin %"]
            st.dataframe(show_pp, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Edit Products")
        all_prods = load_products()
        edited = st.data_editor(
            all_prods[["id","name","channel","cost_ratio","default_price"]],
            use_container_width=True, num_rows="fixed",
            column_config={
                "id":            st.column_config.TextColumn("ID",disabled=True),
                "name":          st.column_config.TextColumn("Name"),
                "channel":       st.column_config.TextColumn("Channel",disabled=True),
                "cost_ratio":    st.column_config.NumberColumn(
                    "Cost Ratio", min_value=0.0, max_value=1.0,
                    step=0.01, format="%.2f"),
                "default_price": st.column_config.NumberColumn(
                    "Default Price", min_value=0, step=100),
            }, hide_index=True)
        sc1, sc2 = st.columns(2)
        with sc1:
            if st.button("Save Changes", type="primary",
                         use_container_width=True):
                for _, row in edited.iterrows():
                    orig = all_prods[all_prods["id"]==row["id"]]
                    if not orig.empty:
                        update_product(row["id"], row["name"],
                            int(orig.iloc[0]["channel_id"]),
                            row["cost_ratio"], row["default_price"])
                bust(); st.success("Saved!"); st.rerun()
        with sc2:
            if not all_prods.empty:
                rem = st.selectbox("Remove",
                    all_prods["id"].tolist(),
                    format_func=lambda i:
                        all_prods[all_prods["id"]==i]["name"].values[0]
                        if len(all_prods[all_prods["id"]==i])>0 else i)
                if st.button("Remove Product", use_container_width=True):
                    deactivate_product(rem)
                    st.success("Removed."); st.rerun()

    # ══════════════════════════════════════════════════════════════
    # P&L REPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[7]:
        st.markdown("### Profit & Loss Report")
        st.caption(f"Period: {from_date} to {to_date}")

        # Payment reconciliation
        if not orders_df.empty:
            st.markdown("#### Payment Method Reconciliation")
            pay_rec = (orders_df.groupby("payment_method")["total_amount"]
                       .agg(["sum","count"]).reset_index()
                       .rename(columns={"payment_method":"Method",
                                        "sum":"Total Received",
                                        "count":"# Orders"})
                       .sort_values("Total Received", ascending=False))
            pay_rec["Total Received"] = pay_rec["Total Received"].map(
                "{:,.0f}".format)
            rec_col1, rec_col2 = st.columns([1,2])
            with rec_col1:
                st.dataframe(pay_rec, use_container_width=True, hide_index=True)
            with rec_col2:
                pay_chart = (orders_df.groupby("payment_method")
                             ["total_amount"].sum().reset_index())
                st.bar_chart(
                    pay_chart.set_index("payment_method")
                    .rename(columns={"total_amount":"Amount"}),
                    height=220)
            st.divider()

        pl_col, ratio_col = st.columns(2)
        by_ec = (exp_df.groupby("category")["amount"].sum().to_dict()
                 if not exp_df.empty else {})
        ch_rev = (items_df.groupby("channel")["total_price"].sum()
                  if not items_df.empty
                  else pd.Series(dtype=float))

        with pl_col:
            st.markdown("#### Income Statement")
            rows_pl = [
                ("REVENUE", "", True),
                *[(f"  {ch}", fmt(v), False) for ch, v in ch_rev.items()],
                ("TOTAL REVENUE",      fmt(kpi["revenue"]), True),
                ("---","","---"),
                ("  Est. COGS", f"({fmt(kpi['cogs'])})", False),
                ("GROSS PROFIT",       fmt(kpi["gp"]),     True),
                ("  Gross Margin",f"{kpi['gp_mar']}%",     False),
                ("---","","---"),
                ("OPERATING EXPENSES","",                   True),
                *[(f"  {c}",f"({fmt(a)})",False)
                  for c,a in sorted(by_ec.items())],
                ("TOTAL EXPENSES",
                 f"({fmt(kpi['expenses'])})",               True),
                ("---","","---"),
                ("NET PROFIT / LOSS",  fmt(kpi["np"]),      True),
                ("  Net Margin",  f"{kpi['np_mar']}%",      False),
            ]
            for label, value, bold in rows_pl:
                if label == "---":
                    st.markdown("---"); continue
                a, b = st.columns([3,1])
                pre = "**" if bold else ""
                a.markdown(f"{pre}{label}{pre}")
                b.markdown(
                    f"<div style='text-align:right'>{pre}{value}{pre}</div>",
                    unsafe_allow_html=True)

        with ratio_col:
            st.markdown("#### Ratios & Notes")
            st.metric("Gross Margin",   f"{kpi['gp_mar']}%")
            st.metric("Net Margin",     f"{kpi['np_mar']}%")
            st.metric("Expense Ratio",
                f"{safe_pct(kpi['expenses'],kpi['revenue'])}%")
            st.metric("COGS Ratio",
                f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
            st.metric("Avg Order Value",fmt(kpi["avg_order"]))
            st.metric("Avg Daily Rev",  fmt(kpi["avg_daily"]))
            st.markdown("---")
            st.warning(
                "**Accountant Notes**\n\n"
                "- COGS estimated from cost ratios — update in Products tab.\n"
                "- Verify B2B payment receipts before closing period.\n"
                "- Reconcile payment methods against bank/Opay statements.\n"
                "- Imprest items need petty cash reconciliation.")

        st.markdown("#### Cost Structure")
        wf = pd.DataFrame({
            "Item":   ["Revenue","Gross Profit","Expenses","COGS","Net Profit"],
            "Amount": [kpi["revenue"],kpi["gp"],kpi["expenses"],
                       kpi["cogs"],kpi["np"]],
        }).set_index("Item")
        st.bar_chart(wf, height=240)

        st.divider()
        st.markdown("#### Export")
        ch_p = pd.DataFrame()
        pp_r = pd.DataFrame()
        if not items_df.empty:
            ch_p = (items_df.groupby("channel")
                    .agg(revenue=("total_price","sum"),
                         cogs=("total_cogs","sum")).reset_index())
            ch_p["gross_profit"] = ch_p["revenue"] - ch_p["cogs"]
            pp_r = (items_df.groupby(["product_name","channel"])
                    .agg(revenue=("total_price","sum"),
                         cogs=("total_cogs","sum")).reset_index())
            pp_r["gross_profit"] = pp_r["revenue"] - pp_r["cogs"]
            pp_r["margin"]       = (
                pp_r["gross_profit"]/pp_r["revenue"]*100).round(1)
            pp_r.rename(columns={"revenue":"total_price"}, inplace=True)
            pp_r = pp_r.sort_values("total_price", ascending=False)

        html_r = generate_pdf_html(
            kpi, from_date, to_date, by_ec, ch_p, pp_r)
        pdf_download_button(html_r,
            f"Kokari_Report_{from_date}_{to_date}.html")
        st.download_button("Export P&L CSV",
            data=pd.DataFrame({
                "Metric": ["Revenue","COGS","Gross Profit","Gross Margin",
                           "Expenses","Net Profit","Net Margin","Orders",
                           "Avg Order"],
                "Value":  [kpi["revenue"],kpi["cogs"],kpi["gp"],
                           f"{kpi['gp_mar']}%",kpi["expenses"],kpi["np"],
                           f"{kpi['np_mar']}%",kpi["orders"],kpi["avg_order"]],
            }).to_csv(index=False).encode(),
            file_name=f"kokari_PnL_{from_date}_{to_date}.csv",
            mime="text/csv")

    # ══════════════════════════════════════════════════════════════
    # SETTINGS
    # ══════════════════════════════════════════════════════════════
    with tabs[8]:
        st.markdown("### Settings")
        exp_cats = load_exp_cats()
        s1, s2   = st.columns(2)

        with s1:
            st.markdown("#### Expense Categories")
            with st.form("add_ec"):
                nec = st.text_input("New Category",
                    placeholder="e.g. Insurance, Equipment")
                if st.form_submit_button("Add", type="primary"):
                    if nec.strip():
                        add_exp_cat(nec.strip())
                        st.success("Added!"); st.rerun()
                    else:
                        st.error("Enter a name.")
            for cat in exp_cats:
                c1,c2,c3 = st.columns([3,2,1])
                c1.write(cat)
                ncv = c2.text_input("",key=f"rnec_{cat}",
                    label_visibility="collapsed",placeholder="Rename…")
                with c3:
                    if st.button("Rename",key=f"rcec_{cat}"):
                        if ncv.strip():
                            rename_exp_cat(cat,ncv.strip())
                            st.success("Renamed!"); st.rerun()
                    if st.button("Del",key=f"delec_{cat}"):
                        ok=delete_exp_cat(cat)
                        if ok: st.success("Deleted!"); st.rerun()
                        else:  st.error("Has transactions.")

        with s2:
            st.markdown("#### Change Password")
            with st.form("chpw"):
                cp  = st.text_input("Current Password", type="password")
                np1 = st.text_input("New Password",     type="password")
                np2 = st.text_input("Confirm",          type="password")
                if st.form_submit_button("Update Password", type="primary"):
                    if not verify_login(username, cp):
                        st.error("Current password incorrect.")
                    elif np1 != np2:
                        st.error("Passwords don't match.")
                    elif len(np1) < 6:
                        st.error("Minimum 6 characters.")
                    else:
                        change_password(username, np1)
                        st.success("Updated! Please sign in again.")
                        for k in ["logged_in","username","role"]:
                            st.session_state.pop(k, None)
                        st.rerun()
            st.markdown("---")
            st.info(
                f"**User:** {username}  \n**Role:** {role}  \n"
                f"**DB:** {DB_PATH}  \n"
                "Default login: admin / kokari2026")


if __name__ == "__main__":
    main()
