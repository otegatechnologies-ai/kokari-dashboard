"""
KOKARI CAFE FINANCIAL DASHBOARD — v2.1
========================================
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

DB_PATH         = "kokari_cafe.db"
PAYMENT_METHODS = ["Opay", "Bank Transfer", "Cash", "POS", "Other"]
ORDER_TYPES     = ["Dine-in", "Take-out", "Delivery"]
ORDER_STATUSES  = ["Confirmed", "Pending", "Cancelled"]

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

def get_period_dates(preset):
    today = date.today()
    if preset == "Today":           return today, today
    elif preset == "Yesterday":
        y = today - timedelta(days=1); return y, y
    elif preset == "This Week":     return today - timedelta(days=today.weekday()), today
    elif preset == "Last Week":
        end = today - timedelta(days=today.weekday() + 1)
        return end - timedelta(days=6), end
    elif preset == "This Month":    return today.replace(day=1), today
    elif preset == "Last Month":
        first = today.replace(day=1) - timedelta(days=1)
        return first.replace(day=1), first
    elif preset == "This Quarter":
        q_start = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=q_start, day=1), today
    elif preset == "This Year":     return today.replace(month=1, day=1), today
    elif preset == "Last 7 Days":   return today - timedelta(days=6), today
    elif preset == "Last 30 Days":  return today - timedelta(days=29), today
    elif preset == "Last 90 Days":  return today - timedelta(days=89), today
    elif preset == "All Time":      return date(2020, 1, 1), today
    else:                           return date(2026, 2, 9), today

def phone_norm(raw):
    if not raw:
        return ""
    digits = re.sub(r"[^\d+]", "", str(raw))
    if digits.startswith("0") and len(digits) == 11:
        digits = "+234" + digits[1:]
    return digits

# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'accountant'
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS expense_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_id INTEGER,
            cost_ratio REAL NOT NULL DEFAULT 0.40,
            default_price REAL NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        );
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            phone TEXT DEFAULT '',
            email TEXT DEFAULT '',
            note TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            customer_name TEXT NOT NULL DEFAULT 'Walk-in',
            customer_id INTEGER,
            order_type TEXT NOT NULL DEFAULT 'Dine-in',
            payment_method TEXT NOT NULL DEFAULT 'Cash',
            status TEXT NOT NULL DEFAULT 'Confirmed',
            total_amount REAL NOT NULL DEFAULT 0,
            note TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id TEXT,
            product_name TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'Cafe',
            category TEXT NOT NULL DEFAULT 'Cafe',
            qty REAL NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL DEFAULT 0,
            total_price REAL NOT NULL DEFAULT 0,
            unit_cogs REAL NOT NULL DEFAULT 0,
            total_cogs REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            product_id TEXT,
            name TEXT,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            cogs REAL NOT NULL DEFAULT 0,
            note TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)

        # safe column migrations for existing DBs
        migrations = [
            "ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'Confirmed'",
            "ALTER TABLE orders ADD COLUMN customer_id INTEGER",
            "ALTER TABLE order_items ADD COLUMN category TEXT NOT NULL DEFAULT 'Cafe'",
        ]
        for sql in migrations:
            try:
                cur.execute(sql)
            except Exception:
                pass

        # seeds
        if cur.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
            cur.execute("INSERT INTO users (username,password,role) VALUES (?,?,?)",
                        ("admin", hash_pw("kokari2026"), "admin"))

        if cur.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0:
            for ch in ["Cafe","B2B","Packaged","Retail","Other"]:
                cur.execute("INSERT INTO channels (name) VALUES (?)", (ch,))

        if cur.execute("SELECT COUNT(*) FROM expense_categories").fetchone()[0] == 0:
            for ec in ["Ingredients","Utilities","Staff/Wages","Packaging","Rent",
                       "Transport","Logistics","Stationery","Marketing","Maintenance","Miscellaneous"]:
                cur.execute("INSERT INTO expense_categories (name) VALUES (?)", (ec,))

        if cur.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
            ch = {r[0]:r[1] for r in cur.execute("SELECT name,id FROM channels")}
            cur.executemany("INSERT INTO products (id,name,channel_id,cost_ratio,default_price) VALUES (?,?,?,?,?)",[
                ("p01","Pancakes",            ch["Cafe"],    0.38, 4300),
                ("p02","Fruit Smoothie",       ch["Cafe"],    0.40, 4840),
                ("p03","Books",                ch["Retail"],  0.55,10975),
                ("p04","Puff Puff",            ch["Cafe"],    0.30, 3765),
                ("p05","Spicy Chicken Wrap",   ch["Cafe"],    0.42, 4600),
                ("p06","Chicken Wings",        ch["Cafe"],    0.45, 8065),
                ("p07","Tapioca",              ch["Cafe"],    0.35, 4300),
                ("p08","Coffee",               ch["Cafe"],    0.28, 4300),
                ("p09","Iced Coffee",          ch["Cafe"],    0.30, 4840),
                ("p10","Zobo",                 ch["Cafe"],    0.25, 3765),
                ("p11","Parfait & Wings Combo",ch["Cafe"],    0.45, 8600),
                ("p12","Parfait",              ch["Cafe"],    0.40, 5375),
                ("p13","Granola 500g",         ch["Packaged"],0.50, 6757),
                ("p14","Spicy Coconut Flakes", ch["Packaged"],0.48, 3765),
                ("p15","Honey Coconut Cashew", ch["Packaged"],0.50,10750),
                ("p16","CCB",                  ch["Packaged"],0.50, 9675),
                ("p17","Wholesale (B2B)",       ch["B2B"],    0.55, 1000),
                ("p18","Iced Tea",             ch["Cafe"],    0.30, 4840),
                ("p19","Water",                ch["Cafe"],    0.20,  500),
                ("p20","Space Rental",         ch["Other"],   0.05, 3500),
            ])

        if cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0:
            _seed_orders(cur)
        if cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0:
            _seed_expenses(cur)

        conn.commit()
    finally:
        conn.close()


def _seed_orders(cur):
    daily = [
        ("2026-02-09",[("p01","Pancakes","Cafe",3,5020,15060),("p08","Coffee","Cafe",2,4300,8600),
                       ("p09","Iced Coffee","Cafe",4,4554,18215),("p11","Parfait & Wings Combo","Cafe",3,8785,26355),
                       ("p12","Parfait","Cafe",4,5375,21500)]),
        ("2026-02-10",[("p01","Pancakes","Cafe",3,3765,11295),("p08","Coffee","Cafe",3,3790,11370),
                       ("p09","Iced Coffee","Cafe",1,4840,4840),("p10","Zobo","Cafe",1,3765,3765),
                       ("p11","Parfait & Wings Combo","Cafe",1,8600,8600),("p14","Spicy Coconut Flakes","Packaged",1,3765,3765)]),
        ("2026-02-11",[("p01","Pancakes","Cafe",3,3765,11295),("p04","Puff Puff","Cafe",1,3765,3765),
                       ("p08","Coffee","Cafe",3,3755,11265),("p09","Iced Coffee","Cafe",5,4840,24200),
                       ("p10","Zobo","Cafe",1,3765,3765),("p11","Parfait & Wings Combo","Cafe",1,8600,8600),
                       ("p12","Parfait","Cafe",2,5375,10750)]),
        ("2026-02-12",[("p01","Pancakes","Cafe",5,4563,22815),("p02","Fruit Smoothie","Cafe",3,4840,14520),
                       ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),("p06","Chicken Wings","Cafe",4,8315,33260),
                       ("p07","Tapioca","Cafe",1,4300,4300),("p08","Coffee","Cafe",2,4570,9140),
                       ("p09","Iced Coffee","Cafe",6,4842,29050),("p10","Zobo","Cafe",3,3765,11295),
                       ("p13","Granola 500g","Packaged",1,6757,6757)]),
        ("2026-02-13",[("p01","Pancakes","Cafe",5,3765,18825),("p02","Fruit Smoothie","Cafe",5,4840,24200),
                       ("p03","Books","Retail",2,10975,21950),("p04","Puff Puff","Cafe",1,3765,3765),
                       ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),("p06","Chicken Wings","Cafe",1,8065,8065),
                       ("p08","Coffee","Cafe",1,3765,3765),("p09","Iced Coffee","Cafe",1,4840,4840),
                       ("p12","Parfait","Cafe",1,5375,5375),("p14","Spicy Coconut Flakes","Packaged",1,3765,3765),
                       ("p17","Wholesale (B2B)","B2B",1,504513,504513)]),
        ("2026-02-14",[("p01","Pancakes","Cafe",6,4393,26355),("p02","Fruit Smoothie","Cafe",1,4840,4840),
                       ("p05","Spicy Chicken Wrap","Cafe",1,10750,10750),("p06","Chicken Wings","Cafe",1,8065,8065),
                       ("p07","Tapioca","Cafe",5,4300,21500),("p08","Coffee","Cafe",7,4225,29575),
                       ("p09","Iced Coffee","Cafe",2,4840,9680),("p10","Zobo","Cafe",3,3765,11295),
                       ("p11","Parfait & Wings Combo","Cafe",3,8600,25800)]),
    ]
    for sale_date,items in daily:
        total = sum(i[5] for i in items)
        cur.execute("INSERT INTO orders (date,customer_name,order_type,payment_method,status,total_amount,note) VALUES (?,?,?,?,?,?,?)",
                    (sale_date,"Daily Batch","Dine-in","Bank Transfer","Confirmed",total,"Seeded"))
        oid = cur.lastrowid
        for pid,pname,chan,qty,unit_p,tot_p in items:
            row = cur.execute("SELECT cost_ratio FROM products WHERE id=?",(pid,)).fetchone()
            ratio = row[0] if row else 0.40
            cur.execute("INSERT INTO order_items (order_id,product_id,product_name,channel,category,qty,unit_price,total_price,unit_cogs,total_cogs) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (oid,pid,pname,chan,chan,qty,unit_p,tot_p,round(unit_p*ratio),round(tot_p*ratio)))


def _seed_expenses(cur):
    for e in [
        ("2026-02-09","Ingredients","Sugar",9000),("2026-02-09","Ingredients","Flour",5400),
        ("2026-02-09","Ingredients","Chicken Wings",20178),("2026-02-09","Ingredients","Chicken",11000),
        ("2026-02-09","Ingredients","Bread",1200),("2026-02-09","Ingredients","Mayonnaise",4000),
        ("2026-02-09","Ingredients","Banana",2000),("2026-02-09","Ingredients","Oil",4400),
        ("2026-02-09","Ingredients","Carrot and Cabbage",3000),("2026-02-09","Ingredients","Eggs",5900),
        ("2026-02-09","Ingredients","Groundnut",2000),("2026-02-09","Ingredients","Powder Milk",44000),
        ("2026-02-09","Ingredients","Pineapple",3000),("2026-02-09","Ingredients","Ginger",2000),
        ("2026-02-09","Ingredients","Cinnamon",2000),("2026-02-09","Ingredients","Cloves",1000),
        ("2026-02-09","Ingredients","Grapes",6000),("2026-02-09","Ingredients","Honey",6000),
        ("2026-02-09","Ingredients","Liquid Milk",10200),("2026-02-09","Utilities","NEPA Electricity",10000),
        ("2026-02-09","Utilities","Data",3500),("2026-02-09","Utilities","Recharge Card",2000),
        ("2026-02-09","Utilities","Water CWay",3400),("2026-02-09","Packaging","Zobo Bottles",4400),
        ("2026-02-09","Packaging","Foil",3000),("2026-02-09","Packaging","Serviettes",2000),
        ("2026-02-09","Packaging","Spoons",3000),("2026-02-09","Packaging","Water retail",2500),
        ("2026-02-09","Transport","Transport",1500),("2026-02-09","Miscellaneous","Printing",400),
        ("2026-02-09","Miscellaneous","Bank Charges",400),("2026-02-09","Miscellaneous","Phone Repair",500),
        ("2026-02-09","Logistics","Bucket",3000),("2026-02-09","Logistics","Item Delivery x7",2000),
        ("2026-02-09","Utilities","NEPA Imprest",3000),("2026-02-09","Stationery","Battery and Book",1000),
        ("2026-02-09","Packaging","Serviettes Imprest",1000),("2026-02-14","Ingredients","Chicken Wings",36321),
        ("2026-02-14","Ingredients","Flour",6400),("2026-02-14","Ingredients","Eggs",6000),
        ("2026-02-14","Packaging","Straws",3700),("2026-02-14","Packaging","Water retail",4400),
        ("2026-02-14","Transport","Transport",500),
    ]:
        cur.execute("INSERT INTO transactions (date,type,product_id,name,category,amount,cogs,note) VALUES (?,?,NULL,?,?,?,?,'')",
                    (e[0],"expense",e[2],e[1],e[3],e[3]))


# ─────────────────────────────────────────────────────────────────
# WHATSAPP PARSER
# ─────────────────────────────────────────────────────────────────
PRODUCT_ALIASES = {
    "iced coffee":("p09","Cafe"),"ice coffee":("p09","Cafe"),
    "iced tea":("p18","Cafe"),"ice tea":("p18","Cafe"),
    "coffee":("p08","Cafe"),"zobo":("p10","Cafe"),
    "pancake":("p01","Cafe"),"pancakes":("p01","Cafe"),
    "puff puff":("p04","Cafe"),"puff-puff":("p04","Cafe"),
    "smoothie":("p02","Cafe"),"fruit smoothie":("p02","Cafe"),
    "wrap":("p05","Cafe"),"chicken wrap":("p05","Cafe"),"spicy wrap":("p05","Cafe"),
    "wings":("p06","Cafe"),"chicken wings":("p06","Cafe"),
    "tapioca":("p07","Cafe"),
    "parfait + wings":("p11","Cafe"),"parfait and wings":("p11","Cafe"),
    "parfait & wings":("p11","Cafe"),"parfait wings":("p11","Cafe"),
    "combo":("p11","Cafe"),"parfait":("p12","Cafe"),
    "granola":("p13","Packaged"),"granola 500g":("p13","Packaged"),
    "spicy coconut":("p14","Packaged"),"coconut flakes":("p14","Packaged"),
    "cashew":("p15","Packaged"),"honey cashew":("p15","Packaged"),
    "coconut cashew":("p15","Packaged"),"ccb":("p16","Packaged"),
    "water":("p19","Cafe"),"space":("p20","Other"),"space rental":("p20","Other"),
    "books":("p03","Retail"),"book":("p03","Retail"),
    "wholesale":("p17","B2B"),"b2b":("p17","B2B"),"bulk":("p17","B2B"),
}


def parse_whatsapp_sales(text, sale_date, products_df):
    """
    Supported line formats:
      ✅ Name / 08012345678 / #4300 / (1 iced coffee) / Cafe
      ✅ Name --- #4300 (1 coffee, take out)
      ✅ Name #4300 (2 zobo, 1 granola)
    """
    lines      = text.strip().split("\n")
    orders     = []
    prod_ids   = products_df["id"].tolist()
    prod_names = products_df["name"].str.lower().tolist()
    prod_prices= products_df["default_price"].tolist()
    prod_ratios= products_df["cost_ratio"].tolist()
    prod_chans = products_df["channel"].tolist()

    def best_match(item_text):
        item_lower = item_text.lower().strip()
        best_pid, best_cat, best_len = None, "Cafe", 0
        for alias,(pid,cat) in PRODUCT_ALIASES.items():
            if alias in item_lower and len(alias) > best_len:
                best_pid,best_cat,best_len = pid,cat,len(alias)
        if best_pid and best_pid in prod_ids:
            idx = prod_ids.index(best_pid)
            return {"product_id":best_pid,"product_name":products_df.iloc[idx]["name"],
                    "channel":prod_chans[idx],"category":best_cat,
                    "unit_price":prod_prices[idx],"cost_ratio":prod_ratios[idx],"confidence":"high"}
        for i,pname in enumerate(prod_names):
            if pname in item_lower or item_lower in pname:
                return {"product_id":prod_ids[i],"product_name":products_df.iloc[i]["name"],
                        "channel":prod_chans[i],"category":prod_chans[i],
                        "unit_price":prod_prices[i],"cost_ratio":prod_ratios[i],"confidence":"medium"}
        return {"product_id":None,"product_name":item_text.strip().title(),
                "channel":"Cafe","category":"Cafe","unit_price":0,"cost_ratio":0.40,"confidence":"low"}

    def parse_items(items_text):
        parts = re.split(r"[,+&]", items_text.lower())
        result = []
        for part in parts:
            part = part.strip()
            if not part: continue
            m = re.match(r"^(\d+(?:\.\d+)?)\s*(?:kg|g|pcs|pc|x)?\s*(.+)$", part)
            qty      = float(m.group(1)) if m else 1.0
            item_str = m.group(2).strip() if m else part
            result.append({**best_match(item_str), "qty": qty})
        return result

    # slash format: ✅ Name / phone / #amount / (items) / channel
    slash_pat = re.compile(
        r"[✅☑✓\*]\S*\s*([^/\n]+?)\s*/\s*([\d+\-\s]{7,15})\s*/\s*#?([\d,]+)"
        r"(?:\s*/\s*\(([^)]*)\))?(?:\s*/\s*(\w+))?", re.UNICODE)
    # standard format: ✅ Name --- #amount (items)
    order_pat  = re.compile(
        r"[✅☑✓\*]\S*\s*([^#\-\n]+?)[-–—#\s]*#?([\d,]+)(?:\s*\(([^)]+)\))?", re.UNICODE)

    for line in lines:
        line = line.strip()
        if not line: continue
        if re.search(r"\btotal\b|\btransfer\b|opay:?$|gtb|access|zenith|balance|summary", line.lower()):
            continue

        phone = ""; customer = ""; amount = 0.0; items_str = ""; chan_hint = ""

        ms = slash_pat.search(line)
        if ms:
            customer  = ms.group(1).strip().strip("-—").strip()
            phone     = phone_norm(ms.group(2))
            try: amount = float(ms.group(3).replace(",",""))
            except Exception: continue
            items_str = ms.group(4) or ""
            chan_hint  = ms.group(5) or ""
        else:
            mo = order_pat.search(line)
            if not mo: continue
            customer  = mo.group(1).strip().strip("-—").strip()
            try: amount = float(mo.group(2).replace(",",""))
            except Exception: continue
            items_str = mo.group(3) or ""
            pm = re.search(r"(?:^|[\s/|])(\+?234\d{10}|0[789]\d{9})(?:\s|/|$)", line)
            if pm: phone = phone_norm(pm.group(1))

        customer = re.sub(r"\s+", " ", customer)
        if not customer or customer.lower() in ("walk-in","walk in",""):
            customer = "Walk-in"

        pay = "Bank Transfer"
        if "cash" in line.lower():    pay = "Cash"
        elif "pos" in line.lower():   pay = "POS"
        elif "opay" in line.lower():  pay = "Opay"

        order_type = "Take-out" if any(x in line.lower() for x in
                                        ["take out","takeout","take-out","to go"]) else "Dine-in"

        items = parse_items(items_str) if items_str else []
        if chan_hint:
            for it in items: it["channel"] = chan_hint.title(); it["category"] = chan_hint.title()

        if items:
            total_d = sum(i["qty"]*i["unit_price"] for i in items if i["unit_price"]>0)
            if total_d == 0:
                share = amount / len(items)
                for it in items: it["unit_price"]=round(share/it["qty"]); it["total_price"]=round(share)
            else:
                scale = amount / total_d
                for it in items:
                    it["unit_price"]  = round(it["unit_price"]*scale) if it["unit_price"]>0 else round(amount/len(items)/it["qty"])
                    it["total_price"] = round(it["unit_price"]*it["qty"])
            for it in items:
                it["unit_cogs"]  = round(it["unit_price"]*it["cost_ratio"])
                it["total_cogs"] = round(it["unit_cogs"]*it["qty"])
        else:
            items = [{"product_id":None,"product_name":"Unknown Item","channel":"Cafe","category":"Cafe",
                      "qty":1,"unit_price":amount,"total_price":amount,"cost_ratio":0.40,
                      "unit_cogs":round(amount*0.40),"total_cogs":round(amount*0.40),"confidence":"low"}]

        orders.append({"date":str(sale_date),"customer_name":customer,"customer_phone":phone,
                       "order_type":order_type,"payment_method":pay,"status":"Confirmed",
                       "total_amount":amount,"note":"","items":items})
    return orders


# ─────────────────────────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────────────────────────
def upsert_customer(conn, name, phone=""):
    if not name or name.lower() in ("walk-in","daily batch",""): return
    existing = conn.execute("SELECT id,phone FROM customers WHERE name=?", (name,)).fetchone()
    if existing:
        if phone and not existing["phone"]:
            conn.execute("UPDATE customers SET phone=? WHERE name=?", (phone, name))
    else:
        conn.execute("INSERT INTO customers (name,phone) VALUES (?,?)", (name, phone))


def save_parsed_orders(orders):
    conn = get_conn()
    try:
        for o in orders:
            conn.execute("INSERT INTO orders (date,customer_name,order_type,payment_method,status,total_amount,note) VALUES (?,?,?,?,?,?,?)",
                         (o["date"],o["customer_name"],o["order_type"],o["payment_method"],
                          o.get("status","Confirmed"),o["total_amount"],o["note"]))
            oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for it in o["items"]:
                conn.execute("INSERT INTO order_items (order_id,product_id,product_name,channel,category,qty,unit_price,total_price,unit_cogs,total_cogs) VALUES (?,?,?,?,?,?,?,?,?,?)",
                             (oid,it.get("product_id"),it["product_name"],it.get("channel","Cafe"),
                              it.get("category","Cafe"),it["qty"],it["unit_price"],it["total_price"],
                              it["unit_cogs"],it["total_cogs"]))
            upsert_customer(conn, o["customer_name"], o.get("customer_phone",""))
        conn.commit()
    finally:
        conn.close()
    bust()


def save_single_order(order_date, customer, order_type, payment, status, total, note, items, phone=""):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO orders (date,customer_name,order_type,payment_method,status,total_amount,note) VALUES (?,?,?,?,?,?,?)",
                     (str(order_date),customer,order_type,payment,status,total,note))
        oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for it in items:
            conn.execute("INSERT INTO order_items (order_id,product_id,product_name,channel,category,qty,unit_price,total_price,unit_cogs,total_cogs) VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (oid,it.get("product_id"),it["product_name"],it.get("channel","Cafe"),
                          it.get("category","Cafe"),it["qty"],it["unit_price"],it["total_price"],
                          it["unit_cogs"],it["total_cogs"]))
        upsert_customer(conn, customer, phone)
        conn.commit()
    finally:
        conn.close()
    bust()


def update_order_status(oid, status):
    conn = get_conn()
    try:
        conn.execute("UPDATE orders SET status=? WHERE id=?", (status, oid))
        conn.commit()
    finally:
        conn.close()
    bust()


def add_expense(date_val, name, cat, amount, note=""):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO transactions (date,type,product_id,name,category,amount,cogs,note) VALUES (?,?,NULL,?,?,?,?,?)",
                     (str(date_val),"expense",name,cat,amount,amount,note))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_order(oid):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
        conn.execute("DELETE FROM orders WHERE id=?", (oid,))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_expense(tid):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM transactions WHERE id=?", (tid,))
        conn.commit()
    finally:
        conn.close()
    bust()


def add_product(name, channel_id, cost_ratio, default_price):
    pid = "u" + str(abs(hash(name + str(date.today()))))[:8]
    conn = get_conn()
    try:
        conn.execute("INSERT INTO products (id,name,channel_id,cost_ratio,default_price) VALUES (?,?,?,?,?)",
                     (pid,name,channel_id,cost_ratio,default_price))
        conn.commit()
    finally:
        conn.close()
    bust()


def update_product(pid, name, channel_id, cost_ratio, default_price):
    conn = get_conn()
    try:
        conn.execute("UPDATE products SET name=?,channel_id=?,cost_ratio=?,default_price=? WHERE id=?",
                     (name,channel_id,cost_ratio,default_price,pid))
        conn.commit()
    finally:
        conn.close()
    bust()


def deactivate_product(pid):
    conn = get_conn()
    try:
        conn.execute("UPDATE products SET active=0 WHERE id=?", (pid,))
        conn.commit()
    finally:
        conn.close()
    bust()


def add_channel(name):
    conn = get_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO channels (name) VALUES (?)", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()


def rename_channel(cid, new_name):
    conn = get_conn()
    try:
        conn.execute("UPDATE channels SET name=? WHERE id=?", (new_name, cid))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_channel(cid):
    conn = get_conn()
    try:
        if conn.execute("SELECT COUNT(*) FROM products WHERE channel_id=?", (cid,)).fetchone()[0] > 0:
            return False
        conn.execute("DELETE FROM channels WHERE id=?", (cid,))
        conn.commit()
    finally:
        conn.close()
    bust()
    return True


def add_exp_cat(name):
    conn = get_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO expense_categories (name) VALUES (?)", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()


def rename_exp_cat(old, new):
    conn = get_conn()
    try:
        conn.execute("UPDATE expense_categories SET name=? WHERE name=?", (new, old))
        conn.execute("UPDATE transactions SET category=? WHERE category=?", (new, old))
        conn.commit()
    finally:
        conn.close()
    bust()


def delete_exp_cat(name):
    conn = get_conn()
    try:
        if conn.execute("SELECT COUNT(*) FROM transactions WHERE category=? AND type='expense'", (name,)).fetchone()[0] > 0:
            return False
        conn.execute("DELETE FROM expense_categories WHERE name=?", (name,))
        conn.commit()
    finally:
        conn.close()
    bust()
    return True


def update_customer(cid, name, phone, email, note):
    conn = get_conn()
    try:
        conn.execute("UPDATE customers SET name=?,phone=?,email=?,note=? WHERE id=?",
                     (name,phone,email,note,cid))
        conn.commit()
    finally:
        conn.close()
    bust()


def verify_login(username, password):
    conn = get_conn()
    try:
        row = conn.execute("SELECT role FROM users WHERE username=? AND password=?",
                           (username, hash_pw(password))).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def change_password(username, new_password):
    conn = get_conn()
    try:
        conn.execute("UPDATE users SET password=? WHERE username=?", (hash_pw(new_password), username))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_orders(from_d, to_d):
    conn = get_conn()
    try:
        return pd.read_sql_query(
            "SELECT * FROM orders WHERE date BETWEEN ? AND ? ORDER BY date DESC, id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=10)
def load_order_items(from_d, to_d):
    conn = get_conn()
    try:
        return pd.read_sql_query(
            "SELECT oi.*, o.date, o.customer_name, o.payment_method, o.order_type, o.status "
            "FROM order_items oi JOIN orders o ON oi.order_id=o.id "
            "WHERE o.date BETWEEN ? AND ? ORDER BY o.date DESC, o.id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=10)
def load_expenses(from_d, to_d):
    conn = get_conn()
    try:
        return pd.read_sql_query(
            "SELECT * FROM transactions WHERE type='expense' AND date BETWEEN ? AND ? ORDER BY date DESC, id DESC",
            conn, params=(str(from_d), str(to_d)))
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_products():
    conn = get_conn()
    try:
        return pd.read_sql_query(
            "SELECT p.id, p.name, COALESCE(c.name,'Unassigned') AS channel, "
            "p.channel_id, p.cost_ratio, p.default_price, p.active "
            "FROM products p LEFT JOIN channels c ON p.channel_id=c.id "
            "WHERE p.active=1 ORDER BY c.name, p.name", conn)
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_channels():
    conn = get_conn()
    try:
        return pd.read_sql_query("SELECT * FROM channels ORDER BY name", conn)
    finally:
        conn.close()

@st.cache_data(ttl=30)
def load_exp_cats():
    conn = get_conn()
    try:
        return [r[0] for r in conn.execute("SELECT name FROM expense_categories ORDER BY name")]
    finally:
        conn.close()

@st.cache_data(ttl=15)
def load_customers():
    conn = get_conn()
    try:
        return pd.read_sql_query("SELECT * FROM customers ORDER BY name", conn)
    finally:
        conn.close()

@st.cache_data(ttl=10)
def load_all_orders():
    conn = get_conn()
    try:
        return pd.read_sql_query("SELECT * FROM orders ORDER BY date DESC, id DESC", conn)
    finally:
        conn.close()

def bust():
    for fn in [load_orders,load_order_items,load_expenses,
               load_products,load_channels,load_exp_cats,
               load_customers,load_all_orders]:
        fn.clear()


def compute_kpis(orders_df, items_df, exp_df):
    revenue  = float(orders_df["total_amount"].sum()) if not orders_df.empty else 0
    cogs     = float(items_df["total_cogs"].sum())    if not items_df.empty  else 0
    expenses = float(exp_df["amount"].sum())          if not exp_df.empty    else 0
    gp = revenue - cogs; np_ = gp - expenses
    days = int(orders_df["date"].nunique()) if not orders_df.empty else 0
    return dict(revenue=revenue,cogs=cogs,expenses=expenses,gp=gp,np=np_,
                gp_mar=safe_pct(gp,revenue),np_mar=safe_pct(np_,revenue),
                days=days,avg_daily=revenue/days if days else 0,
                orders=len(orders_df),
                avg_order=revenue/len(orders_df) if len(orders_df)>0 else 0)


# ─────────────────────────────────────────────────────────────────
# PDF REPORT
# ─────────────────────────────────────────────────────────────────
def generate_pdf_html(kpi, from_date, to_date, by_ec, ch_data, prod_data, period_label=""):
    rows_pl = ""
    pl_items = [("Revenue",kpi["revenue"],False),("  Cost of Goods",-kpi["cogs"],False),
                ("Gross Profit",kpi["gp"],True),("",None,False)]
    for cat,amt in sorted(by_ec.items()):
        pl_items.append((f"  {cat}",-amt,False))
    pl_items += [("Total Expenses",-kpi["expenses"],True),("",None,False),("NET PROFIT / LOSS",kpi["np"],True)]
    for label,val,bold in pl_items:
        if val is None: rows_pl += "<tr><td colspan='2'><hr></td></tr>"; continue
        s = "font-weight:bold;" if bold else ""
        c = ("color:#16a34a;" if val>=0 else "color:#dc2626;") if bold else ""
        d = f"({fmt(abs(val))})" if val<0 else fmt(val)
        rows_pl += f"<tr><td style='{s}'>{label}</td><td style='text-align:right;{s}{c}'>{d}</td></tr>"
    rows_ch = "".join(
        f"<tr><td>{r['channel']}</td><td style='text-align:right'>{fmt(r['revenue'])}</td>"
        f"<td style='text-align:right'>{fmt(r['gross_profit'])}</td>"
        f"<td style='text-align:right'>{safe_pct(r['revenue'],kpi['revenue'])}%</td></tr>"
        for _,r in ch_data.iterrows()) if not ch_data.empty else ""
    rows_prod = "".join(
        f"<tr><td>{r['product_name']}</td><td>{r.get('channel','')}</td>"
        f"<td style='text-align:right'>{fmt(r['total_price'])}</td>"
        f"<td style='text-align:right'>{r.get('margin',0)}%</td></tr>"
        for _,r in prod_data.head(10).iterrows()) if not prod_data.empty else ""
    period_str = period_label if period_label else f"{from_date} to {to_date}"
    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'><title>Kokari Cafe Report</title>
<style>body{{font-family:Arial,sans-serif;font-size:13px;color:#111;margin:40px}}
h1{{font-size:22px;color:#2563eb}}h2{{font-size:15px;color:#374151;border-bottom:2px solid #2563eb;padding-bottom:4px;margin-top:30px}}
.sub{{color:#6b7280;font-size:12px;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;margin-top:10px}}
th{{background:#2563eb;color:white;padding:7px 10px;text-align:left;font-size:12px}}
td{{padding:6px 10px;border-bottom:1px solid #f0f0f0}}tr:nth-child(even) td{{background:#f9fafb}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}}
.kpi{{background:#f3f4f6;border-radius:8px;padding:12px;text-align:center}}
.kpi-val{{font-size:18px;font-weight:bold;color:#2563eb}}.kpi-lbl{{font-size:11px;color:#6b7280;margin-top:2px}}
@media print{{button{{display:none}}}}</style></head><body>
<h1>☕ Kokari Cafe Financial Report</h1>
<div class='sub'>Period: <strong>{period_str}</strong> ({from_date} → {to_date}) | Generated: {date.today()} | {kpi['orders']} orders over {kpi['days']} day(s)</div>
<div class='kpi-grid'>
<div class='kpi'><div class='kpi-val'>{fmt(kpi['revenue'])}</div><div class='kpi-lbl'>Revenue</div></div>
<div class='kpi'><div class='kpi-val' style='color:#16a34a'>{fmt(kpi['gp'])}</div><div class='kpi-lbl'>Gross Profit ({kpi['gp_mar']}%)</div></div>
<div class='kpi'><div class='kpi-val' style='color:{"#16a34a" if kpi["np"]>=0 else "#dc2626"}'>{fmt(kpi['np'])}</div><div class='kpi-lbl'>Net Profit ({kpi['np_mar']}%)</div></div>
<div class='kpi'><div class='kpi-val' style='color:#ea580c'>{fmt(kpi['expenses'])}</div><div class='kpi-lbl'>Expenses</div></div></div>
<h2>Profit &amp; Loss</h2><table><tr><th>Item</th><th style='text-align:right'>Amount</th></tr>{rows_pl}</table>
<h2>Revenue by Channel</h2><table><tr><th>Channel</th><th>Revenue</th><th>Gross Profit</th><th>Share</th></tr>{rows_ch}</table>
<h2>Top 10 Products</h2><table><tr><th>Product</th><th>Channel</th><th>Revenue</th><th>Margin</th></tr>{rows_prod}</table>
<div style='margin-top:40px;font-size:11px;color:#9ca3af;border-top:1px solid #e5e7eb;padding-top:10px'>Kokari Cafe v2.1</div>
<br><button onclick='window.print()' style='background:#2563eb;color:white;border:none;padding:10px 24px;border-radius:6px;font-size:14px;cursor:pointer'>🖨️ Print / Save as PDF</button>
</body></html>"""


def pdf_download_button(html, filename):
    b64 = base64.b64encode(html.encode()).decode()
    st.markdown(
        f'<a href="data:text/html;base64,{b64}" download="{filename}" '
        f'style="display:inline-block;background:#2563eb;color:white;padding:8px 20px;'
        f'border-radius:6px;text-decoration:none;font-size:13px;font-weight:600;">'
        f'📥 Download Report</a>', unsafe_allow_html=True)
    st.caption("Open in browser → Ctrl+P → Save as PDF")


# ─────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────
def login_screen():
    st.markdown("""<div style='max-width:380px;margin:80px auto;text-align:center'>
    <h1 style='color:#2563eb'>☕ Kokari Cafe</h1>
    <p style='color:#6b7280'>Financial Dashboard v2.1</p></div>""", unsafe_allow_html=True)
    col = st.columns([1,2,1])[1]
    with col:
        with st.form("login"):
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            if st.form_submit_button("Sign In", use_container_width=True, type="primary"):
                role = verify_login(u, p)
                if role:
                    st.session_state.update(logged_in=True, username=u, role=role)
                    st.rerun()
                else:
                    st.error("Incorrect username or password.")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    init_db()
    if not st.session_state.get("logged_in"):
        login_screen(); st.stop()

    username = st.session_state["username"]
    role     = st.session_state["role"]

    # ── SIDEBAR ──────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## ☕ Kokari Cafe")
        st.caption(f"**{username}** · {role}")
        if st.button("Sign Out", use_container_width=True):
            for k in ["logged_in","username","role","parsed_orders","manual_items","order_header"]:
                st.session_state.pop(k, None)
            st.rerun()
        st.divider()

        st.markdown("### 📅 Report Period")
        PRESETS = ["Today","Yesterday","This Week","Last Week","This Month","Last Month",
                   "This Quarter","This Year","Last 7 Days","Last 30 Days","Last 90 Days","All Time","Custom"]
        preset = st.selectbox("Period", PRESETS, index=6)
        if preset == "Custom":
            c1,c2 = st.columns(2)
            from_date = c1.date_input("From", value=date(2026,2,9))
            to_date   = c2.date_input("To",   value=date.today())
        else:
            from_date, to_date = get_period_dates(preset)
            st.caption(f"📆 {from_date} → {to_date}")
        period_label = preset

        st.divider()
        st.markdown("### ⚡ Quick Expense")
        exp_cats_sidebar = load_exp_cats()
        with st.form("qexp", clear_on_submit=True):
            qe_cat  = st.selectbox("Category", exp_cats_sidebar if exp_cats_sidebar else ["Miscellaneous"])
            qe_name = st.text_input("Description")
            qe_amt  = st.number_input("Amount (₦)", min_value=0, step=100)
            qe_date = st.date_input("Date", value=date.today())
            qe_note = st.text_input("Note (optional)")
            if st.form_submit_button("Save Expense", use_container_width=True, type="primary"):
                if qe_amt > 0 and qe_name.strip():
                    add_expense(qe_date, qe_name.strip(), qe_cat, qe_amt, qe_note)
                    st.success("✅ Saved!")
                else:
                    st.error("Fill description & amount.")

    # ── LOAD DATA ─────────────────────────────────────────────────
    orders_df = load_orders(from_date, to_date)
    items_df  = load_order_items(from_date, to_date)
    exp_df    = load_expenses(from_date, to_date)
    kpi       = compute_kpis(orders_df, items_df, exp_df)
    prods_df  = load_products()

    st.title("☕ Kokari Cafe · Financial Dashboard")
    st.caption(f"**{period_label}** · {from_date} → {to_date} · {kpi['days']} day(s) · "
               f"{kpi['orders']} orders · Avg: {fmt(kpi['avg_order'])}")

    tabs = st.tabs(["📊 Dashboard","💬 WhatsApp Import","✍️ Manual Entry",
                    "📦 Orders","👤 Customers","💸 Expenses",
                    "🏪 Channels","🛒 Products","📋 P&L Report","⚙️ Settings"])

    # ══════════════════════════════════════════════════════════════
    # TAB 0 — DASHBOARD
    # ══════════════════════════════════════════════════════════════
    with tabs[0]:
        r1 = st.columns(4)
        r1[0].metric("💰 Revenue",      fmt(kpi["revenue"]),  f"{kpi['days']} days")
        r1[1].metric("📈 Gross Profit", fmt(kpi["gp"]),       f"Margin {kpi['gp_mar']}%")
        r1[2].metric("✅ Net Profit",   fmt(kpi["np"]),        f"Margin {kpi['np_mar']}%")
        r1[3].metric("💸 Expenses",     fmt(kpi["expenses"]),  f"COGS {fmt(kpi['cogs'])}")
        r2 = st.columns(4)
        r2[0].metric("🧾 Orders",       kpi["orders"])
        r2[1].metric("🧾 Avg Order",    fmt(kpi["avg_order"]))
        r2[2].metric("📅 Avg Daily",    fmt(kpi["avg_daily"]))
        r2[3].metric("⚖️ COGS Ratio",   f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
        st.divider()

        if not orders_df.empty:
            daily = orders_df.groupby("date")["total_amount"].sum().reset_index(name="revenue")
            daily_c = (items_df.groupby("date")["total_cogs"].sum().reset_index(name="cogs")
                       if not items_df.empty else pd.DataFrame(columns=["date","cogs"]))
            daily_e = (exp_df.groupby("date")["amount"].sum().reset_index(name="expenses")
                       if not exp_df.empty else pd.DataFrame(columns=["date","expenses"]))
            daily = (daily.merge(daily_c,on="date",how="left")
                     .merge(daily_e,on="date",how="left").fillna(0).sort_values("date"))
            daily["gross_profit"] = daily["revenue"] - daily["cogs"]
            daily["net_profit"]   = daily["gross_profit"] - daily["expenses"]

            ca,cb = st.columns(2)
            with ca:
                st.markdown("#### Daily Revenue vs Profit")
                dc = daily.set_index("date")[["revenue","gross_profit","net_profit","expenses"]]
                dc.columns = ["Revenue","Gross Profit","Net Profit","Expenses"]
                st.bar_chart(dc, height=280)
            with cb:
                if not items_df.empty:
                    st.markdown("#### Revenue by Channel")
                    ch_rev = items_df.groupby("channel")["total_price"].sum().reset_index()
                    ch_rev.columns = ["Channel","Revenue"]
                    st.bar_chart(ch_rev.set_index("Channel"), height=280)

            cc,cd = st.columns(2)
            with cc:
                if not items_df.empty:
                    st.markdown("#### Top Products")
                    pp = (items_df.groupby("product_name")["total_price"].sum()
                          .reset_index().sort_values("total_price",ascending=True).tail(10))
                    st.bar_chart(pp.set_index("product_name").rename(columns={"total_price":"Revenue"}), height=280)
            with cd:
                st.markdown("#### Revenue Trend")
                dl = daily.set_index("date")[["revenue","net_profit"]]
                dl.columns = ["Revenue","Net Profit"]
                st.line_chart(dl, height=280)

            st.divider()
            bd  = daily.loc[daily["revenue"].idxmax()]
            bpd = daily.loc[daily["net_profit"].idxmax()]
            no_batch = orders_df[orders_df["customer_name"] != "Daily Batch"]
            top_c = (no_batch.groupby("customer_name")["total_amount"].sum().idxmax()
                     if not no_batch.empty else "—")
            ic = st.columns(4)
            ic[0].success(f"**🏆 Best Day**\n\n{bd['date']}\n\n{fmt(bd['revenue'])}")
            ic[1].success(f"**💚 Best Profit Day**\n\n{bpd['date']}\n\n{fmt(bpd['net_profit'])}")
            ic[2].info(   f"**⭐ Top Customer**\n\n{top_c}")
            ic[3].warning(f"**📦 Orders**\n\n{kpi['orders']}")
        else:
            st.info("No sales data for this period.")

    # ══════════════════════════════════════════════════════════════
    # TAB 1 — WHATSAPP IMPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("### 💬 WhatsApp Sales Import")
        st.info(
            "**Supported formats (any mix in one message):**\n\n"
            "- `✅ Name / 08012345678 / #4300 / (1 iced coffee) / Cafe`\n"
            "- `✅ Name --- #4300 (1 coffee, take out)`\n"
            "- `✅ Name #4300 (2 zobo, 1 granola)`\n\n"
            "Phone numbers are auto-extracted and saved to the customer database.")

        col1,col2 = st.columns(2)
        wa_date        = col1.date_input("Sales Date", value=date.today())
        default_payment = col2.selectbox("Default Payment Method", PAYMENT_METHODS)

        wa_text = st.text_area("Paste WhatsApp message here", height=200,
            placeholder=(
                "February 25th, 2026 Sales\n"
                "✅ Janet Johnson / 08012345678 / #9680 / (2 iced coffee) / Cafe\n"
                "✅ Deborah / 08098765432 / #10522 / (1 granola 500g, 2 zobo)\n"
                "✅ Walk-in --- #4300 (1 coffee, take out)"))

        if st.button("🔍 Parse Sales Report", type="primary", use_container_width=True):
            if wa_text.strip():
                parsed = parse_whatsapp_sales(wa_text, wa_date, prods_df)
                if parsed:
                    for o in parsed:
                        if o["payment_method"] == "Bank Transfer":
                            o["payment_method"] = default_payment
                    st.session_state["parsed_orders"] = parsed
                    # store feedback msg to render after button
                    st.session_state["wa_msg"] = ("success", f"✅ Parsed **{len(parsed)} orders**. Review and save below.")
                else:
                    st.session_state["wa_msg"] = ("warning", "No orders found. Lines need ✅ marker and #amount.")
            else:
                st.session_state["wa_msg"] = ("error", "Paste the WhatsApp message first.")

        # Render feedback outside button callback to avoid session_state conflict
        if "wa_msg" in st.session_state:
            lvl, msg = st.session_state.pop("wa_msg")
            if lvl=="success": st.success(msg)
            elif lvl=="warning": st.warning(msg)
            else: st.error(msg)

        if "parsed_orders" in st.session_state:
            parsed = st.session_state["parsed_orders"]
            st.divider()
            st.markdown(f"#### 📋 Review {len(parsed)} Parsed Orders")
            total_parsed = sum(o["total_amount"] for o in parsed)
            st.metric("Total Parsed", fmt(total_parsed))

            # Channel/category preview
            all_items_flat = [it for o in parsed for it in o["items"]]
            if all_items_flat:
                cat_sum = {}
                for it in all_items_flat:
                    c = it.get("category","Cafe"); cat_sum[c] = cat_sum.get(c,0)+it.get("total_price",0)
                cat_cols = st.columns(max(len(cat_sum),1))
                for i,(cat,val) in enumerate(cat_sum.items()):
                    cat_cols[i].metric(f"📁 {cat}", fmt(val))

            prod_options = ["— unmatched —"] + prods_df["name"].tolist()
            for oi, order in enumerate(parsed):
                low_conf = any(i.get("confidence","high")=="low" for i in order["items"])
                icon = "⚠️" if low_conf else "✅"
                phone_d = f" · 📱{order.get('customer_phone','')}" if order.get("customer_phone") else ""
                with st.expander(
                    f"{icon}  {order['customer_name']}{phone_d}  ·  "
                    f"{fmt(order['total_amount'])}  ·  {order['payment_method']}  ·  {order['order_type']}",
                    expanded=low_conf):

                    oc1,oc2,oc3,oc4 = st.columns(4)
                    order["customer_name"]  = oc1.text_input("Customer", value=order["customer_name"], key=f"cust_{oi}")
                    order["customer_phone"] = oc2.text_input("Phone",    value=order.get("customer_phone",""), key=f"ph_{oi}")
                    order["payment_method"] = oc3.selectbox("Payment", PAYMENT_METHODS,
                        index=PAYMENT_METHODS.index(order["payment_method"]) if order["payment_method"] in PAYMENT_METHODS else 0,
                        key=f"pay_{oi}")
                    order["order_type"] = oc4.selectbox("Type", ORDER_TYPES,
                        index=ORDER_TYPES.index(order["order_type"]) if order["order_type"] in ORDER_TYPES else 0,
                        key=f"otype_{oi}")

                    st.markdown("**Line Items:**")
                    for ii,item in enumerate(order["items"]):
                        ic1,ic2,ic3,ic4,ic5 = st.columns([3,1,1,1,1])
                        cur_idx = prod_options.index(item["product_name"]) if item["product_name"] in prod_options else 0
                        sel = ic1.selectbox("Product", prod_options, index=cur_idx,
                                            key=f"prod_{oi}_{ii}", label_visibility="collapsed")
                        if sel != "— unmatched —":
                            row = prods_df[prods_df["name"]==sel]
                            if not row.empty:
                                item.update(product_name=sel, product_id=row.iloc[0]["id"],
                                            channel=row.iloc[0]["channel"], category=row.iloc[0]["channel"],
                                            cost_ratio=row.iloc[0]["cost_ratio"])
                        ic2.markdown(f"<small>📁{item.get('channel','Cafe')}</small>", unsafe_allow_html=True)
                        item["qty"]        = ic3.number_input("Qty",   min_value=0.0, step=0.5,   value=float(item["qty"]),       key=f"qty_{oi}_{ii}", label_visibility="collapsed")
                        item["unit_price"] = ic4.number_input("Price", min_value=0.0, step=100.0, value=float(item["unit_price"]), key=f"up_{oi}_{ii}",  label_visibility="collapsed")
                        item["total_price"] = round(item["qty"]*item["unit_price"])
                        item["unit_cogs"]   = round(item["unit_price"]*item["cost_ratio"])
                        item["total_cogs"]  = round(item["unit_cogs"]*item["qty"])
                        conf_icon = {"high":"🟢","medium":"🟡","low":"🔴"}.get(item.get("confidence","high"),"🟡")
                        ic5.markdown(f"<div style='text-align:center;padding-top:8px'>{conf_icon} {fmt(item['total_price'])}</div>", unsafe_allow_html=True)

                    derived = sum(i["total_price"] for i in order["items"])
                    diff = order["total_amount"] - derived
                    if abs(diff) > 1:
                        st.warning(f"Reported: {fmt(order['total_amount'])} | Items: {fmt(derived)} | Diff: {fmt(abs(diff))}")
                    else:
                        st.success(f"✅ Balanced: {fmt(order['total_amount'])}")

            st.divider()
            sc1,sc2 = st.columns(2)
            with sc1:
                if st.button("💾 Save All Orders", type="primary", use_container_width=True):
                    save_parsed_orders(st.session_state["parsed_orders"])
                    st.session_state.pop("parsed_orders",None)
                    st.success("✅ All orders saved!"); st.rerun()
            with sc2:
                if st.button("🗑️ Discard", use_container_width=True):
                    st.session_state.pop("parsed_orders",None); st.rerun()

    # ══════════════════════════════════════════════════════════════
    # TAB 2 — MANUAL ENTRY  (bugs fixed)
    # ══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("### ✍️ Manual Order Entry")
        st.caption("Build a detailed order step-by-step with full product review before saving.")

        if "manual_items" not in st.session_state:
            st.session_state["manual_items"] = []

        # Step 1 — header
        st.markdown("#### 1️⃣ Order Details")
        with st.form("order_header"):
            h1,h2 = st.columns(2)
            m_date  = h1.date_input("Order Date", value=date.today())
            m_cust  = h2.text_input("Customer Name", value="Walk-in")
            h3,h4,h5 = st.columns(3)
            m_otype  = h3.selectbox("Order Type",     ORDER_TYPES)
            m_pay    = h4.selectbox("Payment Method", PAYMENT_METHODS)
            m_status = h5.selectbox("Status",         ORDER_STATUSES)
            m_phone  = st.text_input("Customer Phone (optional)", placeholder="08012345678")
            m_note   = st.text_input("Order Note (optional)")
            hdr_btn  = st.form_submit_button("✅ Set Order Header", type="primary", use_container_width=True)

        if hdr_btn:
            st.session_state["order_header"] = {
                "date":m_date,"customer":m_cust,"order_type":m_otype,
                "payment":m_pay,"status":m_status,
                "phone":phone_norm(m_phone),"note":m_note}
            st.success("Header saved — now add products below.")

        # Step 2 — add items
        if "order_header" in st.session_state:
            hdr = st.session_state["order_header"]
            phone_d = f" · 📱{hdr['phone']}" if hdr.get("phone") else ""
            st.markdown(f"**📋 Order:** `{hdr['customer']}`{phone_d} · `{hdr['date']}` · "
                        f"`{hdr['order_type']}` · `{hdr['payment']}` · `{hdr['status']}`")

            st.markdown("#### 2️⃣ Add Products")

            # Build display labels BEFORE the form (avoids closure/lambda bug with range())
            prod_names  = prods_df["name"].tolist()
            prod_ids    = prods_df["id"].tolist()
            prod_chans  = prods_df["channel"].tolist()
            prod_ratios = prods_df["cost_ratio"].tolist()
            prod_prices = prods_df["default_price"].tolist()
            # Use plain string labels — no lambda needed, no range() selectbox
            prod_labels = [f"{prod_names[i]}  [{prod_chans[i]}]  ·  {fmt(prod_prices[i])}"
                           for i in range(len(prod_names))]

            with st.form("add_item", clear_on_submit=True):
                ai1,ai2,ai3 = st.columns([3,1,1])
                sel_label  = ai1.selectbox("Product", prod_labels if prod_labels else ["No products"])
                sel_i      = prod_labels.index(sel_label) if sel_label in prod_labels else 0
                ai_qty     = ai2.number_input("Qty",            min_value=0.0, step=0.5,   value=1.0)
                default_p  = float(prod_prices[sel_i]) if prod_prices else 0.0
                ai_price   = ai3.number_input("Unit Price (₦)", min_value=0.0, step=100.0, value=default_p)
                st.caption(f"Channel: **{prod_chans[sel_i] if prod_chans else '—'}** · "
                           f"Cost ratio: **{prod_ratios[sel_i]:.0%} if prod_ratios else '—'** · "
                           f"Default: **{fmt(prod_prices[sel_i]) if prod_prices else '₦0'}**")
                add_btn = st.form_submit_button("➕ Add to Order", type="primary", use_container_width=True)

            if add_btn and ai_qty > 0 and prod_names:
                ratio = prod_ratios[sel_i]
                st.session_state["manual_items"].append({
                    "product_id":   prod_ids[sel_i],
                    "product_name": prod_names[sel_i],
                    "channel":      prod_chans[sel_i],
                    "category":     prod_chans[sel_i],
                    "qty":          ai_qty,
                    "unit_price":   ai_price,
                    "total_price":  round(ai_qty*ai_price),
                    "cost_ratio":   ratio,
                    "unit_cogs":    round(ai_price*ratio),
                    "total_cogs":   round(ai_qty*ai_price*ratio),
                })

            # Step 3 — review & save
            items = st.session_state["manual_items"]
            if items:
                st.markdown("#### 3️⃣ Order Review")
                review_rows = [{
                    "#":idx+1,"Product":it["product_name"],"Channel":it["channel"],
                    "Qty":it["qty"],"Unit Price":f"₦{it['unit_price']:,.0f}",
                    "Total":f"₦{it['total_price']:,.0f}","Est. COGS":f"₦{it['total_cogs']:,.0f}",
                    "Margin":f"{safe_pct(it['total_price']-it['total_cogs'],it['total_price'])}%",
                } for idx,it in enumerate(items)]
                st.dataframe(pd.DataFrame(review_rows), use_container_width=True, hide_index=True)

                total   = sum(i["total_price"] for i in items)
                tot_cog = sum(i["total_cogs"]  for i in items)
                gp_val  = total - tot_cog
                mc1,mc2,mc3,mc4 = st.columns(4)
                mc1.metric("Order Total",  fmt(total))
                mc2.metric("Est. COGS",    fmt(tot_cog))
                mc3.metric("Gross Profit", fmt(gp_val))
                mc4.metric("GP Margin",    f"{safe_pct(gp_val,total)}%")

                # Remove item selector
                rem_opts = [f"{i+1}. {items[i]['product_name']} x{items[i]['qty']}" for i in range(len(items))]
                rm1,rm2 = st.columns([3,1])
                rem_sel = rm1.selectbox("Remove item", rem_opts, label_visibility="collapsed")
                if rm2.button("Remove ❌"):
                    st.session_state["manual_items"].pop(rem_opts.index(rem_sel)); st.rerun()

                rc1,rc2,rc3 = st.columns(3)
                with rc1:
                    if st.button("💾 Save Order", type="primary", use_container_width=True):
                        save_single_order(
                            hdr["date"],hdr["customer"],hdr["order_type"],
                            hdr["payment"],hdr["status"],total,hdr["note"],
                            st.session_state["manual_items"],hdr.get("phone",""))
                        st.session_state.pop("order_header",None)
                        st.session_state["manual_items"] = []
                        st.success("✅ Order saved!"); st.rerun()
                with rc2:
                    if st.button("🗑️ Clear Items", use_container_width=True):
                        st.session_state["manual_items"] = []; st.rerun()
                with rc3:
                    if st.button("❌ Cancel Order", use_container_width=True):
                        st.session_state.pop("order_header",None)
                        st.session_state["manual_items"] = []; st.rerun()
            else:
                st.info("No items yet. Select a product above and click ➕ Add to Order.")

    # ══════════════════════════════════════════════════════════════
    # TAB 3 — ORDERS
    # ══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("### 📦 Orders Ledger")
        if not orders_df.empty:
            fc1,fc2,fc3,fc4 = st.columns([3,2,2,2])
            sq = fc1.text_input("🔍 Search", placeholder="Customer name…", label_visibility="collapsed")
            tf = fc2.selectbox("Type",    ["All Types"]+ORDER_TYPES,    label_visibility="collapsed")
            sf = fc3.selectbox("Status",  ["All Statuses"]+ORDER_STATUSES, label_visibility="collapsed")
            pf = fc4.selectbox("Payment", ["All"]+PAYMENT_METHODS,     label_visibility="collapsed")

            view = orders_df.copy()
            if sq: view = view[view["customer_name"].str.contains(sq,case=False,na=False)]
            if tf != "All Types": view = view[view["order_type"]==tf]
            if sf != "All Statuses" and "status" in view.columns:
                view = view[view["status"]==sf]
            if pf != "All": view = view[view["payment_method"]==pf]

            sm1,sm2,sm3 = st.columns(3)
            sm1.metric("Showing", len(view))
            sm2.metric("Revenue",  fmt(view["total_amount"].sum()))
            sm3.metric("Avg Order",fmt(view["total_amount"].mean() if not view.empty else 0))

            disp_cols = ["id","date","customer_name","order_type","payment_method","total_amount","note"]
            if "status" in view.columns:
                disp_cols = ["id","date","customer_name","order_type","payment_method","status","total_amount","note"]
            st.dataframe(
                view[disp_cols].rename(columns={"id":"ID","date":"Date","customer_name":"Customer",
                    "order_type":"Type","payment_method":"Payment","status":"Status",
                    "total_amount":"Amount","note":"Note"}).style.format({"Amount":"{:,.0f}"}),
                use_container_width=True, height=300, hide_index=True)

            st.divider()
            st.markdown("#### 🔍 Order Detail & Actions")
            oa1,oa2 = st.columns([2,3])
            with oa1:
                default_id = int(view["id"].iloc[0]) if not view.empty else 1
                sel_oid = st.number_input("Order ID", min_value=1, step=1, value=default_id)
                order_row = orders_df[orders_df["id"]==sel_oid]
                if not order_row.empty:
                    ord_r = order_row.iloc[0]
                    st.markdown(f"""
**Customer:** {ord_r['customer_name']}  
**Date:** {ord_r['date']}  
**Type:** {ord_r['order_type']}  
**Payment:** {ord_r['payment_method']}  
**Status:** {ord_r.get('status','Confirmed')}  
**Total:** {fmt(ord_r['total_amount'])}  
                    """)
                    if "status" in orders_df.columns:
                        cur_stat = ord_r.get("status","Confirmed")
                        new_stat = st.selectbox("Update Status", ORDER_STATUSES,
                            index=ORDER_STATUSES.index(cur_stat) if cur_stat in ORDER_STATUSES else 0)
                        if st.button("Update Status"):
                            update_order_status(int(sel_oid), new_stat)
                            st.success(f"Status → {new_stat}"); st.rerun()
                    if st.button("🗑️ Delete Order"):
                        delete_order(int(sel_oid))
                        st.success(f"Order #{sel_oid} deleted."); st.rerun()
            with oa2:
                items_sel = items_df[items_df["order_id"]==sel_oid]
                if not items_sel.empty:
                    show_c = ["product_name","channel","qty","unit_price","total_price","unit_cogs","total_cogs"]
                    if "category" in items_sel.columns:
                        show_c = ["product_name","channel","category","qty","unit_price","total_price","unit_cogs","total_cogs"]
                    st.dataframe(
                        items_sel[show_c].rename(columns={
                            "product_name":"Product","channel":"Channel","category":"Category",
                            "qty":"Qty","unit_price":"Unit Price","total_price":"Total",
                            "unit_cogs":"Unit COGS","total_cogs":"Total COGS"})
                        .style.format({"Unit Price":"{:,.0f}","Total":"{:,.0f}","Unit COGS":"{:,.0f}","Total COGS":"{:,.0f}"}),
                        use_container_width=True, hide_index=True)
                else:
                    st.info("No items for this order.")

            st.divider()
            st.download_button("📥 Export Orders CSV",
                data=orders_df.to_csv(index=False).encode(),
                file_name=f"kokari_orders_{from_date}_{to_date}.csv", mime="text/csv")
        else:
            st.info("No orders in this date range.")

    # ══════════════════════════════════════════════════════════════
    # TAB 4 — CUSTOMERS
    # ══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("### 👤 Customer Management")
        all_orders_df = load_all_orders()
        customers_df  = load_customers()

        if not all_orders_df.empty:
            no_batch = all_orders_df[all_orders_df["customer_name"] != "Daily Batch"]
            cust_stats = (no_batch.groupby("customer_name")
                          .agg(total_spend=("total_amount","sum"),visits=("id","count"),
                               first_visit=("date","min"),last_visit=("date","max"))
                          .reset_index().sort_values("total_spend",ascending=False))
            if not customers_df.empty:
                cust_stats = cust_stats.merge(
                    customers_df[["name","phone","email"]].rename(columns={"name":"customer_name"}),
                    on="customer_name",how="left")
            else:
                cust_stats["phone"] = ""; cust_stats["email"] = ""

            ck1,ck2,ck3,ck4 = st.columns(4)
            ck1.metric("Total Customers",   len(cust_stats))
            ck2.metric("Repeat (2+ visits)",len(cust_stats[cust_stats["visits"]>=2]))
            ck3.metric("Avg Customer Value",fmt(cust_stats["total_spend"].mean()))
            ck4.metric("Top Spend",         fmt(cust_stats["total_spend"].max()))

            st.divider()
            cq1,cq2 = st.columns([3,1])
            cust_search = cq1.text_input("🔍 Search", placeholder="Name or phone…", label_visibility="collapsed")
            cust_filter = cq2.selectbox("Filter",["All","Repeat (2+)","VIP (5+)"], label_visibility="collapsed")

            view_cust = cust_stats.copy()
            if cust_search:
                mask = (view_cust["customer_name"].str.contains(cust_search,case=False,na=False) |
                        view_cust["phone"].fillna("").str.contains(cust_search,na=False))
                view_cust = view_cust[mask]
            if cust_filter == "Repeat (2+)": view_cust = view_cust[view_cust["visits"]>=2]
            elif cust_filter == "VIP (5+)":  view_cust = view_cust[view_cust["visits"]>=5]

            disp = view_cust.copy()
            disp["total_spend"] = disp["total_spend"].map("₦{:,.0f}".format)
            show_cols_c = [c for c in ["customer_name","phone","total_spend","visits","first_visit","last_visit"] if c in disp.columns]
            st.dataframe(disp[show_cols_c].rename(columns={
                "customer_name":"Customer","phone":"Phone","total_spend":"Total Spend",
                "visits":"Visits","first_visit":"First Visit","last_visit":"Last Visit"}),
                use_container_width=True, hide_index=True, height=260)

            st.divider()
            st.markdown("#### 📋 Customer Profile & Edit")
            cust_list = cust_stats["customer_name"].tolist()
            if cust_list:
                sel_cust = st.selectbox("Select Customer", cust_list)
                cust_row = cust_stats[cust_stats["customer_name"]==sel_cust].iloc[0]
                cp1,cp2,cp3,cp4 = st.columns(4)
                cp1.metric("Total Spend", fmt(cust_row["total_spend"]))
                cp2.metric("Visits",      cust_row["visits"])
                cp3.metric("First Visit", cust_row["first_visit"])
                cp4.metric("Last Visit",  cust_row["last_visit"])

                cust_db = customers_df[customers_df["name"]==sel_cust] if not customers_df.empty else pd.DataFrame()
                ex_phone = cust_db.iloc[0]["phone"] if not cust_db.empty else ""
                ex_email = cust_db.iloc[0]["email"] if not cust_db.empty else ""
                ex_note  = cust_db.iloc[0]["note"]  if not cust_db.empty else ""
                ex_id    = int(cust_db.iloc[0]["id"]) if not cust_db.empty else None

                with st.form("edit_customer"):
                    ec1,ec2 = st.columns(2)
                    new_phone = ec1.text_input("📱 Phone", value=ex_phone or "")
                    new_email = ec2.text_input("📧 Email", value=ex_email or "")
                    new_note  = st.text_input("Note",      value=ex_note  or "")
                    if st.form_submit_button("💾 Save Customer", type="primary"):
                        if ex_id:
                            update_customer(ex_id,sel_cust,phone_norm(new_phone),new_email,new_note)
                        else:
                            conn2 = get_conn()
                            try:
                                conn2.execute("INSERT OR IGNORE INTO customers (name,phone,email,note) VALUES (?,?,?,?)",
                                             (sel_cust,phone_norm(new_phone),new_email,new_note))
                                conn2.commit()
                            finally:
                                conn2.close()
                            bust()
                        st.success("✅ Customer updated!"); st.rerun()

                cust_orders = all_orders_df[all_orders_df["customer_name"]==sel_cust].copy()
                cod = cust_orders[["id","date","order_type","payment_method","total_amount","note"]].copy()
                cod.columns = ["ID","Date","Type","Payment","Amount","Note"]
                cod["Amount"] = cod["Amount"].map("₦{:,.0f}".format)
                st.markdown(f"**Order History — {sel_cust}** ({len(cust_orders)} orders)")
                st.dataframe(cod, use_container_width=True, hide_index=True)
                avg_ord = cust_row["total_spend"]/cust_row["visits"]
                st.info(f"Avg order: **{fmt(avg_ord)}** · Since: **{cust_row['first_visit']}** · Last: **{cust_row['last_visit']}**")
        else:
            st.info("No customer data yet.")

    # ══════════════════════════════════════════════════════════════
    # TAB 5 — EXPENSES
    # ══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown("### 💸 Expense Analysis")
        if not exp_df.empty:
            total_exp = exp_df["amount"].sum()
            by_cat = exp_df.groupby("category")["amount"].sum().reset_index().sort_values("amount",ascending=False)
            by_day = exp_df.groupby("date")["amount"].sum().reset_index().sort_values("date")

            ec1,ec2 = st.columns(2)
            with ec1:
                st.markdown("#### By Category")
                st.bar_chart(by_cat.set_index("category").rename(columns={"amount":"Amount"}), height=260)
            with ec2:
                st.markdown("#### Daily Spend")
                st.bar_chart(by_day.set_index("date").rename(columns={"amount":"Spend"}), height=260)

            bc = by_cat.copy()
            bc["pct"] = (bc["amount"]/total_exp*100).round(1)
            bc["amount"] = bc["amount"].map("{:,.0f}".format)
            bc.columns = ["Category","Amount","% Total"]
            st.markdown("#### Summary")
            st.dataframe(bc, use_container_width=True, hide_index=True)

            exp_d = exp_df[["id","date","category","name","amount","note"]].copy()
            exp_d.columns = ["ID","Date","Category","Description","Amount","Note"]
            st.markdown("#### All Items")
            st.dataframe(exp_d.style.format({"Amount":"{:,.0f}"}),
                         use_container_width=True, hide_index=True, height=280)

            st.divider()
            dc1,dc2 = st.columns(2)
            with dc1:
                del_id = st.number_input("Expense ID to delete", min_value=1, step=1,
                                         value=int(exp_df["id"].iloc[0]))
                if st.button("🗑️ Delete Expense"):
                    if del_id in exp_df["id"].values:
                        delete_expense(int(del_id)); st.success(f"Deleted #{del_id}"); st.rerun()
            with dc2:
                st.download_button("📥 Export CSV",
                    data=exp_df.to_csv(index=False).encode(),
                    file_name=f"kokari_exp_{from_date}_{to_date}.csv", mime="text/csv")
        else:
            st.info("No expense data in this period.")

    # ══════════════════════════════════════════════════════════════
    # TAB 6 — CHANNELS
    # ══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.markdown("### 🏪 Channel Performance")
        channels_df = load_channels()

        if not items_df.empty:
            ch_perf = (items_df.groupby("channel")
                       .agg(revenue=("total_price","sum"),cogs=("total_cogs","sum"),
                            orders=("order_id","nunique"),qty=("qty","sum")).reset_index())
            ch_perf["gross_profit"] = ch_perf["revenue"] - ch_perf["cogs"]
            ch_perf["margin"]       = (ch_perf["gross_profit"]/ch_perf["revenue"]*100).round(1)
            ch_perf["rev_share"]    = (ch_perf["revenue"]/kpi["revenue"]*100).round(1)
            ch_perf = ch_perf.sort_values("revenue",ascending=False)
            show_ch = ch_perf.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_ch[c] = show_ch[c].map("{:,.0f}".format)
            show_ch.columns = ["Channel","Revenue","COGS","Orders","Qty Sold","Gross Profit","Margin %","Rev Share %"]
            st.dataframe(show_ch, use_container_width=True, hide_index=True)
            cch = ch_perf.set_index("channel")[["revenue","gross_profit"]]
            cch.columns = ["Revenue","Gross Profit"]
            st.bar_chart(cch, height=260)

        st.divider()
        st.markdown("#### Manage Channels")
        with st.form("add_ch"):
            nc = st.text_input("New Channel Name", placeholder="e.g. Online Orders, Catering")
            if st.form_submit_button("➕ Add Channel", type="primary"):
                if nc.strip(): add_channel(nc.strip()); st.success("✅ Added!"); st.rerun()
                else: st.error("Enter a name.")

        for _,row in channels_df.iterrows():
            c1,c2,c3 = st.columns([3,2,2])
            c1.write(f"**{row['name']}**")
            nn = c2.text_input("Rename",key=f"rch_{row['id']}",label_visibility="collapsed",placeholder="New name…")
            with c3:
                cr,cd_ = st.columns(2)
                if cr.button("Rename",key=f"rcb_{row['id']}"):
                    if nn.strip(): rename_channel(row["id"],nn.strip()); st.success("Renamed!"); st.rerun()
                if cd_.button("Delete",key=f"dch_{row['id']}"):
                    ok = delete_channel(row["id"])
                    if ok: st.success("Deleted!"); st.rerun()
                    else:  st.error("Products assigned — cannot delete.")

    # ══════════════════════════════════════════════════════════════
    # TAB 7 — PRODUCTS  (selectbox crash fixed: use string list)
    # ══════════════════════════════════════════════════════════════
    with tabs[7]:
        st.markdown("### 🛒 Product Management")
        channels_df = load_channels()
        ch_names = channels_df["name"].tolist() if not channels_df.empty else ["Cafe"]
        ch_ids   = channels_df["id"].tolist()   if not channels_df.empty else [1]

        st.markdown("#### Add New Product")
        with st.form("add_prod"):
            ap1,ap2,ap3,ap4 = st.columns(4)
            np_name   = ap1.text_input("Product Name")
            # Fixed: use plain string list, no range()/lambda
            np_ch_sel = ap2.selectbox("Channel", ch_names)
            np_price  = ap3.number_input("Default Price (₦)", min_value=0.0, step=100.0)
            np_ratio  = ap4.number_input("Cost Ratio", min_value=0.0, max_value=1.0, step=0.01, value=0.40)
            if st.form_submit_button("➕ Add Product", type="primary"):
                if np_name.strip():
                    np_ch_id = ch_ids[ch_names.index(np_ch_sel)] if np_ch_sel in ch_names else ch_ids[0]
                    add_product(np_name.strip(),np_ch_id,np_ratio,np_price)
                    st.success(f"✅ '{np_name}' added!"); st.rerun()
                else:
                    st.error("Enter a product name.")

        st.divider()
        if not items_df.empty:
            st.markdown("#### Product Performance This Period")
            pp = (items_df.groupby(["product_name","channel"])
                  .agg(revenue=("total_price","sum"),qty=("qty","sum"),cogs=("total_cogs","sum")).reset_index())
            pp["gross_profit"] = pp["revenue"] - pp["cogs"]
            pp["margin"] = (pp["gross_profit"]/pp["revenue"]*100).round(1)
            pp = pp.sort_values("revenue",ascending=False)
            show_pp = pp.copy()
            for c in ["revenue","cogs","gross_profit"]:
                show_pp[c] = show_pp[c].map("{:,.0f}".format)
            show_pp.columns = ["Product","Channel","Revenue","Qty Sold","COGS","Gross Profit","Margin %"]
            st.dataframe(show_pp, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Edit Products")
        all_prods = load_products()
        if not all_prods.empty:
            edited = st.data_editor(
                all_prods[["id","name","channel","cost_ratio","default_price"]],
                use_container_width=True, num_rows="fixed",
                column_config={
                    "id":            st.column_config.TextColumn("ID",disabled=True),
                    "name":          st.column_config.TextColumn("Name"),
                    "channel":       st.column_config.TextColumn("Channel",disabled=True),
                    "cost_ratio":    st.column_config.NumberColumn("Cost Ratio",min_value=0.0,max_value=1.0,step=0.01,format="%.2f"),
                    "default_price": st.column_config.NumberColumn("Default Price (₦)",min_value=0,step=100),
                }, hide_index=True)
            sc1,sc2 = st.columns(2)
            with sc1:
                if st.button("💾 Save Changes", type="primary", use_container_width=True):
                    for _,row in edited.iterrows():
                        orig = all_prods[all_prods["id"]==row["id"]]
                        if not orig.empty:
                            update_product(row["id"],row["name"],int(orig.iloc[0]["channel_id"]),
                                           row["cost_ratio"],row["default_price"])
                    bust(); st.success("✅ Saved!"); st.rerun()
            with sc2:
                rem = st.selectbox("Remove Product", all_prods["name"].tolist())
                if st.button("🗑️ Remove Product", use_container_width=True):
                    pid_rem = all_prods[all_prods["name"]==rem]["id"].values[0]
                    deactivate_product(pid_rem); st.success("Removed."); st.rerun()

    # ══════════════════════════════════════════════════════════════
    # TAB 8 — P&L REPORT
    # ══════════════════════════════════════════════════════════════
    with tabs[8]:
        st.markdown("### 📋 Profit & Loss Report")
        st.caption(f"**{period_label}** · {from_date} → {to_date} · {kpi['days']} day(s)")

        if not orders_df.empty:
            st.markdown("#### 💳 Payment Reconciliation")
            pay_rec = (orders_df.groupby("payment_method")["total_amount"]
                       .agg(["sum","count"]).reset_index()
                       .rename(columns={"payment_method":"Method","sum":"Total","count":"Orders"})
                       .sort_values("Total",ascending=False))
            pay_rec["Total"] = pay_rec["Total"].map("{:,.0f}".format)
            rc1,rc2 = st.columns([1,2])
            with rc1: st.dataframe(pay_rec, use_container_width=True, hide_index=True)
            with rc2:
                pay_chart = orders_df.groupby("payment_method")["total_amount"].sum().reset_index()
                st.bar_chart(pay_chart.set_index("payment_method").rename(columns={"total_amount":"Amount"}), height=220)
            st.divider()

        by_ec  = exp_df.groupby("category")["amount"].sum().to_dict() if not exp_df.empty else {}
        ch_rev = items_df.groupby("channel")["total_price"].sum() if not items_df.empty else pd.Series(dtype=float)

        pl_col,ratio_col = st.columns(2)
        with pl_col:
            st.markdown("#### 📊 Income Statement")
            rows_pl = [
                ("REVENUE","",True),
                *[(f"  {ch}",fmt(v),False) for ch,v in ch_rev.items()],
                ("TOTAL REVENUE",fmt(kpi["revenue"]),True),("---","","---"),
                ("  Est. COGS",f"({fmt(kpi['cogs'])})",False),
                ("GROSS PROFIT",fmt(kpi["gp"]),True),("  Gross Margin",f"{kpi['gp_mar']}%",False),("---","","---"),
                ("OPERATING EXPENSES","",True),
                *[(f"  {c}",f"({fmt(a)})",False) for c,a in sorted(by_ec.items())],
                ("TOTAL EXPENSES",f"({fmt(kpi['expenses'])})",True),("---","","---"),
                ("NET PROFIT / LOSS",fmt(kpi["np"]),True),("  Net Margin",f"{kpi['np_mar']}%",False),
            ]
            for label,value,bold in rows_pl:
                if label=="---": st.markdown("---"); continue
                a,b = st.columns([3,1])
                pre = "**" if bold else ""
                a.markdown(f"{pre}{label}{pre}")
                b.markdown(f"<div style='text-align:right'>{pre}{value}{pre}</div>",unsafe_allow_html=True)

        with ratio_col:
            st.markdown("#### 📐 Key Ratios")
            st.metric("Gross Margin",   f"{kpi['gp_mar']}%")
            st.metric("Net Margin",     f"{kpi['np_mar']}%")
            st.metric("Expense Ratio",  f"{safe_pct(kpi['expenses'],kpi['revenue'])}%")
            st.metric("COGS Ratio",     f"{safe_pct(kpi['cogs'],kpi['revenue'])}%")
            st.metric("Avg Order Value",fmt(kpi["avg_order"]))
            st.metric("Avg Daily Rev",  fmt(kpi["avg_daily"]))
            st.markdown("---")
            st.warning("**📝 Notes**\n\n- COGS estimated from cost ratios.\n"
                       "- Verify B2B receipts before period close.\n"
                       "- Reconcile payment methods vs bank/Opay.\n"
                       "- Imprest items need petty cash recon.")

        if not orders_df.empty and kpi["days"] > 1:
            st.divider()
            st.markdown("#### 📅 Period Breakdown")
            bd_by = st.selectbox("Group by",["Daily","Weekly","Monthly"])
            dr = orders_df.groupby("date")["total_amount"].sum().reset_index()
            dr["date"] = pd.to_datetime(dr["date"])
            if bd_by=="Weekly":    dr["period"] = dr["date"].dt.to_period("W").astype(str)
            elif bd_by=="Monthly": dr["period"] = dr["date"].dt.to_period("M").astype(str)
            else:                  dr["period"] = dr["date"].dt.strftime("%Y-%m-%d")
            bd_df = dr.groupby("period")["total_amount"].agg(["sum","count"]).reset_index()
            bd_df.columns = ["Period","Revenue","Orders"]
            bd_df["Avg Order"] = (bd_df["Revenue"]/bd_df["Orders"]).round(0).map("₦{:,.0f}".format)
            bd_df["Revenue"]   = bd_df["Revenue"].map("₦{:,.0f}".format)
            st.dataframe(bd_df, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### 📥 Export")
        ch_p,pp_r = pd.DataFrame(),pd.DataFrame()
        if not items_df.empty:
            ch_p = items_df.groupby("channel").agg(revenue=("total_price","sum"),cogs=("total_cogs","sum")).reset_index()
            ch_p["gross_profit"] = ch_p["revenue"] - ch_p["cogs"]
            pp_r = items_df.groupby(["product_name","channel"]).agg(revenue=("total_price","sum"),cogs=("total_cogs","sum")).reset_index()
            pp_r["gross_profit"] = pp_r["revenue"] - pp_r["cogs"]
            pp_r["margin"] = (pp_r["gross_profit"]/pp_r["revenue"]*100).round(1)
            pp_r.rename(columns={"revenue":"total_price"},inplace=True)
            pp_r = pp_r.sort_values("total_price",ascending=False)

        html_r = generate_pdf_html(kpi,from_date,to_date,by_ec,ch_p,pp_r,period_label)
        ex1,ex2 = st.columns(2)
        with ex1:
            pdf_download_button(html_r,f"Kokari_Report_{period_label.replace(' ','_')}_{from_date}_{to_date}.html")
        with ex2:
            st.download_button("📊 Export P&L CSV",
                data=pd.DataFrame({
                    "Metric":["Period","From","To","Revenue","COGS","Gross Profit","Gross Margin",
                              "Expenses","Net Profit","Net Margin","Orders","Avg Order","Days"],
                    "Value": [period_label,str(from_date),str(to_date),kpi["revenue"],kpi["cogs"],
                              kpi["gp"],f"{kpi['gp_mar']}%",kpi["expenses"],kpi["np"],
                              f"{kpi['np_mar']}%",kpi["orders"],kpi["avg_order"],kpi["days"]],
                }).to_csv(index=False).encode(),
                file_name=f"kokari_PnL_{period_label.replace(' ','_')}_{from_date}_{to_date}.csv",
                mime="text/csv")

    # ══════════════════════════════════════════════════════════════
    # TAB 9 — SETTINGS
    # ══════════════════════════════════════════════════════════════
    with tabs[9]:
        st.markdown("### ⚙️ Settings")
        exp_cats = load_exp_cats()
        s1,s2 = st.columns(2)

        with s1:
            st.markdown("#### Expense Categories")
            with st.form("add_ec"):
                nec = st.text_input("New Category",placeholder="e.g. Insurance, Equipment")
                if st.form_submit_button("➕ Add",type="primary"):
                    if nec.strip(): add_exp_cat(nec.strip()); st.success("Added!"); st.rerun()
                    else: st.error("Enter a name.")
            for cat in exp_cats:
                c1,c2,c3 = st.columns([3,2,2])
                c1.write(cat)
                ncv = c2.text_input("",key=f"rnec_{cat}",label_visibility="collapsed",placeholder="Rename…")
                with c3:
                    cr2,cd2 = st.columns(2)
                    if cr2.button("Rename",key=f"rcec_{cat}"):
                        if ncv.strip(): rename_exp_cat(cat,ncv.strip()); st.success("Renamed!"); st.rerun()
                    if cd2.button("Del",key=f"delec_{cat}"):
                        ok=delete_exp_cat(cat)
                        if ok: st.success("Deleted!"); st.rerun()
                        else:  st.error("Has transactions.")

        with s2:
            st.markdown("#### 🔐 Change Password")
            with st.form("chpw"):
                cp  = st.text_input("Current Password",type="password")
                np1 = st.text_input("New Password",    type="password")
                np2 = st.text_input("Confirm",         type="password")
                if st.form_submit_button("Update Password",type="primary"):
                    if not verify_login(username,cp):  st.error("Current password incorrect.")
                    elif np1!=np2:                     st.error("Passwords don't match.")
                    elif len(np1)<6:                   st.error("Minimum 6 characters.")
                    else:
                        change_password(username,np1)
                        st.success("Updated! Please sign in again.")
                        for k in ["logged_in","username","role"]: st.session_state.pop(k,None)
                        st.rerun()
            st.divider()
            st.markdown("#### ℹ️ System Info")
            st.info(f"**User:** {username}  \n**Role:** {role}  \n**DB:** {DB_PATH}  \n"
                    f"**v2.1**  \nDefault: admin / kokari2026")

            st.markdown("#### 🔗 WhatsApp Product Aliases")
            st.caption("Keywords in WhatsApp messages → product + sales category")
            alias_rows = []
            for alias,(pid,cat) in PRODUCT_ALIASES.items():
                match = prods_df[prods_df["id"]==pid]
                pname = match.iloc[0]["name"] if not match.empty else pid
                alias_rows.append({"Keyword":alias,"Maps to":pname,"Category":cat})
            st.dataframe(pd.DataFrame(alias_rows),use_container_width=True,hide_index=True,height=300)


if __name__ == "__main__":
    main()
