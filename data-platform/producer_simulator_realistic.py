"""
Bitka Data Platform - Traffic Generator (Producer) v2
-----------------------------------------------------
Description:
    Simulates user activity and market data, inserting directly into the Source DB.
    Compatible with new schema (UUIDs, JSONB audit logs).
    
    Features:
    - Real-time Price Simulation (Geometric Brownian Motion)
    - State Initialization from DB (Resume pricing)
    - Realistic Traffic Patterns (Login rates based on time)
"""

import time
import random
import uuid
import os
import json
import math
import psycopg2
from decimal import Decimal
from datetime import datetime, timezone
from faker import Faker

# ==========================================
# CONFIGURATION
# ==========================================
# Source DB Connection (Operational Layer)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = "bitka_main"

# Data Warehouse Connection (Analytical Layer - for initialization only)
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = os.getenv("DW_PORT", "5433") # Note: External port for DW
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

fake = Faker()

# ==========================================
# STATE MANAGEMENT
# ==========================================
SYMBOLS = ["BTC_THB", "ETH_THB", "USDT_THB", "DOGE_THB"]
USERS = [] # Cache created user IDs

# Initial Prices & Volatility
price_state = {
    "BTC_THB": 1_500_000.0,
    "ETH_THB": 50_000.0,
    "USDT_THB": 36.0,
    "DOGE_THB": 5.0
}

vol_24h_state = {s: 0.0 for s in SYMBOLS}

# Approximate daily volatility (fraction)
DAILY_VOL = {
    "BTC_THB": 0.04,
    "ETH_THB": 0.06,
    "USDT_THB": 0.01,
    "DOGE_THB": 0.08
}

def get_connection(host, port, user, password, dbname):
    """Generic connection helper with retry"""
    while True:
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=dbname
            )
            return conn
        except Exception as e:
            print(f"⏳ Connecting to {dbname}... ({e})")
            time.sleep(2)

def init_state_from_dw():
    """Load latest prices from DW 'tickers' table to resume simulation state."""
    global price_state
    print("🔄 Initializing state from Data Warehouse...")
    try:
        # Connect to DW
        conn = get_connection(DW_HOST, DW_PORT, DW_USER, DW_PASS, DW_NAME)
        cur = conn.cursor()
        
        for sym in SYMBOLS:
            # Query compatible with new schema (table 'tickers', col 'last_price')
            cur.execute(
                "SELECT last_price FROM tickers WHERE symbol = %s ORDER BY updated_at DESC LIMIT 1",
                (sym,)
            )
            row = cur.fetchone()
            if row and row[0]:
                price_state[sym] = float(row[0])
                print(f"   -> Resumed {sym} @ {price_state[sym]:.2f}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Could not load state from DW: {e}. Using defaults.")

# ==========================================
# SIMULATION LOGIC
# ==========================================

def step_price(symbol, dt_sec=1.0):
    """Update price using Geometric Brownian Motion"""
    s = price_state[symbol]
    sigma = DAILY_VOL[symbol] / math.sqrt(86400) # Daily -> Seconds
    mu = 0.0
    z = random.gauss(0, 1)
    
    s_new = max(0.01, s * math.exp(mu * dt_sec + sigma * math.sqrt(dt_sec) * z))
    price_state[symbol] = s_new
    return s_new

def gen_user(curr):
    """Create new user identity"""
    user_id = str(uuid.uuid4())
    email = fake.email()
    sql = "INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s)"
    curr.execute(sql, (user_id, email, random.choice([1, 2, 3])))
    USERS.append(user_id)
    print(f"👤 [New User] {email}")

def gen_login(curr, now_utc):
    """Simulate login based on time of day"""
    if not USERS: return
    
    # Simple circadian rhythm (UTC+7 approximation)
    hour = (now_utc.hour + 7) % 24
    prob = 0.12 if 12 <= hour <= 22 else 0.04
    
    if random.random() < prob:
        status = random.choices(["success", "failed"], weights=[0.9, 0.1])[0]
        sql = """
            INSERT INTO login_history (user_id, ip_address, device_id, status)
            VALUES (%s, %s, %s, %s)
        """
        curr.execute(sql, (
            random.choice(USERS),
            fake.ipv4(),
            uuid.uuid4().hex[:16],
            status
        ))

def gen_market_activity(curr, symbol):
    """Generate Ticker, Orders, and Matches for a symbol"""
    if not USERS: return

    # 1. Update Ticker (Upsert)
    price = price_state[symbol]
    vol = vol_24h_state[symbol]
    
    sql_ticker = """
        INSERT INTO tickers (symbol, last_price, volume_24h)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE 
        SET last_price = EXCLUDED.last_price,
            volume_24h = EXCLUDED.volume_24h,
            updated_at = CURRENT_TIMESTAMP
    """
    curr.execute(sql_ticker, (symbol, Decimal(f"{price:.2f}"), Decimal(f"{vol:.4f}")))
    
    # 2. Place Order
    if random.random() < 0.7:
        side = random.choice(["buy", "sell"])
        order_price = price * (1 + random.uniform(-0.005, 0.005)) # +/- 0.5%
        qty = random.uniform(0.001, 1.0)
        
        sql_order = """
            INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        curr.execute(sql_order, (
            str(uuid.uuid4()), random.choice(USERS), symbol, side, "limit",
            Decimal(f"{order_price:.2f}"), Decimal(f"{qty:.8f}"), "open"
        ))
        print(f"📝 [Order] {side.upper()} {symbol} {qty:.4f} @ {order_price:.2f}")

    # 3. Execute Match (Trade)
    if random.random() < 0.3:
        trade_price = price
        trade_qty = random.uniform(0.001, 0.5)
        vol_24h_state[symbol] += trade_qty # Update local vol state
        
        sql_match = """
            INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        curr.execute(sql_match, (
            str(uuid.uuid4()), symbol, Decimal(f"{trade_price:.2f}"), 
            Decimal(f"{trade_qty:.8f}"), random.choice(USERS), random.choice(USERS)
        ))
        print(f"🤝 [Match] {symbol} {trade_qty:.4f} @ {trade_price:.2f}")

def gen_finance(curr):
    """Generate Deposits/Withdrawals"""
    if not USERS: return
    
    if random.random() < 0.1:
        uid = random.choice(USERS)
        asset = random.choice(["THB", "USDT", "BTC"])
        amt = random.uniform(100, 50000)
        
        if random.random() < 0.6: # Deposit
            sql = "INSERT INTO deposits (tx_id, user_id, asset, amount, network) VALUES (%s, %s, %s, %s, %s)"
            curr.execute(sql, (str(uuid.uuid4()), uid, asset, Decimal(f"{amt:.2f}"), "ERC20"))
            print(f"💰 [Deposit] {amt:.2f} {asset}")
        else: # Withdrawal
            sql = "INSERT INTO withdrawals (withdrawal_id, user_id, asset, amount, dest_address, status) VALUES (%s, %s, %s, %s, %s, %s)"
            curr.execute(sql, (str(uuid.uuid4()), uid, asset, Decimal(f"{amt:.2f}"), f"0x{uuid.uuid4().hex}", "requested"))

def gen_audit(curr):
    """Generate Audit Logs (JSONB)"""
    if not USERS: return
    if random.random() < 0.05:
        sql = """
            INSERT INTO audit_logs (log_id, actor_id, action, resource, details_before, details_after)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        before = {"level": 1}
        after = {"level": 2}
        curr.execute(sql, (
            str(uuid.uuid4()), random.choice(USERS), "kyc_upgrade", "user",
            json.dumps(before), json.dumps(after)
        ))

# ==========================================
# MAIN LOOP
# ==========================================

def main():
    print("🚀 Starting Bitka Traffic Generator...")
    
    # 1. Initialize State
    init_state_from_dw()
    
    # 2. Connect to Source DB
    conn = get_connection(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME)
    
    # 3. Pre-seed users if empty
    with conn:
        with conn.cursor() as curr:
            curr.execute("SELECT count(*) FROM users")
            if curr.fetchone()[0] < 5:
                print("🌱 Seeding initial users...")
                for _ in range(5): gen_user(curr)

    try:
        while True:
            now = datetime.now(timezone.utc)
            
            with conn:
                with conn.cursor() as curr:
                    # Update Prices
                    for s in SYMBOLS:
                        step_price(s)
                    
                    # Core Loop
                    if random.random() < 0.1: gen_user(curr)
                    gen_login(curr, now)
                    
                    active_symbol = random.choices(SYMBOLS, weights=[0.4, 0.3, 0.2, 0.1])[0]
                    gen_market_activity(curr, active_symbol)
                    
                    gen_finance(curr)
                    gen_audit(curr)
            
            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("\n🛑 Stopping Simulator.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()