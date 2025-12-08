import psycopg2
import time
import random
import uuid
import os
from decimal import Decimal
from faker import Faker
from datetime import datetime

# --- Configuration ---
# ใช้ localhost เพราะเรารัน script นี้จากเครื่องเรา ยิงเข้า Container ผ่าน Port Mapping
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = "bitka_main"

fake = Faker()

def get_connection():
    while True: # วนลูปไปเรื่อยๆ จนกว่าจะต่อได้
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                dbname=DB_NAME
            )
            print("✅ Connected to Database!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"⏳ Database not ready yet... waiting 2 seconds ({e})")
            time.sleep(2) # รอ 2 วินาทีแล้วลองใหม่
        except Exception as e:
            print(f"❌ Unexpected Error: {e}")
            time.sleep(2)

# --- Mock Data Helpers ---
USERS = [] # Cache users to reference them in orders
SYMBOLS = ["BTC_THB", "ETH_THB", "DOGE_THB", "USDT_THB"]

def create_user(curr):
    user_id = str(uuid.uuid4())
    email = fake.email()
    sql = "INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s)"
    curr.execute(sql, (user_id, email, random.choice([1, 2, 3])))
    USERS.append(user_id)
    print(f"👤 [Identity] New User: {email}")
    
    # Simulate Login
    if random.random() > 0.5:
        sql_login = "INSERT INTO login_history (user_id, ip_address, device_id, status) VALUES (%s, %s, %s, %s)"
        curr.execute(sql_login, (user_id, fake.ipv4(), fake.md5(), "success"))

def create_order(curr):
    if not USERS: return
    user_id = random.choice(USERS)
    symbol = random.choice(SYMBOLS)
    side = random.choice(["buy", "sell"])
    price = round(random.uniform(100, 3000000), 2)
    qty = round(random.uniform(0.01, 10), 8)
    
    order_id = str(uuid.uuid4())
    sql = """
        INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    curr.execute(sql, (order_id, user_id, symbol, side, "limit", price, qty, "open"))
    print(f"📈 [Trading] Order Placed: {side} {symbol} @ {price}")

    # Chance to Match immediately (Simulate Matching Engine)
    if random.random() > 0.7:
        create_match(curr, symbol, price, qty, user_id)

def create_match(curr, symbol, price, qty, taker_id):
    if not USERS: return
    match_id = str(uuid.uuid4())
    maker_id = random.choice(USERS)
    
    sql = """
        INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    curr.execute(sql, (match_id, symbol, price, qty, maker_id, taker_id))
    print(f"🔥 [Trading] Match Executed! {symbol} Vol: {qty}")
    
    # Update Ticker Logic (Simplified)
    update_ticker(curr, symbol, price, qty)

def update_ticker(curr, symbol, last_price, vol):
    # Upsert Ticker
    sql = """
        INSERT INTO tickers (symbol, last_price, volume_24h)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE 
        SET last_price = EXCLUDED.last_price,
            volume_24h = tickers.volume_24h + EXCLUDED.volume_24h,
            updated_at = CURRENT_TIMESTAMP
    """
    curr.execute(sql, (symbol, last_price, vol))
    print(f"📊 [Market] Ticker Update: {symbol} -> {last_price}")

def create_transaction(curr):
    if not USERS: return
    user_id = random.choice(USERS)
    asset = random.choice(["THB", "BTC", "USDT"])
    amount = round(random.uniform(100, 50000), 2)
    
    # 50% Deposit, 50% Withdrawal
    if random.random() > 0.5:
        tx_id = str(uuid.uuid4())
        sql = "INSERT INTO deposits (tx_id, user_id, asset, amount, network) VALUES (%s, %s, %s, %s, %s)"
        curr.execute(sql, (tx_id, user_id, asset, amount, "ERC20"))
        print(f"💰 [Accounting] Deposit: {amount} {asset}")
    else:
        wd_id = str(uuid.uuid4())
        sql = """
            INSERT INTO withdrawals (withdrawal_id, user_id, asset, amount, dest_address, status) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        curr.execute(sql, (wd_id, user_id, asset, amount, f"0x{fake.md5()}", "requested"))
        print(f"💸 [Accounting] Withdrawal Request: {amount} {asset}")

# แก้ไขฟังก์ชัน main นิดหน่อยเพื่อความชัวร์
def main():
    print("🚀 Starting Producer Simulator...")
    
    # บรรทัดนี้จะวนรอจนกว่าจะได้ Connection มา
    conn = get_connection() 
    
    try:
        while True:
            # เพิ่มการเช็ค Connection หลุดกลางทาง
            if conn.closed:
                print("⚠️ Connection lost, reconnecting...")
                conn = get_connection()

            with conn: # Auto Commit block
                with conn.cursor() as curr:
                    action = random.choices(
                        ["user", "order", "tx", "sleep"], 
                        weights=[10, 50, 20, 10], 
                        k=1
                    )[0]
                    
                    if action == "user":
                        create_user(curr)
                    elif action == "order":
                        create_order(curr)
                    elif action == "tx":
                        create_transaction(curr)
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping Simulator.")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()