import psycopg2
import time
import random
import uuid
import os
import json
from decimal import Decimal
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable 

# --- Configuration ---
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = "bitka_main" 
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")



required_vars = [("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASS", DB_PASS), ("KAFKA_BROKER", KAFKA_BROKER)]
for var_name, value in required_vars:
    if not value:
        raise ValueError(f"❌ Missing required environment variable: {var_name}")

fake = Faker()

# --- Connection Helpers ---

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
            )
            print("✅ Connected to Database!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"⏳ Database not ready... waiting 2s ({e})")
            time.sleep(2)
        except Exception as e:
            print(f"❌ DB Error: {e}")
            time.sleep(2)

def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready... waiting 2s")
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Kafka Error: {e}")
            time.sleep(2)

# --- Mock Data Helpers ---
USERS = []
SYMBOLS = ["BTC_THB", "ETH_THB", "DOGE_THB", "USDT_THB"]

def create_user(curr):
    user_id = str(uuid.uuid4())
    email = fake.email()
    sql = "INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s)"
    curr.execute(sql, (user_id, email, random.choice([1, 2, 3])))
    USERS.append(user_id)
    print(f"👤 [Identity] New User: {email}")
    
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
    update_ticker(curr, symbol, price, qty)

def update_ticker(curr, symbol, last_price, vol):
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

# --- Function สร้าง Audit Log ยิงตรง Kafka ---
def create_audit_log(curr, producer): # รับ producer เข้ามาเป็น Argument
    if not USERS or not producer: return
    
    actor_id = random.choice(USERS)
    log_id = str(uuid.uuid4())
    
    events = [
        ("login_failed", "auth", {"attempt": 1}, {"attempt": 2}),
        ("change_password", "security", {"changed": False}, {"changed": True}),
        ("view_sensitive_data", "privacy", {}, {"viewed": True})
    ]
    action, resource, before, after = random.choice(events)
    
    message = {
        "log_id": log_id,
        "actor_id": actor_id,
        "action": action,
        "resource": resource,
        "details_before": json.dumps(before),
        "details_after": json.dumps(after),
        "created_at": time.time()
    }
    
    try:
        producer.send('app.events.audit_logs', message)
        print(f"⚡ [Direct Kafka] Audit Log sent: {action}")
    except Exception as e:
        print(f"⚠️ Failed to send to Kafka: {e}")

def main():
    print("🚀 Starting Producer Simulator...")
    
    # 1. เชื่อมต่อ DB และ Kafka ให้สำเร็จก่อนเริ่มลูป
    conn = get_db_connection()
    producer = get_kafka_producer()
    
    try:
        while True:
            # Reconnect DB check
            if conn.closed:
                print("⚠️ DB Connection lost, reconnecting...")
                conn = get_db_connection()

            with conn: 
                with conn.cursor() as curr:
                    action = random.choices(
                        ["user", "order", "tx", "audit", "sleep"], 
                        weights=[10, 40, 20, 20, 10],
                        k=1
                    )[0]
                    
                    if action == "user":
                        create_user(curr)
                    elif action == "order":
                        create_order(curr)
                    elif action == "tx":
                        create_transaction(curr)
                    elif action == "audit":
                        # ส่ง producer เข้าไปในฟังก์ชันด้วย
                        create_audit_log(curr, producer)
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping Simulator.")
    finally:
        if conn: conn.close()
        if producer: producer.close()

if __name__ == "__main__":
    main()