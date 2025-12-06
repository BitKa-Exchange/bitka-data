import json
import psycopg2
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime
from decimal import Decimal 

# --- Load Config ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER_EXTERNAL', 'localhost:19092')
DW_CONFIG = {
    "host": "localhost",
    "port": os.getenv('DW_PORT_EXTERNAL', '5433'),
    "database": os.getenv('DW_DB_NAME', 'bitka_dw'),
    "user": os.getenv('DW_USER', 'warehouse_admin'),
    "password": os.getenv('DW_PASS', 'warehouse_password')
}

TOPICS = [
    'bitka.public.transfers', 
    'bitka.public.orders',
    'bitka.public.trade_executions',
    'bitka.public.order_status_history',
    'bitka.public.wallet_transactions',
    'bitka.public.market_prices'
]

# --- Global DB Connection ---
conn = None
cursor = None

def connect_db():
    global conn, cursor
    while True:
        try:
            print("🔌 Connecting to DB...")
            conn = psycopg2.connect(**DW_CONFIG)
            conn.autocommit = True
            cursor = conn.cursor()
            print("✅ Connected!")
            return
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            print("🔄 Retrying in 5 seconds...")
            time.sleep(5)

# --- ⭐ ฟังก์ชันใหม่: สร้างตารางถ้ายังไม่มี ---
def create_tables_if_not_exist():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS fact_transfers (
            transfer_id INT PRIMARY KEY,
            sender_id INT,
            receiver_id INT,
            amount DECIMAL(18, 2),
            currency VARCHAR(10),
            event_time TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INT PRIMARY KEY,
            user_id INT,
            symbol VARCHAR(10),
            side VARCHAR(4),
            price DECIMAL(18, 2),
            amount DECIMAL(18, 8),
            event_time TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_trade_executions (
            execution_id INT PRIMARY KEY,
            order_id INT,
            symbol VARCHAR(10),
            side VARCHAR(4),
            price DECIMAL(18, 2),
            quantity DECIMAL(18, 8),
            fee DECIMAL(18, 8),
            role VARCHAR(10),
            event_time TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_order_status (
            history_id INT PRIMARY KEY,
            order_id INT,
            status VARCHAR(20),
            reason TEXT,
            event_time TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_wallet_transactions (
            transaction_id INT PRIMARY KEY,
            user_id INT,
            currency VARCHAR(10),
            amount_change DECIMAL(18, 8),
            transaction_type VARCHAR(20),
            balance_after DECIMAL(18, 8),
            event_time TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_market_prices (
            tick_id INT PRIMARY KEY,
            symbol VARCHAR(10),
            price DECIMAL(18, 2),
            event_time TIMESTAMP
        );
        """
    ]
    
    print("🛠 Checking tables...")
    try:
        for q in queries:
            cursor.execute(q)
        print("✅ All tables are ready.")
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        exit(1) # ถ้าสร้างตารางไม่ได้ ให้ปิดโปรแกรมเลย

def convert_timestamp(micro_ts):
    if micro_ts is None: return datetime.now()
    try: return datetime.fromtimestamp(micro_ts / 1000000)
    except: return datetime.now()

def to_decimal(value):
    if value is None: return None
    return Decimal(str(value)) 

# --- Main Execution ---
connect_db()
create_tables_if_not_exist() # ⭐ เรียกใช้ฟังก์ชันสร้างตารางตรงนี้

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"🚀 Trading Consumer Started (Listening to {len(TOPICS)} topics)")

for message in consumer:
    try:
        data = message.value
        if data is None or data.get('payload') is None or data['payload'].get('after') is None:
            continue

        row = data['payload']['after']
        topic = message.topic
        event_time = convert_timestamp(row.get('created_at') or row.get('executed_at') or row.get('timestamp') or row.get('updated_at'))
        
        sql = ""
        val = ()
        log_msg = ""

        # --- Mapping Topics ---
        if 'transfers' in topic:
            sql = "INSERT INTO fact_transfers (transfer_id, sender_id, receiver_id, amount, currency, event_time) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (transfer_id) DO NOTHING;"
            val = (row['transfer_id'], row['sender_id'], row['receiver_id'], to_decimal(row['amount']), row['currency'], event_time)
            log_msg = f"💰 Transfer: {row['amount']} {row['currency']}"

        elif 'orders' in topic:
            sql = "INSERT INTO fact_orders (order_id, user_id, symbol, side, price, amount, event_time) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING;"
            val = (row['order_id'], row['user_id'], row['symbol'], row['side'], to_decimal(row['price']), to_decimal(row['amount']), event_time)
            log_msg = f"🛒 Order: {row['side']} {row['symbol']}"

        elif 'trade_executions' in topic:
            sql = "INSERT INTO fact_trade_executions (execution_id, order_id, symbol, side, price, quantity, fee, role, event_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (execution_id) DO NOTHING;"
            val = (row['execution_id'], row['order_id'], row['symbol'], row['side'], to_decimal(row['price']), to_decimal(row['quantity']), to_decimal(row['fee']), row['role'], event_time)
            log_msg = f"🤝 Trade: {row['quantity']} {row['symbol']} @ {row['price']}"

        elif 'order_status_history' in topic:
            sql = "INSERT INTO fact_order_status (history_id, order_id, status, reason, event_time) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (history_id) DO NOTHING;"
            val = (row['history_id'], row['order_id'], row['status'], row['reason'], event_time)
            log_msg = f"📝 Status: ID {row['order_id']} -> {row['status']}"

        elif 'wallet_transactions' in topic:
            sql = "INSERT INTO fact_wallet_transactions (transaction_id, user_id, currency, amount_change, transaction_type, balance_after, event_time) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING;"
            val = (row['transaction_id'], row['user_id'], row['currency'], to_decimal(row['amount_change']), row['transaction_type'], to_decimal(row['balance_after']), event_time)
            log_msg = f"💳 Wallet: {row['transaction_type']} {row['amount_change']} {row['currency']}"

        elif 'market_prices' in topic:
            sql = "INSERT INTO fact_market_prices (tick_id, symbol, price, event_time) VALUES (%s, %s, %s, %s) ON CONFLICT (tick_id) DO NOTHING;"
            val = (row['tick_id'], row['symbol'], to_decimal(row['price']), event_time)

        # --- Execute with Retry ---
        if sql:
            while True:
                try:
                    cursor.execute(sql, val)
                    if log_msg: print(log_msg)
                    break
                except psycopg2.OperationalError:
                    print("⚠️ DB Connection Lost. Reconnecting...")
                    connect_db()
                except Exception as e:
                    print(f"❌ SQL Error: {e}")
                    break

    except Exception as e:
        print(f"⚠️ Consumer Error: {e}")
        time.sleep(1)