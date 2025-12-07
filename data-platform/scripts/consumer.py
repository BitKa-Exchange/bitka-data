import json
import psycopg2
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime
from decimal import Decimal, InvalidOperation

# --- Load Config ---
load_dotenv()

# Kafka Settings
KAFKA_BROKER = os.getenv('KAFKA_BROKER_EXTERNAL', 'localhost:19092')

# Data Warehouse Settings
DW_CONFIG = {
    "host": "localhost",
    "port": int(os.getenv('DW_PORT_EXTERNAL', 5433)),
    "database": os.getenv('DW_DB_NAME', 'bitka_dw'),
    "user": os.getenv('DW_USER', 'warehouse_admin'),
    "password": os.getenv('DW_PASS', 'warehouse_password')
}

# 📡 Topics Definition (10 Events defined in spec)
TOPICS = [
    'trading.orders.created',
    'trading.orders.cancelled',
    'trading.matches.executed',
    'accounting.deposit.confirmed',
    'accounting.withdrawal.requested',
    'accounting.withdrawal.sent',
    'identity.user.login',
    'identity.kyc.updated',
    'market.ticker.update',
    'system.audit.entry'
]

# --- Global DB Connection ---
conn = None
cursor = None

def connect_db():
    global conn, cursor
    while True:
        try:
            print(f"🔌 Connecting to DW at localhost:{DW_CONFIG['port']}...")
            conn = psycopg2.connect(**DW_CONFIG)
            conn.autocommit = True
            cursor = conn.cursor()
            print("✅ Connected to Data Warehouse!")
            return
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            print("🔄 Retrying in 5 seconds...")
            time.sleep(5)

# --- Helper Functions ---
def to_decimal(value):
    """แปลงค่าตัวเลขหรือ String ให้เป็น Decimal สำหรับการเงิน"""
    if value is None or value == "": return None
    try: return Decimal(str(value))
    except InvalidOperation: return None

def parse_iso_time(timestamp_str):
    """แปลง ISO8601 String เป็น Python Datetime"""
    if not timestamp_str: return datetime.now()
    try:
        # รองรับ format เช่น "2025-12-06T10:00:00Z"
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError:
        return datetime.now()

# --- 🛠️ Schema Management ---
def create_tables_if_not_exist():
    queries = [
        # 1. 📈 Domain: Trading
        """
        CREATE TABLE IF NOT EXISTS fact_orders_created (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            order_id UUID,
            user_id UUID,
            symbol VARCHAR(20),
            side VARCHAR(10),       -- buy, sell
            type VARCHAR(10),       -- limit, market
            price DECIMAL(30, 10),
            quantity DECIMAL(30, 10),
            time_in_force VARCHAR(10) -- GTC, IOC, FOK
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_orders_cancelled (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            order_id UUID,
            user_id UUID,
            symbol VARCHAR(20),
            reason VARCHAR(255),
            remaining_qty DECIMAL(30, 10)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_matches_executed (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            match_id UUID,
            symbol VARCHAR(20),
            price DECIMAL(30, 10),
            quantity DECIMAL(30, 10),
            
            maker_user_id UUID,
            maker_order_id UUID,
            maker_fee DECIMAL(30, 10),
            maker_fee_asset VARCHAR(10),
            
            taker_user_id UUID,
            taker_order_id UUID,
            taker_fee DECIMAL(30, 10),
            taker_fee_asset VARCHAR(10),
            taker_side VARCHAR(10) -- buy, sell
        );
        """,

        # 2. 💰 Domain: Accounting
        """
        CREATE TABLE IF NOT EXISTS fact_deposits (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            tx_id UUID,
            user_id UUID,
            asset VARCHAR(10),
            amount DECIMAL(30, 10),
            chain_tx_hash VARCHAR(255),
            network VARCHAR(50),
            confirmations INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_withdrawals (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            withdrawal_id UUID,
            user_id UUID,
            asset VARCHAR(10),
            amount DECIMAL(30, 10),
            fee DECIMAL(30, 10),
            dest_address VARCHAR(255),
            chain_tx_hash VARCHAR(255),
            network_fee DECIMAL(30, 10),
            status VARCHAR(20) -- REQUESTED, SENT
        );
        """,

        # 3. 👤 Domain: Identity
        """
        CREATE TABLE IF NOT EXISTS dim_user_logins (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            user_id UUID,
            ip_address VARCHAR(45),
            device_id VARCHAR(255),
            location_geo VARCHAR(100),
            status VARCHAR(20),
            user_agent TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_kyc_history (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            user_id UUID,
            old_level INT,
            new_level INT,
            reviewer_id UUID,
            reason TEXT
        );
        """,

        # 4. 📊 Domain: Market Data
        """
        CREATE TABLE IF NOT EXISTS fact_market_tickers (
            event_id UUID PRIMARY KEY,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            symbol VARCHAR(20),
            last_price DECIMAL(30, 10),
            open_24h DECIMAL(30, 10),
            high_24h DECIMAL(30, 10),
            low_24h DECIMAL(30, 10),
            volume_24h DECIMAL(30, 10),
            quote_vol_24h DECIMAL(30, 10)
        );
        """,

        # 5. 🛡️ Domain: System (Audit)
        """
        CREATE TABLE IF NOT EXISTS fact_system_audit (
            event_id UUID PRIMARY KEY,
            correlation_id UUID,
            producer VARCHAR(50),
            event_time TIMESTAMP,
            
            actor_id UUID,
            action VARCHAR(100),
            resource VARCHAR(100),
            details_before JSONB,
            details_after JSONB,
            severity VARCHAR(20)
        );
        """
    ]
    
    print("🛠 Checking schema consistency...")
    try:
        for q in queries:
            cursor.execute(q)
        print("✅ All tables are ready.")
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        exit(1)

# --- 🎯 Handlers: Map JSON to SQL ---

def extract_envelope(msg):
    """ดึงข้อมูลส่วนหัว (Metadata)"""
    return (
        msg.get('event_id'),
        msg.get('correlation_id'),
        msg.get('producer'),
        parse_iso_time(msg.get('timestamp'))
    )

def handle_message(topic, msg):
    envelope = extract_envelope(msg)
    data = msg.get('data', {})
    sql = ""
    val = ()

    # --- 1. Trading ---
    if topic == 'trading.orders.created':
        sql = """INSERT INTO fact_orders_created (event_id, correlation_id, producer, event_time, 
                 order_id, user_id, symbol, side, type, price, quantity, time_in_force)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('order_id'), data.get('user_id'), data.get('symbol'), 
                          data.get('side'), data.get('type'), 
                          to_decimal(data.get('price')), to_decimal(data.get('quantity')), 
                          data.get('time_in_force'))

    elif topic == 'trading.orders.cancelled':
        sql = """INSERT INTO fact_orders_cancelled (event_id, correlation_id, producer, event_time, 
                 order_id, user_id, symbol, reason, remaining_qty)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('order_id'), data.get('user_id'), data.get('symbol'), 
                          data.get('reason'), to_decimal(data.get('remaining_qty')))

    elif topic == 'trading.matches.executed':
        sql = """INSERT INTO fact_matches_executed (event_id, correlation_id, producer, event_time, 
                 match_id, symbol, price, quantity, 
                 maker_user_id, maker_order_id, maker_fee, maker_fee_asset, 
                 taker_user_id, taker_order_id, taker_fee, taker_fee_asset, taker_side)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('match_id'), data.get('symbol'), 
                          to_decimal(data.get('price')), to_decimal(data.get('quantity')),
                          data.get('maker_user_id'), data.get('maker_order_id'), 
                          to_decimal(data.get('maker_fee')), data.get('maker_fee_asset'),
                          data.get('taker_user_id'), data.get('taker_order_id'), 
                          to_decimal(data.get('taker_fee')), data.get('taker_fee_asset'), 
                          data.get('taker_side'))

    # --- 2. Accounting ---
    elif topic == 'accounting.deposit.confirmed':
        sql = """INSERT INTO fact_deposits (event_id, correlation_id, producer, event_time, 
                 tx_id, user_id, asset, amount, chain_tx_hash, network, confirmations)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('tx_id'), data.get('user_id'), data.get('asset'), 
                          to_decimal(data.get('amount')), data.get('chain_tx_hash'), 
                          data.get('network'), data.get('confirmations'))

    elif topic == 'accounting.withdrawal.requested':
        sql = """INSERT INTO fact_withdrawals (event_id, correlation_id, producer, event_time, 
                 withdrawal_id, user_id, asset, amount, fee, dest_address, status)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'REQUESTED') 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('withdrawal_id'), data.get('user_id'), data.get('asset'), 
                          to_decimal(data.get('amount')), to_decimal(data.get('fee')), 
                          data.get('dest_address'))

    elif topic == 'accounting.withdrawal.sent':
        sql = """UPDATE fact_withdrawals
                SET chain_tx_hash = %s,
                    network_fee = %s,
                    status = 'SENT'
                WHERE withdrawal_id = %s;"""
        val = (data.get('chain_tx_hash'),
            to_decimal(data.get('network_fee')),
            data.get('withdrawal_id'))


    # --- 3. Identity ---
    elif topic == 'identity.user.login':
        sql = """INSERT INTO dim_user_logins (event_id, correlation_id, producer, event_time, 
                 user_id, ip_address, device_id, location_geo, status, user_agent)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('user_id'), data.get('ip_address'), data.get('device_id'), 
                          data.get('location_geo'), data.get('status'), data.get('user_agent'))

    elif topic == 'identity.kyc.updated':
        sql = """INSERT INTO dim_kyc_history (event_id, correlation_id, producer, event_time, 
                 user_id, old_level, new_level, reviewer_id, reason)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('user_id'), data.get('old_level'), data.get('new_level'), 
                          data.get('reviewer_id'), data.get('reason'))

    # --- 4. Market Data ---
    elif topic == 'market.ticker.update':
        sql = """INSERT INTO fact_market_tickers (event_id, producer, event_time, 
                 symbol, last_price, open_24h, high_24h, low_24h, volume_24h, quote_vol_24h)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        # Market Ticker อาจไม่มี correlation_id ส่งมาใน Envelope บางที ให้ข้ามไป
        val = (envelope[0], envelope[2], envelope[3], 
               data.get('symbol'), to_decimal(data.get('last_price')), 
               to_decimal(data.get('open_24h')), to_decimal(data.get('high_24h')), 
               to_decimal(data.get('low_24h')), to_decimal(data.get('volume_24h')), 
               to_decimal(data.get('quote_vol_24h')))

    # --- 5. System Audit ---
    elif topic == 'system.audit.entry':
        sql = """INSERT INTO fact_system_audit (event_id, correlation_id, producer, event_time, 
                 actor_id, action, resource, details_before, details_after, severity)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (event_id) DO NOTHING;"""
        val = envelope + (data.get('actor_id'), data.get('action'), data.get('resource'), 
                          json.dumps(data.get('details_before')), json.dumps(data.get('details_after')), 
                          data.get('severity'))

    # Execute SQL
    if sql:
        cursor.execute(sql, val)
        print(f"📥 [{topic}] Saved EventID: {envelope[0]}")


# --- Main Execution ---
if __name__ == "__main__":
    print(f"🚀 Bitka Event-Driven Consumer Started")
    connect_db()
    create_tables_if_not_exist()
    
    print(f"🎧 Listening to {len(TOPICS)} topics...")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', 
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        try:
            msg_data = message.value
            if not msg_data: continue
            handle_message(message.topic, msg_data)

        except psycopg2.OperationalError:
            print("⚠️ DB Connection Lost. Reconnecting...")
            connect_db()
        except Exception as e:
            print(f"❌ Error processing {message.topic}: {e}")
            # ไม่ exit เพื่อให้ consumer ทำงานต่อกับ event ถัดไป
            time.sleep(0.1)