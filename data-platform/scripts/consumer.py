import json
import psycopg2
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime

# --- Load Config ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER_EXTERNAL', 'localhost:19092')
TOPIC_NAME = 'bitka.public.users'

DW_CONFIG = {
    "host": "localhost",
    "port": os.getenv('DW_PORT_EXTERNAL', '5433'),
    "database": os.getenv('DW_DB_NAME', 'bitka_dw'),
    "user": os.getenv('DW_USER', 'warehouse_admin'),
    "password": os.getenv('DW_PASS', 'warehouse_password')
}

# --- Global Variables ---
conn = None
cursor = None

def connect_db():
    global conn, cursor
    while True:
        try:
            print("🔌 Connecting to Data Warehouse...")
            conn = psycopg2.connect(**DW_CONFIG)
            conn.autocommit = True
            cursor = conn.cursor()
            print("✅ Connected!")
            return
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            print("🔄 Retrying in 5 seconds...")
            time.sleep(5)

connect_db()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_users_history (
        history_id SERIAL PRIMARY KEY,
        event_time TIMESTAMP,
        operation VARCHAR(10),
        user_id INT,
        email VARCHAR(255),
        created_at_source TIMESTAMP
    );
    """)
    print("✅ Table schema check passed.")
except Exception as e:
    print(f"⚠️ Table Error: {e}")

# --- Kafka Setup ---
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"🚀 User Consumer Started (Listening to {TOPIC_NAME})")

# --- Main Loop ---
for message in consumer:
    try:
        data = message.value
        if data['payload']['after'] is None:
            continue
            
        user_data = data['payload']['after']
        op = data['payload']['op']
        
        op_map = {'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
        op_text = op_map.get(op, op)
        current_time = datetime.now()

        # Convert Timestamp
        raw_created_at = user_data.get('created_at')
        created_at_source = None
        if raw_created_at:
            try:
                created_at_source = datetime.fromtimestamp(raw_created_at / 1_000_000)
            except:
                created_at_source = None

        # SQL Query
        sql = """
            INSERT INTO dim_users_history (event_time, operation, user_id, email, created_at_source)
            VALUES (%s, %s, %s, %s, %s)
        """
        val = (current_time, op_text, user_data.get('id'), user_data.get('email'), created_at_source)

        while True:
            try:
                cursor.execute(sql, val)
                print(f"✅ Users: {op_text} - {user_data.get('email')}")
                break 
            except psycopg2.OperationalError:
                print("⚠️ DB Connection Lost. Reconnecting...")
                connect_db() 
            except Exception as e:
                print(f"❌ Data Error (Skipping): {e}")
                break 

    except Exception as main_e:
        print(f"🔥 Critical Error: {main_e}")