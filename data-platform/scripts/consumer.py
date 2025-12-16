import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values
from datetime import datetime

# --- Config ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
DW_HOST = os.environ.get("DW_HOST")
DW_PORT = os.environ.get("DW_PORT", "5432")
DW_USER = os.environ.get("DW_USER")
DW_PASS = os.environ.get("DW_PASS")
DW_NAME = os.environ.get("DW_NAME", "bitka_dw")

# ตรวจสอบ Environment Variables
required_vars = [
    ("KAFKA_BROKER", KAFKA_BROKER), 
    ("DW_HOST", DW_HOST), 
    ("DW_USER", DW_USER), 
    ("DW_PASS", DW_PASS)
]
for var_name, value in required_vars:
    if not value:
        print(f"⚠️ Warning: {var_name} is missing, utilizing defaults or risking failure.")

BATCH_SIZE = 100
FLUSH_INTERVAL = 5 

TOPIC_MAPPING = {
    "bitka.public.orders": "orders",
    "bitka.public.matches": "matches",
    "bitka.public.users": "users",
    "bitka.public.deposits": "deposits",
    "bitka.public.withdrawals": "withdrawals",
    "bitka.public.tickers": "tickers",
    "bitka.public.login_history": "login_history",
    "app.events.audit_logs": "audit_logs"
}

def get_db_connection():
    """เชื่อมต่อ Database พร้อมระบบ Retry"""
    while True:
        try:
            conn = psycopg2.connect(
                host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
            )
            print("✅ Connected to Data Warehouse")
            return conn
        except Exception as e:
            print(f"⏳ DW Connect Failed ({e}), retrying in 5s...")
            time.sleep(5)

def clean_data(row, table_name):
    """
    ทำความสะอาดข้อมูล แปลง Type และจัดการ Format ให้เป็นมาตรฐาน
    """
    new_row = row.copy()
    
    # List คอลัมน์ที่ต้องจัดการพิเศษ
    time_cols = ['created_at', 'updated_at', 'completed_at', 'login_time']
    json_cols = ['details_before', 'details_after', 'metadata']
    numeric_cols = ['amount', 'price', 'fee', 'balance', 'last_price', 'volume_24h']

    for key, val in new_row.items():
        # 1. String Cleaning: ตัดช่องว่างหน้าหลัง
        if isinstance(val, str):
            val = val.strip()
            # ถ้าเป็น string ว่างเปล่า ให้เป็น None (NULL)
            if val == "":
                val = None
            new_row[key] = val

        # 2. Numeric Cleaning: จัดการตัวเลขที่อาจมาเป็น String ว่าง
        if key in numeric_cols and val == "":
            new_row[key] = None

    # 3. Table Specific Cleaning (User Email)
    if table_name == 'users' and 'email' in new_row and new_row['email']:
        # แปลง Email เป็นตัวเล็กทั้งหมด
        new_row['email'] = new_row['email'].lower()

    # 4. Timestamp Handling (int -> datetime)
    for col in time_cols:
        if col in new_row and new_row[col] is not None:
            val = new_row[col]
            if isinstance(val, (int, float)):
                try:
                    # Microseconds check (Unix Timestamp vs Milliseconds)
                    if val > 9999999999: 
                        val = val / 1_000_000
                    new_row[col] = datetime.fromtimestamp(val)
                except Exception:
                    pass 

    # 5. JSON Handling (dict -> json string)
    for col in json_cols:
        if col in new_row and isinstance(new_row[col], dict):
            new_row[col] = json.dumps(new_row[col])

    return new_row

def process_batch(conn, table_name, buffer):
    if not buffer: return
    
    # Clean Data
    cleaned_buffer = [clean_data(row, table_name) for row in buffer]
    
    # ระบุ Primary Key
    pkey = "id" # Default
    if table_name == "orders": pkey = "order_id"
    elif table_name == "users": pkey = "user_id"
    elif table_name == "matches": pkey = "match_id"
    elif table_name == "deposits": pkey = "tx_id"
    elif table_name == "withdrawals": pkey = "withdrawal_id"
    elif table_name == "tickers": pkey = "symbol"
    elif table_name == "audit_logs": pkey = "log_id"
    elif table_name == "login_history": pkey = "id"

    # Deduplicate: เอาข้อมูลล่าสุดของ Key นั้นๆ ใน Batch นี้
    deduplicated_map = {}
    for row in cleaned_buffer:
        if pkey in row:
            row_key = row[pkey]
            deduplicated_map[row_key] = row 
    
    final_batch = list(deduplicated_map.values())
    
    if not final_batch: return

    cursor = conn.cursor()
    try:
        keys = final_batch[0].keys()
        columns = ','.join(keys)
        values = [[row.get(k) for k in keys] for row in final_batch]
        
        # สร้าง SQL Query
        if table_name == "tickers":
            sql = f"""
                INSERT INTO {table_name} ({columns}) VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                last_price = EXCLUDED.last_price,
                volume_24h = EXCLUDED.volume_24h,
                updated_at = EXCLUDED.updated_at
            """
        else:
            set_clause = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT ({pkey}) DO UPDATE SET {set_clause}"

        execute_values(cursor, sql, values)
        conn.commit()
        print(f"✅ Inserted {len(final_batch)} rows into {table_name}")
        
    except Exception as e:
        print(f"⚠️ Batch Insert Error ({table_name}): {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    print("⏳ Waiting for Kafka to be ready...")
    time.sleep(10) 
    
    # เชื่อมต่อ Database ครั้งแรก
    conn = get_db_connection()

    print("🚀 Starting Consumer...")
    try:
        consumer = KafkaConsumer(
            *TOPIC_MAPPING.keys(), 
            bootstrap_servers=KAFKA_BROKER,
            # 🔥 แก้ไขจุดที่ 1: เพิ่ม if x else None ป้องกัน Error
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            auto_offset_reset='earliest',
            group_id='bitka_warehouse_group_v3' 
        )
    except Exception as e:
        print(f"❌ Kafka Connection Error: {e}")
        return

    buffers = {table: [] for table in TOPIC_MAPPING.values()}
    last_flush_time = time.time()
    
    for message in consumer:
        # 🔥 แก้ไขจุดที่ 2: ถ้าข้อมูลเป็น None (Decode ไม่ได้ หรือ Tombstone) ให้ข้าม
        if message.value is None:
            continue

        topic = message.topic
        if topic not in TOPIC_MAPPING: continue
        
        table_name = TOPIC_MAPPING[topic]
        val = message.value
        
        data = None
        
        # Logic แกะข้อมูล Debezium (Envelope)
        if isinstance(val, dict) and 'payload' in val:
            payload = val.get('payload')
            # ถ้าเป็น Delete operation (op='d'), 'after' จะเป็น null
            # ในที่นี้เราจะข้าม Delete ไปก่อน หรือถ้าจะทำต้องเช็ค op
            if payload and 'after' in payload:
                data = payload['after']
        elif isinstance(val, dict) and 'payload' not in val:
            # กรณีไม่ได้ใช้ Debezium Envelope
            data = val
            
        if data:
            buffers[table_name].append(data)
            
            # Flush Check: ถ้า Buffer เต็ม
            if len(buffers[table_name]) >= BATCH_SIZE:
                print(f"📦 Batch full for {table_name}, flushing...")
                
                # Reconnect DB if closed
                if conn.closed: conn = get_db_connection()
                
                process_batch(conn, table_name, buffers[table_name])
                buffers[table_name] = [] 

        # Time-based Flush
        current_time = time.time()
        if current_time - last_flush_time > FLUSH_INTERVAL:
            if conn.closed: conn = get_db_connection()
            
            for tbl, buf in buffers.items():
                if buf:
                    print(f"⏰ Time limit reached, flushing {tbl}...")
                    process_batch(conn, tbl, buf)
                    buffers[tbl] = [] 
            last_flush_time = current_time

if __name__ == "__main__":
    main()