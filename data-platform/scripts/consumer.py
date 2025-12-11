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

required_vars = [
    ("KAFKA_BROKER", KAFKA_BROKER), 
    ("DW_HOST", DW_HOST), 
    ("DW_USER", DW_USER), 
    ("DW_PASS", DW_PASS)
]
for var_name, value in required_vars:
    if not value:
        raise ValueError(f"❌ Missing required environment variable: {var_name}")
    
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
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        print(f"❌ DB Connect Error: {e}")
        return None

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
                    # Microseconds check
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
    
    # ✅ 1. เรียกใช้ clean_data โดยส่ง table_name ไปด้วย
    cleaned_buffer = [clean_data(row, table_name) for row in buffer]
    
    # 2. ระบุ Primary Key ของแต่ละตาราง
    pkey = "id" # Default
    if table_name == "orders": pkey = "order_id"
    elif table_name == "users": pkey = "user_id"
    elif table_name == "matches": pkey = "match_id"
    elif table_name == "deposits": pkey = "tx_id"
    elif table_name == "withdrawals": pkey = "withdrawal_id"
    elif table_name == "tickers": pkey = "symbol"
    elif table_name == "audit_logs": pkey = "log_id"
    elif table_name == "login_history": pkey = "id"

    # 3. Deduplicate: กรองเอาเฉพาะข้อมูลล่าสุดของ Key นั้นๆ ใน Batch นี้
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
        values = [[row[k] for k in keys] for row in final_batch]
        
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
        print(f"✅ Inserted {len(final_batch)} rows into {table_name} (Cleaned & Deduped)")
        
    except Exception as e:
        print(f"⚠️ Batch Insert Error ({table_name}): {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    print("⏳ Waiting for Kafka...")
    time.sleep(10) 
    
    print("🚀 Starting Consumer...")
    consumer = KafkaConsumer(
        *TOPIC_MAPPING.keys(), 
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='bitka_warehouse_group_v2' 
    )

    buffers = {table: [] for table in TOPIC_MAPPING.values()}
    last_flush_time = time.time()
    
    conn = get_db_connection()

    for message in consumer:
        topic = message.topic
        if topic not in TOPIC_MAPPING: continue
        
        table_name = TOPIC_MAPPING[topic]
        val = message.value
        
        data = None
        
        # Logic แกะข้อมูล (เหมือนเดิม)
        if isinstance(val, dict) and 'payload' in val:
            payload = val.get('payload')
            if payload and 'after' in payload:
                data = payload['after']
        elif isinstance(val, dict) and 'payload' not in val:
            data = val
            
        if data:
            buffers[table_name].append(data)
            
            # ---------------------------------------------------------
            # ✅ ส่วนที่ต้องเพิ่ม: เช็คว่าต้องบันทึกหรือยัง (Flush Logic)
            # ---------------------------------------------------------
            
            # 1. เช็คจำนวน: ถ้า Buffer ของตารางนี้เต็ม (ครบ 100 แถว) ให้บันทึกทันที
            if len(buffers[table_name]) >= BATCH_SIZE:
                print(f"📦 Batch full for {table_name}, flushing...")
                process_batch(conn, table_name, buffers[table_name])
                buffers[table_name] = [] # เคลียร์ Buffer

        # 2. เช็คเวลา: ถ้าผ่านไปนานเกิน 5 วินาที ให้บันทึกทุกตาราง (ป้องกันข้อมูลค้าง)
        current_time = time.time()
        if current_time - last_flush_time > FLUSH_INTERVAL:
            # วนลูปรอบทุกตารางที่มีข้อมูลค้างอยู่
            for tbl, buf in buffers.items():
                if buf:
                    print(f"⏰ Time limit reached, flushing {tbl}...")
                    process_batch(conn, tbl, buf)
                    buffers[tbl] = [] # เคลียร์ Buffer
            last_flush_time = current_time

if __name__ == "__main__":
    main()