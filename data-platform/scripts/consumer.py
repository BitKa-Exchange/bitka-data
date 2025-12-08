import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values
from datetime import datetime

# --- Config ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

BATCH_SIZE = 100
FLUSH_INTERVAL = 5 

TOPIC_MAPPING = {
    "bitka.public.orders": "orders",
    "bitka.public.matches": "matches",
    "bitka.public.users": "users",
    "bitka.public.deposits": "deposits",
    "bitka.public.withdrawals": "withdrawals",
    "bitka.public.tickers": "tickers",
    "bitka.public.login_history": "login_history"
}

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        print(f"❌ DB Connect Error: {e}")
        return None

# --- ฟังก์ชันช่วยแปลง Timestamp ---
def fix_data_types(row):
    """
    แปลง Unix Timestamp (int) ให้เป็น Python Datetime Object
    เพื่อให้ Postgres เข้าใจ
    """
    new_row = row.copy()
    time_cols = ['created_at', 'updated_at']
    
    for col in time_cols:
        if col in new_row and new_row[col] is not None:
            val = new_row[col]
            # ถ้ามาเป็นตัวเลข (int/float) ให้แปลงเป็น datetime
            if isinstance(val, (int, float)):
                try:
                    # Debezium บางทีส่งมาเป็น Microseconds (เลขหลักล้านล้าน)
                    # ถ้าเลขเยอะเกิน 11 หลัก ให้หาร 1,000,000 เพื่อเป็นวินาที
                    if val > 9999999999: 
                        val = val / 1_000_000
                    
                    new_row[col] = datetime.fromtimestamp(val)
                except Exception:
                    pass # ถ้าแปลงไม่ได้ให้ปล่อยไว้เหมือนเดิม
    return new_row

def process_batch(conn, table_name, buffer):
    if not buffer: return
    
    # 1. แปลงข้อมูล Type (Timestamp) ให้ถูกต้องก่อน
    cleaned_buffer = [fix_data_types(row) for row in buffer]
    
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
    # โดยใช้ Dictionary (เพราะ Key ซ้ำไม่ได้ ถ้าใส่ Key เดิม Value จะถูกทับด้วยตัวใหม่ล่าสุด)
    deduplicated_map = {}
    for row in cleaned_buffer:
        # ถ้า row ไม่มี key ที่เราต้องการ (เช่น ข้อมูลขยะ) ให้ข้าม
        if pkey in row:
            row_key = row[pkey]
            deduplicated_map[row_key] = row # ค่าเก่าจะถูกทับด้วยค่าใหม่เสมอ
    
    # แปลงกลับเป็น List เพื่อเตรียม Insert
    final_batch = list(deduplicated_map.values())
    
    if not final_batch: return

    cursor = conn.cursor()
    try:
        keys = final_batch[0].keys()
        columns = ','.join(keys)
        values = [[row[k] for k in keys] for row in final_batch]
        
        # สร้าง SQL Query
        if table_name == "tickers":
            # Tickers ต้อง Update เสมอ
            sql = f"""
                INSERT INTO {table_name} ({columns}) VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                last_price = EXCLUDED.last_price,
                volume_24h = EXCLUDED.volume_24h,
                updated_at = EXCLUDED.updated_at
            """
        else:
            # Table อื่นๆ Upsert ปกติ
            set_clause = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT ({pkey}) DO UPDATE SET {set_clause}"

        execute_values(cursor, sql, values)
        conn.commit()
        print(f"✅ Inserted {len(final_batch)} rows into {table_name} (Deduplicated form {len(buffer)})")
        
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
        group_id='bitka_warehouse_group'
    )

    buffers = {table: [] for table in TOPIC_MAPPING.values()}
    last_flush_time = time.time()
    
    conn = get_db_connection()

    for message in consumer:
        topic = message.topic
        if topic not in TOPIC_MAPPING: continue
        
        table_name = TOPIC_MAPPING[topic]
        payload = message.value.get('payload')
        
        if payload and payload.get('after'):
            data = payload['after']
            buffers[table_name].append(data)
            
        current_time = time.time()
        is_time_up = (current_time - last_flush_time) >= FLUSH_INTERVAL
        
        for tbl, buf in buffers.items():
            if len(buf) >= BATCH_SIZE or (is_time_up and len(buf) > 0):
                process_batch(conn, tbl, buf)
                buffers[tbl] = [] 
        
        if is_time_up:
            last_flush_time = current_time

if __name__ == "__main__":
    main()