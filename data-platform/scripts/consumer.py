"""
Bitka Data Platform - Data Warehouse Consumer
---------------------------------------------
Author: Data Engineering Team
Description: 
    Script นี้ทำหน้าที่เป็น Kafka Consumer เพื่อดึงข้อมูล Change Data Capture (CDC) 
    จาก Kafka Topics และทำการ Sync ข้อมูลลงไปยัง Data Warehouse (PostgreSQL)
    
    Features:
    - Batch Processing (ตามจำนวน records หรือ เวลาที่กำหนด)
    - Data Type Fixing (Timestamp & JSON handling)
    - In-batch Deduplication (จัดการข้อมูลซ้ำใน Batch เดียวกัน)
    - Upsert Logic (Insert or Update on Conflict)
"""

import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values
from datetime import datetime

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================

# Kafka & Database Connection Settings
# ใช้ Environment Variables เพื่อความปลอดภัยและความยืดหยุ่นในแต่ละ Environment (Dev/Prod)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

# Batch Settings
BATCH_SIZE = 100        # จำนวน Rows ที่จะสะสมก่อนยิงเข้า DB
FLUSH_INTERVAL = 5      # เวลาสูงสุด (วินาที) ที่จะรอหากข้อมูลไม่ครบ Batch

# Topic to Table Mapping
# Key: Kafka Topic Name -> Value: Destination Table Name
TOPIC_MAPPING = {
    "bitka.public.orders": "orders",
    "bitka.public.matches": "matches",
    "bitka.public.users": "users",
    "bitka.public.deposits": "deposits",
    "bitka.public.withdrawals": "withdrawals",
    "bitka.public.tickers": "tickers",
    "bitka.public.login_history": "login_history",
    "bitka.public.audit_logs": "audit_logs"
}

# ==========================================
# DATABASE HELPER FUNCTIONS
# ==========================================

def get_db_connection():
    """
    สร้าง Connection ไปยัง Data Warehouse (PostgreSQL)
    Returns:
        psycopg2.connection หรือ None หากเชื่อมต่อไม่สำเร็จ
    """
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        print(f"❌ DB Connect Error: {e}")
        return None

def fix_data_types(row):
    """
    ทำความสะอาดและแปลงชนิดข้อมูลก่อนนำเข้า Database
    
    Args:
        row (dict): ข้อมูลดิบจาก Debezium payload
    Returns:
        dict: ข้อมูลที่ผ่านการแปลง Type เรียบร้อยแล้ว
    """
    new_row = row.copy()
    
    # 1. Timestamp Normalization
    # Debezium อาจส่งค่ามาเป็น Microseconds (16 หลัก) แต่ Python datetime ต้องการ Seconds (10 หลัก)
    time_cols = ['created_at', 'updated_at']
    for col in time_cols:
        if col in new_row and new_row[col] is not None:
            val = new_row[col]
            if isinstance(val, (int, float)):
                try:
                    # Threshold check: ถ้าเลขมากกว่า 10 หลัก สันนิษฐานว่าเป็น Microseconds
                    if val > 9999999999: 
                        val = val / 1_000_000
                    new_row[col] = datetime.fromtimestamp(val)
                except Exception:
                    pass # หากแปลงไม่ได้ ให้คงค่าเดิมไว้ (Fail-safe)

    # 2. JSON Serialization
    # Postgres Driver ต้องการ JSON String สำหรับคอลัมน์ประเภท JSONB
    json_cols = ['details_before', 'details_after']
    for col in json_cols:
        if col in new_row and isinstance(new_row[col], dict):
            new_row[col] = json.dumps(new_row[col])

    return new_row

# ==========================================
# CORE PROCESSING LOGIC
# ==========================================

def process_batch(conn, table_name, buffer):
    """
    ประมวลผลข้อมูล 1 Batch: แปลงข้อมูล -> กรองซ้ำ -> ยิง SQL Upsert
    """
    if not buffer: return
    
    # Step 1: Pre-process data types
    cleaned_buffer = [fix_data_types(row) for row in buffer]
    
    # Step 2: Identify Primary Key based on table name
    # TODO: ในอนาคตอาจย้าย Logic นี้ไปไว้ใน Config Dictionary เพื่อลด hardcode
    pkey = "id" # Default PK
    if table_name == "orders": pkey = "order_id"
    elif table_name == "users": pkey = "user_id"
    elif table_name == "matches": pkey = "match_id"
    elif table_name == "deposits": pkey = "tx_id"
    elif table_name == "withdrawals": pkey = "withdrawal_id"
    elif table_name == "tickers": pkey = "symbol"
    elif table_name == "audit_logs": pkey = "log_id"
    elif table_name == "login_history": pkey = "id"

    # Step 3: In-Batch Deduplication (Last-Write-Wins Strategy)
    # หากใน Batch มี ID เดียวกันหลายรายการ (เช่น Create แล้ว Update ทันที)
    # เราจะเก็บเฉพาะ State ล่าสุดเท่านั้นเพื่อลดภาระ Database และป้องกัน Error
    deduplicated_map = {}
    for row in cleaned_buffer:
        if pkey in row:
            row_key = row[pkey]
            deduplicated_map[row_key] = row # ทับค่าเก่าด้วยค่าใหม่เสมอ
    
    final_batch = list(deduplicated_map.values())
    if not final_batch: return

    # Step 4: Construct & Execute Upsert Query
    cursor = conn.cursor()
    try:
        keys = final_batch[0].keys()
        columns = ','.join(keys)
        values = [[row[k] for k in keys] for row in final_batch]
        
        # Logic สำหรับ Tickers (Market Data) - เน้น Update ตลอดเวลา
        if table_name == "tickers":
            sql = f"""
                INSERT INTO {table_name} ({columns}) VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                last_price = EXCLUDED.last_price,
                volume_24h = EXCLUDED.volume_24h,
                updated_at = EXCLUDED.updated_at
            """
        else:
            # Logic สำหรับ Transaction/Master Data - Update ทุกฟิลด์เมื่อ Key ชนกัน
            set_clause = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT ({pkey}) DO UPDATE SET {set_clause}"

        # ใช้ execute_values เพื่อประสิทธิภาพสูงในการ Insert หลายแถวพร้อมกัน
        execute_values(cursor, sql, values)
        conn.commit()
        print(f"✅ Inserted {len(final_batch)} rows into {table_name} (Deduplicated from {len(buffer)})")
        
    except Exception as e:
        print(f"⚠️ Batch Insert Error ({table_name}): {e}")
        conn.rollback()
    finally:
        cursor.close()

# ==========================================
# MAIN EXECUTION LOOP
# ==========================================

def main():
    # Wait for Kafka service to be fully ready
    print("⏳ Waiting for Kafka...")
    time.sleep(10) 
    
    print("🚀 Starting Consumer...")
    
    # Initialize Kafka Consumer
    # auto_offset_reset='earliest': อ่านข้อมูลตั้งแต่ต้นถ้าไม่เคยมี Offset
    # group_id: ระบุ Consumer Group (ใช้ v2 เพื่อเริ่มอ่านใหม่กรณีแก้ Logic หรือเปลี่ยน Schema)
    consumer = KafkaConsumer(
        *TOPIC_MAPPING.keys(), 
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='bitka_warehouse_group_v2' 
    )

    # Buffer Storage: { 'table_name': [row1, row2, ...] }
    buffers = {table: [] for table in TOPIC_MAPPING.values()}
    last_flush_time = time.time()
    
    conn = get_db_connection()

    # Main Polling Loop
    for message in consumer:
        topic = message.topic
        if topic not in TOPIC_MAPPING: continue
        
        table_name = TOPIC_MAPPING[topic]
        payload = message.value.get('payload')
        
        # Extract only 'after' state (Change Data Capture)
        if payload and payload.get('after'):
            data = payload['after']
            buffers[table_name].append(data)
            
        # Check Flush Conditions
        current_time = time.time()
        is_time_up = (current_time - last_flush_time) >= FLUSH_INTERVAL
        
        for tbl, buf in buffers.items():
            # Trigger flush if Batch Size reached OR Time Interval passed
            if len(buf) >= BATCH_SIZE or (is_time_up and len(buf) > 0):
                process_batch(conn, tbl, buf)
                buffers[tbl] = [] # Clear buffer after processing
        
        # Reset timer if flush occurred
        if is_time_up:
            last_flush_time = current_time

if __name__ == "__main__":
    main()