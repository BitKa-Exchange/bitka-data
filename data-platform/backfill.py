import psycopg2
import requests
import os
import uuid
import random
import time
from datetime import datetime, timedelta

# --- Config (อ่านจาก Env ของ Container Consumer/Dashboard) ---
DW_HOST = os.environ.get("DW_HOST", "warehouse")
DW_PORT = os.environ.get("DW_PORT", "5432")
DW_USER = os.environ.get("DW_USER", "warehouse_admin")
DW_PASS = os.environ.get("DW_PASS", "warehouse_password")
DW_NAME = os.environ.get("DW_NAME", "bitka_dw")

# ตั้งค่าคู่เหรียญและการแปลงค่าเงิน
PAIRS = {
    "BTCUSDT": "BTC_THB",
    "ETHUSDT": "ETH_THB",
    "DOGEUSDT": "DOGE_THB"
}
USDT_THB_RATE = 34.5  # เรทคงที่สำหรับการจำลอง

def get_connection():
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def fetch_binance_klines(symbol, interval="1h", limit=168): 
    """ดึงข้อมูลกราฟย้อนหลัง"""
    # หมายเหตุ: ค่า default ใน function definition นี้ไม่ได้ถูกใช้จริง
    # เราจะส่งค่าใหม่เข้าไปตอนเรียกใช้ฟังก์ชันนี้ใน backfill()
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        resp = requests.get(url)
        return resp.json()
    except Exception as e:
        print(f"❌ Failed to fetch {symbol}: {e}")
        return []

def generate_matches_from_candle(candle, symbol):
    """
    แตกข้อมูล 1 แท่งเทียน ให้กลายเป็นรายการเทรดย่อยๆ (Matches)
    """
    # Binance format: [Open time, Open, High, Low, Close, Volume, ...]
    open_time = datetime.fromtimestamp(candle[0] / 1000)
    close_time = datetime.fromtimestamp(candle[6] / 1000)
    
    price_open = float(candle[1]) * USDT_THB_RATE
    price_high = float(candle[2]) * USDT_THB_RATE
    price_low = float(candle[3]) * USDT_THB_RATE
    price_close = float(candle[4]) * USDT_THB_RATE
    base_vol = float(candle[5])
    
    matches = []
    
    # เทคนิค: สร้างรายการเทรดกระจายตัวใน 1 แท่งเทียน
    # 1. เทรดราคา Open
    matches.append((open_time, price_open, base_vol * 0.1))
    
    # 2. เทรดราคา High
    random_time_1 = open_time + (close_time - open_time) * 0.3
    matches.append((random_time_1, price_high, base_vol * 0.2))
    
    # 3. เทรดราคา Low
    random_time_2 = open_time + (close_time - open_time) * 0.6
    matches.append((random_time_2, price_low, base_vol * 0.2))
    
    # 4. เทรดราคา Close
    matches.append((close_time, price_close, base_vol * 0.1))
    
    # 5. เทรดสุ่มๆ อีก 3-4 ไม้ เพื่อให้ Volume ดูแน่นขึ้น
    for _ in range(4):
        rand_time = open_time + (close_time - open_time) * random.random()
        rand_price = random.uniform(price_low, price_high)
        matches.append((rand_time, rand_price, base_vol * 0.05))
        
    return matches

def backfill():
    print("⏳ Connecting to Data Warehouse...")
    
    # Retry logic
    conn = None
    for i in range(5):
        conn = get_connection()
        if conn:
            break
        print(f"   ⚠️ Connection retry {i+1}/5 in 5 seconds...")
        time.sleep(5)
    
    if not conn:
        print("❌ Could not connect to Data Warehouse. Exiting.")
        return

    cur = conn.cursor()
    total_inserted = 0
    
    print("🚀 Starting Backfill Process (High Resolution: 5 Min)...")
    
    for binance_sym, bitka_sym in PAIRS.items():
        print(f"   🔎 Checking existing data for {bitka_sym}...")
        
        # เช็คว่ามีข้อมูลเดิมไหม (ถ้ามี ข้ามเลย เพื่อกันข้อมูลซ้ำ)
        cur.execute("SELECT 1 FROM matches WHERE symbol = %s LIMIT 1", (bitka_sym,))
        if cur.fetchone():
            print(f"      ⚠️ Data for {bitka_sym} already exists. SKIPPING...")
            continue

        print(f"      📊 Fetching {binance_sym} -> {bitka_sym}...")
        
        # 🔥 แก้ไขตรงนี้: เปลี่ยนเป็น 5m และ limit 1000 🔥
        # 1000 แท่ง * 5 นาที = ข้อมูลย้อนหลังประมาณ 3.5 วัน (เพียงพอสำหรับ Demo และกราฟสวย)
        klines = fetch_binance_klines(binance_sym, interval="5m", limit=1000)
        
        matches_buffer = []
        for candle in klines:
            trades = generate_matches_from_candle(candle, bitka_sym)
            for t_time, t_price, t_qty in trades:
                match_id = str(uuid.uuid4())
                maker_id = str(uuid.uuid4())
                taker_id = str(uuid.uuid4())
                
                matches_buffer.append((match_id, bitka_sym, t_price, t_qty, maker_id, taker_id, t_time))
        
        # Batch Insert
        if matches_buffer:
            args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in matches_buffer)
            cur.execute("INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id, created_at) VALUES " + args_str)
            conn.commit()
            print(f"      ✅ Inserted {len(matches_buffer)} trades (5m interval) for {bitka_sym}")
            total_inserted += len(matches_buffer)

    cur.close()
    conn.close()
    
    if total_inserted == 0:
        print("🎉 No new data needed backfill. Everything is up to date!")
    else:
        print(f"🎉 Backfill Complete! Total {total_inserted} records added.")

if __name__ == "__main__":
    backfill()