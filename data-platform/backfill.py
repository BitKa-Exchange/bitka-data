import psycopg2
import requests
import os
import uuid
import random
import time
from datetime import datetime, timedelta

# --- Config ---
DW_HOST = os.environ.get("DW_HOST", "warehouse")
DW_PORT = os.environ.get("DW_PORT", "5432")
DW_USER = os.environ.get("DW_USER", "warehouse_admin")
DW_PASS = os.environ.get("DW_PASS", "warehouse_password")
DW_NAME = os.environ.get("DW_NAME", "bitka_dw")

# คู่เหรียญ
PAIRS = {
    "BTCUSDT": "BTC_THB",
    "ETHUSDT": "ETH_THB",
    "DOGEUSDT": "DOGE_THB"
}

def get_connection():
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

# 🔥 ฟังก์ชันใหม่: หาเรทค่าเงินที่ทำให้กราฟต่อกันเนียนที่สุด
def calculate_implied_rate():
    print("🧮 Calibrating Exchange Rate...")
    try:
        # 1. ราคา Bitkub ปัจจุบัน
        bk_res = requests.get("https://api.bitkub.com/api/market/ticker?sym=THB_BTC")
        bk_price = float(bk_res.json()['THB_BTC']['last'])
        
        # 2. ราคา Binance ปัจจุบัน
        bn_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        bn_price = float(bn_res.json()['price'])
        
        # 3. คำนวณ Ratio
        implied_rate = bk_price / bn_price
        print(f"   ✅ Calibrated Rate: 1 USD = {implied_rate:.2f} THB (Based on BTC)")
        return implied_rate
    except Exception as e:
        print(f"   ⚠️ Calibration failed ({e}), using default 34.0")
        return 34.0

def fetch_binance_klines(symbol, interval="5m", limit=1000): 
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        resp = requests.get(url)
        return resp.json()
    except Exception as e:
        print(f"❌ Failed to fetch {symbol}: {e}")
        return []

def generate_matches_from_candle(candle, symbol, rate):
    # Binance: [Time, Open, High, Low, Close, Vol, ...]
    open_time = datetime.fromtimestamp(candle[0] / 1000)
    close_time = datetime.fromtimestamp(candle[6] / 1000)
    
    # 🔥 คูณ Rate ที่คำนวณมาใหม่
    price_open = float(candle[1]) * rate
    price_high = float(candle[2]) * rate
    price_low = float(candle[3]) * rate
    price_close = float(candle[4]) * rate
    base_vol = float(candle[5])
    
    matches = []
    
    # กระจาย Trade ในแท่งเทียน
    matches.append((open_time, price_open, base_vol * 0.1))
    matches.append((open_time + (close_time - open_time) * 0.3, price_high, base_vol * 0.2))
    matches.append((open_time + (close_time - open_time) * 0.6, price_low, base_vol * 0.2))
    matches.append((close_time, price_close, base_vol * 0.1))
    
    for _ in range(3):
        rand_time = open_time + (close_time - open_time) * random.random()
        rand_price = random.uniform(price_low, price_high)
        matches.append((rand_time, rand_price, base_vol * 0.05))
        
    return matches

def backfill():
    print("⏳ Connecting to Data Warehouse...")
    conn = None
    for i in range(5):
        conn = get_connection()
        if conn: break
        time.sleep(5)
    
    if not conn: return

    # 🔥 คำนวณเรทก่อนเริ่มงาน
    CURRENT_RATE = calculate_implied_rate()

    cur = conn.cursor()
    
    # 🔥 ล้างข้อมูลเก่าก่อนเสมอ (เพื่อให้กราฟใหม่ทั้งชุด)
    print("🧹 Cleaning old mismatched data...")
    cur.execute("TRUNCATE TABLE matches, orders, tickers CASCADE;")
    conn.commit()

    total_inserted = 0
    print(f"🚀 Starting Backfill (Using Rate: {CURRENT_RATE:.2f})...")
    
    for binance_sym, bitka_sym in PAIRS.items():
        print(f"      📊 Fetching {binance_sym} -> {bitka_sym}...")
        klines = fetch_binance_klines(binance_sym, interval="5m", limit=1000) # ย้อนหลัง ~3.5 วัน
        
        matches_buffer = []
        for candle in klines:
            # ส่ง Rate เข้าไปคูณ
            trades = generate_matches_from_candle(candle, bitka_sym, CURRENT_RATE)
            for t_time, t_price, t_qty in trades:
                match_id = str(uuid.uuid4())
                matches_buffer.append((match_id, bitka_sym, t_price, t_qty, str(uuid.uuid4()), str(uuid.uuid4()), t_time))
        
        if matches_buffer:
            args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in matches_buffer)
            cur.execute("INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id, created_at) VALUES " + args_str)
            conn.commit()
            print(f"      ✅ Inserted {len(matches_buffer)} trades for {bitka_sym}")
            total_inserted += len(matches_buffer)

    cur.close()
    conn.close()
    print("🎉 Backfill Complete! Graph should be smooth now.")

if __name__ == "__main__":
    backfill()