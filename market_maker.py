import time
import requests
import psycopg2
import os
import uuid
import random
import json

# --- Config ---
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_NAME = "bitka_main"

# คู่เหรียญที่เราจะทำ MM (Binance Symbol -> Bitka Symbol)
# หมายเหตุ: เราจะใช้เรท USDT/THB คงที่ประมาณ 34.5 เพื่อความง่ายในการแปลง
PAIRS = {
    "BTCUSDT": "BTC_THB",
    "ETHUSDT": "ETH_THB",
    "DOGEUSDT": "DOGE_THB"
}
USDT_THB_RATE = 34.5 

SPREAD_PERCENT = 0.005  # 0.5% Spread (ช่องว่างระหว่างราคาซื้อกับขาย)
ORDER_SIZE_MIN = 1000   # บาท
ORDER_SIZE_MAX = 50000  # บาท

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
    except Exception as e:
        print(f"❌ DB Connect Error: {e}")
        return None

def get_binance_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        return float(data['price'])
    except Exception as e:
        print(f"⚠️ Binance API Error: {e}")
        return None

def ensure_bot_user(conn):
    """สร้าง User พิเศษสำหรับ Bot (ถ้ายังไม่มี)"""
    bot_id = "00000000-0000-0000-0000-000000000001" # Fixed UUID for Bot
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users WHERE user_id = %s", (bot_id,))
        if not cur.fetchone():
            print("🤖 Creating Market Maker Bot User...")
            cur.execute(
                "INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s)",
                (bot_id, "market_maker_bot@bitka.com", 3)
            )
            conn.commit()
    return bot_id

def place_orders(conn, bot_id):
    for binance_sym, bitka_sym in PAIRS.items():
        # 1. ดึงราคาจริง
        raw_price = get_binance_price(binance_sym)
        if not raw_price: continue
        
        # 2. แปลงเป็น THB
        mid_price = raw_price * USDT_THB_RATE
        
        # 3. คำนวณ Bid (รับซื้อต่ำกว่าราคาจริง) / Ask (ขายแพงกว่าราคาจริง)
        # Random Spread นิดหน่อยเพื่อความเนียน (0.4% - 0.6%)
        dynamic_spread = SPREAD_PERCENT * random.uniform(0.8, 1.2)
        
        bid_price = mid_price * (1 - dynamic_spread) # ตั้งรับซื้อ ต่ำกว่าตลาด
        ask_price = mid_price * (1 + dynamic_spread) # ตั้งขาย สูงกว่าตลาด
        
        # 4. ยิงคำสั่งลง DB
        with conn.cursor() as cur:
            sql = """
                INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # --- Place BID (Buy) ---
            bid_thb_amt = random.uniform(ORDER_SIZE_MIN, ORDER_SIZE_MAX)
            bid_qty = bid_thb_amt / bid_price
            cur.execute(sql, (str(uuid.uuid4()), bot_id, bitka_sym, "buy", "limit", bid_price, bid_qty, "open"))
            
            # --- Place ASK (Sell) ---
            ask_thb_amt = random.uniform(ORDER_SIZE_MIN, ORDER_SIZE_MAX)
            ask_qty = ask_thb_amt / ask_price
            cur.execute(sql, (str(uuid.uuid4()), bot_id, bitka_sym, "sell", "limit", ask_price, ask_qty, "open"))
            
        conn.commit()
        print(f"🤖 [MM] {bitka_sym}: Real={mid_price:,.2f} | Bid={bid_price:,.2f} | Ask={ask_price:,.2f}")

def main():
    print("🚀 Market Maker Bot Starting...")
    time.sleep(10) # รอ DB พร้อม
    
    conn = get_db_connection()
    if not conn: return
    
    bot_id = ensure_bot_user(conn)
    
    while True:
        try:
            place_orders(conn, bot_id)
            # พัก 5 วินาที แล้วทำรอบใหม่ (ความถี่ในการ update ราคา)
            time.sleep(5) 
        except Exception as e:
            print(f"❌ MM Loop Error: {e}")
            conn = get_db_connection() # Reconnect try
            time.sleep(5)

if __name__ == "__main__":
    main()