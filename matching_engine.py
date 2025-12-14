import psycopg2
import time
import os
import uuid

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_NAME = "bitka_main"

SYMBOL = "BTC_THB"

def get_db():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
            return conn
        except psycopg2.OperationalError:
            print("⏳ Matching Engine waiting for DB...")
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ DB Error: {e}")
            time.sleep(3)

def match_orders():
    print("⚙️ Matching Engine Starting...")
    conn = get_db()
    
    while True:
        try:
            if conn.closed: conn = get_db()
            
            with conn:
                with conn.cursor() as cur:
                    # ดึง Bid สูงสุด
                    cur.execute(f"SELECT order_id, price, quantity, user_id FROM orders WHERE symbol='{SYMBOL}' AND side='buy' AND status='open' ORDER BY price DESC, created_at ASC LIMIT 1")
                    best_bid = cur.fetchone()
                    
                    # ดึง Ask ต่ำสุด
                    cur.execute(f"SELECT order_id, price, quantity, user_id FROM orders WHERE symbol='{SYMBOL}' AND side='sell' AND status='open' ORDER BY price ASC, created_at ASC LIMIT 1")
                    best_ask = cur.fetchone()
                    
                    if not best_bid or not best_ask:
                        time.sleep(0.5)
                        continue
                        
                    bid_id, bid_price, bid_qty, bid_user = best_bid
                    ask_id, ask_price, ask_qty, ask_user = best_ask
                    
                    # 🔥 แปลงเป็น float เพื่อการเปรียบเทียบที่ปลอดภัยและคำนวณ
                    f_bid_price = float(bid_price)
                    f_ask_price = float(ask_price)
                    f_bid_qty = float(bid_qty)
                    f_ask_qty = float(ask_qty)

                    if f_bid_price < f_ask_price:
                        time.sleep(0.5)
                        continue
                        
                    # Match!
                    match_price = f_bid_price # ให้ราคาคนซื้อเป็นหลัก
                    match_qty = min(f_bid_qty, f_ask_qty)
                    
                    print(f"⚡ MATCH! {match_qty} BTC @ {match_price:,.2f} THB")
                    
                    # Insert Match
                    cur.execute("""
                        INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (str(uuid.uuid4()), SYMBOL, match_price, match_qty, ask_user, bid_user))
                    
                    # Update Ticker
                    cur.execute("""
                        INSERT INTO tickers (symbol, last_price, volume_24h) VALUES (%s, %s, %s)
                        ON CONFLICT (symbol) DO UPDATE SET last_price = EXCLUDED.last_price, volume_24h = tickers.volume_24h + EXCLUDED.volume_24h
                    """, (SYMBOL, match_price, match_qty))
                    
                    # ลบ Order ที่ Match แล้ว (Simple Logic)
                    cur.execute("DELETE FROM orders WHERE order_id IN (%s, %s)", (bid_id, ask_id))
                    
        except Exception as e:
            print(f"❌ Matching Error: {e}")
            try: conn.close()
            except: pass
            conn = get_db()
            time.sleep(2)

if __name__ == "__main__":
    match_orders()