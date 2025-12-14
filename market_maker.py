import time
import requests
import psycopg2
import os
import uuid

# --- CONFIG ---
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_NAME = "bitka_main"

# Market Maker ID
MM_USER_ID = "market_maker_bot"

SYMBOL = "BTC_THB" 
BITKUB_SYM = "THB_BTC"
SPREAD = 0.002
ORDER_SIZE_THB = 5000 

def get_db():
    # 🔥 Retry Logic: วนลูปจนกว่าจะต่อติด
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
            return conn
        except psycopg2.OperationalError:
            print("⏳ Database not ready... waiting 3s to retry...")
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ Unexpected DB Error: {e}")
            time.sleep(3)

def get_bitkub_price():
    try:
        r = requests.get(f"https://api.bitkub.com/api/market/ticker?sym={BITKUB_SYM}")
        data = r.json()
        return float(data[BITKUB_SYM]['last'])
    except:
        return None

def main():
    print(f"🤖 Market Maker Started for {SYMBOL}")
    
    # รอต่อ DB ให้ติดก่อนเริ่มงาน
    conn = get_db()
    
    # Init User
    with conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (MM_USER_ID, 'mm@bitka.com', 3))
    
    while True:
        try:
            ref_price = get_bitkub_price()
            if not ref_price:
                time.sleep(2)
                continue
            
            bid_price = int(ref_price * (1 - SPREAD))
            ask_price = int(ref_price * (1 + SPREAD))
            
            qty_bid = round(ORDER_SIZE_THB / bid_price, 6)
            qty_ask = round(ORDER_SIZE_THB / ask_price, 6)

            # Re-connect ถ้า Connection หลุด
            if conn.closed: conn = get_db()

            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM orders WHERE user_id = %s AND symbol = %s", (MM_USER_ID, SYMBOL))
                    
                    cur.execute("""
                        INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
                        VALUES (%s, %s, %s, %s, 'limit', %s, %s, 'open')
                    """, (str(uuid.uuid4()), MM_USER_ID, SYMBOL, 'buy', bid_price, qty_bid))
                    
                    cur.execute("""
                        INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
                        VALUES (%s, %s, %s, %s, 'limit', %s, %s, 'open')
                    """, (str(uuid.uuid4()), MM_USER_ID, SYMBOL, 'sell', ask_price, qty_ask))
            
            print(f"🧱 Wall Updated: Buy {bid_price:,} | Sell {ask_price:,} (Ref: {ref_price:,})")
            time.sleep(5)

        except Exception as e:
            print(f"❌ Error: {e}")
            # ถ้า Error ให้ลองต่อ DB ใหม่
            try:
                conn.close()
            except: 
                pass
            conn = get_db()
            time.sleep(5)

if __name__ == "__main__":
    main()