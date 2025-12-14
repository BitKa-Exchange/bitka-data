import psycopg2
import time
import random
import uuid
import os

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
            print("⏳ Simulator waiting for DB...")
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ DB Error: {e}")
            time.sleep(3)

def main():
    print("👥 User Traffic Simulator Started...")
    conn = get_db()
    users = []
    
    while True:
        try:
            if conn.closed: conn = get_db()

            with conn:
                with conn.cursor() as cur:
                    # 1. สร้าง User ใหม่บ้างเป็นบางครั้ง
                    if not users or random.random() < 0.1:
                        uid = str(uuid.uuid4())
                        cur.execute("INSERT INTO users (user_id, email) VALUES (%s, %s)", (uid, f"user_{uid[:4]}@gmail.com"))
                        users.append(uid)
                        print(f"New User joined: {uid[:8]}")
                    
                    if not users: 
                        time.sleep(1)
                        continue

                    # 2. จำลองการเทรด
                    user = random.choice(users)
                    side = random.choice(['buy', 'sell'])
                    
                    # ดึงราคาล่าสุด
                    cur.execute(f"SELECT price FROM orders WHERE symbol='{SYMBOL}' AND side != '{side}' ORDER BY created_at DESC LIMIT 1")
                    res = cur.fetchone()
                    
                    # 🔥 แก้ไขตรงนี้: แปลง Decimal เป็น float ก่อนใช้งาน
                    market_price = float(res[0]) if res else 3000000.0 
                    
                    price_offset = random.uniform(-0.005, 0.005) 
                    
                    # คำนวณราคา
                    my_price = int(market_price * (1 + price_offset))
                    qty = round(random.uniform(0.001, 0.01), 6)
                    
                    cur.execute("""
                        INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
                        VALUES (%s, %s, %s, %s, 'limit', %s, %s, 'open')
                    """, (str(uuid.uuid4()), user, SYMBOL, side, my_price, qty))
                    
                    print(f"🛒 User {user[:4]} placed {side.upper()} @ {my_price:,}")
            
            time.sleep(random.uniform(0.5, 2.0)) # เร็วขึ้นหน่อย

        except Exception as e:
            print(f"Error: {e}")
            try: conn.close()
            except: pass
            conn = get_db()
            time.sleep(2)

if __name__ == "__main__":
    main()