import psycopg2
import time
import uuid
import os
import sys

# --- CONFIG ---
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_NAME = "bitka_main"

SYMBOL = "BTC_THB"
WHALE_ID = "a0000000-0000-0000-0000-000000000001"
ORDER_VALUE_THB = 100_000_000_000_000  # 100 ล้านบาท! 💸

def get_db():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
            return conn
        except psycopg2.OperationalError:
            print("⏳ Connecting to Database...")
            time.sleep(1)

def main():
    if len(sys.argv) < 2:
        print("⚠️  Usage: python whale_bot.py [buy|sell]")
        return

    side = sys.argv[1].lower()
    if side not in ['buy', 'sell']:
        print("❌ Invalid side. Use 'buy' or 'sell'")
        return

    print(f"🐋 WHALE INCOMING! Preparing to {side.upper()} {ORDER_VALUE_THB:,} THB...")
    
    conn = get_db()
    
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. สร้าง User วาฬก่อน (ถ้ายังไม่มี)
                cur.execute("INSERT INTO users (user_id, email, kyc_level) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
                           (WHALE_ID, 'whale@ocean.com', 3))

                # 2. เช็คราคาตลาดล่าสุด
                cur.execute(f"SELECT last_price FROM tickers WHERE symbol='{SYMBOL}'")
                res = cur.fetchone()
                current_price = float(res[0]) if res else 3000000.0
                
                # 3. คำนวณราคาและจำนวน
                # เทคนิค: เพื่อให้กราฟขยับ เราต้องตั้งราคาให้เวอร์กว่าตลาด (กวาด Order Book)
                if side == 'buy':
                    # จะซื้อ: ยอมจ่ายแพงกว่าตลาด 20% เพื่อกวาด Offer ให้เรียบ
                    target_price = int(current_price * 5.20)
                    # คำนวณจำนวน BTC ที่จะได้ (ประมาณการ)
                    qty = round(ORDER_VALUE_THB / current_price, 4) 
                else:
                    # จะขาย: ยอมขายถูกกว่าตลาด 20% เพื่อทุบ Bid ให้ยับ
                    target_price = int(current_price * 0.80)
                    qty = round(ORDER_VALUE_THB / current_price, 4)

                print(f"💰 Current Price: {current_price:,.2f}")
                print(f"🎯 Target Price (Aggressive): {target_price:,.2f}")
                print(f"📦 Quantity: {qty} BTC")

                # 4. ส่งคำสั่งยักษ์ลงตลาด
                order_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status)
                    VALUES (%s, %s, %s, %s, 'limit', %s, %s, 'open')
                """, (order_id, WHALE_ID, SYMBOL, side, target_price, qty))
                
                print(f"🚀 ORDER PLACED! {side.upper()} {qty} BTC @ {target_price:,} THB")
                print("🌊 Watch the dashboard for tsunami...")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
