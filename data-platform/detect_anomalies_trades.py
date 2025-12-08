# detect_anomalies_trades.py

import psycopg2
import pandas as pd
import numpy as np

# การตั้งค่าการเชื่อมต่อ (เหมือนเดิม)
DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

def load_trades(days=7):
    conn = psycopg2.connect(**DW)
    
    # ปรับ Query ให้ตรงกับ new.sql (Table: matches, Time: created_at)
    query = f"""
        SELECT
            created_at AS event_time,
            taker_user_id AS user_id,
            symbol,
            price,
            quantity,
            (price * quantity) AS notional_thb
        FROM matches
        WHERE created_at >= NOW() - INTERVAL '{days} days'
          AND symbol LIKE '%_THB';
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def main():
    print("Loading trades from Data Warehouse...")
    df = load_trades(days=7)
    
    if df.empty:
        print("No trades found in the last 7 days.")
        return

    # แปลงข้อมูลให้เป็นตัวเลขเพื่อคำนวณ (เผื่อกรณี Decimal จาก DB มาเป็น Object)
    df["notional_thb"] = pd.to_numeric(df["notional_thb"], errors="coerce").fillna(0)
    
    # คำนวณ Z-Score
    mu = df["notional_thb"].mean()
    sigma = df["notional_thb"].std(ddof=1)
    
    # ป้องกันการหารด้วยศูนย์กรณีที่มี trade เดียวหรือค่าเท่ากันหมด
    if sigma == 0:
        sigma = 1.0

    df["z_score"] = (df["notional_thb"] - mu) / sigma
    
    # หาค่าผิดปกติ (Anomalies) ที่มี Z-Score มากกว่าหรือเท่ากับ 4 (หรือน้อยกว่า -4)
    anomalies = df[np.abs(df["z_score"]) >= 4].sort_values("notional_thb", ascending=False)

    print(f"Total trades analyzed: {len(df)}")
    print(f"Anomalies found (|z|>=4): {len(anomalies)}")
    
    if not anomalies.empty:
        print("\nTop 50 Anomalies:")
        # แสดงผลโดยจัด Format ให้อ่านง่ายขึ้น
        print(anomalies.head(50).to_string(index=False))
    else:
        print("\nNo anomalies detected.")

if __name__ == "__main__":
    main()