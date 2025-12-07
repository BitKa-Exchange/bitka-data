# detect_anomalies_trades.py

import psycopg2
import pandas as pd
import numpy as np

DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

def load_trades(days=7):
    conn = psycopg2.connect(**DW)
    df = pd.read_sql(
        f"""
        SELECT
            event_time,
            taker_user_id AS user_id,
            symbol,
            price,
            quantity,
            price * quantity AS notional_thb
        FROM fact_matches_executed
        WHERE event_time >= NOW() - INTERVAL '{days} days'
          AND symbol LIKE '%_THB';
        """,
        conn,
    )
    conn.close()
    return df

def main():
    df = load_trades(days=7)
    if df.empty:
        print("No trades in last 7 days.")
        return

    df["notional_thb"] = pd.to_numeric(df["notional_thb"], errors="coerce").fillna(0)
    mu = df["notional_thb"].mean()
    sigma = df["notional_thb"].std(ddof=1) or 1.0

    df["z_score"] = (df["notional_thb"] - mu) / sigma
    anomalies = df[np.abs(df["z_score"]) >= 4].sort_values("notional_thb", ascending=False)

    print(f"Total trades: {len(df)}")
    print(f"Anomalies (|z|>=4): {len(anomalies)}")
    print(anomalies.head(50))

if __name__ == "__main__":
    main()
