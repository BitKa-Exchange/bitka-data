import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import sys
import logging

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

# ==========================================
# DATA LOADING & FEATURE EXTRACTION (SELF-CONTAINED)
# ==========================================

def load_features():
    """
    คำนวณ Features สำหรับ Clustering จากตารางดิบ (matches, login_history) 
    แทนการดึงจาก dim_user_segments ที่อาจยังไม่มีอยู่
    """
    logger.info("Starting feature calculation from base tables (30-day window)...")
    conn = psycopg2.connect(**DW)
    
    # SQL Query: คำนวณ Features ที่จำเป็นสำหรับ Clustering (Trades, Volume, Active Days)
    # เรา JOIN กับ users เพื่อให้มั่นใจว่าได้ user_id ทั้งหมด (รวมถึงผู้ที่ไม่เคยเทรด)
    sql_query = """
        WITH base_trades AS (
            SELECT
                taker_user_id AS user_id,
                price,
                quantity,
                created_at
            FROM matches
            WHERE created_at >= NOW() - INTERVAL '30 days'
        ),
        
        -- Feature 1, 2, 3: Volume, Trade Count, Last Trade
        trades_summary AS (
            SELECT
                user_id,
                COUNT(*) AS trades_30d,
                SUM(price * quantity) AS volume_30d_thb,
                COUNT(DISTINCT DATE(created_at)) AS active_days_30d
            FROM base_trades
            GROUP BY user_id
        )
        
        SELECT
            u.user_id,
            -- COALESCE เพื่อให้ผู้ที่ไม่เคยเทรดได้ค่า 0.0 แทน NULL
            COALESCE(t.trades_30d, 0)::NUMERIC           AS trades_30d,
            COALESCE(t.volume_30d_thb, 0)::NUMERIC       AS volume_30d_thb,
            COALESCE(t.active_days_30d, 0)::NUMERIC      AS active_days_30d,
            
            -- Feature 4: Avg Trades per Active Day
            CASE
                WHEN COALESCE(t.active_days_30d, 0) > 0
                    THEN COALESCE(t.trades_30d, 0)::NUMERIC / t.active_days_30d
                ELSE 0
            END AS avg_trades_per_active_day_30d
            
        FROM users u
        LEFT JOIN trades_summary t ON u.user_id = t.user_id;
    """

    df = pd.read_sql(sql_query, conn)
    conn.close()
    logger.info(f"Features calculated for {len(df)} users.")
    return df

def save_clusters(df_with_cluster):
    """
    บันทึกผลลัพธ์ Cluster ID กลับไปยังตาราง users โดยตรง (แทน dim_user_segments)
    """
    logger.info("Saving cluster results back to 'users' table...")
    try:
        conn = psycopg2.connect(**DW)
        cur = conn.cursor()
        
        # 1. Schema Evolution: เพิ่ม Column ในตาราง users
        cur.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS cluster_kmeans_4 INT;
            """
        )
        conn.commit()

        # 2. Update Cluster ID
        for _, row in df_with_cluster.iterrows():
            cur.execute(
                """
                UPDATE users
                SET cluster_kmeans_4 = %s
                WHERE user_id = %s;
                """,
                (int(row["cluster_kmeans_4"]), row["user_id"]),
            )
        conn.commit()
        logger.info(f"✅ Successfully saved {len(df_with_cluster)} cluster results.")
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error during cluster update: {e}")
        # Note: เราต้องรัน ALTER TABLE user REPLICA IDENTITY FULL; ใน Source DB ด้วย 
        # (ซึ่งเราไม่ได้ทำใน init.sql ของ Source DB) 
        # แต่ใน DW เราสามารถรัน UPDATE ได้ตามปกติ

def main():
    logger.info("🚀 Starting Unsupervised Segmentation (K-Means)")
    df = load_features()
    
    if df.empty or len(df) < 4: # K-Means ต้องการอย่างน้อย K=4 users
        logger.warning("⚠️ Insufficient data for clustering (need at least 4 samples).")
        return

    # ----------------------------------------------------
    # CLUSTERING PROCESS
    # ----------------------------------------------------
    
    feature_cols = [
        "trades_30d",
        "volume_30d_thb",
        "active_days_30d",
        "avg_trades_per_active_day_30d",
    ]
    
    # เตรียมข้อมูล: Fill NaN ด้วย 0.0 และดึงเฉพาะ Feature Column
    X = df[feature_cols].fillna(0.0).astype(float)
    logger.info(f"Prepared features for {len(X)} users.")

    # 1. Standardization: ปรับ Scale ให้ทุก Feature มีผลเท่ากัน
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 2. K-Means: กำหนด 4 กลุ่ม (Whale, Active, Casual, Dormant)
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    # 3. Save Result
    df["cluster_kmeans_4"] = clusters
    
    logger.info("Clustering finished. Saving results...")
    print(df[["user_id", "cluster_kmeans_4"]].head())

    # บันทึกผลลัพธ์กลับไปยังตาราง Users
    save_clusters(df)

if __name__ == "__main__":
    main()