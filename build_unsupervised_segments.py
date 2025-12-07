# build_unsupervised_segments.py

import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

def load_features():
    conn = psycopg2.connect(**DW)
    df = pd.read_sql(
        """
        SELECT
            user_id,
            trades_30d,
            volume_30d_thb,
            active_days_30d,
            avg_trades_per_active_day_30d
        FROM dim_user_segments;
        """,
        conn,
    )
    conn.close()
    return df

def save_clusters(df_with_cluster):
    conn = psycopg2.connect(**DW)
    cur = conn.cursor()
    cur.execute(
        """
        ALTER TABLE dim_user_segments
        ADD COLUMN IF NOT EXISTS cluster_kmeans_4 INT;
        """
    )
    conn.commit()

    for _, row in df_with_cluster.iterrows():
        cur.execute(
            """
            UPDATE dim_user_segments
            SET cluster_kmeans_4 = %s
            WHERE user_id = %s;
            """,
            (int(row["cluster_kmeans_4"]), row["user_id"]),
        )
    conn.commit()
    cur.close()
    conn.close()

def main():
    df = load_features()
    if df.empty:
        print("No data in dim_user_segments.")
        return

    feature_cols = [
        "trades_30d",
        "volume_30d_thb",
        "active_days_30d",
        "avg_trades_per_active_day_30d",
    ]
    X = df[feature_cols].fillna(0.0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    df["cluster_kmeans_4"] = clusters
    print(df[["user_id", "cluster_kmeans_4"]].head())

    save_clusters(df)

if __name__ == "__main__":
    main()
