# fee_simulation.py

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


def get_volume_by_segment(days=30) -> pd.DataFrame:
    """
    ดึง volume (THB) แยกตาม segment จาก dim_user_segments + fact_matches_executed
    """
    conn = psycopg2.connect(**DW)
    query = f"""
        SELECT
            s.segment,
            SUM(m.price * m.quantity) AS volume_thb
        FROM fact_matches_executed m
        JOIN dim_user_segments s
          ON s.user_id = m.taker_user_id
        WHERE m.event_time >= NOW() - INTERVAL '{days} days'
          AND m.symbol LIKE '%_THB'
        GROUP BY s.segment
        ORDER BY volume_thb DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["volume_thb"] = df["volume_thb"].fillna(0)
    return df


def simulate_fee_revenue(
    volume_df: pd.DataFrame,
    fee_schedule_bps: dict,
    default_bps: float,
    elasticity: float = 0.0,
    baseline_bps: float | None = None,
) -> pd.DataFrame:
    """
    volume_df: columns = [segment, volume_thb]
    fee_schedule_bps: mapping segment -> fee_bps
    default_bps: ใช้ถ้า segment ไม่อยู่ใน dict
    elasticity: ค่าความไวของ volume ต่อการเปลี่ยนแปลง fee (ประมาณ)
        0   = volume ไม่เปลี่ยนเลย
        -1  = ถ้า fee เพิ่ม 10% → volume ลด 10%
        +1  = ถ้า fee เพิ่ม 10% → volume เพิ่ม 10% (ไม่ค่อย real แต่เอาไว้เล่น)
    baseline_bps: fee เดิมที่ volume_thb ถูกวัดจากมัน
        ถ้า None → ถือว่า volume เป็น baseline ที่ fee ตอนนั้น
    """
    rows = []
    for _, row in volume_df.iterrows():
        seg = row["segment"]
        base_volume = float(row["volume_thb"])
        fee_bps = fee_schedule_bps.get(seg, default_bps)

        # ปรับ volume ตาม elasticity (ง่าย ๆ)
        if baseline_bps is not None and elasticity != 0:
            # เปอร์เซ็นต์เปลี่ยน fee เทียบ baseline
            fee_change_pct = (fee_bps - baseline_bps) / baseline_bps
            vol_factor = 1.0 + elasticity * fee_change_pct
            adj_volume = max(0.0, base_volume * vol_factor)
        else:
            adj_volume = base_volume

        revenue = adj_volume * (fee_bps / 10000.0)

        rows.append(
            dict(
                segment=seg,
                base_volume_thb=base_volume,
                adj_volume_thb=adj_volume,
                fee_bps=fee_bps,
                revenue_thb=revenue,
            )
        )

    return pd.DataFrame(rows)


def sweep_fee_range(
    volume_df: pd.DataFrame,
    bps_range=range(5, 51),
    elasticity: float = 0.0,
    baseline_bps: float | None = None,
) -> pd.DataFrame:
    """
    กวาดลอง fee หลาย ๆ ค่า (flat fee) แล้วดู total revenue
    """
    data = []
    for bps in bps_range:
        sim = simulate_fee_revenue(
            volume_df,
            fee_schedule_bps={},
            default_bps=bps,
            elasticity=elasticity,
            baseline_bps=baseline_bps,
        )
        total_rev = sim["revenue_thb"].sum()
        data.append({"fee_bps": bps, "total_revenue_thb": total_rev})
    return pd.DataFrame(data)


def main():
    volume_df = get_volume_by_segment(days=30)

    print("=== Volume by segment (last 30d) ===")
    print(volume_df)

    if volume_df.empty:
        print("\n❗ No trade data found in last 30 days.")
        print("   → Make sure backfill/producer has generated trades.")
        return

    # -----------------------------
    # Scenario 1: flat 0.15%
    # -----------------------------
    flat_15 = simulate_fee_revenue(volume_df, fee_schedule_bps={}, default_bps=15)
    print("\n=== Scenario 1: Flat taker fee 0.15% (15 bps) ===")
    print(flat_15)
    print("Total revenue:", flat_15["revenue_thb"].sum())

    # -----------------------------
    # Scenario 2: flat 0.25%
    # -----------------------------
    flat_25 = simulate_fee_revenue(volume_df, fee_schedule_bps={}, default_bps=25)
    print("\n=== Scenario 2: Flat taker fee 0.25% (25 bps) ===")
    print(flat_25)
    print("Total revenue:", flat_25["revenue_thb"].sum())

    # -----------------------------
    # Scenario 3: segmented fee
    # -----------------------------
    seg_fee = {
        "whale": 5,
        "active_trader": 10,
        "casual_trader": 18,
        "new_explorer": 25,
        "dormant": 20,
    }
    seg_scenario = simulate_fee_revenue(
        volume_df, fee_schedule_bps=seg_fee, default_bps=20
    )
    print("\n=== Scenario 3: Segmented fee schedule ===")
    print(seg_scenario)
    print("Total revenue:", seg_scenario["revenue_thb"].sum())

    # -----------------------------
    # Fee sweep (optimization curve)
    # -----------------------------
    print("\n=== Fee sweep (flat fee, no elasticity) ===")
    sweep_df = sweep_fee_range(volume_df, bps_range=range(5, 51), elasticity=0.0)
    print(sweep_df.head())
    print("Best fee (no elasticity):")
    best_row = sweep_df.loc[sweep_df["total_revenue_thb"].idxmax()]
    print(best_row)

    # -----------------------------
    # Sweep with elasticity example
    # -----------------------------
    print("\n=== Fee sweep with elasticity = -0.5 (volume drops if fee up) ===")
    sweep_el = sweep_fee_range(
        volume_df, bps_range=range(5, 51), elasticity=-0.5, baseline_bps=15
    )
    best_row_el = sweep_el.loc[sweep_el["total_revenue_thb"].idxmax()]
    print("Best fee (elasticity=-0.5, baseline_bps=15):")
    print(best_row_el)


if __name__ == "__main__":
    main()
