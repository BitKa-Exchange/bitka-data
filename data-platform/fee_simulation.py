# fee_simulation.py

import psycopg2
import pandas as pd
import numpy as np

# การตั้งค่า Database (เหมือนเดิม)
DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)


def get_volume_by_segment(days=30) -> pd.DataFrame:
    """
    ดึง volume (THB) แยกตาม segment
    หมายเหตุ: ปรับให้คำนวณ Segment สดจากตาราง 'matches' (Schema ใหม่) 
    โดยใช้ Logic เดียวกับ old.sql เพื่อให้ทำงานได้ทันทีโดยไม่ต้องรอสร้างตาราง dim
    """
    conn = psycopg2.connect(**DW)
    
    # SQL นี้ทำ 3 ขั้นตอนในครั้งเดียว:
    # 1. user_stats: สรุปยอดเทรดรายคนจากตาราง matches
    # 2. user_segments: จัดกลุ่ม user เป็น whale, active, casual ฯลฯ
    # 3. Final Select: รวม Volume ตามกลุ่ม
    query = f"""
        WITH user_stats AS (
            SELECT
                taker_user_id AS user_id,
                COUNT(*) AS trade_count,
                SUM(price * quantity) AS total_volume_thb
            FROM matches
            WHERE created_at >= NOW() - INTERVAL '{days} days'
              AND symbol LIKE '%_THB'
            GROUP BY taker_user_id
        ),
        user_segments AS (
            SELECT
                user_id,
                total_volume_thb,
                CASE
                    -- Logic การแบ่งกลุ่ม (Simplified version of old.sql)
                    WHEN total_volume_thb >= 5000000 THEN 'whale'
                    WHEN trade_count >= 50 THEN 'active_trader'
                    WHEN trade_count BETWEEN 5 AND 49 THEN 'casual_trader'
                    WHEN trade_count BETWEEN 1 AND 4 THEN 'new_explorer'
                    ELSE 'dormant'
                END AS segment
            FROM user_stats
        )
        SELECT
            segment,
            SUM(total_volume_thb) AS volume_thb
        FROM user_segments
        GROUP BY segment
        ORDER BY volume_thb DESC;
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error executing query: {e}")
        df = pd.DataFrame(columns=["segment", "volume_thb"])
    finally:
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
    จำลองรายได้ (Revenue) ตามค่าธรรมเนียม (Fee) ที่กำหนด
    """
    rows = []
    for _, row in volume_df.iterrows():
        seg = row["segment"]
        base_volume = float(row["volume_thb"])
        fee_bps = fee_schedule_bps.get(seg, default_bps)

        # ปรับ volume ตาม elasticity (ความอ่อนไหวต่อราคา)
        if baseline_bps is not None and elasticity != 0:
            # เปอร์เซ็นต์เปลี่ยน fee เทียบ baseline
            if baseline_bps == 0:
                fee_change_pct = 0 
            else:
                fee_change_pct = (fee_bps - baseline_bps) / baseline_bps
                
            vol_factor = 1.0 + elasticity * fee_change_pct
            adj_volume = max(0.0, base_volume * vol_factor)
        else:
            adj_volume = base_volume

        # คำนวณรายได้: Volume * (Basis Points / 10000)
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
    ทดสอบ Fee หลายๆ ช่วงเพื่อหาจุดที่ทำรายได้สูงสุด
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
    print("--- Starting Fee Simulation ---")
    volume_df = get_volume_by_segment(days=30)

    print("\n=== Volume by segment (last 30d) ===")
    print(volume_df)

    if volume_df.empty:
        print("\n❗ No trade data found in last 30 days.")
        print("   → Make sure the producer/simulator is running and generating trades.")
        return

    # -----------------------------
    # Scenario 1: flat 0.15%
    # -----------------------------
    flat_15 = simulate_fee_revenue(volume_df, fee_schedule_bps={}, default_bps=15)
    print("\n=== Scenario 1: Flat taker fee 0.15% (15 bps) ===")
    print(flat_15)
    print(f"Total revenue: {flat_15['revenue_thb'].sum():,.2f} THB")

    # -----------------------------
    # Scenario 2: flat 0.25%
    # -----------------------------
    flat_25 = simulate_fee_revenue(volume_df, fee_schedule_bps={}, default_bps=25)
    print("\n=== Scenario 2: Flat taker fee 0.25% (25 bps) ===")
    print(flat_25)
    print(f"Total revenue: {flat_25['revenue_thb'].sum():,.2f} THB")

    # -----------------------------
    # Scenario 3: segmented fee
    # -----------------------------
    seg_fee = {
        "whale": 5,          # Whale ได้ถูกสุด
        "active_trader": 10,
        "casual_trader": 18,
        "new_explorer": 25,  # ขาจรแพงหน่อย
        "dormant": 20,
    }
    seg_scenario = simulate_fee_revenue(
        volume_df, fee_schedule_bps=seg_fee, default_bps=20
    )
    print("\n=== Scenario 3: Segmented fee schedule ===")
    print(seg_scenario)
    print(f"Total revenue: {seg_scenario['revenue_thb'].sum():,.2f} THB")

    # -----------------------------
    # Fee sweep (optimization curve)
    # -----------------------------
    print("\n=== Fee sweep (flat fee, no elasticity) ===")
    sweep_df = sweep_fee_range(volume_df, bps_range=range(5, 51), elasticity=0.0)
    print(sweep_df.head())
    
    best_idx = sweep_df["total_revenue_thb"].idxmax()
    best_row = sweep_df.loc[best_idx]
    print(f"\nBest fee (no elasticity): {best_row['fee_bps']:.0f} bps -> Revenue: {best_row['total_revenue_thb']:,.2f} THB")

    # -----------------------------
    # Sweep with elasticity example
    # -----------------------------
    # สมมติ elasticity = -0.5 (ถ้าราคาขึ้น 10%, volume ลด 5%)
    print("\n=== Fee sweep with elasticity = -0.5 (volume drops if fee up) ===")
    sweep_el = sweep_fee_range(
        volume_df, bps_range=range(5, 51), elasticity=-0.5, baseline_bps=15
    )
    best_row_el = sweep_el.loc[sweep_el["total_revenue_thb"].idxmax()]
    print(f"Best fee (elasticity=-0.5, baseline_bps=15): {best_row_el['fee_bps']:.0f} bps -> Revenue: {best_row_el['total_revenue_thb']:,.2f} THB")


if __name__ == "__main__":
    main()