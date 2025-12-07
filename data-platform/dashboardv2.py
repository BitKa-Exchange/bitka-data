# ======================================================
# Bitka Dashboard v2 — Improved Visualization
# ======================================================

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px

# ------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------
st.set_page_config(
    page_title="Bitka Dashboard v2",
    page_icon="📊",
    layout="wide"
)

# ------------------------------------------------------
# DB CONNECTION HELPER
# ------------------------------------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="bitka_dw",
        user="warehouse_admin",
        password="warehouse_password",
    )

def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=params)
    return df

# ------------------------------------------------------
# STYLING HELPERS
# ------------------------------------------------------
def style_fig(fig, height=400, title=None):
    fig.update_layout(
        height=height,
        margin=dict(l=40, r=40, t=60, b=40),
        template="plotly_white",
        title=title,
        title_x=0.0,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
    )
    return fig

# ------------------------------------------------------
# SIDEBAR NAV
# ------------------------------------------------------
with st.sidebar:
    st.title("📊 Bitka Dashboard v2")

    page = st.radio(
        "ไปหน้าไหนดี?",
        options=[
            "Market Overview",
            "User Intelligence",
            "Fee Lab"
        ],
        index=0
    )
    st.session_state.page = {
        "Market Overview": "market",
        "User Intelligence": "users",
        "Fee Lab": "fee_lab",
    }[page]

    st.markdown("---")
    st.caption("🧪 Data: simulated 3-month backfill\nDB: PostgreSQL (bitka_dw)")

# ------------------------------------------------------
# MARKET OVERVIEW PAGE
# ------------------------------------------------------
if st.session_state.page == "market":

    st.title("📈 Market Overview — 30-day Snapshot")

    # --------------------------------------------------------------
    # 0️⃣ LOAD LATEST 30-DAY MATCHES (+ basic sanity check)
    # --------------------------------------------------------------
    matches = query("""
        SELECT
            event_time,
            symbol,
            price,
            quantity
        FROM fact_matches_executed
        WHERE event_time >= NOW() - INTERVAL '30 days'
        ORDER BY event_time ASC;
    """)

    if matches.empty:
        st.info("No trade data yet.")
        st.stop()

    # Compute notional volume
    matches["notional"] = matches["price"] * matches["quantity"]
    matches["date"] = matches["event_time"].dt.date

    # ---------------------------------------------------------------
    # 1️⃣ TOTAL VOLUME BY DAY (ALL SYMBOLS)
    # ---------------------------------------------------------------
    st.subheader("🔹 Total Trading Volume (THB) — 30 Days")

    df_daily = (
        matches.groupby("date")["notional"]
        .sum()
        .reset_index()
        .sort_values("date")
    )

    fig_daily = go.Figure()
    fig_daily.add_trace(
        go.Bar(
            x=df_daily["date"],
            y=df_daily["notional"],
            name="Volume (THB)",
            hovertemplate="Date=%{x}<br>Volume=฿%{y:,.0f}",
        )
    )
    style_fig(fig_daily, height=360, title="Daily Notional Volume (All Pairs)")
    st.plotly_chart(fig_daily, use_container_width=True)

    # ---------------------------------------------------------------
    # 2️⃣ PER-PAIR SUMMARY + PIE CHART
    # ---------------------------------------------------------------
    st.subheader("🔹 Volume by Trading Pair")

    df_pair = (
        matches.groupby("symbol")["notional"]
        .sum()
        .reset_index()
        .sort_values("notional", ascending=False)
    )

    total_vol = df_pair["notional"].sum()

    # PIE chart — labels outside for readability
    fig_share = px.pie(
        df_pair,
        names="symbol",
        values="notional",
    )
    fig_share.update_traces(
        texttemplate="<b>%{label}</b><br>%{percent}<br>฿%{value:,.0f}",
        textposition="outside",
        hovertemplate="Pair=%{label}<br>Volume=฿%{value:,.0f}<br>%{percent}",
        showlegend=True,
    )
    style_fig(fig_share, height=380, title="Volume Distribution by Symbol (%)")
    st.plotly_chart(fig_share, use_container_width=True)

    st.caption("💡 Whale-heavy pairs จะ dominate แต่ตอนนี้ยังเห็นทุกคู่เพราะ pie chart แสดงเปอร์เซ็นต์ + มูลค่าจริง")

    # --------------------------------------------------------------
    # 3️⃣ TOP PAIRS TABLE
    # --------------------------------------------------------------
    st.subheader("🔹 Top Pairs by Volume")

    df_pair["share_pct"] = df_pair["notional"] / total_vol * 100
    df_pair_display = df_pair.copy()
    df_pair_display["notional"] = df_pair_display["notional"].round(2)
    df_pair_display["share_pct"] = df_pair_display["share_pct"].round(2)

    st.dataframe(df_pair_display, use_container_width=True)

    st.markdown("---")
    st.caption("Data source: fact_matches_executed (30 days)")

# ------------------------------------------------------
# USER INTELLIGENCE PAGE
# ------------------------------------------------------
elif st.session_state.page == "users":

    st.title("👥 User Intelligence — Segments & Behavior")

    # --------------------------------------------------------------
    # LOAD SEGMENTS + BASIC CHECK
    # --------------------------------------------------------------
    df_segments = query("""
        SELECT
            user_id,
            segment,
            trades_30d,
            volume_30d_thb,
            active_days_30d,
            avg_trades_per_active_day_30d,
            last_trade_at,
            last_login_at
        FROM dim_user_segments;
    """)

    if df_segments.empty:
        st.info("No user segments yet. Run refresh_user_segments.sql first.")
        st.stop()

    # --------------------------------------------------------------
    # 1️⃣ SEGMENT DISTRIBUTION (COUNT + SHARE)
    # --------------------------------------------------------------
    st.subheader("🔹 Segment Distribution (User Count)")

    seg_counts = (
        df_segments.groupby("segment")["user_id"]
        .count()
        .reset_index()
        .rename(columns={"user_id": "n_users"})
        .sort_values("n_users", ascending=False)
    )

    c1, c2 = st.columns([2, 1])

    with c1:
        fig = px.pie(
            seg_counts,
            names="segment",
            values="n_users",
        )
        fig.update_traces(
            texttemplate="<b>%{label}</b><br>%{percent}<br>%{value} users",
            textposition="outside",
            hovertemplate="Segment=%{label}<br>Users=%{value}<br>Share=%{percent}",
            showlegend=True,
        )
        style_fig(fig, height=380, title="User Segments (Count + Share)")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**Segment Counts**")
        seg_counts_disp = seg_counts.copy()
        total_users = seg_counts_disp["n_users"].sum()
        seg_counts_disp["share_pct"] = (seg_counts_disp["n_users"] / total_users * 100).round(2)
        st.dataframe(seg_counts_disp, use_container_width=True)

    # -------------------- Volume By Segment (log scale) --------------------
    st.subheader("📌 Volume By Segment (Last 30 Days)")

    df_vol = query("""
        SELECT s.segment, SUM(m.price*m.quantity) AS volume
        FROM fact_matches_executed m
        JOIN dim_user_segments s ON s.user_id = m.taker_user_id
        WHERE m.event_time >= NOW() - INTERVAL '30 days'
        GROUP BY s.segment
        ORDER BY volume DESC;
    """)

    if not df_vol.empty:
        fig_vol = go.Figure()
        fig_vol.add_trace(
            go.Bar(
                x=df_vol["segment"],
                y=df_vol["volume"],
                text=[f"฿{v:,.0f}" for v in df_vol["volume"]],
                textposition="outside",
                hovertemplate="Segment=%{x}<br>Volume=฿%{y:,.0f}",
            )
        )
        fig_vol.update_yaxes(type="log", title="Notional Volume (log scale)")
        style_fig(fig_vol, height=420, title="Volume by Segment (30d, log scale)")
        st.plotly_chart(fig_vol, use_container_width=True)

    # --------------------------------------------------------------
    # 2️⃣ RAW TABLE (OPTIONAL)
    # --------------------------------------------------------------
    st.subheader("🔍 Raw User Segment Table")
    st.dataframe(df_segments, use_container_width=True)

    st.markdown("---")
    st.caption("Segments มาจาก refresh_user_segments.sql + build_unsupervised_segments.py")

# ------------------------------------------------------
# FEE LAB PAGE
# ------------------------------------------------------
elif st.session_state.page == "fee_lab":

    st.title("💸 Fee Lab 2.0 – Per-Segment Revenue Curves")
    st.caption("ปรับ fee + elasticity ต่อ segment แล้วดูว่า revenue เปลี่ยนยังไง")

    # ดึง volume 30 วันล่าสุดแยกตาม segment
    df = query("""
        SELECT s.segment, SUM(m.price*m.quantity) AS volume
        FROM fact_matches_executed m
        JOIN dim_user_segments s ON s.user_id=m.taker_user_id
        WHERE m.event_time >= NOW() - INTERVAL '30 days'
        GROUP BY s.segment
        ORDER BY volume DESC;
    """)

    if df.empty:
        st.info("No trades in last 30 days. Fee Lab cannot run.")
        st.stop()

    df["volume"] = df["volume"].fillna(0)

    st.subheader("🔹 Base Volume by Segment (last 30 days)")
    base_total = df["volume"].sum()
    df_display = df.copy()
    df_display["volume_thb"] = df_display["volume"].round(2)
    df_display["share_pct"] = (df_display["volume"] / base_total * 100).round(2)
    st.dataframe(df_display[["segment", "volume_thb", "share_pct"]], use_container_width=True)

    # ---------------------- Fee Config ------------------------
    st.markdown("### 🎛 Fee Settings")

    cfee1, cfee2, cfee3 = st.columns(3)

    with cfee1:
        baseline_bps = st.number_input(
            "Baseline taker fee (bps)",
            min_value=0,
            max_value=100,
            value=15,
            step=1,
            help="ค่าธรรมเนียมปัจจุบันในหน่วย basis points (15 bps = 0.15%)",
        )
    with cfee2:
        fee_min = st.number_input(
            "Min fee (bps) for sweep",
            min_value=0,
            max_value=200,
            value=5,
            step=1,
        )
    with cfee3:
        fee_max = st.number_input(
            "Max fee (bps) for sweep",
            min_value=1,
            max_value=300,
            value=50,
            step=1,
        )

    if fee_min >= fee_max:
        st.error("fee_min ต้องน้อยกว่า fee_max")
        st.stop()

    fee_step = 1

    # ---------------------- Elasticity Input ------------------------
    st.markdown("### 🧠 Elasticity by Segment")

    st.caption(
        "Elasticity = ความไวของ volume ต่อการเปลี่ยน fee (negative = volume ลดเมื่อ fee เพิ่ม). "
        "ค่าพื้นฐานเสนอจาก behavior: whales แพ้ fee มากกว่า dormant เป็นต้น"
    )

    seg_fees = {}
    seg_elastic = {}

    for _, row in df.iterrows():
        seg_name = row["segment"]

        with st.expander(f"Segment: {seg_name}", expanded=True):
            c1, c2 = st.columns([2, 2])
            with c1:
                seg_fees[seg_name] = st.slider(
                    f"{seg_name} fee (bps)",
                    fee_min, fee_max, int(baseline_bps),
                    step=1,
                    key=f"fee_{seg_name}",
                )
            with c2:
                # ----- Determine default elasticity for this segment -----
                name = seg_name.lower()
                if "whale" in name:
                    default_elasticity = -1.2          # HFT / whale แพ้ fee มาก
                elif "active" in name:
                    default_elasticity = -0.6          # active_trader
                elif "casual" in name:
                    default_elasticity = -0.25         # casual_trader
                elif "new_explorer" in name or "new" in name:
                    default_elasticity = -0.1          # user ใหม่ ลองตลาด
                elif "dormant" in name or "inactive" in name:
                    default_elasticity = -0.02         # dormant/inactive แทบไม่สน fee
                else:
                    default_elasticity = -0.2          # fallback

                # ----- Slider -----
                seg_elastic[seg_name] = st.slider(
                    f"{seg_name} elasticity",
                    -1.5, 1.5,
                    value=default_elasticity,
                    step=0.1,
                    key=f"elas_{seg_name}",
                    help="Negative = volume ลดเมื่อ fee เพิ่ม (ปกติควรเป็นลบ)",
                )

            st.caption(
                f"Base volume (30d): ฿{row['volume']:,.2f}  |  "
                f"Current fee: {seg_fees[seg_name]} bps  |  "
                f"Elasticity: {seg_elastic[seg_name]:.2f}"
            )

    st.markdown("---")

    # ---------------------- Simulation Logic ------------------------
    st.subheader("🔹 Scenario Simulation")

    def simulate_revenue_per_segment(base_volume, base_fee_bps, new_fee_bps, elasticity):
        """
        base_volume: volume ปัจจุบัน (THB) ที่ fee_bps = base_fee_bps
        new_fee_bps: ค่าธรรมเนียมใหม่ที่ต้องการลอง
        elasticity:  (%ΔVolume / %ΔFee)
        """
        base_fee = base_fee_bps / 10000.0
        new_fee = new_fee_bps / 10000.0

        if base_fee_bps > 0:
            fee_change_pct = (new_fee_bps - base_fee_bps) / base_fee_bps
        else:
            fee_change_pct = 0.0

        adj_volume = base_volume * (1.0 + elasticity * fee_change_pct)
        if adj_volume < 0:
            adj_volume = 0.0

        revenue = adj_volume * new_fee
        return adj_volume, revenue

    rows = []
    for _, row in df.iterrows():
        seg_name = row["segment"]
        base_vol = float(row["volume"])
        new_fee_bps = seg_fees[seg_name]
        eps = seg_elastic[seg_name]

        adj_vol, rev = simulate_revenue_per_segment(
            base_volume=base_vol,
            base_fee_bps=baseline_bps,
            new_fee_bps=new_fee_bps,
            elasticity=eps,
        )
        rows.append({
            "segment": seg_name,
            "base_volume_thb": base_vol,
            "adj_volume_thb": adj_vol,
            "fee_bps": new_fee_bps,
            "revenue_thb": rev,
            "elasticity": eps,
        })

    sim_df = pd.DataFrame(rows)
    total_rev = sim_df["revenue_thb"].sum()

    st.markdown("### Snapshot at Current Settings")
    st.metric("Total Simulated Revenue (30d)", f"฿{total_rev:,.2f}")

    display_df = sim_df.copy()
    for c in ["base_volume_thb", "adj_volume_thb", "revenue_thb"]:
        display_df[c] = display_df[c].round(2)
    st.dataframe(display_df, use_container_width=True)

    st.markdown("---")
    st.subheader("📈 Revenue vs Fee Curves by Segment")

    # --------- สร้างกราฟ curve แยกทีละ segment ----------
    fee_range = list(range(fee_min, fee_max + 1, fee_step))

    for _, row in df.iterrows():
        seg_name = row["segment"]
        base_vol = float(row["volume"])
        eps = seg_elastic[seg_name]

        # สร้าง curve
        curve_rows = []
        for bps in fee_range:
            if baseline_bps > 0:
                fee_change_pct = (bps - baseline_bps) / baseline_bps
            else:
                fee_change_pct = 0.0

            adj_volume = base_vol * (1.0 + eps * fee_change_pct)
            if adj_volume < 0:
                adj_volume = 0.0

            fee_rate = bps / 10000.0
            revenue = adj_volume * fee_rate

            curve_rows.append({
                "fee_bps": bps,
                "adj_volume_thb": adj_volume,
                "revenue_thb": revenue,
            })

        curve_df = pd.DataFrame(curve_rows)
        idx_max = curve_df["revenue_thb"].idxmax()
        best_row = curve_df.loc[idx_max]

        # สร้าง figure
        fig_seg = go.Figure()
        fig_seg.add_trace(
            go.Scatter(
                x=curve_df["fee_bps"],
                y=curve_df["revenue_thb"],
                mode="lines+markers",
                name="Revenue",
                hovertemplate="Fee=%{x} bps<br>Revenue=฿%{y:,.0f}",
            )
        )

        # vline ที่จุด optimal
        fig_seg.add_vline(
            x=best_row["fee_bps"],
            line_width=2,
            line_dash="dash",
            line_color="#FFA726",
            annotation_text=f"opt={best_row['fee_bps']:.0f} bps",
            annotation_position="top right",
        )

        fig_seg.update_xaxes(title="Fee (bps)")
        fig_seg.update_yaxes(title="Revenue (THB)")
        style_fig(fig_seg, height=400, title=f"Segment: {seg_name} — Revenue vs Fee")
        st.plotly_chart(fig_seg, use_container_width=True)

    st.markdown("---")
    st.caption("Fee Lab model = simple elasticity model. ต่อไปสามารถเปลี่ยนเป็น log/log demand หรือ calibration จาก real data ได้ในอนาคต.")
