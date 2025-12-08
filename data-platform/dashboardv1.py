#########################
#  Bitka Dashboard v1.2 (Updated Schema)
#########################

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go


# --------------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------------
st.set_page_config(
    page_title="Bitka Command Center v1.2",
    page_icon="🚀",
    layout="wide",
)

# --------------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------------
DW = dict(
    host="localhost",
    port="5433",
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

# --------------------------------------------------------
# QUERY HELPER
# --------------------------------------------------------
@st.cache_data(ttl=2)
def run_query(query, params=None):
    try:
        conn = psycopg2.connect(**DW)
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


# --------------------------------------------------------
# AUTO REFRESH (3 seconds)
# --------------------------------------------------------
st_autorefresh(interval=3000, key="bitka_refresh")

st.title("🚀 Bitka Exchange — Command Center v1.2")


# ========================================================
# 1) GLOBAL FILTERS (ALL TIME WINDOWS)
# ========================================================

colA, colB = st.columns([2, 1])

time_window_label = colA.selectbox(
    "Time Window",
    [
        "1 minute",
        "5 minutes",
        "15 minutes",
        "30 minutes",
        "1 hour",
        "5 hours",
        "1 day",
        "3 days",
        "7 days",
        "1 month",
        "3 months",
    ],
    index=2  # default = 15 minutes
)

symbol = colB.selectbox(
    "Symbol",
    ["ALL", "BTC_THB", "ETH_THB", "USDT_THB"],
    index=1
)

# แปลง label → SQL interval
window_map = {
    "1 minute": "1 minute",
    "5 minutes": "5 minutes",
    "15 minutes": "15 minutes",
    "30 minutes": "30 minutes",
    "1 hour": "1 hour",
    "5 hours": "5 hours",
    "1 day": "1 day",
    "3 days": "3 days",
    "7 days": "7 days",
    "1 month": "30 days",
    "3 months": "90 days",
}

window_sql = window_map[time_window_label]


# ========================================================
# 2) LOAD DATA (Updated Table & Column Names)
# ========================================================

# Tickers: ใช้ updated_at แทน event_time
tickers = run_query(f"""
    SELECT *, updated_at as event_time 
    FROM tickers
    WHERE updated_at >= NOW() - INTERVAL '{window_sql}'
    ORDER BY updated_at ASC;
""")

# Orders: ใช้ created_at แทน event_time
orders = run_query(f"""
    SELECT *, created_at as event_time 
    FROM orders
    WHERE created_at >= NOW() - INTERVAL '{window_sql}'
    ORDER BY created_at DESC;
""")

# Matches: ใช้ created_at แทน event_time
matches = run_query(f"""
    SELECT *, created_at as event_time 
    FROM matches
    WHERE created_at >= NOW() - INTERVAL '{window_sql}'
    ORDER BY created_at DESC;
""")

# Logins: เปลี่ยนจาก dim_user_logins เป็น login_history
logins = run_query(f"""
    SELECT *, created_at as event_time 
    FROM login_history
    WHERE created_at >= NOW() - INTERVAL '{window_sql}'
    ORDER BY created_at DESC;
""")

# Deposits: ใช้ created_at แทน event_time
deposits = run_query(f"""
    SELECT *, created_at as event_time 
    FROM deposits
    WHERE created_at >= NOW() - INTERVAL '{window_sql}'
    ORDER BY created_at DESC;
""")

# Normalize timestamps
# หมายเหตุ: เรา Alias ชื่อคอลัมน์เวลาเป็น 'event_time' ใน SQL แล้ว เพื่อให้โค้ดส่วนล่างไม่ต้องแก้เยอะ
for df in [tickers, orders, matches, logins, deposits]:
    if not df.empty and "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")


# SYMBOL FILTER
if symbol != "ALL":
    # กรองเฉพาะตารางที่มี column symbol
    if not tickers.empty: tickers = tickers[tickers["symbol"] == symbol]
    if not orders.empty: orders = orders[orders["symbol"] == symbol]
    if not matches.empty: matches = matches[matches["symbol"] == symbol]


# ========================================================
# 3) SYSTEM HEALTH
# ========================================================

def compute_lag(df):
    if df.empty:
        return "No data"
    latest = df["event_time"].max()
    if pd.isna(latest):
        return "No data"

    lag = (datetime.utcnow() - latest.to_pydatetime()).total_seconds()

    if lag < 5:
        return f"🟢 {lag:.1f}s (FRESH)"
    elif lag < 60:
        return f"🟠 {lag:.1f}s (SLOW)"
    else:
        return f"🔴 {lag:.1f}s (STALE)"


col1, col2, col3 = st.columns(3)
col1.metric("📡 Ticker Freshness", compute_lag(tickers))
col2.metric("🤝 Trade Freshness", compute_lag(matches))
col3.metric("📝 Order Freshness", compute_lag(orders))

st.markdown("---")


# ========================================================
# 4) LINE GRAPH AUTO-SMOOTH
# ========================================================

def adaptive_downsample(df, max_points):
    """Reduce number of points depending on target max_points."""
    if len(df) <= max_points:
        return df
    step = max(1, len(df) // max_points)
    return df.iloc[::step]


def smooth_factor_for_window(window_label):
    """
    Bigger window → fewer points (smoother line).
    Smaller window → more points (more detail).
    """
    factors = {
        "1 minute": 800,
        "5 minutes": 700,
        "15 minutes": 600,
        "30 minutes": 500,
        "1 hour": 450,
        "5 hours": 400,
        "1 day": 350,
        "3 days": 300,
        "7 days": 250,
        "1 month": 200,
        "3 months": 150,
    }
    return factors.get(window_label, 300)


# ========================================================
# 5) CHART RENDER (LINE + CANDLE)
# ========================================================

st.subheader(f"📈 Market Overview — {symbol}")

chart_type = st.radio(
    "Chart Type",
    ["Line", "Candlestick"],
    index=0,
    horizontal=True,
)

if tickers.empty:
    st.info("No ticker data available.")
else:
    tick = tickers.copy()
    tick["event_time"] = pd.to_datetime(tick["event_time"])
    tick["last_price"] = pd.to_numeric(tick["last_price"], errors="coerce")
    tick = tick.dropna(subset=["last_price"]).sort_values("event_time")

    # ----------------------------
    # LINE MODE (AutoSmooth v1.1)
    # ----------------------------
    if chart_type == "Line":
        max_points = smooth_factor_for_window(time_window_label)
        tick_ds = adaptive_downsample(tick, max_points)

        fig_line = go.Figure()
        fig_line.add_trace(
            go.Scatter(
                x=tick_ds["event_time"],
                y=tick_ds["last_price"],
                mode="lines",
                line=dict(width=2, color="#00CC96"),
                name="Price",
            )
        )
        fig_line.update_layout(
            template="plotly_dark",
            title=f"Price Movement ({symbol}) — {time_window_label}",
            height=450,
            hovermode="x unified",
            margin=dict(l=40, r=20, t=60, b=40),
            xaxis_title="Time",
            yaxis_title="Price",
        )
        fig_line.update_xaxes(showgrid=False)
        fig_line.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.1)")
        st.plotly_chart(fig_line, use_container_width=True)

    # ----------------------------
    # CANDLESTICK MODE
    # ----------------------------
    else:
        # Dynamic candle timeframe per window
        tf_map = {
            "1 minute": "30S",     # ถ้ามี tick ถี่ ใช้ 30 วินาที
            "5 minutes": "1T",
            "15 minutes": "1T",
            "30 minutes": "2T",
            "1 hour": "5T",
            "5 hours": "15T",
            "1 day": "30T",
            "3 days": "1H",
            "7 days": "2H",
            "1 month": "4H",
            "3 months": "1D",
        }
        tf_rule = tf_map.get(time_window_label, "5T")

        ohlc = (
            tick.set_index("event_time")["last_price"]
            .resample(tf_rule)
            .agg(["first", "max", "min", "last"])
            .dropna()
            .rename(
                columns={
                    "first": "open",
                    "max": "high",
                    "min": "low",
                    "last": "close",
                }
            )
        )

        if len(ohlc) < 2:
            st.info("Not enough data for candlestick.")
        else:
            df_candle = ohlc.reset_index()

            fig = go.Figure(
                data=[
                    go.Candlestick(
                        x=df_candle["event_time"],
                        open=df_candle["open"],
                        high=df_candle["high"],
                        low=df_candle["low"],
                        close=df_candle["close"],
                        increasing_line_color="#00CC96",
                        decreasing_line_color="#EF553B",
                        name="Price",
                    )
                ]
            )

            fig.update_layout(
                template="plotly_dark",
                title=f"Candlestick ({symbol}) — {time_window_label}",
                height=450,
                hovermode="x unified",
                margin=dict(l=40, r=20, t=60, b=40),
                xaxis=dict(
                    rangeslider=dict(visible=True),
                    showgrid=False,
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(255,255,255,0.1)",
                    title="Price",
                ),
            )

            st.plotly_chart(fig, use_container_width=True)


# ========================================================
# 6) KPIs + USER / FINANCE PANELS
# ========================================================

st.markdown("---")

total_trades = len(matches)
notional = (matches["price"] * matches["quantity"]).sum() if not matches.empty else 0
avg_trade_size = matches["quantity"].mean() if not matches.empty else 0

tc1, tc2, tc3 = st.columns(3)
tc1.metric("Trades", total_trades)
tc2.metric("Notional Traded", f"฿{notional:,.2f}")
tc3.metric("Avg Trade Size", f"{avg_trade_size:,.4f}")

st.markdown("---")

colU, colF = st.columns(2)

with colU:
    st.subheader("👥 User Activity")
    active_users = logins["user_id"].nunique() if not logins.empty else 0
    
    # Check for 'failed' status in login_history (assuming case insensitive)
    if not logins.empty and "status" in logins.columns:
        failed_logins = logins["status"].astype(str).str.lower().str.contains("fail").sum()
    else:
        failed_logins = 0

    st.metric("Active Users", active_users)
    st.metric("Failed Logins", failed_logins)

    # Display columns available in new schema
    login_cols = [c for c in logins.columns if c in ["event_time", "user_id", "ip_address", "device_id", "status"]]
    st.dataframe(
        logins[login_cols],
        hide_index=True,
        use_container_width=True,
    )

with colF:
    st.subheader("💳 Finance")
    total_deposits = deposits["amount"].sum() if not deposits.empty else 0
    st.metric("Net Deposits", f"฿{total_deposits:,.2f}")

    # Display columns available in new schema
    deposit_cols = [c for c in deposits.columns if c in ["event_time", "user_id", "asset", "amount", "network", "status"]]
    st.dataframe(
        deposits[deposit_cols],
        hide_index=True,
        use_container_width=True,
    )

# ========================================================
# 7) RAW DATA TABS (ถ้ายังอยากดู detail)
# ========================================================
st.markdown("---")
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Market Data", "⚖️ Trades", "📝 Orders", "🛡️ Security Logs"]
)

with tab1:
    st.dataframe(tickers, hide_index=True, use_container_width=True)

with tab2:
    st.dataframe(matches, hide_index=True, use_container_width=True)

with tab3:
    st.dataframe(orders, hide_index=True, use_container_width=True)

with tab4:
    st.dataframe(logins, hide_index=True, use_container_width=True)