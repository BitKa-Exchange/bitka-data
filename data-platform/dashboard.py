import streamlit as st
import pandas as pd
import psycopg2
import os
import time
import altair as alt
from datetime import timedelta

# --- Config ---
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

st.set_page_config(page_title="Bitka Pro Trade", page_icon="🕯️", layout="wide")

# --- Styling ---
# ปรับ Theme สีให้ดูเหมือน Trading Platform (Dark Mode Recommended)
st.markdown("""
<style>
    .stMetric {
        background-color: #1e1e1e;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #333;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
    }
</style>
""", unsafe_allow_html=True)

ALL_TABLES = ["orders", "matches", "users", "deposits", "withdrawals", "tickers", "login_history", "audit_logs"]

# --- Database Functions ---
@st.cache_resource
def get_connection():
    try:
        return psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
    except Exception as e:
        st.error(f"❌ DB Connection Failed: {e}")
        return None

def load_kpi_data(conn):
    try:
        # Market Data
        df_ticker = pd.read_sql("SELECT symbol, last_price, volume_24h FROM tickers ORDER BY symbol", conn)
        # User Stats
        df_users = pd.read_sql("SELECT count(*) as count FROM users", conn)
        return df_ticker, df_users
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

def load_market_data(conn, symbol, time_frame='1Min'):
    """
    ดึงข้อมูล Trade History มาคำนวณเป็นแท่งเทียน (OHLCV)
    """
    try:
        # 1. Matches (Trade History) -> แปลงเป็น Candlestick
        # ดึงย้อนหลังเยอะหน่อยเพื่อให้พล็อตได้สวย
        trade_sql = f"""
            SELECT created_at, price, quantity 
            FROM matches 
            WHERE symbol = '{symbol}' 
            AND created_at >= NOW() - INTERVAL '7 DAYS'  -- 👈 แก้จาก 6 HOURS เป็น 7 DAYS
            ORDER BY created_at ASC
        """
        df_trades = pd.read_sql(trade_sql, conn)
        
        df_ohlc = pd.DataFrame()
        if not df_trades.empty:
            df_trades['created_at'] = pd.to_datetime(df_trades['created_at'])
            df_trades.set_index('created_at', inplace=True)
            
            # Resample ข้อมูลดิบให้เป็นแท่งเทียน (Open, High, Low, Close, Volume)
            df_ohlc = df_trades.resample(time_frame).agg({
                'price': ['first', 'max', 'min', 'last'],
                'quantity': 'sum'
            })
            df_ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
            df_ohlc.dropna(inplace=True) # ตัดช่วงเวลาที่ไม่มีการเทรดออก
            df_ohlc.reset_index(inplace=True)

        # 2. Order Book Depth
        depth_sql = f"""
            SELECT side, price, SUM(quantity) as volume 
            FROM orders 
            WHERE symbol = '{symbol}' AND status = 'open'
            GROUP BY side, price
            ORDER BY price ASC
        """
        df_depth = pd.read_sql(depth_sql, conn)
        
        # 3. Recent Trades List
        recent_trades_sql = f"""
            SELECT price, quantity, created_at, 
                   CASE WHEN taker_user_id IS NOT NULL THEN 'sell' ELSE 'buy' END as side -- สมมติ Logic
            FROM matches 
            WHERE symbol = '{symbol}' 
            ORDER BY created_at DESC LIMIT 15
        """
        df_recent = pd.read_sql(recent_trades_sql, conn)

        return df_ohlc, df_depth, df_recent
        
    except Exception as e:
        st.error(f"Error loading market data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def load_table_data(conn, table_name):
    try:
        sort_col = "created_at"
        if table_name == "tickers": sort_col = "updated_at"
        query = f"SELECT * FROM {table_name} ORDER BY {sort_col} DESC LIMIT 100"
        return pd.read_sql(query, conn)
    except Exception:
        return pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)

# --- Charting Functions ---
def plot_candlestick(df):
    if df.empty:
        return alt.Chart(pd.DataFrame({'x':[]})).mark_text(text="Waiting for data...")
    
    # กำหนดสีแท่งเทียน (เขียว/แดง)
    open_close_color = alt.condition(
        "datum.open <= datum.close",
        alt.value("#00C087"), # Green (Bullish)
        alt.value("#FF4D4D")  # Red (Bearish)
    )

    base = alt.Chart(df).encode(
        x=alt.X('created_at:T', axis=alt.Axis(title=None, format='%H:%M')),
        tooltip=['created_at', 'open', 'high', 'low', 'close', 'volume']
    )

    # 1. ไส้เทียน (High-Low)
    rule = base.mark_rule().encode(
        y=alt.Y('low:Q', scale=alt.Scale(zero=False), axis=alt.Axis(title='Price')),
        y2='high:Q',
        color=open_close_color
    )

    # 2. ตัวแท่งเทียน (Open-Close)
    bar = base.mark_bar().encode(
        y='open:Q',
        y2='close:Q',
        color=open_close_color
    )
    
    # 3. Volume Bar ด้านล่าง (Optional)
    # volume = base.mark_bar(opacity=0.3).encode(
    #     y=alt.Y('volume:Q', axis=alt.Axis(title='Vol', orient='right')),
    #     color=open_close_color
    # ).properties(height=100)

    return (rule + bar).properties(height=400).interactive()

def plot_depth(df):
    if df.empty: return alt.Chart(pd.DataFrame({'x':[]})).mark_text(text="No Orders")
    
    domain = ['buy', 'sell']
    range_ = ['#00C087', '#FF4D4D']

    # Area Chart แบบทับซ้อน (Layered) หรือ Stack
    return alt.Chart(df).mark_area(opacity=0.6, interpolate='step-after').encode(
        x=alt.X('price', axis=alt.Axis(title='Price')),
        y=alt.Y('volume', axis=alt.Axis(title='Size')),
        color=alt.Color('side', scale=alt.Scale(domain=domain, range=range_), legend=None),
        tooltip=['side', 'price', 'volume']
    ).properties(height=300).interactive()

# --- Main UI ---
def main():
    st.sidebar.title("⚡ Bitka Pro")
    
    # Settings Sidebar
    symbol_filter = st.sidebar.selectbox("Asset Pair:", ["BTC_THB", "ETH_THB", "DOGE_THB", "USDT_THB"], index=0)
    time_frame = st.sidebar.select_slider("Timeframe:", options=['1Min', '5Min', '15Min', '1H'], value='5Min')
    auto_refresh = st.sidebar.checkbox("Auto Refresh (3s)", value=True)
    
    st.sidebar.markdown("---")
    menu = st.sidebar.radio("View:", ["📈 Trade View", "🔍 Warehouse Data"])

    conn = get_connection()
    placeholder = st.empty()

    while True:
        with placeholder.container():
            if menu == "📈 Trade View":
                # --- Load Data ---
                df_ticker, df_users = load_kpi_data(conn)
                df_ohlc, df_depth, df_recent = load_market_data(conn, symbol_filter, time_frame.replace('Min', 'T').replace('H', 'H'))

                # --- Header: Ticker Info ---
                curr_ticker = df_ticker[df_ticker['symbol'] == symbol_filter]
                last_price = curr_ticker['last_price'].iloc[0] if not curr_ticker.empty else 0.0
                vol_24h = curr_ticker['volume_24h'].iloc[0] if not curr_ticker.empty else 0.0
                
                # Header Layout
                c1, c2, c3, c4 = st.columns([2, 2, 3, 2])
                with c1: st.metric(label=symbol_filter, value=f"{last_price:,.2f}")
                with c2: st.metric(label="24h Vol", value=f"{vol_24h:,.2f}")
                with c3: 
                    # คำนวณ % เปลี่ยนแปลงจากแท่งเทียนแรกเทียบแท่งล่าสุด (คร่าวๆ)
                    if not df_ohlc.empty:
                        chg = ((df_ohlc.iloc[-1]['close'] - df_ohlc.iloc[0]['open']) / df_ohlc.iloc[0]['open']) * 100
                        st.metric(label="Change (Session)", value=f"{chg:.2f}%", delta=f"{chg:.2f}%")
                    else:
                        st.metric(label="Change", value="0.00%")
                
                st.markdown("---")

                # --- Main Layout ---
                # แบ่งเป็น 3 คอลัมน์: Chart (60%) | Order Book (20%) | Recent Trades (20%)
                col_chart, col_depth, col_trades = st.columns([3, 1.5, 1])

                with col_chart:
                    st.subheader(f"🕯️ Price Chart ({time_frame})")
                    chart = plot_candlestick(df_ohlc)
                    st.altair_chart(chart, use_container_width=True)

                with col_depth:
                    st.subheader("🏔️ Depth")
                    depth_chart = plot_depth(df_depth)
                    st.altair_chart(depth_chart, use_container_width=True)
                    
                    st.caption("Order Book (Ask/Bid)")
                    if not df_depth.empty:
                        # แยก Buy/Sell เพื่อโชว์เป็นตาราง Bid/Ask แบบ Classic
                        asks = df_depth[df_depth['side']=='sell'].sort_values('price', ascending=True).head(5)
                        bids = df_depth[df_depth['side']=='buy'].sort_values('price', ascending=False).head(5)
                        
                        # Display Asks (Red - Top)
                        st.markdown("**Asks (Sell)**")
                        for _, row in asks.iterrows():
                            st.markdown(f"<div style='color:#FF4D4D; display:flex; justify-content:space-between;'><span>{row['price']:,.2f}</span> <span>{row['volume']:.4f}</span></div>", unsafe_allow_html=True)
                        
                        st.markdown("---")
                        
                        # Display Bids (Green - Bottom)
                        st.markdown("**Bids (Buy)**")
                        for _, row in bids.iterrows():
                            st.markdown(f"<div style='color:#00C087; display:flex; justify-content:space-between;'><span>{row['price']:,.2f}</span> <span>{row['volume']:.4f}</span></div>", unsafe_allow_html=True)

                with col_trades:
                    st.subheader("🔥 Trades")
                    if not df_recent.empty:
                        for _, row in df_recent.iterrows():
                            color = "#FF4D4D" if row['side'] == 'sell' else "#00C087" # แดง/เขียว
                            price_fmt = f"{row['price']:,.2f}"
                            qty_fmt = f"{row['quantity']:.4f}"
                            # สร้าง HTML เล็กๆ เพื่อโชว์รายการเทรด
                            st.markdown(
                                f"<div style='border-bottom:1px solid #333; font-size:0.8rem; padding:2px; display:flex; justify-content:space-between;'>"
                                f"<span style='color:{color}'>{price_fmt}</span>"
                                f"<span style='color:#ccc'>{qty_fmt}</span>"
                                f"</div>", 
                                unsafe_allow_html=True
                            )
                    else:
                        st.info("No trades")

            elif menu == "🔍 Warehouse Data":
                st.subheader("Explorer")
                selected_table = st.selectbox("Table:", ALL_TABLES)
                df = load_table_data(conn, selected_table)
                st.dataframe(df, use_container_width=True, height=600)

        if auto_refresh:
            time.sleep(3)
        else:
            break 

if __name__ == "__main__":
    main()