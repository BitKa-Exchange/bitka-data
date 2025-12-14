import streamlit as st
import pandas as pd
import psycopg2
import os
import time
import altair as alt
from datetime import datetime, timedelta

# --- Config ---
# (คงเดิม)
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

st.set_page_config(page_title="Bitka Pro Trade", page_icon="🕯️", layout="wide")

# --- Styling ---
st.markdown("""
<style>
    .stMetric { background-color: #1e1e1e; padding: 10px; border-radius: 5px; border: 1px solid #333; }
    [data-testid="stMetricValue"] { font-size: 1.5rem !important; }
    /* ปรับแต่ง Scrollbar ให้ดู Dark Mode */
    ::-webkit-scrollbar { width: 10px; }
    ::-webkit-scrollbar-track { background: #1e1e1e; }
    ::-webkit-scrollbar-thumb { background: #888; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# --- Database Functions ---
# ⚠️ เปลี่ยนวิธีต่อ DB: ไม่ Cache Connection Object เพื่อกัน connection หลุด (Stale Connection)
def run_query(query):
    """เปิด-ปิด Connection ทุกครั้งเพื่อความชัวร์ (เหมาะกับ Dashboard ที่ Traffic ไม่ถล่มทลาย)"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
        )
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"❌ DB Error: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def load_kpi_data():
    df_ticker = run_query("SELECT symbol, last_price, volume_24h FROM tickers ORDER BY symbol")
    # ไม่จำเป็นต้อง query users ถ้าระบบใหญ่ขึ้น (เปลือง resource)
    return df_ticker

def load_market_data(symbol, time_frame='5Min'):
    # 1. Matches -> OHLCV
    # Optimize: ดึงแค่เท่าที่จำเป็นตาม Timeframe (เช่นถ้าดู 1Min ดึง 1 วันก็พอ)
    days_back = 7
    if 'Min' in time_frame: days_back = 2 
    
    trade_sql = f"""
        SELECT created_at, price, quantity 
        FROM matches 
        WHERE symbol = '{symbol}' 
        AND created_at >= NOW() - INTERVAL '{days_back} DAYS'
        ORDER BY created_at ASC
    """
    df_trades = run_query(trade_sql)
    
    df_ohlc = pd.DataFrame()
    if not df_trades.empty:
        df_trades['created_at'] = pd.to_datetime(df_trades['created_at'])
        df_trades.set_index('created_at', inplace=True)
        
        # Resample
        df_ohlc = df_trades.resample(time_frame).agg({
            'price': ['first', 'max', 'min', 'last'],
            'quantity': 'sum'
        })
        df_ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
        df_ohlc.dropna(subset=['open'], inplace=True) # เอาเฉพาะที่มีราคาเปิด
        df_ohlc.reset_index(inplace=True)

    # 2. Order Book (Depth)
    depth_sql = f"""
        SELECT side, price, SUM(quantity) as volume 
        FROM orders 
        WHERE symbol = '{symbol}' AND status = 'open'
        GROUP BY side, price
        ORDER BY price ASC
    """
    df_depth = run_query(depth_sql)
    
    # 🔥 Logic แก้ไข: คำนวณ Cumulative Sum เพื่อสร้าง Wall
    if not df_depth.empty:
        df_bids = df_depth[df_depth['side'] == 'buy'].sort_values('price', ascending=False).copy()
        df_bids['cumulative_vol'] = df_bids['volume'].cumsum()
        
        df_asks = df_depth[df_depth['side'] == 'sell'].sort_values('price', ascending=True).copy()
        df_asks['cumulative_vol'] = df_asks['volume'].cumsum()
        
        df_depth = pd.concat([df_bids, df_asks])

    # 3. Recent Trades
    recent_trades_sql = f"""
        SELECT price, quantity, created_at, 
               CASE WHEN taker_user_id IS NOT NULL THEN 'sell' ELSE 'buy' END as side 
        FROM matches 
        WHERE symbol = '{symbol}' 
        ORDER BY created_at DESC LIMIT 20
    """
    df_recent = run_query(recent_trades_sql)

    return df_ohlc, df_depth, df_recent

# --- Charting ---
def plot_candlestick(df):
    if df.empty: return alt.Chart(pd.DataFrame({'x':[]})).mark_text(text="Waiting for data...")
    
    open_close_color = alt.condition("datum.open <= datum.close", alt.value("#00C087"), alt.value("#FF4D4D"))
    
    base = alt.Chart(df).encode(
        x=alt.X('created_at:T', axis=alt.Axis(title=None, format='%H:%M', grid=False)),
        tooltip=['created_at', 'open', 'high', 'low', 'close', 'volume']
    )

    rule = base.mark_rule().encode(
        y=alt.Y('low:Q', scale=alt.Scale(zero=False), axis=alt.Axis(title='Price')),
        y2='high:Q',
        color=open_close_color
    )

    bar = base.mark_bar().encode(
        y='open:Q',
        y2='close:Q',
        color=open_close_color
    )
    
    return (rule + bar).properties(height=400).interactive()

def plot_depth(df):
    if df.empty: return alt.Chart(pd.DataFrame({'x':[]})).mark_text(text="No Orders")
    
    # ใช้ cumulative_vol แทน volume ปกติ
    return alt.Chart(df).mark_area(opacity=0.6, interpolate='step-after').encode(
        x=alt.X('price', axis=alt.Axis(title='Price'), scale=alt.Scale(zero=False)),
        y=alt.Y('cumulative_vol', axis=alt.Axis(title='Size')),
        color=alt.Color('side', scale=alt.Scale(domain=['buy', 'sell'], range=['#00C087', '#FF4D4D']), legend=None),
        tooltip=['side', 'price', 'cumulative_vol']
    ).properties(height=300)

# --- Main UI ---
def main():
    st.sidebar.title("⚡ Bitka Pro")
    
    # ⚠️ ย้าย Auto Refresh ไปไว้บนสุด หรือใช้ session_state คุม
    if 'last_update' not in st.session_state:
        st.session_state.last_update = time.time()

    symbol_filter = st.sidebar.selectbox("Asset Pair:", ["BTC_THB", "ETH_THB", "DOGE_THB", "USDT_THB"])
    time_frame = st.sidebar.select_slider("Timeframe:", options=['1Min', '5Min', '15Min', '1H', '4H', '1D'], value='5Min')
    
    # Auto Refresh Setting
    refresh_sec = st.sidebar.number_input("Refresh Rate (s)", min_value=1, value=3)
    auto_refresh = st.sidebar.toggle("Auto Refresh", value=True)
    
    st.sidebar.markdown("---")
    menu = st.sidebar.radio("View:", ["📈 Trade View", "🔍 Warehouse Data"])

    # --- Page Content ---
    if menu == "📈 Trade View":
        # Load Data
        df_ticker = load_kpi_data()
        df_ohlc, df_depth, df_recent = load_market_data(symbol_filter, time_frame)

        # KPIs
        curr_ticker = df_ticker[df_ticker['symbol'] == symbol_filter]
        last_price = curr_ticker['last_price'].iloc[0] if not curr_ticker.empty else 0.0
        vol_24h = curr_ticker['volume_24h'].iloc[0] if not curr_ticker.empty else 0.0
        
        c1, c2, c3 = st.columns(3)
        c1.metric(symbol_filter, f"{last_price:,.2f}")
        c2.metric("24h Vol", f"{vol_24h:,.2f}")
        
        if not df_ohlc.empty:
            start_p = df_ohlc.iloc[0]['open']
            end_p = df_ohlc.iloc[-1]['close']
            chg = ((end_p - start_p) / start_p) * 100
            c3.metric(f"Change ({time_frame})", f"{chg:.2f}%", delta=f"{chg:.2f}%")
        else:
            c3.metric("Change", "0.00%")

        st.markdown("---")

        # Layout: Chart | Depth | Trades
        col_chart, col_depth, col_trades = st.columns([3, 1.5, 1])

        with col_chart:
            st.subheader(f"🕯️ {symbol_filter} ({time_frame})")
            st.altair_chart(plot_candlestick(df_ohlc), use_container_width=True)

        with col_depth:
            st.subheader("🏔️ Depth")
            st.altair_chart(plot_depth(df_depth), use_container_width=True)
            
            # Simple Order Book Table
            if not df_depth.empty:
                st.caption("Top 5 Bid/Ask")
                asks = df_depth[df_depth['side']=='sell'].sort_values('price').head(5)
                bids = df_depth[df_depth['side']=='buy'].sort_values('price', ascending=False).head(5)
                
                # Asks (Sell) - แสดงจากราคาสูงไปต่ำ (เพื่อให้ราคาต่ำสุดอยู่ล่างสุดใกล้กับ Bids)
                for _, r in asks.sort_values('price', ascending=False).iterrows():
                    st.markdown(f"<div style='color:#FF4D4D; font-size:0.8rem; display:flex; justify-content:space-between;'><span>{r['price']:,.2f}</span> <span>{r['volume']:.4f}</span></div>", unsafe_allow_html=True)
                
                st.markdown("<div style='border-top: 1px dashed #555; margin: 2px 0;'></div>", unsafe_allow_html=True)

                # Bids (Buy)
                for _, r in bids.iterrows():
                    st.markdown(f"<div style='color:#00C087; font-size:0.8rem; display:flex; justify-content:space-between;'><span>{r['price']:,.2f}</span> <span>{r['volume']:.4f}</span></div>", unsafe_allow_html=True)

        with col_trades:
            st.subheader("Recent Trades")
            if not df_recent.empty:
                for _, row in df_recent.iterrows():
                    color = "#FF4D4D" if row['side'] == 'sell' else "#00C087"
                    st.markdown(
                        f"<div style='border-bottom:1px solid #333; font-size:0.8rem; padding:4px; display:flex; justify-content:space-between;'>"
                        f"<span style='color:{color}; font-weight:bold;'>{row['price']:,.2f}</span>"
                        f"<span style='color:#bbb'>{row['quantity']:.4f}</span>"
                        f"<span style='color:#666; font-size:0.7rem'>{row['created_at'].strftime('%H:%M:%S')}</span>"
                        f"</div>", 
                        unsafe_allow_html=True
                    )
            else:
                st.info("No trades")

    elif menu == "🔍 Warehouse Data":
        st.subheader("Data Explorer")
        table = st.selectbox("Table", ["orders", "matches", "users", "deposits", "tickers"])
        df = run_query(f"SELECT * FROM {table} ORDER BY created_at DESC LIMIT 50")
        st.dataframe(df, use_container_width=True)

    # ⚠️ Logic Auto Refresh ที่ถูกต้องของ Streamlit
    if auto_refresh:
        time.sleep(refresh_sec)
        st.rerun()

if __name__ == "__main__":
    main()