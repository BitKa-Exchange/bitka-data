import streamlit as st
import pandas as pd
import psycopg2
import os
import time
import altair as alt

# --- Config ---
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

st.set_page_config(page_title="Bitka Executive Dashboard", page_icon="📈", layout="wide")

ALL_TABLES = ["orders", "matches", "users", "deposits", "withdrawals", "tickers", "login_history", "audit_logs"]

# --- Database Functions ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
    )

def load_kpi_data(conn):
    try:
        # 1. Market Data
        df_ticker = pd.read_sql("SELECT symbol, last_price, volume_24h FROM tickers ORDER BY symbol", conn)

        # 2. Recent Orders (Active Interest)
        df_orders = pd.read_sql("SELECT created_at, symbol, side, price, quantity FROM orders ORDER BY created_at DESC LIMIT 8", conn)
        
        # 3. Recent Matches (Real Trades) - เพิ่มส่วนนี้
        df_matches = pd.read_sql("SELECT created_at, symbol, price, quantity FROM matches ORDER BY created_at DESC LIMIT 8", conn)

        # 4. Financial Stats - เพิ่มส่วนนี้
        # รวมยอดฝาก (เฉพาะ THB เพื่อความง่ายในการรวม)
        dep_sql = "SELECT COALESCE(SUM(amount), 0) as total FROM deposits WHERE asset = 'THB'"
        total_dep = pd.read_sql(dep_sql, conn)['total'].iloc[0]
        
        # รวมยอดถอน (เฉพาะ THB)
        wd_sql = "SELECT COALESCE(SUM(amount), 0) as total FROM withdrawals WHERE asset = 'THB'"
        total_wd = pd.read_sql(wd_sql, conn)['total'].iloc[0]
        
        # User Stats
        df_users = pd.read_sql("SELECT count(*) as count FROM users", conn)
        
        return df_ticker, df_orders, df_matches, df_users, total_dep, total_wd
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, 0

def load_table_data(conn, table_name):
    try:
        sort_col = "created_at"
        if table_name == "tickers": sort_col = "updated_at"
        query = f"SELECT * FROM {table_name} ORDER BY {sort_col} DESC LIMIT 100"
        return pd.read_sql(query, conn)
    except Exception:
        return pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)

# --- Main UI ---
def main():
    st.sidebar.title("📌 Navigation")
    menu = st.sidebar.radio("Go to", ["📊 Executive Overview", "🔍 Data Explorer"])
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (2s)", value=True)
    
    selected_table = ALL_TABLES[0]
    if menu == "🔍 Data Explorer":
        st.subheader("🔍 Warehouse Explorer")
        selected_table = st.selectbox("Select Table:", ALL_TABLES)

    conn = get_connection()
    placeholder = st.empty()

    while True:
        with placeholder.container():
            if menu == "📊 Executive Overview":
                st.title("📈 Bitka Exchange: Executive View")
                
                df_ticker, df_orders, df_matches, df_users, total_dep, total_wd = load_kpi_data(conn)
                
                # --- Row 1: High Level KPIs ---
                k1, k2, k3, k4 = st.columns(4)
                
                user_count = df_users['count'].iloc[0] if not df_users.empty else 0
                k1.metric("👥 Total Users", f"{user_count:,.0f}")
                
                # Show Net Flow (Deposit - Withdrawal)
                net_flow = total_dep - total_wd
                k2.metric("💰 Net Money Flow (THB)", f"{net_flow:,.0f} ฿", delta=f"{total_dep:,.0f} In / {total_wd:,.0f} Out")

                btc = df_ticker[df_ticker['symbol'] == 'BTC_THB']
                price_btc = btc['last_price'].iloc[0] if not btc.empty else 0
                k3.metric("BTC/THB", f"{price_btc:,.2f} ฿")
                
                eth = df_ticker[df_ticker['symbol'] == 'ETH_THB']
                price_eth = eth['last_price'].iloc[0] if not eth.empty else 0
                k4.metric("ETH/THB", f"{price_eth:,.2f} ฿")
                
                st.markdown("---")

                # --- Row 2: Charts & Matches ---
                c1, c2 = st.columns([2, 1])
                
                with c1:
                    st.subheader("📊 Market Activity (Volume)")
                    if not df_ticker.empty:
                        base = alt.Chart(df_ticker).encode(x=alt.X('symbol', sort='-y'))
                        bar = base.mark_bar().encode(y='volume_24h', color='symbol')
                        text = base.mark_text(dy=-10).encode(y='volume_24h', text=alt.Text('volume_24h', format='.2f'))
                        st.altair_chart((bar + text).interactive(), use_container_width=True)
                
                with c2:
                    st.subheader("🔥 Recent Trades (Matches)")
                    if not df_matches.empty:
                        # แต่งตารางให้ดูง่ายขึ้น
                        st.dataframe(
                            df_matches[['symbol', 'price', 'quantity']], 
                            hide_index=True, 
                            height=300,
                            use_container_width=True
                        )
                    else:
                        st.info("No trades yet.")

                # --- Row 3: Order Book Stream ---
                st.subheader("📝 Live Order Stream")
                if not df_orders.empty:
                    st.dataframe(df_orders, hide_index=True, use_container_width=True)

            elif menu == "🔍 Data Explorer":
                df = load_table_data(conn, selected_table)
                st.info(f"Showing latest 100 rows from **{selected_table}**")
                st.dataframe(df, use_container_width=True, height=600, hide_index=True)
                st.caption(f"Total rows fetched: {len(df)}")

        if auto_refresh:
            time.sleep(2)
        else:
            break 

if __name__ == "__main__":
    main()