"""
Bitka Executive Dashboard (Streamlit)
-------------------------------------
Author: Data Engineering Team
Description: 
    Web Application สำหรับแสดงผลข้อมูล Real-time จาก Data Warehouse
    
    Features:
    - Executive Overview: สรุป KPIs สำคัญ, กราฟ Volume, และรายการเทรดล่าสุด
    - Data Explorer: เครื่องมือสำหรับตรวจสอบข้อมูลดิบในทุกตาราง (Audit)
    - Auto-refresh mechanism: อัปเดตหน้าจออัตโนมัติทุก 2 วินาที
"""

import streamlit as st
import pandas as pd
import psycopg2
import os
import time
import altair as alt

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================

# Database Connection Settings
# ดึงค่าจาก Environment Variables เพื่อให้รองรับการรันผ่าน Docker Compose
DW_HOST = os.getenv("DW_HOST", "warehouse")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_USER = os.getenv("DW_USER", "warehouse_admin")
DW_PASS = os.getenv("DW_PASS", "warehouse_password")
DW_NAME = os.getenv("DW_NAME", "bitka_dw")

# ตั้งค่าหน้าเว็บ (Browser Tab Title & Layout)
st.set_page_config(
    page_title="Bitka Executive Dashboard", 
    page_icon="📈", 
    layout="wide" # ใช้พื้นที่เต็มความกว้างหน้าจอ
)

# รายชื่อตารางทั้งหมดใน Warehouse สำหรับเมนู Data Explorer
ALL_TABLES = [
    "orders", 
    "matches", 
    "users", 
    "deposits", 
    "withdrawals", 
    "tickers", 
    "login_history", 
    "audit_logs"
]

# ==========================================
# DATA LAYER (Database Functions)
# ==========================================

@st.cache_resource
def get_connection():
    """
    สร้างและ Cache Database Connection
    
    Note: @st.cache_resource ช่วยให้ Streamlit ไม่ต้อง Re-connect Database 
    ทุกครั้งที่มีการ Refresh หน้าจอ (ช่วยลด Load และเพิ่มความเร็ว)
    """
    return psycopg2.connect(
        host=DW_HOST, port=DW_PORT, user=DW_USER, password=DW_PASS, dbname=DW_NAME
    )

def load_kpi_data(conn):
    """
    ดึงข้อมูลสรุป (Aggregated Data) สำหรับหน้า Executive View
    
    Returns:
        tuple: ประกอบด้วย DataFrames และตัวเลขสรุป (Dep/Wd)
    """
    try:
        # 1. Market Data: ราคาล่าสุดและ Volume ของแต่ละเหรียญ
        df_ticker = pd.read_sql("SELECT symbol, last_price, volume_24h FROM tickers ORDER BY symbol", conn)

        # 2. Recent Orders: 8 รายการล่าสุด (Active Interest)
        df_orders = pd.read_sql("SELECT created_at, symbol, side, price, quantity FROM orders ORDER BY created_at DESC LIMIT 8", conn)
        
        # 3. Recent Matches: 8 รายการที่จับคู่สำเร็จจริง (Real Trades)
        df_matches = pd.read_sql("SELECT created_at, symbol, price, quantity FROM matches ORDER BY created_at DESC LIMIT 8", conn)

        # 4. Financial Stats: ยอดรวมฝาก-ถอน (Money Flow)
        # ใช้ COALESCE(SUM(...), 0) ป้องกันค่าเป็น NULL กรณีไม่มีข้อมูล
        dep_sql = "SELECT COALESCE(SUM(amount), 0) as total FROM deposits WHERE asset = 'THB'"
        total_dep = pd.read_sql(dep_sql, conn)['total'].iloc[0]
        
        wd_sql = "SELECT COALESCE(SUM(amount), 0) as total FROM withdrawals WHERE asset = 'THB'"
        total_wd = pd.read_sql(wd_sql, conn)['total'].iloc[0]
        
        # 5. User Stats: จำนวนผู้ใช้ทั้งหมด
        df_users = pd.read_sql("SELECT count(*) as count FROM users", conn)
        
        return df_ticker, df_orders, df_matches, df_users, total_dep, total_wd
    
    except Exception as e:
        # Fail-safe: หาก DB Error ให้ส่งค่าว่างกลับไป เพื่อไม่ให้หน้าเว็บ Crash
        # st.error(f"Data Load Error: {e}") # Uncomment for debugging
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, 0

def load_table_data(conn, table_name):
    """
    ดึงข้อมูลดิบของตารางที่เลือก (สำหรับ Explorer View)
    พร้อม Logic การ Sort ตาม Column เวลาที่ถูกต้อง
    """
    try:
        # แต่ละตารางอาจมี timestamp column ไม่เหมือนกัน
        sort_col = "created_at"
        if table_name == "tickers": sort_col = "updated_at"
        
        query = f"SELECT * FROM {table_name} ORDER BY {sort_col} DESC LIMIT 100"
        return pd.read_sql(query, conn)
    except Exception:
        # Fallback กรณีหา column sort ไม่เจอ
        return pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)

# ==========================================
# UI LAYER (Presentation)
# ==========================================

def main():
    # 1. Sidebar Navigation
    st.sidebar.title("📌 Navigation")
    menu = st.sidebar.radio("Go to", ["📊 Executive Overview", "🔍 Data Explorer"])
    st.sidebar.markdown("---")
    
    # Checkbox ควบคุมการ Refresh (ถ้าติ๊กออก Loop จะหยุดทำงาน)
    auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (2s)", value=True)
    
    # เลือกตาราง (แสดงเฉพาะตอนอยู่หน้า Data Explorer)
    selected_table = ALL_TABLES[0]
    if menu == "🔍 Data Explorer":
        st.subheader("🔍 Warehouse Explorer")
        selected_table = st.selectbox("Select Table:", ALL_TABLES)

    # Initialize Connection & UI Placeholder
    conn = get_connection()
    placeholder = st.empty() # Placeholder ช่วยให้เรา update เนื้อหาได้โดยไม่ต้อง refresh ทั้งหน้า

    # 2. Main Event Loop (Real-time Updates)
    while True:
        with placeholder.container():
            # ----------------------------------
            # VIEW 1: EXECUTIVE OVERVIEW
            # ----------------------------------
            if menu == "📊 Executive Overview":
                st.title("📈 Bitka Exchange: Executive View")
                
                # Fetch fresh data
                df_ticker, df_orders, df_matches, df_users, total_dep, total_wd = load_kpi_data(conn)
                
                # --- Row 1: High Level KPIs ---
                k1, k2, k3, k4 = st.columns(4)
                
                # KPI: Total Users
                user_count = df_users['count'].iloc[0] if not df_users.empty else 0
                k1.metric("👥 Total Users", f"{user_count:,.0f}")
                
                # KPI: Net Money Flow (Cash In - Cash Out)
                # 'delta' parameter แสดงตัวเลขเปรียบเทียบ (สีเขียว/แดง อัตโนมัติ)
                net_flow = total_dep - total_wd
                k2.metric(
                    "💰 Net Money Flow (THB)", 
                    f"{net_flow:,.0f} ฿", 
                    delta=f"{total_dep:,.0f} In / {total_wd:,.0f} Out"
                )

                # KPI: Crypto Prices
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
                        # สร้างกราฟแท่งด้วย Altair (Interactive Chart)
                        base = alt.Chart(df_ticker).encode(x=alt.X('symbol', sort='-y'))
                        bar = base.mark_bar().encode(y='volume_24h', color='symbol')
                        text = base.mark_text(dy=-10).encode(y='volume_24h', text=alt.Text('volume_24h', format='.2f'))
                        st.altair_chart((bar + text).interactive(), use_container_width=True)
                
                with c2:
                    st.subheader("🔥 Recent Trades (Matches)")
                    if not df_matches.empty:
                        st.dataframe(
                            df_matches[['symbol', 'price', 'quantity']], 
                            hide_index=True, 
                            height=300,
                            use_container_width=True
                        )
                    else:
                        st.info("No trades yet.")

                # --- Row 3: Live Order Stream ---
                st.subheader("📝 Live Order Stream")
                if not df_orders.empty:
                    st.dataframe(df_orders, hide_index=True, use_container_width=True)

            # ----------------------------------
            # VIEW 2: DATA EXPLORER
            # ----------------------------------
            elif menu == "🔍 Data Explorer":
                df = load_table_data(conn, selected_table)
                st.info(f"Showing latest 100 rows from **{selected_table}**")
                
                # แสดง Dataframe แบบเต็มความกว้าง
                st.dataframe(df, use_container_width=True, height=600, hide_index=True)
                st.caption(f"Total rows fetched: {len(df)}")

        # Loop Control
        if auto_refresh:
            time.sleep(2) # รอ 2 วินาทีก่อนวนลูปใหม่
        else:
            break # ถ้าปิด Auto Refresh ให้หยุดการทำงาน (ประหยัด Resource)

if __name__ == "__main__":
    main()