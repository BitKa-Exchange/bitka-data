import streamlit as st
import psycopg2
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- 1. Page Configuration ---
st.set_page_config(
    page_title="Bitka Exchange | Command Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for "Beautiful" Look
st.markdown("""
<style>
    .stMetric {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #333;
    }
    .stDataFrame {
        border: 1px solid #333;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. Database Configuration ---
DW_CONFIG = {
    "host": "localhost",
    "port": "5433", # Port External ของ Warehouse
    "database": "bitka_dw",
    "user": "warehouse_admin",
    "password": "warehouse_password"
}

# --- 3. Data Loading Function ---
@st.cache_data(ttl=1) # Cache 1 วินาที เพื่อลดภาระการ connect ซ้ำซ้อน
def run_query(query):
    try:
        conn = psycopg2.connect(**DW_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        # st.error(f"Database Error: {e}")
        return pd.DataFrame()

def load_dashboard_data():
    # Limit 200 ตามคำขอ
    limit = 200
    
    data = {}
    
    # 1. Market Tickers (Price)
    data['tickers'] = run_query(f"SELECT * FROM fact_market_tickers ORDER BY event_time DESC LIMIT {limit}")
    
    # 2. Orders
    data['orders'] = run_query(f"SELECT * FROM fact_orders_created ORDER BY event_time DESC LIMIT {limit}")
    
    # 3. Matches (Executions)
    data['matches'] = run_query(f"SELECT * FROM fact_matches_executed ORDER BY event_time DESC LIMIT {limit}")
    
    # 4. Deposits
    data['deposits'] = run_query(f"SELECT * FROM fact_deposits ORDER BY event_time DESC LIMIT {limit}")
    
    # 5. Logins (Security)
    data['logins'] = run_query(f"SELECT * FROM dim_user_logins ORDER BY event_time DESC LIMIT {limit}")
    
    return data

# --- 4. Main Dashboard Loop ---
st.title("🚀 Bitka Exchange: Live Monitor")
st.markdown("Real-time Event Sourcing Dashboard connected to **Data Warehouse**")

placeholder = st.empty()

while True:
    # Fetch Data
    db = load_dashboard_data()
    
    with placeholder.container():
        # --- Top Row: KPIs ---
        k1, k2, k3, k4 = st.columns(4)
        
        # Calculate Real-time Metrics
        last_price = db['tickers']['last_price'].iloc[0] if not db['tickers'].empty else 0
        total_vol = db['tickers']['volume_24h'].iloc[0] if not db['tickers'].empty else 0
        order_count = len(db['orders'])
        active_users = db['logins']['user_id'].nunique() if not db['logins'].empty else 0

        k1.metric("💰 BTC Price (THB)", f"฿{last_price:,.2f}", delta_color="normal")
        k2.metric("📊 24h Volume", f"{total_vol:,.2f} BTC")
        k3.metric("📝 Recent Orders", f"{order_count} Orders")
        k4.metric("👥 Active Users", f"{active_users} Users")

        st.markdown("---")

        # --- Tabs Section ---
        tab_market, tab_trade, tab_finance, tab_security = st.tabs([
            "📈 Market Overview", 
            "⚖️ Order Book & Trades", 
            "💳 Finance (Ledger)",
            "🛡️ Security & Audit"
        ])

        # === TAB 1: Market Overview ===
        with tab_market:
            c1, c2 = st.columns([3, 1])
            with c1:
                st.subheader("Price Movement (BTC_THB)")
                if not db['tickers'].empty:
                    # Create nice Line Chart with Plotly
                    fig = px.line(db['tickers'], x='event_time', y='last_price', 
                                  title='Real-time Price Feed', markers=True)
                    fig.update_traces(line_color='#00CC96')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Waiting for market data...")
            
            with c2:
                st.subheader("Latest Tickers")
                st.dataframe(
                    db['tickers'][['event_time', 'symbol', 'last_price']], 
                    hide_index=True,
                    use_container_width=True
                )

        # === TAB 2: Trading ===
        with tab_trade:
            c1, c2 = st.columns(2)
            
            with c1:
                st.subheader("🛒 Latest Orders")
                # แสดงตารางแบบสวยงาม ใส่สี Buy=Green, Sell=Red
                if not db['orders'].empty:
                    st.data_editor(
                        db['orders'][['event_time', 'side', 'type', 'price', 'quantity', 'symbol']],
                        column_config={
                            "side": st.column_config.TextColumn(
                                "Side",
                                help="Buy or Sell",
                                validate="^(buy|sell)$"
                            ),
                            "price": st.column_config.NumberColumn(
                                "Price",
                                format="฿ %.2f"
                            ),
                        },
                        hide_index=True,
                        use_container_width=True,
                        disabled=True
                    )
                else:
                    st.write("No orders yet.")

            with c2:
                st.subheader("⚖️ Buy vs Sell Ratio")
                if not db['orders'].empty:
                    # Pie Chart ดูสัดส่วน Buy/Sell
                    fig_pie = px.pie(db['orders'], names='side', title='Order Side Distribution',
                                     color='side', color_discrete_map={'buy':'#00CC96', 'sell':'#EF553B'},
                                     hole=0.4)
                    st.plotly_chart(fig_pie, use_container_width=True)

            st.subheader("🤝 Recent Matches (Executed Trades)")
            st.dataframe(db['matches'][['event_time', 'symbol', 'price', 'quantity', 'maker_user_id', 'taker_side']], hide_index=True, use_container_width=True)

        # === TAB 3: Finance ===
        with tab_finance:
            st.subheader("💰 Recent Deposits")
            if not db['deposits'].empty:
                st.dataframe(
                    db['deposits'][['event_time', 'user_id', 'asset', 'amount', 'network', 'status' if 'status' in db['deposits'].columns else 'chain_tx_hash']],
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("No deposit activities.")

        # === TAB 4: Security ===
        with tab_security:
            st.subheader("🚨 Login Activities")
            if not db['logins'].empty:
                # Highlight Failed Logins
                st.dataframe(
                    db['logins'][['event_time', 'user_id', 'ip_address', 'location_geo', 'status']],
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.write("No login logs.")

    # Refresh Rate (1.5 seconds)
    time.sleep(1.5)