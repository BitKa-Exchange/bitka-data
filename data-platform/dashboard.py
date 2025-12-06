import streamlit as st
import psycopg2
import pandas as pd
import time
import plotly.express as px 

st.set_page_config(
    page_title="Bitka Exchange Master Dashboard",
    page_icon="💹",
    layout="wide"
)

# Config Warehouse
DW_CONFIG = {
    "host": "127.0.0.1",
    "port": "5433",
    "database": "bitka_dw",
    "user": "warehouse_admin",
    "password": "warehouse_password"
}

def get_data(table_name, limit=50):
    try:
        conn = psycopg2.connect(**DW_CONFIG)
        query = f"SELECT * FROM {table_name} ORDER BY event_time DESC LIMIT {limit};"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

st.title("💹 Bitka Exchange: Real-time Monitor")
st.markdown("---")

placeholder = st.empty()

while True:
    df_users = get_data("dim_users_history")
    df_transfers = get_data("fact_transfers")
    df_orders = get_data("fact_orders")
    df_trades = get_data("fact_trade_executions")
    df_wallet = get_data("fact_wallet_transactions")
    df_prices = get_data("fact_market_prices")

    with placeholder.container():
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        with kpi1:
            st.metric("Total Users", len(df_users) if not df_users.empty else 0)
        with kpi2:
            st.metric("Recent Orders", len(df_orders) if not df_orders.empty else 0)
        with kpi3:
            st.metric("Trades Executed", len(df_trades) if not df_trades.empty else 0)
        with kpi4:
            if not df_wallet.empty:
                deposits = df_wallet[df_wallet['transaction_type'] == 'DEPOSIT']['amount_change'].sum()
                st.metric("Total Deposits (THB)", f"{deposits:,.0f}")
            else:
                st.metric("Total Deposits", 0)

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🛒 Orders & Trades", 
            "💰 Transfers & Wallet", 
            "📈 Market Prices",
            "👥 User Activity",
            "📝 Raw Data"
        ])

        with tab1:
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Latest Orders")
                st.dataframe(df_orders, height=300, hide_index=True)
            with col_b:
                st.subheader("Trade Executions (Matched)")
                st.dataframe(df_trades, height=300, hide_index=True)

        with tab2:
            st.subheader("Wallet Transactions (Ledger)")
            if not df_wallet.empty:
                st.dataframe(
                    df_wallet.style.applymap(
                        lambda x: 'color: green' if x > 0 else 'color: red', 
                        subset=['amount_change']
                    ), 
                    height=400, 
                    hide_index=True
                )
            else:
                st.write("No wallet activity yet.")

        with tab3:
            st.subheader("BTC_THB Live Price")
            if not df_prices.empty:
                btc_data = df_prices[df_prices['symbol'] == 'BTC_THB'].sort_values('event_time')
                st.line_chart(btc_data, x='event_time', y='price')
                
                st.caption("Recent Price Ticks")
                st.dataframe(df_prices, hide_index=True)
            else:
                st.info("Waiting for market data...")

        with tab4:
            st.subheader("User Signups & Changes")
            st.dataframe(df_users, hide_index=True)

        with tab5:
            st.warning("All raw tables (Debug Mode)")
            st.json({
                "last_transfer": df_transfers.iloc[0].to_dict() if not df_transfers.empty else {},
                "last_order": df_orders.iloc[0].to_dict() if not df_orders.empty else {}
            })

    time.sleep(3)