import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime

# ----------------------------------------------------
# 1. PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(
    page_title="Bitka Exchange | Command Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    .stMetric {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #333;
    }
</style>
""",
    unsafe_allow_html=True,
)

# ----------------------------------------------------
# 2. DATABASE CONFIG (LOCALHOST)
# ----------------------------------------------------
DW_CONFIG = {
    "host": "localhost",      # external host
    "port": "5433",           # external port from docker-compose
    "database": "bitka_dw",
    "user": "warehouse_admin",
    "password": "warehouse_password",
}

# ----------------------------------------------------
# 3. QUERY HELPERS
# ----------------------------------------------------
@st.cache_data(ttl=1)
def run_query(query: str) -> pd.DataFrame:
    try:
        conn = psycopg2.connect(**DW_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        # uncomment if you want to see DB errors
        # st.error(f"DB error: {e}")
        return pd.DataFrame()


def load_dashboard_data():
    limit = 200
    data = {}
    data["tickers"] = run_query(
        f"SELECT * FROM fact_market_tickers ORDER BY event_time DESC LIMIT {limit}"
    )
    data["orders"] = run_query(
        f"SELECT * FROM fact_orders_created ORDER BY event_time DESC LIMIT {limit}"
    )
    data["matches"] = run_query(
        f"SELECT * FROM fact_matches_executed ORDER BY event_time DESC LIMIT {limit}"
    )
    data["deposits"] = run_query(
        f"SELECT * FROM fact_deposits ORDER BY event_time DESC LIMIT {limit}"
    )
    data["logins"] = run_query(
        f"SELECT * FROM dim_user_logins ORDER BY event_time DESC LIMIT {limit}"
    )
    return data


# ----------------------------------------------------
# 4. TITLE + (OPTIONAL) AUTO-REFRESH
# ----------------------------------------------------
st.title("🚀 Bitka Exchange: Live Monitor")
st.markdown(
    "Real-time Event Sourcing Dashboard connected to **Data Warehouse**"
)

# If you want auto-refresh every 1.5s, keep this.
# If it ever causes issues, you can comment these 2 lines out.
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=1500, key="bitka_autorefresh")

# Load data once per run
db = load_dashboard_data()

# ----------------------------------------------------
# 5. TOP KPIs
# ----------------------------------------------------
tickers = db["tickers"]
orders = db["orders"]
matches = db["matches"]
deposits = db["deposits"]
logins = db["logins"]

last_price = (
    tickers["last_price"].iloc[0] if not tickers.empty else 0
)
vol_24h = (
    tickers["volume_24h"].iloc[0]
    if (not tickers.empty and "volume_24h" in tickers.columns)
    else 0
)
order_count = len(orders) if not orders.empty else 0
active_users = logins["user_id"].nunique() if not logins.empty else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 BTC Price (THB)", f"฿{last_price:,.2f}")
k2.metric("📊 24h Volume", f"{vol_24h:,.2f} BTC")
k3.metric("📝 Recent Orders", f"{order_count} Orders")
k4.metric("👥 Active Users", f"{active_users} Users")

st.markdown("---")

# ----------------------------------------------------
# 6. TABS
# ----------------------------------------------------
tab_market, tab_trade, tab_finance, tab_security = st.tabs(
    ["📈 Market Overview", "⚖️ Order Book & Trades", "💳 Finance (Ledger)", "🛡️ Security & Audit"]
)

# ===================== TAB 1: MARKET =====================
with tab_market:
    c1, c2 = st.columns([3, 1])

    with c1:
        st.subheader("Price Movement (BTC_THB)")
        if not tickers.empty:
            fig_price = px.line(
                tickers,
                x="event_time",
                y="last_price",
                title="Real-time Price Feed",
                markers=True,
            )
            fig_price.update_traces(line_color="#00CC96")
            st.plotly_chart(
                fig_price,
                use_container_width=True,
                key="price_chart_main",  # UNIQUE KEY
            )
        else:
            st.info("Waiting for market data...")

    with c2:
        st.subheader("Latest Tickers")
        st.dataframe(
            tickers[["event_time", "symbol", "last_price"]],
            hide_index=True,
            use_container_width=True,
        )

# ===================== TAB 2: TRADING =====================
with tab_trade:
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🛒 Latest Orders")
        if not orders.empty:
            cols = [
                c
                for c in ["event_time", "side", "type", "price", "quantity", "symbol"]
                if c in orders.columns
            ]
            st.dataframe(
                orders[cols],
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No orders yet.")

    with c2:
        st.subheader("⚖️ Buy vs Sell Ratio")
        if not orders.empty and "side" in orders.columns:
            fig_side = px.pie(
                orders,
                names="side",
                title="Order Side Distribution",
                color="side",
                color_discrete_map={"buy": "#00CC96", "sell": "#EF553B"},
                hole=0.4,
            )
            st.plotly_chart(
                fig_side,
                use_container_width=True,
                key="order_side_pie",  # UNIQUE KEY
            )
        else:
            st.info("No side information yet.")

    st.subheader("🤝 Recent Matches (Executed Trades)")
    if not matches.empty:
        match_cols = [
            c
            for c in [
                "event_time",
                "symbol",
                "price",
                "quantity",
                "maker_user_id",
                "taker_side",
            ]
            if c in matches.columns
        ]
        st.dataframe(
            matches[match_cols],
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No executed trades yet.")

# ===================== TAB 3: FINANCE =====================
with tab_finance:
    st.subheader("💰 Recent Deposits")
    if not deposits.empty:
        dep_cols = [
            c
            for c in ["event_time", "user_id", "currency", "amount"]
            if c in deposits.columns
        ]
        st.dataframe(
            deposits[dep_cols],
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No deposit activities.")

# ===================== TAB 4: SECURITY =====================
with tab_security:
    st.subheader("🚨 Login Activities")
    if not logins.empty:
        log_cols = [
            c
            for c in ["event_time", "user_id", "success"]
            if c in logins.columns
        ]
        st.dataframe(
            logins[log_cols],
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No login logs yet.")
