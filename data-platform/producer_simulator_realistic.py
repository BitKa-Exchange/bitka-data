import time, json, random, uuid, math
from datetime import datetime, timezone
from kafka import KafkaProducer
from decimal import Decimal
import psycopg2   # new


# --------------------------------------------------------
# CONFIG
# --------------------------------------------------------
KAFKA_BROKER = "localhost:19092"

TOPICS = {
    'ORDER_CREATED': 'trading.orders.created',
    'MATCH_EXECUTED': 'trading.matches.executed',
    'DEPOSIT': 'accounting.deposit.confirmed',
    'LOGIN': 'identity.user.login',
    'TICKER': 'market.ticker.update',
}

SYMBOLS = ['BTC_THB', 'ETH_THB', 'USDT_THB']
USERS = [str(uuid.uuid4()) for _ in range(20)]
ASSETS = ['BTC', 'ETH', 'USDT', 'THB']
DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)


# --------------------------------------------------------
# HELPERS
# --------------------------------------------------------
def get_iso_time():
    return datetime.now(timezone.utc).isoformat()


def random_decimal_str(a, b, decimals=8):
    v = random.uniform(a, b)
    q = Decimal(str(v)).quantize(Decimal("1." + "0" * decimals))
    return format(q.normalize(), "f")


def create_envelope(producer_name, payload):
    """Standard event envelope your consumer expects."""
    return {
        "event_id": str(uuid.uuid4()),
        "correlation_id": str(uuid.uuid4()),
        "producer": producer_name,
        "timestamp": get_iso_time(),
        "data": payload,
    }


# --------------------------------------------------------
# PRICE & VOLUME STATE (REALISTIC)
# --------------------------------------------------------
price_state = {
    "BTC_THB": 1_500_000.0,
    "ETH_THB": 50_000.0,
    "USDT_THB": 36.0,
}
def init_price_state_from_dw():
    """
    Load latest last_price per symbol from fact_market_tickers.
    If nothing found for a symbol, keep the default price_state.
    """
    global price_state
    try:
        conn = psycopg2.connect(**DW)
        cur = conn.cursor()

        for sym in SYMBOLS:
            cur.execute(
                """
                SELECT last_price
                FROM fact_market_tickers
                WHERE symbol = %s
                ORDER BY event_time DESC
                LIMIT 1;
                """,
                (sym,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                price_state[sym] = float(row[0])

        cur.close()
        conn.close()
        print(f"✅ Loaded initial prices from DW: {price_state}")

    except Exception as e:
        print(f"⚠️ Could not load prices from DW, using defaults. Reason: {e}")


vol_24h_state = {s: 0.0 for s in SYMBOLS}

# approximate daily volatility (as fraction)
DAILY_VOL = {
    "BTC_THB": 0.04,  # 4% daily
    "ETH_THB": 0.06,
    "USDT_THB": 0.01,
}


def step_price(symbol, dt_sec: float = 1.0):
    """Geometric Brownian Motion step for price."""
    s = price_state[symbol]
    sigma_daily = DAILY_VOL[symbol]
    sigma = sigma_daily / math.sqrt(24 * 60 * 60)  # convert to per-second volatility
    mu = 0.0  # no drift for now

    z = random.gauss(0, 1)
    factor = math.exp(mu * dt_sec + sigma * math.sqrt(dt_sec) * z)

    s_new = max(0.01, s * factor)
    price_state[symbol] = s_new
    return s_new


# --------------------------------------------------------
# EVENT GENERATORS
# --------------------------------------------------------
def gen_ticker(symbol: str):
    """Ticker update based on current simulated mid price."""
    price = price_state[symbol]
    vol_24h = max(0.01, vol_24h_state[symbol])

    payload = {
        "symbol": symbol,
        "last_price": f"{price:.2f}",
        "open_24h": f"{price * 0.97:.2f}",
        "high_24h": f"{price * 1.03:.2f}",
        "low_24h": f"{price * 0.94:.2f}",
        "volume_24h": random_decimal_str(vol_24h * 0.8, vol_24h * 1.2, 6),
        "quote_vol_24h": random_decimal_str(price * vol_24h * 0.8, price * vol_24h * 1.2, 2),
    }
    return TOPICS["TICKER"], create_envelope("market-data-service", payload)


def gen_match(symbol: str):
    """Executed trade, price near mid, realistic quantity and fees."""
    mid_price = price_state[symbol]
    slip_bp = random.uniform(-10, 10)  # +/- 10 basis points
    price = mid_price * (1 + slip_bp / 10000.0)

    qty = random.uniform(0.001, 0.5)
    vol_24h_state[symbol] += qty

    base_asset = symbol.split("_")[0]
    maker_fee = qty * 0.001   # 0.1%
    taker_fee = qty * 0.0015  # 0.15%

    payload = {
        "match_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": f"{price:.2f}",
        "quantity": random_decimal_str(qty, qty, 8),
        "maker_user_id": random.choice(USERS),
        "maker_order_id": str(uuid.uuid4()),
        "maker_fee": random_decimal_str(maker_fee, maker_fee, 8),
        "maker_fee_asset": "THB",
        "taker_user_id": random.choice(USERS),
        "taker_order_id": str(uuid.uuid4()),
        "taker_fee": random_decimal_str(taker_fee, taker_fee, 8),
        "taker_fee_asset": base_asset,
        "taker_side": random.choice(["buy", "sell"]),
    }
    return TOPICS["MATCH_EXECUTED"], create_envelope("matching-engine", payload)


def gen_order(symbol: str):
    """Order flow: mostly limit, some market, around current mid price."""
    side = random.choice(["buy", "sell"])
    order_type = random.choices(["limit", "market"], weights=[0.7, 0.3])[0]
    mid = price_state[symbol]

    if order_type == "limit":
        offset_bp = random.uniform(-30, 30)  # +/- 30 bps around mid
        price = mid * (1 + offset_bp / 10000.0)
    else:
        price = 0.0  # market order

    qty = random.uniform(0.001, 1.0)

    payload = {
        "order_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "price": f"{price:.2f}",
        "quantity": random_decimal_str(qty, qty, 8),
        "time_in_force": random.choice(["GTC", "IOC", "FOK"]),
    }
    return TOPICS["ORDER_CREATED"], create_envelope("order-service", payload)


def gen_deposit():
    """Rare-ish deposit event, realistic amount."""
    asset = random.choice(ASSETS)
    amount = random.uniform(50, 5000)

    payload = {
        "tx_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "asset": asset,
        "amount": random_decimal_str(amount, amount, 6),
        "chain_tx_hash": "0x" + uuid.uuid4().hex,
        "network": "ERC20" if asset != "BTC" else "BITCOIN",
        "confirmations": random.randint(1, 12),
    }
    return TOPICS["DEPOSIT"], create_envelope("ledger-service", payload)


def login_rate(now_utc: datetime) -> float:
    """Login probability per tick, based on (approx) Bangkok local hour."""
    # Approximate Bangkok local hour (UTC+7)
    hour = (now_utc.hour + 7) % 24

    if 2 <= hour <= 6:
        base = 0.02   # low (night)
    elif 7 <= hour <= 11:
        base = 0.05   # morning
    elif 12 <= hour <= 22:
        base = 0.12   # peak daytime/evening
    else:
        base = 0.04   # late night
    return base


def gen_login(now_utc: datetime):
    status = random.choices(
        ["success", "failed_pass", "failed_2fa"],
        weights=[85, 10, 5],
    )[0]

    payload = {
        "user_id": random.choice(USERS),
        "ip_address": f"192.168.1.{random.randint(1, 255)}",
        "device_id": uuid.uuid4().hex[:16],
        "location_geo": "Bangkok, TH",
        "status": status,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    return TOPICS["LOGIN"], create_envelope("auth-service", payload)


# --------------------------------------------------------
# MAIN LOOP
# --------------------------------------------------------
def main():
    random.seed(42)
    init_price_state_from_dw()
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("🚀 Realistic Bitka simulator started. Ctrl+C to stop.")

    try:
        while True:
            now = datetime.now(timezone.utc)

            # Step prices for all symbols
            for s in SYMBOLS:
                step_price(s)

            # Choose an "active" symbol this tick
            active_symbol = random.choices(SYMBOLS, weights=[0.5, 0.3, 0.2])[0]

            # 1) Always send at least one ticker
            topic, msg = gen_ticker(active_symbol)
            producer.send(topic, value=msg)
            print(f"📈 TICKER {active_symbol} @ {msg['data']['last_price']}")

            # 2) Trading activity cluster
            if random.random() < 0.8:
                # Trades
                n_trades = random.randint(1, 4)
                for _ in range(n_trades):
                    topic, msg = gen_match(active_symbol)
                    producer.send(topic, value=msg)

                # New orders
                n_orders = random.randint(0, 3)
                for _ in range(n_orders):
                    topic, msg = gen_order(active_symbol)
                    producer.send(topic, value=msg)

            # 3) Occasional deposits
            if random.random() < 0.05:
                topic, msg = gen_deposit()
                producer.send(topic, value=msg)

            # 4) User logins with circadian pattern
            if random.random() < login_rate(now):
                topic, msg = gen_login(now)
                producer.send(topic, value=msg)

            producer.flush()
            time.sleep(random.uniform(0.2, 0.8))

    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
