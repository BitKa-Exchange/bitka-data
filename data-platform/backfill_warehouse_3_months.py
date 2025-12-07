import psycopg2
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import math

# ==============================
# CONFIG: Postgres Warehouse
# ==============================
DW = dict(
    host="localhost",
    port=5433,
    database="bitka_dw",
    user="warehouse_admin",
    password="warehouse_password",
)

# ==============================
# SIMULATION CONFIG
# ==============================
N_USERS = 300              # จำนวน user ทั้งหมดที่จำลอง
BACKFILL_DAYS = 90         # ~3 เดือนย้อนหลัง
SYMBOLS = ["BTC_THB", "ETH_THB", "USDT_THB"]

START_AT = datetime.utcnow() - timedelta(days=BACKFILL_DAYS)
END_AT = datetime.utcnow()

PRICE_VOL = {
    "BTC_THB": 0.02,
    "ETH_THB": 0.03,
    "USDT_THB": 0.001,
}

BASE_PRICE = {
    "BTC_THB": 1_500_000,
    "ETH_THB": 80_000,
    "USDT_THB": 36,
}
PRICE_DRIFT = {
    "BTC_THB": 0.15,   # ~15% ต่อปี (bullish เบาๆ)
    "ETH_THB": 0.20,   # ETH โตไวกว่า
    "USDT_THB": 0.00,  # stablecoin ไม่ควรมี drift
}

REGIMES = {
    "calm":     {"vol_scale": 0.5, "drift_scale": 0.4},
    "normal":   {"vol_scale": 1.0, "drift_scale": 1.0},
    "volatile": {"vol_scale": 2.0, "drift_scale": 0.8},
    "rally":    {"vol_scale": 2.0, "drift_scale": 2.0},
    "crash":    {"vol_scale": 3.0, "drift_scale": -2.0},
}

REGIME_TRANSITION = {
    # P(next_regime | current_regime)
    "calm":   [("calm", 0.60), ("normal", 0.30), ("volatile", 0.10)],
    "normal": [("normal", 0.55), ("calm", 0.20), ("volatile", 0.15), ("rally", 0.05), ("crash", 0.05)],
    "volatile": [("volatile", 0.50), ("normal", 0.30), ("rally", 0.10), ("crash", 0.10)],
    "rally":  [("rally", 0.40), ("normal", 0.40), ("volatile", 0.10), ("crash", 0.10)],
    "crash":  [("crash", 0.40), ("normal", 0.40), ("volatile", 0.20)],
}

PRODUCER_NAME = "simulator"
TICKER_INTERVAL_MINUTES = 5  # เดิมคือ 30


# ==============================
# Archetype configs
# ==============================

class Archetype:
    def __init__(
        self,
        name,
        daily_active_prob,
        orders_per_active_mean,
        size_mean_log,
        size_sigma_log,
        symbol_weights,
        market_order_ratio,
        buy_bias,
        deposit_range_thb,
    ):
        self.name = name
        self.daily_active_prob = daily_active_prob
        self.orders_per_active_mean = orders_per_active_mean
        self.size_mean_log = size_mean_log
        self.size_sigma_log = size_sigma_log
        self.symbol_weights = symbol_weights
        self.market_order_ratio = market_order_ratio
        self.buy_bias = buy_bias
        self.deposit_range_thb = deposit_range_thb


ARCHETYPES = [
    # New Explorer
    (0.35, Archetype(
        "new_explorer",
        daily_active_prob=0.2,
        orders_per_active_mean=1.5,
        size_mean_log=7.0,   # ~ 1k–5k THB
        size_sigma_log=0.4,
        symbol_weights={"BTC_THB": 0.6, "USDT_THB": 0.4},
        market_order_ratio=0.8,
        buy_bias=0.6,
        deposit_range_thb=(500, 5_000),
    )),
    # Casual Retail
    (0.30, Archetype(
        "casual_retail",
        daily_active_prob=0.5,
        orders_per_active_mean=3,
        size_mean_log=9.0,   # ~ 2k–20k
        size_sigma_log=0.5,
        symbol_weights={"BTC_THB": 0.5, "ETH_THB": 0.3, "USDT_THB": 0.2},
        market_order_ratio=0.5,
        buy_bias=0.55,
        deposit_range_thb=(5_000, 50_000),
    )),
    # Active Trader
    (0.15, Archetype(
        "active_trader",
        daily_active_prob=0.9,
        orders_per_active_mean=15,
        size_mean_log=10.5,  # ~ 5k–100k
        size_sigma_log=0.6,
        symbol_weights={"BTC_THB": 0.4, "ETH_THB": 0.3, "USDT_THB": 0.3},
        market_order_ratio=0.3,
        buy_bias=0.5,
        deposit_range_thb=(50_000, 500_000),
    )),
    # Whale
    (0.03, Archetype(
        "whale",
        daily_active_prob=0.3,
        orders_per_active_mean=2,
        size_mean_log=12.0,  # ~ 100k–1M
        size_sigma_log=0.7,
        symbol_weights={"BTC_THB": 0.7, "ETH_THB": 0.3},
        market_order_ratio=0.2,
        buy_bias=0.5,
        deposit_range_thb=(500_000, 5_000_000),
    )),
    # Dormant / Returning
    (0.17, Archetype(
        "dormant",
        daily_active_prob=0.1,
        orders_per_active_mean=1,
        size_mean_log=8.5,
        size_sigma_log=0.6,
        symbol_weights={"BTC_THB": 0.6, "USDT_THB": 0.4},
        market_order_ratio=0.6,
        buy_bias=0.6,
        deposit_range_thb=(2_000, 30_000),
    )),
]

# ==============================
# Utility
# ==============================

def choose_archetype():
    r = random.random()
    cumulative = 0.0
    for weight, arch in ARCHETYPES:
        cumulative += weight
        if r <= cumulative:
            return arch
    return ARCHETYPES[-1][1]


def sample_lognormal_thb(mean_log, sigma_log):
    val = math.exp(random.normalvariate(mean_log, sigma_log))
    return max(100, val)


def weighted_choice(weights_dict):
    r = random.random()
    cumulative = 0.0
    for k, w in weights_dict.items():
        cumulative += w
        if r <= cumulative:
            return k
    return list(weights_dict.keys())[-1]


def sample_poisson(lam: float) -> int:
    # simple Poisson sampler (Knuth)
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def connect_dw():
    return psycopg2.connect(**DW)


def make_envelope(ts: datetime):
    """
    ให้ event_id, correlation_id, producer, event_time ตาม schema consumer.py
    """
    event_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    producer = PRODUCER_NAME
    event_time = ts
    return (event_id, correlation_id, producer, event_time)

def sample_from_weighted_list(weighted_list):
    r = random.random()
    cumulative = 0.0
    for value, w in weighted_list:
        cumulative += w
        if r <= cumulative:
            return value
    return weighted_list[-1][0]


def generate_daily_regimes():
    """
    กำหนดภาวะตลาดรายวัน (regime ต่อวัน) ระหว่าง START_AT ถึง END_AT
    ใช้ Markov-like transition จาก REGIME_TRANSITION
    """
    regimes_by_day = []
    num_days = (END_AT.date() - START_AT.date()).days + 1

    current = "normal"
    for _ in range(num_days):
        regimes_by_day.append(current)
        transitions = REGIME_TRANSITION.get(current, REGIME_TRANSITION["normal"])
        current = sample_from_weighted_list(transitions)

    return regimes_by_day


# ==============================
# Price Simulation (per 5 minutes)
# ==============================

DAILY_VOL = {
    "BTC_THB": 0.04,
    "ETH_THB": 0.06,
    "USDT_THB": 0.01,
}

def generate_price_series():
    series = {sym: [] for sym in SYMBOLS}
    step = timedelta(minutes=5)
    dt_sec = step.total_seconds()

    n_steps = int((END_AT - START_AT) / step) + 1

    for sym in SYMBOLS:
        price = BASE_PRICE[sym]
        sigma_daily = DAILY_VOL[sym]
        sigma = sigma_daily / math.sqrt(24 * 60 * 60)  # same as producer

        t = START_AT
        for _ in range(n_steps):
            z = random.gauss(0, 1)
            mu = 0.0  # match producer
            factor = math.exp(mu * dt_sec + sigma * math.sqrt(dt_sec) * z)
            price = max(1.0, price * factor)

            series[sym].append((t, price))
            t += step

    return series


def get_price_at(series, symbol, ts):
    arr = series[symbol]
    last_price = arr[0][1]
    for t, p in arr:
        if t > ts:
            break
        last_price = p
    return last_price


# ==============================
# Generate Users
# ==============================

def generate_users():
    users = []
    for _ in range(N_USERS):
        user_id = str(uuid.uuid4())
        arch = choose_archetype()
        init_deposit = random.uniform(*arch.deposit_range_thb)
        users.append({
            "user_id": user_id,
            "arch": arch,
            "init_deposit": init_deposit,
        })
    return users


# ==============================
# Main Backfill
# ==============================

def main():
    random.seed(42)
    conn = connect_dw()
    cur = conn.cursor()

    print("🔁 Generating price series...")
    price_series = generate_price_series()

    print("👤 Generating users...")
    users = generate_users()

    # --- Initial Deposits ---
    print("💰 Inserting initial deposits...")
    deposits_init = []
    for u in users:
        # สุ่มเวลาฝากในช่วงวันแรก ๆ ของ window
        event_time = START_AT + timedelta(minutes=random.randint(0, 60 * 48))
        env = make_envelope(event_time)
        tx_id = str(uuid.uuid4())
        amount = Decimal(str(round(u["init_deposit"], 2)))
        chain_tx_hash = "0x" + uuid.uuid4().hex[:32]
        network = "bank_transfer"
        confirmations = random.randint(6, 20)

        deposits_init.append(
            env + (
                tx_id,
                u["user_id"],
                "THB",
                amount,
                chain_tx_hash,
                network,
                confirmations,
            )
        )

    cur.executemany(
        """
        INSERT INTO fact_deposits
            (event_id, correlation_id, producer, event_time,
             tx_id, user_id, asset, amount, chain_tx_hash, network, confirmations)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        deposits_init,
    )
    conn.commit()

    print("📆 Backfilling daily events...")
    day_count = 0
    current_day = START_AT.date()

    while current_day <= END_AT.date():
        day_start = datetime.combine(current_day, datetime.min.time())
        day_end = day_start + timedelta(days=1)

        orders_to_insert = []
        matches_to_insert = []
        logins_to_insert = []
        tickers_to_insert = []
        deposits_today = []

        # --- TICKERS (ทุก 5 นาที) ---
        for sym in SYMBOLS:
            t = day_start
            while t < day_end and t <= END_AT:
                price = get_price_at(price_series, sym, t)
                last_price = Decimal(str(round(price, 2)))

                # fake OHLC + volume
                open_24h = last_price * Decimal("0.99")
                high_24h = last_price * Decimal("1.02")
                low_24h = last_price * Decimal("0.98")
                volume_24h = Decimal(str(round(random.uniform(1, 100), 6)))
                quote_vol_24h = volume_24h * last_price

                event_id = str(uuid.uuid4())
                tickers_to_insert.append(
                    (
                        event_id,
                        PRODUCER_NAME,
                        t,
                        sym,
                        last_price,
                        open_24h,
                        high_24h,
                        low_24h,
                        volume_24h,
                        quote_vol_24h,
                    )
                )
                t += timedelta(minutes=TICKER_INTERVAL_MINUTES)

        # --- USER BEHAVIOR ---
        for u in users:
            arch = u["arch"]

            # dormant ส่วนใหญ่ไม่ active
            if arch.name == "dormant" and random.random() < 0.8:
                continue

            if random.random() > arch.daily_active_prob:
                continue

            # LOGIN events
            n_logins = random.randint(1, 3)
            login_hours_candidates = [10, 14, 20]
            for _ in range(n_logins):
                hour = random.choice(login_hours_candidates)
                minute = random.randint(0, 59)
                login_time = day_start + timedelta(hours=hour, minutes=minute)
                if login_time > END_AT:
                    continue

                env = make_envelope(login_time)
                status = random.choices(
                    ["SUCCESS", "FAILED_PASS", "FAILED_2FA"],
                    weights=[0.8, 0.15, 0.05],
                )[0]
                ip_address = f"192.168.1.{random.randint(1, 254)}"

                logins_to_insert.append(
                    env + (
                        u["user_id"],
                        ip_address,
                        "device_sim",
                        "Bangkok, TH",
                        status,
                        "Mozilla/5.0 (simulator)",
                    )
                )

            # ORDERS
            n_orders = sample_poisson(arch.orders_per_active_mean)
            if n_orders <= 0:
                continue

            for _ in range(n_orders):
                hour = random.randint(9, 23)
                minute = random.randint(0, 59)
                sec = random.randint(0, 59)
                order_time = day_start + timedelta(hours=hour, minutes=minute, seconds=sec)
                if order_time > END_AT:
                    continue

                symbol = weighted_choice(arch.symbol_weights)
                side = "buy" if random.random() < arch.buy_bias else "sell"
                is_market = random.random() < arch.market_order_ratio
                order_type = "market" if is_market else "limit"

                mkt_price = get_price_at(price_series, symbol, order_time)
                notional_thb = sample_lognormal_thb(arch.size_mean_log, arch.size_sigma_log)
                quantity = notional_thb / mkt_price if mkt_price > 0 else 0.0
                quantity = max(quantity, 0.0001)

                price_val = None
                if not is_market:
                    jitter = 1 + random.uniform(-0.005, 0.005)
                    if side == "buy":
                        price_val = mkt_price * jitter * (1 - random.uniform(0, 0.003))
                    else:
                        price_val = mkt_price * jitter * (1 + random.uniform(0, 0.003))
                    price_val = max(price_val, 1)

                order_id = str(uuid.uuid4())
                env = make_envelope(order_time)
                price_dec = Decimal(str(round(price_val, 2))) if price_val is not None else None
                qty_dec = Decimal(str(round(quantity, 8)))
                time_in_force = "GTC"

                orders_to_insert.append(
                    env + (
                        order_id,
                        u["user_id"],
                        symbol,
                        side,
                        order_type,
                        price_dec,
                        qty_dec,
                        time_in_force,
                    )
                )

                # MATCH (simplified)
                match_prob = 0.9 if is_market else 0.6
                if random.random() < match_prob:
                    if is_market:
                        trade_price = mkt_price * (1 + random.uniform(-0.001, 0.001))
                    else:
                        trade_price = (price_val or mkt_price) * (1 + random.uniform(-0.001, 0.001))
                    trade_price = max(trade_price, 1)
                    trade_price_dec = Decimal(str(round(trade_price, 2)))

                    match_env = make_envelope(order_time)
                    match_id = str(uuid.uuid4())
                    maker_user = random.choice(users)["user_id"]
                    maker_order_id = str(uuid.uuid4())

                    maker_fee = Decimal("0.0")
                    taker_fee = Decimal("0.0")

                    matches_to_insert.append(
                        match_env + (
                            match_id,
                            symbol,
                            trade_price_dec,
                            qty_dec,
                            maker_user,
                            maker_order_id,
                            maker_fee,
                            "THB",
                            u["user_id"],      # taker_user_id = original user
                            order_id,          # taker_order_id
                            taker_fee,
                            "THB",
                            side,              # taker_side = 'buy' or 'sell'
                        )
                    )

            # small chance of extra deposit (top-up)
            if random.random() < 0.03:
                t = day_start + timedelta(hours=random.randint(9, 21), minutes=random.randint(0, 59))
                if t <= END_AT:
                    env = make_envelope(t)
                    dep_id = str(uuid.uuid4())   # tx_id
                    amt = Decimal(str(round(random.uniform(500, 20_000), 2)))
                    chain_tx_hash = "0x" + uuid.uuid4().hex[:32]
                    deposits_today.append(
                        env + (
                            dep_id,
                            u["user_id"],
                            "THB",
                            amt,
                            chain_tx_hash,
                            "bank_transfer",
                            random.randint(6, 20),
                        )
                    )

        # === INSERT DAILY BATCHES ===

        if logins_to_insert:
            cur.executemany(
                """
                INSERT INTO dim_user_logins
                    (event_id, correlation_id, producer, event_time,
                     user_id, ip_address, device_id, location_geo, status, user_agent)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                logins_to_insert,
            )

        if deposits_today:
            cur.executemany(
                """
                INSERT INTO fact_deposits
                    (event_id, correlation_id, producer, event_time,
                     tx_id, user_id, asset, amount, chain_tx_hash, network, confirmations)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                deposits_today,
            )

        if orders_to_insert:
            cur.executemany(
                """
                INSERT INTO fact_orders_created
                    (event_id, correlation_id, producer, event_time,
                     order_id, user_id, symbol, side, type, price, quantity, time_in_force)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                orders_to_insert,
            )

        if matches_to_insert:
            cur.executemany(
                """
                INSERT INTO fact_matches_executed
                    (event_id, correlation_id, producer, event_time,
                     match_id, symbol, price, quantity,
                     maker_user_id, maker_order_id, maker_fee, maker_fee_asset,
                     taker_user_id, taker_order_id, taker_fee, taker_fee_asset, taker_side)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                matches_to_insert,
            )

        if tickers_to_insert:
            cur.executemany(
                """
                INSERT INTO fact_market_tickers
                    (event_id, producer, event_time,
                     symbol, last_price, open_24h, high_24h, low_24h,
                     volume_24h, quote_vol_24h)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                tickers_to_insert,
            )

        conn.commit()
        day_count += 1
        print(
            f"✅ Backfilled day {day_count}: {current_day.isoformat()} "
            f"({len(orders_to_insert)} orders, {len(matches_to_insert)} matches, "
            f"{len(logins_to_insert)} logins, {len(tickers_to_insert)} tickers, "
            f"{len(deposits_today)} extra deposits)"
        )

        current_day += timedelta(days=1)

    cur.close()
    conn.close()
    print("🎉 Finished backfilling 3 months into warehouse.")


if __name__ == "__main__":
    main()
