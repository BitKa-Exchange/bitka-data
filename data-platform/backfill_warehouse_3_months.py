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

BASE_PRICE = {
    "BTC_THB": 1_500_000,
    "ETH_THB": 80_000,
    "USDT_THB": 36,
}

TICKER_INTERVAL_MINUTES = 5

# ==============================
# Archetype configs (คงเดิม)
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
    (0.35, Archetype("new_explorer", 0.2, 1.5, 7.0, 0.4, {"BTC_THB": 0.6, "USDT_THB": 0.4}, 0.8, 0.6, (500, 5_000))),
    (0.30, Archetype("casual_retail", 0.5, 3, 9.0, 0.5, {"BTC_THB": 0.5, "ETH_THB": 0.3, "USDT_THB": 0.2}, 0.5, 0.55, (5_000, 50_000))),
    (0.15, Archetype("active_trader", 0.9, 15, 10.5, 0.6, {"BTC_THB": 0.4, "ETH_THB": 0.3, "USDT_THB": 0.3}, 0.3, 0.5, (50_000, 500_000))),
    (0.03, Archetype("whale", 0.3, 2, 12.0, 0.7, {"BTC_THB": 0.7, "ETH_THB": 0.3}, 0.2, 0.5, (500_000, 5_000_000))),
    (0.17, Archetype("dormant", 0.1, 1, 8.5, 0.6, {"BTC_THB": 0.6, "USDT_THB": 0.4}, 0.6, 0.6, (2_000, 30_000))),
]

# ==============================
# Utility Functions
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
    if lam <= 0: return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

def connect_dw():
    return psycopg2.connect(**DW)

# ==============================
# Price Simulation
# ==============================
DAILY_VOL = {"BTC_THB": 0.04, "ETH_THB": 0.06, "USDT_THB": 0.01}

def generate_price_series():
    series = {sym: [] for sym in SYMBOLS}
    step = timedelta(minutes=5)
    dt_sec = step.total_seconds()
    n_steps = int((END_AT - START_AT) / step) + 1

    for sym in SYMBOLS:
        price = BASE_PRICE[sym]
        sigma_daily = DAILY_VOL[sym]
        sigma = sigma_daily / math.sqrt(24 * 60 * 60)
        t = START_AT
        for _ in range(n_steps):
            z = random.gauss(0, 1)
            mu = 0.0
            factor = math.exp(mu * dt_sec + sigma * math.sqrt(dt_sec) * z)
            price = max(1.0, price * factor)
            series[sym].append((t, price))
            t += step
    return series

def get_price_at(series, symbol, ts):
    arr = series[symbol]
    last_price = arr[0][1]
    for t, p in arr:
        if t > ts: break
        last_price = p
    return last_price

# ==============================
# Generate Users Structure
# ==============================
def generate_users():
    users = []
    for i in range(N_USERS):
        user_id = str(uuid.uuid4())
        arch = choose_archetype()
        init_deposit = random.uniform(*arch.deposit_range_thb)
        # สร้าง email ปลอมสำหรับใส่ใน table users
        email = f"user_{i}_{arch.name}@example.com"
        kyc_level = random.choice([1, 2, 3])
        
        users.append({
            "user_id": user_id,
            "email": email,
            "kyc_level": kyc_level,
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

    print("👤 Generating users logic...")
    users = generate_users()

    # --- 1. Populate 'users' table ---
    print("👤 Inserting users into DB...")
    users_to_insert = []
    for u in users:
        # created_at สุ่มในช่วงวันแรกๆ
        created_at = START_AT + timedelta(minutes=random.randint(0, 60 * 24))
        users_to_insert.append((
            u["user_id"],
            u["email"],
            u["kyc_level"],
            created_at,
            created_at
        ))
    
    cur.executemany(
        """
        INSERT INTO users (user_id, email, kyc_level, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
        """,
        users_to_insert
    )
    conn.commit()

    # --- 2. Initial Deposits ---
    print("💰 Inserting initial deposits...")
    deposits_init = []
    for u in users:
        # สุ่มเวลาฝากหลังจาก user create เล็กน้อย
        user_data = next(x for x in users_to_insert if x[0] == u["user_id"])
        user_created_at = user_data[3]
        event_time = user_created_at + timedelta(minutes=random.randint(5, 60))
        
        tx_id = str(uuid.uuid4())
        amount = Decimal(str(round(u["init_deposit"], 2)))
        network = "bank_transfer"
        status = "confirmed"

        deposits_init.append((
            tx_id,
            u["user_id"],
            "THB",
            amount,
            network,
            status,
            event_time
        ))

    cur.executemany(
        """
        INSERT INTO deposits (tx_id, user_id, asset, amount, network, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tx_id) DO NOTHING;
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
        tickers_to_upsert = [] # Changed to upsert because symbol is PK
        deposits_today = []

        # --- TICKERS (ทุก 5 นาที) ---
        # Note: เนื่องจาก schema ใหม่ใช้ symbol เป็น PK
        # การ insert ย้อนหลังจะจบที่ราคาสุดท้ายของวันนั้นๆ ในการรันจริง
        # เราจะจำลองการ update ราคาไปเรื่อยๆ เพื่อให้ logic ทำงานได้
        for sym in SYMBOLS:
            t = day_start
            while t < day_end and t <= END_AT:
                price = get_price_at(price_series, sym, t)
                last_price = Decimal(str(round(price, 2)))

                high_24h = last_price * Decimal("1.02")
                low_24h = last_price * Decimal("0.98")
                volume_24h = Decimal(str(round(random.uniform(1, 100), 6)))

                # ใน schema ใหม่ tickers มี updated_at
                tickers_to_upsert.append(
                    (
                        sym,
                        last_price,
                        high_24h,
                        low_24h,
                        volume_24h,
                        t
                    )
                )
                t += timedelta(minutes=TICKER_INTERVAL_MINUTES)

        # --- USER BEHAVIOR ---
        for u in users:
            arch = u["arch"]
            
            # ตรวจสอบว่า user เกิดหรือยัง
            user_created_at = next(x[3] for x in users_to_insert if x[0] == u["user_id"])
            if day_start < user_created_at:
                continue

            if arch.name == "dormant" and random.random() < 0.8:
                continue
            if random.random() > arch.daily_active_prob:
                continue

            # LOGIN events
            n_logins = random.randint(1, 3)
            for _ in range(n_logins):
                hour = random.choice([10, 14, 20])
                minute = random.randint(0, 59)
                login_time = day_start + timedelta(hours=hour, minutes=minute)
                if login_time > END_AT: continue

                status = random.choices(["SUCCESS", "FAILED"], weights=[0.9, 0.1])[0]
                ip_address = f"192.168.1.{random.randint(1, 254)}"
                device_id = f"dev_{uuid.uuid4().hex[:8]}"

                logins_to_insert.append((
                    u["user_id"],
                    ip_address,
                    device_id,
                    status,
                    login_time
                ))

            # ORDERS
            n_orders = sample_poisson(arch.orders_per_active_mean)
            if n_orders <= 0: continue

            for _ in range(n_orders):
                hour = random.randint(9, 23)
                minute = random.randint(0, 59)
                sec = random.randint(0, 59)
                order_time = day_start + timedelta(hours=hour, minutes=minute, seconds=sec)
                if order_time > END_AT: continue

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
                    price_val = mkt_price * jitter
                    price_val = max(price_val, 1)

                order_id = str(uuid.uuid4())
                price_dec = Decimal(str(round(price_val, 2))) if price_val is not None else None
                qty_dec = Decimal(str(round(quantity, 8)))
                
                # Match logic
                match_prob = 0.9 if is_market else 0.6
                order_status = "open"
                
                if random.random() < match_prob:
                    order_status = "filled"
                    if is_market:
                        trade_price = mkt_price * (1 + random.uniform(-0.001, 0.001))
                    else:
                        trade_price = (price_val or mkt_price) * (1 + random.uniform(-0.001, 0.001))
                    
                    trade_price = max(trade_price, 1)
                    trade_price_dec = Decimal(str(round(trade_price, 2)))

                    match_id = str(uuid.uuid4())
                    maker_user = random.choice(users)["user_id"]

                    matches_to_insert.append((
                        match_id,
                        symbol,
                        trade_price_dec,
                        qty_dec,
                        maker_user,
                        u["user_id"], # taker
                        order_time
                    ))

                orders_to_insert.append((
                    order_id,
                    u["user_id"],
                    symbol,
                    side,
                    order_type,
                    price_dec,
                    qty_dec,
                    order_status,
                    order_time,
                    order_time # updated_at
                ))

            # EXTRA DEPOSITS
            if random.random() < 0.03:
                t = day_start + timedelta(hours=random.randint(9, 21), minutes=random.randint(0, 59))
                if t <= END_AT:
                    dep_id = str(uuid.uuid4())
                    amt = Decimal(str(round(random.uniform(500, 20_000), 2)))
                    deposits_today.append((
                        dep_id,
                        u["user_id"],
                        "THB",
                        amt,
                        "bank_transfer",
                        "confirmed",
                        t
                    ))

        # === INSERT DAILY BATCHES ===

        # LOGINS
        if logins_to_insert:
            cur.executemany(
                """
                INSERT INTO login_history (user_id, ip_address, device_id, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                logins_to_insert
            )

        # ORDERS
        if orders_to_insert:
            cur.executemany(
                """
                INSERT INTO orders (order_id, user_id, symbol, side, type, price, quantity, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING;
                """,
                orders_to_insert
            )

        # MATCHES
        if matches_to_insert:
            cur.executemany(
                """
                INSERT INTO matches (match_id, symbol, price, quantity, maker_user_id, taker_user_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO NOTHING;
                """,
                matches_to_insert
            )

        # EXTRA DEPOSITS
        if deposits_today:
            cur.executemany(
                """
                INSERT INTO deposits (tx_id, user_id, asset, amount, network, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tx_id) DO NOTHING;
                """,
                deposits_today
            )

        # TICKERS — use UPSERT
        if tickers_to_upsert:
            cur.executemany(
                """
                INSERT INTO tickers (symbol, last_price, high_24h, low_24h, volume_24h, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol)
                DO UPDATE SET
                    last_price = EXCLUDED.last_price,
                    high_24h = EXCLUDED.high_24h,
                    low_24h = EXCLUDED.low_24h,
                    volume_24h = EXCLUDED.volume_24h,
                    updated_at = EXCLUDED.updated_at;
                """,
                tickers_to_upsert
            )

        conn.commit()

        day_count += 1
        print(
            f"✅ Backfilled day {day_count}: {current_day.isoformat()} "
            f"({len(orders_to_insert)} orders, {len(matches_to_insert)} matches, "
            f"{len(logins_to_insert)} logins, {len(deposits_today)} extra deposits)"
        )

        current_day += timedelta(days=1)

    cur.close()
    conn.close()
    print("🎉 Finished backfilling 3 months into warehouse (New Schema).")

if __name__ == "__main__":
    main()