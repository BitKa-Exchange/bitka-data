import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from decimal import Decimal

# --- Configuration ---
KAFKA_BROKER = 'localhost:19092'

# หัวข้อที่เราจะยิงข้อมูลเข้าไป (ต้องตรงกับ Consumer)
TOPICS = {
    'ORDER_CREATED': 'trading.orders.created',
    'ORDER_CANCELLED': 'trading.orders.cancelled',
    'MATCH_EXECUTED': 'trading.matches.executed',
    'DEPOSIT': 'accounting.deposit.confirmed',
    'WITHDRAW_REQ': 'accounting.withdrawal.requested',
    'WITHDRAW_SENT': 'accounting.withdrawal.sent',
    'LOGIN': 'identity.user.login',
    'KYC': 'identity.kyc.updated',
    'TICKER': 'market.ticker.update',
    'AUDIT': 'system.audit.entry'
}

# --- Data Pools (Mock Data) ---
SYMBOLS = ['BTC_THB', 'ETH_THB', 'DOGE_THB', 'USDT_THB']
USERS = [str(uuid.uuid4()) for _ in range(10)] # สร้าง User ปลอม 10 คน
ASSETS = ['BTC', 'ETH', 'DOGE', 'USDT', 'THB']
PRODUCERS = ['order-service', 'matching-engine', 'ledger-service', 'auth-service', 'market-data-service']

# --- Helper Functions ---
def get_iso_time():
    return datetime.utcnow().isoformat() + 'Z'

def random_decimal_str(min_val, max_val, precision=2):
    """สุ่มตัวเลขและคืนค่าเป็น String เพื่อความแม่นยำแบบการเงิน"""
    val = random.uniform(min_val, max_val)
    return f"{val:.{precision}f}"

def create_envelope(producer_name, payload):
    """ห่อข้อมูลตามมาตรฐาน Envelope"""
    return {
        "event_id": str(uuid.uuid4()),
        "correlation_id": str(uuid.uuid4()),
        "producer": producer_name,
        "timestamp": get_iso_time(),
        "data": payload
    }

# --- Generators for Each Domain ---

def gen_order_created():
    user = random.choice(USERS)
    symbol = random.choice(SYMBOLS)
    side = random.choice(['buy', 'sell'])
    order_type = random.choice(['limit', 'market'])
    price = random_decimal_str(1000, 3000000) if order_type == 'limit' else "0"
    
    payload = {
        "order_id": str(uuid.uuid4()),
        "user_id": user,
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "price": price,
        "quantity": random_decimal_str(0.01, 5, 8),
        "time_in_force": random.choice(['GTC', 'IOC', 'FOK'])
    }
    return TOPICS['ORDER_CREATED'], create_envelope('order-service', payload)

def gen_match_executed():
    symbol = random.choice(SYMBOLS)
    price = random_decimal_str(1000, 3000000)
    qty = random_decimal_str(0.01, 2, 8)
    
    payload = {
        "match_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": price,
        "quantity": qty,
        "maker_user_id": random.choice(USERS),
        "maker_order_id": str(uuid.uuid4()),
        "maker_fee": random_decimal_str(0, 10),
        "maker_fee_asset": "THB",
        "taker_user_id": random.choice(USERS),
        "taker_order_id": str(uuid.uuid4()),
        "taker_fee": random_decimal_str(0, 0.001, 8),
        "taker_fee_asset": symbol.split('_')[0],
        "taker_side": random.choice(['buy', 'sell'])
    }
    return TOPICS['MATCH_EXECUTED'], create_envelope('matching-engine', payload)

def gen_deposit():
    asset = random.choice(ASSETS)
    payload = {
        "tx_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "asset": asset,
        "amount": random_decimal_str(10, 5000),
        "chain_tx_hash": "0x" + uuid.uuid4().hex,
        "network": "ERC20" if asset != 'BTC' else 'BITCOIN',
        "confirmations": random.randint(1, 12)
    }
    return TOPICS['DEPOSIT'], create_envelope('ledger-service', payload)

def gen_login():
    status = random.choices(['success', 'failed_pass', 'failed_2fa'], weights=[80, 15, 5])[0]
    payload = {
        "user_id": random.choice(USERS),
        "ip_address": f"192.168.1.{random.randint(1, 255)}",
        "device_id": uuid.uuid4().hex[:16],
        "location_geo": "Bangkok, TH",
        "status": status,
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)..."
    }
    return TOPICS['LOGIN'], create_envelope('auth-service', payload)

def gen_ticker():
    symbol = random.choice(SYMBOLS)
    base_price = random.uniform(10000, 1000000)
    payload = {
        "symbol": symbol,
        "last_price": f"{base_price:.2f}",
        "open_24h": f"{base_price * 0.95:.2f}",
        "high_24h": f"{base_price * 1.05:.2f}",
        "low_24h": f"{base_price * 0.90:.2f}",
        "volume_24h": random_decimal_str(10, 500),
        "quote_vol_24h": random_decimal_str(1000000, 50000000)
    }
    # Ticker often doesn't need correlation_id, but our envelope requires it, so we generate one.
    return TOPICS['TICKER'], create_envelope('market-data-service', payload)

def gen_audit():
    actions = ['change_password', 'create_api_key', 'export_data', 'update_kyc_rules']
    action = random.choice(actions)
    payload = {
        "actor_id": random.choice(USERS),
        "action": action,
        "resource": "user_security" if "password" in action else "system_config",
        "details_before": {"enabled": False},
        "details_after": {"enabled": True},
        "severity": "WARN" if "export" in action else "INFO"
    }
    return TOPICS['AUDIT'], create_envelope('audit-service', payload)

# --- Main Simulation Loop ---
if __name__ == "__main__":
    print(f"🚀 Bitka Producer Simulator Connecting to {KAFKA_BROKER}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka!")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        exit(1)

    print("🎲 Starting Data Simulation... (Press Ctrl+C to stop)")
    
    try:
        while True:
            # Randomly select an event type to generate
            # Weights make some events more frequent than others (e.g., Tickers > Logins)
            choice = random.choices(
                [gen_order_created, gen_match_executed, gen_deposit, gen_login, gen_ticker, gen_audit],
                weights=[30, 20, 10, 15, 40, 5], 
                k=1
            )[0]

            topic, message = choice()
            
            # Send to Kafka
            producer.send(topic, value=message)
            
            # Print simplified log
            event_type = topic.split('.')[-1].upper()
            print(f"📤 Sent [{event_type}] to {topic} | ID: {message['event_id']}")
            
            # Sleep a bit to simulate realistic traffic (100ms - 500ms)
            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("\n🛑 Simulation Stopped.")
        producer.close()