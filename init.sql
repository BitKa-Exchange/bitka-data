-- init.sql

-- 1. Trading Domain
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL, -- buy, sell
    type VARCHAR(10) NOT NULL, -- limit, market
    price DECIMAL(20, 8),
    quantity DECIMAL(20, 8) NOT NULL,
    status VARCHAR(20) NOT NULL, -- open, filled, cancelled
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS matches (
    match_id UUID PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    maker_user_id UUID,
    taker_user_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Accounting Domain
CREATE TABLE IF NOT EXISTS deposits (
    tx_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    asset VARCHAR(10) NOT NULL,
    amount DECIMAL(20, 8) NOT NULL,
    network VARCHAR(20),
    status VARCHAR(20) DEFAULT 'confirmed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS withdrawals (
    withdrawal_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    asset VARCHAR(10) NOT NULL,
    amount DECIMAL(20, 8) NOT NULL,
    dest_address VARCHAR(255),
    tx_hash VARCHAR(255), -- จะ update เมื่อส่งแล้ว (sent)
    status VARCHAR(20) NOT NULL, -- requested, sent
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Identity Domain
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    kyc_level INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS login_history (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    ip_address VARCHAR(50),
    device_id VARCHAR(255),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Market Data Domain
CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    last_price DECIMAL(20, 8),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    volume_24h DECIMAL(20, 8),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. System (Audit)
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id UUID PRIMARY KEY,
    actor_id UUID,
    action VARCHAR(50),
    resource VARCHAR(50),
    details_before JSONB,
    details_after JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ตั้งค่าให้ Replica Identity เป็น FULL เพื่อให้ Debezium จับข้อมูลได้ครบถ้วนตอน Update/Delete
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE withdrawals REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE tickers REPLICA IDENTITY FULL;