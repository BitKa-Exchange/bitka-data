/*
 * Bitka Exchange - Core Database Schema
 * -------------------------------------
 * Description: 
 * Initializes the relational schema for the Trading Engine and Ledger system.
 * Designed to support High Precision Financial Data and Change Data Capture (CDC).
 *
 * Key Architectural Decisions:
 * 1. UUIDs: Used for Primary Keys to ensure global uniqueness across distributed systems.
 * 2. DECIMAL(20,8): Mandatory for crypto/fiat currency to prevent floating-point errors.
 * 3. REPLICA IDENTITY: Configured for Debezium to capture 'BEFORE' states during updates.
 */

-- ==========================================
-- 1. TRADING DOMAIN (Order Book & Matching)
-- ==========================================

-- Stores all user orders (Active & Historical)
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL, -- Enum: 'buy', 'sell'
    type VARCHAR(10) NOT NULL, -- Enum: 'limit', 'market'
    
    -- Financial Precision: 20 digits total, 8 decimal places (Standard for Crypto)
    price DECIMAL(20, 8),
    quantity DECIMAL(20, 8) NOT NULL,
    
    status VARCHAR(20) NOT NULL, -- Enum: 'open', 'filled', 'cancelled'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stores executed trades (Result of order matching)
CREATE TABLE IF NOT EXISTS matches (
    match_id UUID PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    maker_user_id UUID, -- Liquidity Provider
    taker_user_id UUID, -- Liquidity Taker
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 2. ACCOUNTING DOMAIN (Ledger)
-- ==========================================

-- Inbound transactions from Blockchain
CREATE TABLE IF NOT EXISTS deposits (
    tx_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    asset VARCHAR(10) NOT NULL, -- e.g., 'BTC', 'USDT'
    amount DECIMAL(20, 8) NOT NULL,
    network VARCHAR(20),      -- e.g., 'ERC20', 'TRC20'
    status VARCHAR(20) DEFAULT 'confirmed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outbound transactions requests
CREATE TABLE IF NOT EXISTS withdrawals (
    withdrawal_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    asset VARCHAR(10) NOT NULL,
    amount DECIMAL(20, 8) NOT NULL,
    dest_address VARCHAR(255),
    tx_hash VARCHAR(255), -- Populated after blockchain broadcast
    status VARCHAR(20) NOT NULL, -- Pipeline: requested -> sent
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 3. IDENTITY DOMAIN (User Management)
-- ==========================================

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    kyc_level INT DEFAULT 1, -- Controls withdrawal limits
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS login_history (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    ip_address VARCHAR(50),
    device_id VARCHAR(255), -- Device Fingerprint
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 4. MARKET DATA DOMAIN
-- ==========================================

-- Snapshot of current market state (Updated frequently)
CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    last_price DECIMAL(20, 8),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    volume_24h DECIMAL(20, 8),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 5. SYSTEM AUDIT DOMAIN
-- ==========================================

-- Immutable log of sensitive actions
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id UUID PRIMARY KEY,
    actor_id UUID,
    action VARCHAR(50),
    resource VARCHAR(50),
    
    -- JSONB allows flexible schema for different event types (NoSQL-like storage)
    details_before JSONB,
    details_after JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- CDC CONFIGURATION (Critical for Debezium)
-- ==========================================
-- By default, Postgres WAL only contains the Primary Key for UPDATE/DELETE events.
-- 'REPLICA IDENTITY FULL' forces Postgres to log the entire row state (Before & After).
-- This enables the Data Warehouse to handle row updates and deletions correctly.
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE withdrawals REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE tickers REPLICA IDENTITY FULL;
-- Note: audit_logs doesn't need this as it's append-only (Insert Only).