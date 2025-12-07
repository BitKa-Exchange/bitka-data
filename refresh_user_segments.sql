-- =====================================================================
-- Refresh dim_user_segments (30-day behavior based segmentation, v2)
-- Segments:
--   - whale
--   - active_trader
--   - casual_trader
--   - new_explorer
--   - dormant
--   - inactive (optional)
-- =====================================================================

-- 0) สร้างตารางเปล่าก่อน (ถ้ายังไม่มี)
CREATE TABLE IF NOT EXISTS dim_user_segments (
    user_id UUID PRIMARY KEY,
    segment VARCHAR(32),

    trades_30d INT,
    volume_30d_thb NUMERIC(30,10),
    active_days_30d INT,
    avg_trades_per_active_day_30d NUMERIC(18,4),

    last_trade_at TIMESTAMP,
    last_login_at TIMESTAMP,

    updated_at TIMESTAMP DEFAULT NOW()
);

-- 1) ใช้ CTE สร้าง feature + segment แล้ว upsert ทีเดียว
WITH trades_30d AS (
    SELECT
        taker_user_id AS user_id,
        COUNT(*) AS trades_30d,
        SUM(price * quantity) AS volume_30d_thb,
        MAX(event_time) AS last_trade_at
    FROM fact_matches_executed
    WHERE event_time >= NOW() - INTERVAL '30 days'
      AND symbol LIKE '%_THB'
    GROUP BY taker_user_id
),

activity_days_30d AS (
    SELECT
        taker_user_id AS user_id,
        COUNT(DISTINCT DATE(event_time)) AS active_days_30d
    FROM fact_matches_executed
    WHERE event_time >= NOW() - INTERVAL '30 days'
      AND symbol LIKE '%_THB'
    GROUP BY taker_user_id
),

logins_30d AS (
    SELECT
        user_id,
        MAX(event_time) AS last_login_at
    FROM dim_user_logins
    WHERE event_time >= NOW() - INTERVAL '30 days'
    GROUP BY user_id
),

all_users AS (
    SELECT user_id FROM trades_30d
    UNION
    SELECT user_id FROM logins_30d
),

combined AS (
    SELECT
        u.user_id,
        COALESCE(t.trades_30d, 0)               AS trades_30d,
        COALESCE(t.volume_30d_thb, 0)           AS volume_30d_thb,
        COALESCE(a.active_days_30d, 0)          AS active_days_30d,
        CASE
            WHEN COALESCE(a.active_days_30d, 0) > 0
                THEN COALESCE(t.trades_30d, 0)::NUMERIC
                     / a.active_days_30d
            ELSE 0
        END                                     AS avg_trades_per_active_day_30d,
        t.last_trade_at,
        l.last_login_at
    FROM all_users u
    LEFT JOIN trades_30d        t ON t.user_id = u.user_id
    LEFT JOIN activity_days_30d a ON a.user_id = u.user_id
    LEFT JOIN logins_30d        l ON l.user_id = u.user_id
),

segmented AS (
    SELECT
        c.*,
        CASE
            -- INACTIVE: ไม่เทรด + ไม่เคย login ใน 30 วัน
            WHEN COALESCE(c.trades_30d, 0) = 0
                 AND c.last_login_at IS NULL
                THEN 'inactive'

            -- WHALE:
            --  - volume_30d_thb >= 5M
            --  - หรือ avg_trades_per_active_day >= 20
            WHEN c.volume_30d_thb >= 5000000
                 OR c.avg_trades_per_active_day_30d >= 20
                THEN 'whale'

            -- ACTIVE TRADER:
            --  - trades_30d >= 50
            --  - active_days_30d >= 10
            WHEN c.trades_30d >= 50
                 AND c.active_days_30d >= 10
                THEN 'active_trader'

            -- CASUAL TRADER:
            --  - trades_30d 5–49
            WHEN c.trades_30d BETWEEN 5 AND 49
                THEN 'casual_trader'

            -- NEW EXPLORER:
            --  - trades_30d 1–4
            WHEN c.trades_30d BETWEEN 1 AND 4
                THEN 'new_explorer'

            -- DORMANT:
            --  - ที่เหลือ
            ELSE 'dormant'
        END AS segment
    FROM combined c
)

INSERT INTO dim_user_segments (
    user_id,
    segment,
    trades_30d,
    volume_30d_thb,
    active_days_30d,
    avg_trades_per_active_day_30d,
    last_trade_at,
    last_login_at,
    updated_at
)
SELECT
    s.user_id,
    s.segment,
    s.trades_30d,
    s.volume_30d_thb,
    s.active_days_30d,
    s.avg_trades_per_active_day_30d,
    s.last_trade_at,
    s.last_login_at,
    NOW() AS updated_at
FROM segmented s
ON CONFLICT (user_id) DO UPDATE
SET
    segment                       = EXCLUDED.segment,
    trades_30d                    = EXCLUDED.trades_30d,
    volume_30d_thb                = EXCLUDED.volume_30d_thb,
    active_days_30d               = EXCLUDED.active_days_30d,
    avg_trades_per_active_day_30d = EXCLUDED.avg_trades_per_active_day_30d,
    last_trade_at                 = EXCLUDED.last_trade_at,
    last_login_at                 = EXCLUDED.last_login_at,
    updated_at                    = EXCLUDED.updated_at;
