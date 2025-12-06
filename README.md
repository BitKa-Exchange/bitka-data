# 🏗️ Bitka Data Pipeline & Warehouse

  ส่วนนี้รับผิดชอบเรื่องการดึงข้อมูล (Ingestion), การประมวลผลแบบ Real-time (Streaming), และการจัดเก็บลงคลังข้อมูล (Data Warehousing) เพื่อรองรับการวิเคราะห์


--------------------------------------------------------------------------------


## 🔄 Architecture Overview

  ระบบใช้สถาปัตยกรรม **CDC (Change Data Capture)** เพื่อดึงข้อมูล Real-time จาก Database หลักผ่าน Kafka และนำไปจัดเก็บใน Data Warehouse

  graph LR
    Source[(Postgres DB)] -- CDC (Debezium) --> Kafka[Redpanda / Kafka]
    Kafka -- Topics --> Consumer[Python ETL Scripts]
    Consumer -- Clean & Transform --> Warehouse[(Postgres DW)]


--------------------------------------------------------------------------------


Workflow:

  Source: จับการเปลี่ยนแปลงข้อมูล (Insert/Update) จาก auth-service และ account-service

  Ingestion: ใช้ Debezium อ่าน WAL logs และส่ง Event เข้าสู่ Redpanda (Kafka)

  Processing: Python Consumers ดักฟังหัวข้อ (Topics) ต่างๆ, แปลง Data Types (เช่น Timestamp, Decimal), และจัดการ Data Consistency

  Storage: จัดเก็บลง PostgreSQL Data Warehouse ในรูปแบบ Star Schema (Fact & Dimension tables)


--------------------------------------------------------------------------------


🚀 Key Features (สิ่งที่ทำในส่วนนี้)

  Real-time Processing: ข้อมูลไหลเข้า Warehouse ทันทีที่มีการเทรดหรือสมัครสมาชิก

  Robustness: ระบบมี Auto-Reconnect Logic หาก Database หรือ Kafka หลุด Script จะรอและเชื่อมต่อใหม่เองโดยอัตโนมัติ ไม่ Crash

  Data Integrity: จัดการเรื่อง Decimal Precision สำหรับข้อมูลการเงิน (Money/Crypto) อย่างถูกต้องแม่นยำ ป้องกันปัญหาทศนิยมเพี้ยน

  Secure Configuration: ใช้การจัดการ Config ผ่าน .env แยก Environment ชัดเจน ไม่มีการ Hardcode รหัสผ่าน


--------------------------------------------------------------------------------


🛠️ Tech Stack

  Streaming Platform: Redpanda (Kafka Compatible)

  Connectors: Debezium Postgres Connector

  ETL Language: Python 3.x (kafka-python, psycopg2, python-dotenv)

  Data Warehouse: PostgreSQL


--------------------------------------------------------------------------------


📂 Data Warehouse Schema

  ข้อมูลถูกจัดเก็บแยกตามวัตถุประสงค์:

  Table Name	              Type	        Description
  dim_users_history	        Dimension	    เก็บประวัติการเปลี่ยนแปลงข้อมูล User
  fact_orders	Fact	        ข้อมูลการวาง    Order (Buy/Sell)
  fact_trade_executions	    Fact	        ข้อมูลการจับคู่เทรดที่สำเร็จ (Matched)
  fact_transfers	          Fact	        ประวัติการโอนเงินระหว่างบัญชี
  fact_wallet_transactions	Fact	        การเปลี่ยนแปลงยอดเงินในกระเป๋า (Deposit/Withdraw)


--------------------------------------------------------------------------------


⚙️ How to Run (วิธีรันระบบ Data)

  1. Prerequisites

    ตรวจสอบไฟล์ .env ว่าตั้งค่าถูกต้อง (ดูตัวอย่างจาก .env.example)

  2. Install Dependencies

    ติดตั้ง Library ที่จำเป็นผ่าน pip

    Bash
    pip install -r requirements.txt

  3. Start Consumers

    รัน Script เพื่อเริ่มดูดข้อมูล (แนะนำให้รันแยก Terminal หรือรันเป็น Background process)

    Terminal 1: สำหรับข้อมูล Users

    Bash
    python consumer.py

    Terminal 2: สำหรับข้อมูล Trading (Orders, Transactions)

    Bash
    python consumer_transactions.py

  4.dashboard.py
    รันเพื่อดูภาพรวมข้อมูลผ่าน dashboard

    Bash
    python3 -m streamlit run data-platform/dashboard.py 


  5.เข้า sql warehouse

    Bash
    docker exec -it bitka-warehouse psql -U warehouse_admin -d bitka_dw


  6.เทสข้อมูล
    ก็อปโค้ดด้านล่างไปวางจะได้ข้อมูลจำลอง
    Bash
          docker exec -i bitka-postgres psql -U postgres -d auth_db -c "
      -- 1. สร้างกราฟราคา (Market Prices) 50 จุด (จำลองราคา BTC วิ่งแกว่งตัว)
      INSERT INTO market_prices (symbol, price)
      SELECT 
          'BTC_THB',
          3000000 + (random() * 50000 - 25000) -- ราคาแกว่งบวกลบ 25,000
      FROM generate_series(1, 50);

      -- 2. สร้างออเดอร์ (Orders) 40 รายการ (สุ่ม Buy/Sell)
      INSERT INTO orders (user_id, symbol, side, price, amount)
      SELECT 
          (random() * 20 + 1)::INT, -- สุ่ม User ID 1-20
          'BTC_THB',
          CASE WHEN random() > 0.5 THEN 'BUY' ELSE 'SELL' END,
          3000000 + (random() * 10000),
          (random() * 0.5 + 0.01)
      FROM generate_series(1, 40);

      -- 3. สร้างการจับคู่เทรด (Executions) 30 รายการ
      INSERT INTO trade_executions (order_id, match_id, symbol, side, price, quantity, fee, role)
      SELECT 
          i,
          'MATCH-' || i,
          'BTC_THB',
          CASE WHEN random() > 0.5 THEN 'BUY' ELSE 'SELL' END,
          3000000 + (random() * 5000),
          (random() * 0.1 + 0.01),
          (random() * 100),
          CASE WHEN random() > 0.5 THEN 'MAKER' ELSE 'TAKER' END
      FROM generate_series(1, 30) AS i;

      -- 4. สร้างธุรกรรมการเงิน (Wallet) 20 รายการ (เน้นเติมเงิน Deposit)
      INSERT INTO wallet_transactions (user_id, currency, amount_change, transaction_type, balance_after)
      SELECT 
          (random() * 20 + 1)::INT,
          'THB',
          (random() * 500000 + 10000), -- เติมเงิน 10k - 500k
          'DEPOSIT',
          (random() * 1000000)
      FROM generate_series(1, 20);

      -- 5. โอนเงินข้ามบัญชี (Transfers) 10 รายการ
      INSERT INTO transfers (sender_id, receiver_id, amount, currency)
      SELECT 
          (random() * 10 + 1)::INT,
          (random() * 10 + 11)::INT,
          (random() * 1000 + 100),
          'THB'
      FROM generate_series(1, 10);
      "

    Scenario B: ลูกค้าเติมเงิน (Wallet Deposit)

    SQL
    -- รันใน Source DB (bitka_account)
    -- สมมติ user_id = 1
    INSERT INTO wallet_transactions (user_id, currency, amount_change, transaction_type, balance_after, created_at)
    VALUES (1, 'THB', 5000.00, 'DEPOSIT', 5000.00, NOW());
    👉 Expected Result: ดูที่ Terminal consumer_transactions.py จะขึ้น 💳 Wallet: DEPOSIT... และข้อมูลโผล่ใน fact_wallet_transactions

    Scenario C: การส่งคำสั่งซื้อ (Place Order)

    SQL
    -- รันใน Source DB (bitka_account)
    INSERT INTO orders (user_id, symbol, side, price, amount, status, created_at)
    VALUES (1, 'BTC_THB', 'BUY', 1000000, 0.5, 'OPEN', NOW());
    👉 Expected Result: ข้อมูลจะไหลเข้า fact_orders