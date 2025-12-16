#🪙 Bitka Data Platform (End-to-End Data Pipeline PoC)**Project Status:** *Proof of Concept (PoC) / Ready for Integration*

This project simulates and designs a **Data Engineering Pipeline** for a digital asset trading platform (Exchange), focusing on **Real-time Streaming** data management.

The main objective of this project is not to build a perfect trading app, but to answer the engineering question: **"How do we design a pipeline to handle high-velocity, high-volume transactions and display them on a dashboard with minimal delay?"**

Since the main Backend (Core Trading System) is currently under development, to avoid blocking the Data team, this project creates a **Simulation Layer** to mockup data as realistically as possible to test the proposed architecture.

---

##🧩 1. System Architecture & Design DecisionsThe system is designed with a clear separation between **Source (Data Producer)** and **Destination (Data Consumer)**, using Kafka as a middleware for decoupling.

###🛠 Tech Stack and Rationale* **Database (PostgreSQL):** Selected as both Source and Warehouse because it is a standard that handles Relational data well and supports CDC via Write-Ahead Log (WAL).
* **Streaming (Redpanda):** Selected over the original Kafka (Java) because Redpanda is written in C++, consumes fewer resources, starts faster, and is perfect for Local Development or PoCs, while remaining 100% API compatible with Kafka.
* **CDC (Debezium):** The standard tool for extracting data from Database Logs (WAL), allowing us to capture every change (Insert/Update/Delete) in Real-time without modifying Backend code.
* **Consumer (Python):** Used Python to write a custom Consumer to allow flexibility in Data Transformation and Cleaning before loading into the Warehouse.

---

##🤖 2. Simulation LayerTo ensure the Data Pipeline has data flowing like a real Production environment, I wrote Python scripts to simulate various behaviors:

1. **Market Maker Bot (`market_maker.py`):**
* **Role:** Acts as a Liquidity Provider placing Bid/Ask orders.
* **Realism:** This bot hits the API to check **real BTC/THB prices from Bitkub**, then calculates the spread before placing orders in our simulated database. This ensures the dashboard price graph moves according to the real global market, not just random values.


2. **Matching Engine (`matching_engine.py`):**
* **Role:** Loops to check the Order Book. If a matching price is found, it executes the match, creates a transaction in the `matches` table, and updates `tickers` immediately. This creates the State Changes that Debezium needs to capture.


3. **Whale Bot (`whale_bot.py`):**
* **Role:** (Load Testing Tool) Simulates abnormal market activity (Volume Spikes) to test if our pipeline can handle a sudden influx of data and to measure dashboard delay.



---

##🔄 3. Data Flow Deep DiveThe journey of a single transaction is as follows:

1. **Event Occurs:** User (or Bot) orders Bitcoin -> Data is written to the `orders` table in the Source DB.
2. **Capture:** Debezium detects changes in the Transaction Log (WAL) of Postgres.
3. **Stream:** Debezium sends JSON data (identifying old/new values) to the Redpanda Topic named `bitka.public.orders`.
4. **Consume & Transform:**
* Script `consumer.py` listens to this Topic.
* **Deduplication Strategy:** Since Kafka may send duplicate data (At-least-once delivery), the Consumer handles deduplication by checking the latest ID in that Batch.
* **Data Cleaning:** Converts Unix Timestamp to Datetime, handles NULL values, and formats data to match the Warehouse Schema.


5. **Load:** Cleaned data is Inserted/Upserted into the Data Warehouse (Postgres).
6. **Visualize:** Streamlit Dashboard fetches data from the Warehouse to display as graphs.

---

##🧪 4. Testing & ValidationThis project includes scripts to verify system readiness:

* **Load Test:** Running `python whale_bot.py buy` to inject massive orders showed that the Consumer could process in batches (Batch Processing) and load into the DB without excessive lag (observed via the Dashboard where the graph spikes almost immediately).
* **Backfilling:** The `backfill.py` script pulls 3-4 days of historical data from Binance to fix empty graph issues on first run, ensuring the Dashboard has data from the very first second of usage.

---

##🔮 5. Next StepsTODO List for real-world implementation:

1. **Replace Simulation:** Remove the Matching Engine and Bots, then connect the Source DB to the real Backend or receive Data directly from the Backend team's Kafka Topic.
2. **Scalability:** If data volume reaches Production levels (millions of Transactions/sec), we might need to switch from a standard Python Consumer to **Kafka Connect Sink** or **Spark Streaming**.
3. **Monitoring:** Add Grafana to monitor Kafka Consumer Lag to see if consumption is keeping up.

---

###📥 Quick Run1. **Start Infrastructure:**
```bash
docker-compose up -d --build

```


*(Wait about 1 minute for Kafka to be Ready)*
2. **Monitor Dashboard:**
Access via Browser: `http://localhost:8501`
3. **Play with Data (Load Test):**
```bash
# Simulate a whale dumping the market
python whale_bot.py sell

```



---