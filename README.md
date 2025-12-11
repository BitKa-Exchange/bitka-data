# Bitka Data Platform

**Bitka Data Platform** is a production-grade simulation of a cryptocurrency exchange data pipeline. It implements an **Event-Driven Architecture (EDA)** using **Change Data Capture (CDC)** to stream transactional data into a Data Warehouse in near real-time.

## 🏗 System Architecture

The platform consists of four main layers, containerized and orchestrated via Docker Compose:

1.  **Transactional Layer (Source System)**
    * **Service:** `postgres` (PostgreSQL 14)
    * **Role:** Simulates the exchange backend (Orders, Matches, Users, Wallets).
    * **Data Generation:** A Python-based `producer` simulates user activity and trading events.

2.  **Streaming & CDC Layer**
    * **Service:** `redpanda` (Kafka-compatible) & `connect` (Debezium)
    * **Role:** Captures row-level changes (INSERT/UPDATE/DELETE) from the Source DB WAL logs and streams them to Kafka topics.
    * **Automation:** The `connector-setup` service automatically registers the Debezium connector configuration upon startup, injecting secrets via environment variables.

3.  **Data Warehouse Layer (Destination)**
    * **Service:** `warehouse` (PostgreSQL 14)
    * **Role:** Stores analytical data optimized for querying.
    * **ETL Worker:** A Python `consumer` subscribes to Kafka topics, performs data cleaning/deduplication, and loads data into the warehouse.

4.  **Presentation Layer**
    * **Service:** `dashboard` (Streamlit)
    * **Role:** Provides real-time visualization of trading volumes, user growth, and audit logs.

---

## 🚀 Quick Start

### Prerequisites
* Docker Engine (v20.10+)
* Docker Compose (v2.0+)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd bitka-data-platform
    ```

2.  **Configure Environment Variables:**
    Create a `.env` file based on the example. **Ensure no secrets are hardcoded.**
    ```bash
    cp .env.example .env
    # Edit .env to set secure passwords for DB_PASS, DW_PASS, etc.
    ```

3.  **Start Services:**
    ```bash
    docker-compose up -d --build
    ```
    *The system will automatically initialize the databases, start the simulation, and configure the CDC connector.*

4.  **Verify Deployment:**
    * **Dashboard:** [http://localhost:8501](http://localhost:8501)
    * **Redpanda Console (Kafka UI):** [http://localhost:8080](http://localhost:8080)
    * **Data Warehouse (Direct SQL):** `localhost:5433`

5.  **Shutdown:**
    ```bash
    docker-compose down
    # Use -v to remove persisted volumes (resets all data)
    docker-compose down -v
    ```

---

## ⚙️ Configuration & DevOps Notes

The application adheres to **12-Factor App** principles. All configurations are injected via Environment Variables.

### 1. Environment Variables (`.env`)

| Category | Variable | Description | Default (Dev) |
| :--- | :--- | :--- | :--- |
| **Source DB** | `DB_HOST` | Hostname within network | `postgres` |
| | `DB_PORT` | Internal Port | `5432` |
| | `DB_USER` | Admin Username | `postgres` |
| | `DB_PASS` | **[SECRET]** Admin Password | *-* |
| **Warehouse** | `DW_HOST` | Hostname within network | `warehouse` |
| | `DW_PORT_EXTERNAL` | Host Port Mapping | `5433` |
| | `DW_USER` | Warehouse Username | `warehouse_admin` |
| | `DW_PASS` | **[SECRET]** Warehouse Password | *-* |
| **Streaming** | `KAFKA_BROKER` | Internal Broker Address | `redpanda:9092` |

### 2. Connector Configuration (Templating)
The Debezium configuration is defined in `connector.json`. It uses `gettext` (`envsubst`) to inject credentials at runtime.
* **Template:** `connector.json` contains placeholders like `${DB_PASS}`.
* **Execution:** The `connector-setup` container performs variable substitution and posts the config to the Connect REST API.

### 3. Data Persistence
* **Postgres Data:** Persisted in `pg_source_data` and `pg_warehouse_data` volumes.
* **Redpanda/Kafka:** Persisted in `redpanda_data` volume. Events are **not lost** on container restarts.

---

## 🔌 Service Endpoints & Port Mappings

| Service Name | Internal Port | Host Port | Description |
| :--- | :--- | :--- | :--- |
| `postgres` | 5432 | **5432** | Transactional Database |
| `warehouse` | 5432 | **5433** | Analytical Database |
| `dashboard` | 8501 | **8501** | Streamlit Web App |
| `redpanda-console`| 8080 | **8080** | Kafka Management UI |
| `redpanda` | 9092, 19092 | **19092** | Kafka Broker (External Access) |
| `connect` | 8083 | **8085** | Kafka Connect API |

---

## 🛠 Troubleshooting

**Issue: `Kafka not ready` or Consumer fails on startup.**
* **Cause:** Redpanda takes a few seconds to elect a leader on the first run.
* **Resolution:** The Python services have built-in retry logic (`Retrying...`). Wait 30 seconds. If it persists, check logs: `docker logs bitka_redpanda`.

**Issue: Connector Config Failed.**
* **Resolution:** Check the setup logs:
    ```bash
    docker logs bitka_connector_setup
    ```
    Ensure your `.env` variables match those expected in `connector.json`.

---

## 📂 Project Structure

```text
.
├── data-platform/          # Application Code
│   ├── dashboard.py        # Streamlit Dashboard
│   └── scripts/
│       └── consumer.py     # ETL Worker (Kafka -> Warehouse)
├── init.sql                # DB Schema Initialization
├── connector.json          # Debezium Config Template
├── docker-compose.yml      # Orchestration
├── Dockerfile              # Python Services Image
├── producer_simulator.py   # Data Generator
├── requirements.txt        # Python Dependencies
└── .env.example            # Environment Config Template


💡 Key File Descriptions

Core Services:

producer_simulator.py: The primary script used in the Docker container to generate real-time simulated traffic.

data-platform/scripts/consumer.py: The worker process responsible for De-duplication and Data Cleaning before inserting into the Data Warehouse.

connector.json: Configures the CDC pipeline. It acts as a template where credentials are injected at runtime via envsubst.

Analytics & Science:

Files like detect_anomalies_trades.py and Notebook.ipynb are used for developing data models and analyzing patterns within the simulated exchange data.

Infrastructure:

init.sql: Contains the DDL for both the Source Database (Transactional) and the Data Warehouse (Analytical). The system uses REPLICA IDENTITY FULL to support full CDC capabilities.