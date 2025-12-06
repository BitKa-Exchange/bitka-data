# 📖 Bitka Data Pipeline - Setup & Usage Guide

เอกสารนี้อธิบายขั้นตอนการติดตั้ง (Installation), การรันระบบ (Execution), และการตรวจสอบข้อมูล (Verification) สำหรับโปรเจกต์ Bitka Data Pipeline

------------------------------------------------------------------------------------------------------

## 🛠 Part 1: Installation & Infrastructure Setup

ขั้นตอนการเตรียม Environment และการเปิดใช้งาน Infrastructure ผ่าน Docker

### 1\. ติดตั้ง Dependencies

รันคำสั่งเพื่อติดตั้ง Python libraries ที่จำเป็น

```bash
pip3 install -r requirements.txt
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * Terminal จะแสดงรายการติดตั้ง หรือแจ้งว่า `Requirement already satisfied` สำหรับ library ต่างๆ เช่น `pandas`, `kafka-python`, `sqlalchemy`, `streamlit` ฯลฯ

------------------------------------------------------------------------------------------------------

### 2\. เริ่มต้นระบบ Infrastructure (Docker)

รันคำสั่ง Docker Compose เพื่อสร้าง Container สำหรับ Database, Kafka (Redpanda) และ Service อื่นๆ

```bash
docker-compose up -d
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * Docker สร้าง Network `bitka-net`
  * Container ทั้งหมดต้องขึ้นสถานะ **Started** ได้แก่:
      * `bitka-warehouse` (Postgres DW)
      * `bitka-redpanda` (Kafka)
      * `bitka-postgres` (Operational DB)
      * `bitka-console` (Redpanda UI)
      * `bitka-debezium` & `bitka-connector-init`
  * ตรวจสอบสถานะด้วย `docker-compose ps` ต้องเห็นสถานะ **Up** ทุกตัว

------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------


## 🚀 Part 2: Running the Pipeline

ขั้นตอนการรันโปรแกรมเพื่อรับ-ส่งข้อมูล (Consumer & Producer)

### 1\. รัน Consumer (ผู้รับข้อมูล)

เปิด Terminal ใหม่ แล้วรัน script นี้เพื่อรอรับข้อมูลเข้าสู่ Data Warehouse

```bash
python3 data-platform/scripts/consumer.py
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * แสดงข้อความ `🚀 Bitka Event-Driven Consumer Started`
  * แสดงการเชื่อมต่อ `Connected to Data Warehouse!` และ `Checking schema consistency...`
  * แสดงสถานะ `🎧 Listening to 10 topics...` และรอรับข้อมูล (Log จะวิ่งเมื่อมีข้อมูลเข้ามา)

------------------------------------------------------------------------------------------------------

### 2\. รัน Producer Simulator (ตัวจำลองข้อมูล)

เปิด Terminal อีกหน้าต่าง แล้วรัน script นี้เพื่อจำลองเหตุการณ์ (Events)

```bash
python3 producer_simulator.py
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * แสดงข้อความ `🚀 Bitka Producer Simulator... Connected to Kafka!`
  * เริ่มส่งข้อมูลจำลอง (Simulation) โดยแสดง Log การส่ง Events ต่างๆ เช่น:
      * `📤 Sent [LOGIN] ...`
      * `📤 Sent [CREATED] ...`
      * `📤 Sent [UPDATE] ...`
  * *Note:* ในหน้าต่าง Consumer (ข้อ 1) คุณควรจะเห็น Log `📥 Saved EventID: ...` เด้งขึ้นมาพร้อมกัน

------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------


## 🔎 Part 3: Data Access & Visualization

ขั้นตอนการเข้าไปตรวจสอบข้อมูลในฐานข้อมูลและการดู Dashboard

------------------------------------------------------------------------------------------------------

### 1\. เข้าถึง Data Warehouse (PostgreSQL)

คำสั่งสำหรับเข้าไป Query ข้อมูลใน Database ผ่าน Terminal

```bash
docker exec -it bitka-warehouse psql -U warehouse_admin -d bitka_dw
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * เข้าสู่หน้าจอ `bitka_dw=#`
  * สามารถใช้คำสั่ง SQL ตรวจสอบข้อมูลได้ เช่น:
      * `\dt` : เพื่อดูรายชื่อตาราง (Tables) ทั้งหมด
      * `SELECT COUNT(*) FROM fact_orders_created;` : เพื่อนับจำนวน Order ที่เข้ามา

------------------------------------------------------------------------------------------------------

### 2\. เปิดใช้งาน Dashboard (Streamlit)

รันคำสั่งเพื่อเปิดหน้า Web Dashboard สำหรับดูภาพรวมข้อมูล

```bash
python3 -m streamlit run data-platform/dashboard.py
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * Terminal แสดง URL สำหรับเข้าใช้งาน:
      * `Local URL: http://localhost:8501`
  * Browser จะเปิดหน้า Dashboard ขึ้นมาโดยอัตโนมัติ (ถ้าขึ้น password ให้กด enter)

------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------
