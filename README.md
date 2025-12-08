# 📖 Bitka Data Pipeline - Setup & Usage Guide

เอกสารนี้อธิบายขั้นตอนการรันระบบ (Execution), การตรวจสอบข้อมูล (Monitoring), และการวิเคราะห์ข้อมูล (Analysis) สำหรับโปรเจกต์ **Bitka Data Pipeline** แบบ End-to-End

-----

## 🛠 Part 1: Prerequisites & Installation

สิ่งที่ต้องมีก่อนเริ่มใช้งาน

1.  **Docker Desktop** (ต้องเปิดใช้งานอยู่)
2.  **Python 3.10+** (สำหรับรัน Jupyter Notebook ในเครื่อง)

### ติดตั้ง Python Dependencies (สำหรับ Local Analysis)

แม้ระบบหลักจะรันบน Docker แต่เราควรลง Library ไว้ในเครื่องเพื่อรัน Notebook วิเคราะห์ข้อมูล

```bash
pip install -r requirements.txt
```

-----

## 🚀 Part 2: Start the System (One-Command Launch)

เราใช้ Docker Compose ในการรันทุก Service (Database, Kafka, Producer, Consumer, Dashboard) ด้วยคำสั่งเดียว

### 1\. เริ่มต้นระบบทั้งหมด

```bash
docker-compose up -d --build
```

**✅ Expected Result (ผลลัพธ์ที่ควรได้):**

  * Docker จะ Build Image ใหม่สำหรับ `producer`, `consumer`, และ `dashboard`
  * Service ทั้งหมดสถานะ **Started**:
      * `bitka_source_db` (Postgres Source)
      * `bitka_warehouse` (Postgres DW)
      * `bitka_redpanda` & `bitka_console` (Kafka)
      * `bitka_debezium` (CDC)
      * `bitka_producer` (Simulator ยิงข้อมูล)
      * `bitka_consumer` (ตัวรับข้อมูลลง Warehouse)
      * `bitka_dashboard` (Streamlit Web App)
  * **Debezium** จะถูก Config อัตโนมัติโดย `connector-setup`

-----

## 🔎 Part 3: Monitoring & Logs

เนื่องจากโปรแกรมรันอยู่เบื้องหลัง (Background) เราจะดูการทำงานผ่าน Logs

### 1\. ดู Producer Simulator (ตัวปั๊มข้อมูล)

ดูว่า Simulator กำลังยิงข้อมูลอะไรออกมาบ้าง

```bash
docker-compose logs -f producer
```

  * *ผลลัพธ์:* เห็น Log เช่น `📈 [Trading] Order Placed...` หรือ `🛡️ [System] Audit Log...`

### 2\. ดู Consumer Worker (ตัวบันทึกข้อมูล)

ดูว่า Consumer รับข้อมูลจาก Kafka และบันทึกลง Warehouse สำเร็จไหม

```bash
docker-compose logs -f consumer
```

  * *ผลลัพธ์:* เห็น Log `✅ Inserted 50 rows into orders...`

### 3\. ดู Kafka Topic (Redpanda Console)

เข้าหน้าเว็บเพื่อดูข้อมูลดิบใน Kafka Topic

  * 👉 **URL:** [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
  * ไปที่เมนู **Topics** จะเห็น Topic เช่น `bitka.public.orders`, `bitka.public.audit_logs`

-----

## 📊 Part 4: Visualization & Analysis

### 1\. Executive Dashboard (Streamlit)

หน้าจอ Real-time สำหรับดูภาพรวมธุรกิจและตรวจสอบข้อมูลทุกตาราง

  * 👉 **URL:** [http://localhost:8501](https://www.google.com/search?q=http://localhost:8501)
  * **Features:**
      * **Overview:** ดูยอด User, ราคาเหรียญ, กระแสเงินสด (Net Flow)
      * **Data Explorer:** กดเลือกดูข้อมูลดิบของทุกตาราง (Users, Orders, Audit Logs, etc.)

### 2\. Data Science Analysis (Jupyter Notebook)

สำหรับการวิเคราะห์ข้อมูลเชิงลึกและการทำ Visualizations

  * เปิดไฟล์ `Notebook.ipynb` ใน VS Code หรือ Jupyter Lab
  * กด **Run All** เพื่อดึงข้อมูลจาก Warehouse มาสร้างกราฟและ Report อัตโนมัติ

-----

## 🗄 Part 5: Direct Database Access

หากต้องการเขียน SQL Query เองใน Terminal

### เข้าถึง Data Warehouse (Postgres)

```bash
docker exec -it bitka_warehouse psql -U warehouse_admin -d bitka_dw
```

**ตัวอย่าง SQL Commands:**

```sql
-- ดูรายชื่อตารางทั้งหมด
\dt

-- ดูรายการเทรดล่าสุด 5 รายการ
SELECT * FROM matches ORDER BY created_at DESC LIMIT 5;

-- ดู Audit Logs ที่เกี่ยวกับการเงิน
SELECT * FROM audit_logs WHERE action LIKE '%kyc%' LIMIT 5;
```

-----

## 🧹 Part 6: Clean Up (Nuclear Option)

หากต้องการลบข้อมูลทั้งหมดแล้วเริ่มใหม่ (Reset from zero)

### แบบที่ 1: ล้างเฉพาะโปรเจกต์นี้ (แนะนำ)

ลบ Container และข้อมูลใน Database ทิ้งทั้งหมด แล้วเริ่มใหม่

```bash
docker-compose down -v
# จากนั้นเริ่มใหม่ด้วย
docker-compose up -d --build
```

### แบบที่ 2: ล้าง Image เก่าทิ้งด้วย (ถ้าแก้โค้ดแล้ว Docker ไม่จำ)

```bash
docker-compose down --rmi all -v
```