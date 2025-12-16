---

#🪙 Bitka Data Platform (End-to-End Data Pipeline PoC)**Project Status:** *Proof of Concept (PoC) / Ready for Integration*

โปรเจกต์นี้คือการจำลองและออกแบบ **Data Engineering Pipeline** สำหรับแพลตฟอร์มซื้อขายสินทรัพย์ดิจิทัล (Exchange) โดยเน้นไปที่การจัดการข้อมูลแบบ **Real-time Streaming**

โจทย์หลักของโปรเจกต์นี้ไม่ใช่การสร้างแอปเทรดที่สมบูรณ์แบบ แต่คือการตอบคำถามทางวิศวกรรมว่า: **"เราจะออกแบบ Pipeline อย่างไร ให้รองรับ Transaction ที่เกิดขึ้นเร็วและเยอะ จากนั้นนำไปแสดงผลบน Dashboard ให้ Delay น้อยที่สุดได้อย่างไร?"**

เนื่องจากในปัจจุบันส่วน Backend หลัก (Core Trading System) ยังอยู่ในระหว่างการพัฒนา เพื่อไม่ให้งาน Data ต้องรอ โปรเจกต์นี้จึงสร้าง **Simulation Layer** ขึ้นมา Mockup ข้อมูลให้สมจริงที่สุด เพื่อทดสอบ Architecture ที่วางไว้

---

##🧩 1. System Architecture & Design Decisionsระบบถูกออกแบบโดยแยกส่วนชัดเจนระหว่าง **Source (ผู้ผลิตข้อมูล)** และ **Destination (ผู้ใช้ข้อมูล)** โดยมี Kafka เป็นตัวกลางลดแรงกระแทก (Decoupling)

###🛠 Tech Stack ที่เลือกใช้และเหตุผล* 
**Database (PostgreSQL):** เลือกใช้เป็นทั้ง Source และ Warehouse เพราะเป็น Standard ที่จัดการข้อมูล Relational ได้ดี และรองรับการทำ CDC ผ่าน Write-Ahead Log (WAL) 


* **Streaming (Redpanda):** เลือกใช้แทน Kafka ตัวเดิม (Java) เพราะ Redpanda เขียนด้วย C++ กินทรัพยากรน้อยกว่า Start เร็วกว่า เหมาะมากสำหรับการทำ Local Development หรือ PoC แต่ยังใช้ API เดียวกับ Kafka ได้ 100%
* **CDC (Debezium):** เครื่องมือมาตรฐานในการดึงข้อมูลจาก Database Log (WAL) ทำให้เราได้ข้อมูลทุกการเปลี่ยนแปลง (Insert/Update/Delete) แบบ Real-time โดยไม่ต้องแก้ Code ฝั่ง Backend
* **Consumer (Python):** ใช้ Python เขียน Consumer เองเพื่อให้มีความยืดหยุ่นในการทำ Data Transformation และ Cleaning ก่อนเอาลง Warehouse

---

##🤖 2. Simulation Layer (ส่วนจำลองข้อมูล)เพื่อให้ Data Pipeline มีข้อมูลไหลผ่านเหมือน Production จริง ผมได้เขียน Script Python จำลองพฤติกรรมต่างๆ ไว้ดังนี้:

1. **Market Maker Bot (`market_maker.py`):**
* **หน้าที่:** ทำหน้าที่เป็น Liquidity Provider คอยวาง Bid/Ask
* **ความสมจริง:** บอทตัวนี้จะยิง API ไปเช็คราคา **BTC/THB จาก Bitkub จริงๆ** แล้วนำมาคำนวณ Spread ก่อนวาง Order ลงใน Database จำลองของเรา ทำให้กราฟราคาใน Dashboard ขยับตามตลาดโลกจริงๆ ไม่ใช่ Random มั่วๆ


2. **Matching Engine (`matching_engine.py`):**
* **หน้าที่:** วนลูปตรวจสอบ Order Book ถ้าเจอราคาที่ตรงกัน (Match) จะทำการจับคู่และสร้าง Transaction ลงตาราง `matches` และอัปเดต `tickers` ทันที สิ่งนี้ช่วยให้เกิด State Change ที่ Debezium ต้องดักจับ


3. **Whale Bot (`whale_bot.py`):**
* **หน้าที่:** (Load Testing Tool) ใช้จำลองเหตุการณ์ที่มีการซื้อขายรุนแรงผิดปกติ (Volume Spike) เพื่อทดสอบว่า Pipeline ของเรารับมือข้อมูลที่ถาโถมเข้ามาทันหรือไม่ และ Dashboard จะแสดงผล Delay แค่ไหน



---

##🔄 3. Data Flow Deep Dive (เจาะลึกการไหลของข้อมูล)กระบวนการเดินทางของข้อมูล 1 Transaction เป็นดังนี้:

1. **Event Occurs:** User (หรือ Bot) สั่งซื้อ Bitcoin -> ข้อมูลถูกเขียนลง Table `orders` ใน Source DB
2. **Capture:** Debezium อ่านเจอการเปลี่ยนแปลงใน Transaction Log (WAL) ของ Postgres
3. **Stream:** Debezium ส่งข้อมูล JSON (ระบุค่าเก่า/ค่าใหม่) ไปที่ Redpanda Topic ชื่อ `bitka.public.orders`
4. **Consume & Transform:**
* Script `consumer.py` จะคอยดักฟัง Topic นี้
* **Deduplication Strategy:** เนื่องจาก Kafka อาจส่งข้อมูลซ้ำ (At-least-once delivery) Consumer จะทำการจัดการข้อมูลซ้ำโดยดูจาก ID ล่าสุดใน Batch นั้นๆ ก่อน
* **Data Cleaning:** แปลง Unix Timestamp เป็น Datetime, จัดการค่า NULL, และจัด Format ให้ตรงกับ Schema ของ Warehouse


5. **Load:** ข้อมูลที่คลีนแล้วถูก Insert/Upsert ลง Data Warehouse (Postgres)
6. **Visualize:** Streamlit Dashboard ดึงข้อมูลจาก Warehouse มาแสดงผลเป็นกราฟ

---

##🧪 4. การทดสอบและผลลัพธ์ (Testing & Validation)ในโปรเจกต์นี้มี Script สำหรับตรวจสอบความพร้อมของระบบ:

* **Load Test:** เมื่อรัน `python whale_bot.py buy` เพื่ออัด Order ปริมาณมหาศาล พบว่า Consumer สามารถประมวลผลแบบ Batch (Batch Processing) และลง DB ได้ทันโดยไม่มี Lag นานเกินไป (สังเกตจาก Dashboard ที่กราฟพุ่งขึ้นเกือบจะทันที)
* 
**Backfilling:** มี Script `backfill.py` เพื่อดึงข้อมูลย้อนหลัง 3-4 วันจาก Binance มาใส่ เพื่อแก้ปัญหากราฟโล่งตอนเริ่มรันระบบครั้งแรก ทำให้ Dashboard ดูมี Data ตั้งแต่วินาทีแรกที่เริ่มใช้งาน 



---

##🔮 5. Next Steps (แผนการในอนาคต)สิ่งที่เป็น TODO List สำหรับการนำไปใช้จริง:

1. **Replace Simulation:** ปลดส่วน Matching Engine และ Bot ออก แล้วนำ Source DB ไปเชื่อมต่อกับ Backend ของจริง หรือรับ Data จาก Kafka Topic ของทีม Backend โดยตรง
2. **Scalability:** หากปริมาณข้อมูลระดับ Production (หลักล้าน Transactions/sec) อาจจะต้องเปลี่ยนจาก Python Consumer ธรรมดา ไปใช้ **Kafka Connect Sink** หรือ **Spark Streaming** แทน
3. **Monitoring:** เพิ่ม Grafana เพื่อดู Lag ของ Kafka Consumer ว่าบริโภคข้อมูลทันหรือไม่

---

###📥 วิธีรันโปรเจกต์ (Quick Run)1. **Start Infrastructure:**
```bash
docker-compose up -d --build

```


*(รอประมาณ 1 นาทีให้ Kafka Ready)*


2. **Monitor Dashboard:**
เข้าผ่าน Browser: `http://localhost:8501`


3. **Play with Data (Load Test):**
```bash
# ลองสั่งวาฬทุบตลาด
python whale_bot.py sell

```



---
