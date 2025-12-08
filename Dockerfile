# ใช้ Python เวอร์ชั่นที่เสถียรและมี Library รองรับครบ
FROM python:3.11-slim

WORKDIR /app

# ลง Library ที่จำเป็น
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy โค้ดเข้าไป
COPY producer_simulator.py .

# รัน Script ทันทีที่ Container เริ่ม
CMD ["python", "-u", "producer_simulator.py"]