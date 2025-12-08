# ==============================================================================
# Dockerfile for Bitka Producer Simulator
# ------------------------------------------------------------------------------
# Description:
#   Builds the Python environment for the Data Producer service.
#   Optimized for build speed (caching) and image size.
# ==============================================================================

# 1. Base Image Selection
# เลือกใช้ 'slim' variant แทน 'alpine' หรือ 'full' image
# - เล็กกว่า full image ช่วยลด Attack Surface และขนาด Image
# - เสถียรกว่า alpine สำหรับ Python เพราะรองรับ Wheels/C-extensions ได้ดีกว่า (ไม่ต้อง compile เอง)
FROM python:3.11-slim

# 2. Working Directory
# กำหนด path ทำงานให้ชัดเจน เพื่อความเป็นระเบียบและป้องกัน file path conflicts
WORKDIR /app

# 3. Dependency Installation (Layer Caching Strategy)
# เทคนิคสำคัญ: Copy เฉพาะ requirements.txt เข้ามาก่อน แล้วสั่ง install
# เหตุผล: Docker จะ Cache layer นี้ไว้ ถ้า requirements.txt ไม่เปลี่ยน
# Docker จะข้ามขั้นตอนการ install ไปเลย ทำให้การ Build รอบถัดไปเร็วมาก
COPY requirements.txt .

# --no-cache-dir: ไม่เก็บ cache ของ pip เพื่อลดขนาด Image ให้เล็กที่สุด
RUN pip install --no-cache-dir -r requirements.txt

# 4. Application Code
# Copy โค้ดส่วนที่เหลือเข้ามา (ส่วนนี้มักมีการเปลี่ยนแปลงบ่อยที่สุด)
# การวางไว้หลัง install requirements จะไม่ทำให้ Cache ของ layer ก่อนหน้าเสียไป
COPY producer_simulator.py .

# 5. Container Entrypoint
# ใช้ flag "-u" (unbuffered) สำคัญมากสำหรับการรัน Python ใน Docker
# เหตุผล: บังคับให้ Python ส่ง Log (stdout/stderr) ออกมาทันทีโดยไม่รอ Buffer เต็ม
# ช่วยให้เราเห็น Log แบบ Real-time ผ่านคำสั่ง 'docker logs'
CMD ["python", "-u", "producer_simulator.py"]