# Base Image: Python 3.11 Slim (เล็กและปลอดภัย)
FROM python:3.11-slim

# ตั้งค่า Environment พื้นฐาน
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/app

WORKDIR $APP_HOME

# 1. Install System Dependencies (ถ้าจำเป็นสำหรับ Library บางตัว)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy Source Code ทั้งหมด
COPY . .

# 4. Default Command (เผื่อไม่ระบุอะไร ให้รัน Dashboard เป็นค่าตั้งต้น)
CMD ["streamlit", "run", "data-platform/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]