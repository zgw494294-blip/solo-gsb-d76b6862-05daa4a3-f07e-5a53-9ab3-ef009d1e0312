FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EXPERIMENT_DATA_DIR=/data

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./
COPY static ./static

RUN mkdir -p /data

EXPOSE 8000

# 只能使用单进程：每个实验的三个 SQLite 连接是 Flask 进程内的长期连接。
CMD ["python", "app.py"]
