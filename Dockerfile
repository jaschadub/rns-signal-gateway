FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gateway.py .
CMD ["python", "-u", "gateway.py", "-c", "/data/config.toml"]
