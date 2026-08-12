FROM python:3.12-slim
# ffmpeg + libcodec2/pycodec2 enable voice transcoding (optional feature)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libcodec2-1.2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pycodec2
COPY gateway.py .
CMD ["python", "-u", "gateway.py", "-c", "/data/config.toml"]
