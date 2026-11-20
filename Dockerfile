FROM python:3.12-slim

WORKDIR /app

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

# Fail the image build if Python syntax or PAPER regression tests fail.
RUN python -m compileall -q bot main.py tests \
    && python -m unittest discover -s tests -v

# Railway volume should be mounted at /data
ENV DB_PATH=/data/polybot.db
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Create data dir so SQLite can create the file even without volume (ephemeral)
RUN mkdir -p /data

EXPOSE 8080

# Railway sets PORT; main.py / BotConfig reads it first
CMD ["python", "main.py"]
