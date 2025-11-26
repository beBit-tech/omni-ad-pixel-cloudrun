FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8070

ENV PORT=8070
ENV PYTHONUNBUFFERED=1

CMD exec gunicorn \
    --bind :$PORT \
    --workers 8 \
    --threads 40 \
    --worker-class gthread \
    --timeout 0 \
    --config /app/gunicorn.conf.py \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:app
