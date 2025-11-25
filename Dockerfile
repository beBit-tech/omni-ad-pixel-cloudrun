FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY buffer_writer.py .

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8070

ENV PORT=8070
ENV PYTHONUNBUFFERED=1

CMD exec gunicorn \
    --bind :$PORT \
    --workers 8 \
    --threads 24 \
    --worker-class gthread \
    --timeout 0 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:app
