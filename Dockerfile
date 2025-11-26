FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY buffer_writer.py .
COPY gunicorn.conf.py .

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8070

ENV PORT=8070
ENV PYTHONUNBUFFERED=1

CMD ["gunicorn", "--config", "gunicorn.conf.py", "app:app"]