FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY alembic.ini /app/alembic.ini
COPY src /app/src
COPY migrations /app/migrations
COPY scripts /app/scripts
COPY configs /app/configs

EXPOSE 9108

CMD ["python", "-m", "src.consumer.main", "consume"]
