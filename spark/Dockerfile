FROM bitnami/spark:latest

USER root

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir kafka-python psycopg2-binary
