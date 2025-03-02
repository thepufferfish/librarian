FROM bitnami/spark:latest

USER root

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir kafka-python psycopg2-binary
RUN wget -O /app/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

