from confluent_kafka import Consumer
import psycopg2
import json
import os
import time

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOSTRAP_SERVERS"),
    "group.id": "librarian",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["bookmarks"])

conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST")
)

cursor = conn.cursor()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error while consuming: {msg.error()}")
        else:
            data = msg.value().decode("utf-8")
            data = json.loads(data)
            cursor.execute(
                    """
                    INSERT INTO bookmarks.raw_data (title, author, publisher, publish_date, genres, reviews, raw_json, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (data["title"], data["author"], data["publisher"], data["publish_date"],
                     json.dumps(data["genres"]), json.dumps(data["reviews"]), json.dumps(data), data["scraped_at"])
            )
            conn.commit()

except KeyboardInterrupt:
    pass
finally:
    cursor.close()
    conn.close()
    consumer.close()

