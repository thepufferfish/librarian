from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, regexp_replace, from_json, row_number, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "bookmarks"

# Define schema
schema = StructType([
    StructField("book_id", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("publisher", StringType()),
    StructField("publish_date", StringType()),
    StructField("description", StringType()),
    StructField("genres", ArrayType(StringType())),
    StructField("link", StringType()),
    StructField("scraped_at", StringType()),
    StructField("reviews", ArrayType(StringType()))
])

spark = SparkSession.builder \
    .appName("BookMarksETL") \
    .master("spark://spark:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# data cleaning
cleaned_df = json_df \
    .withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", "")) \
    .withColumn("author", regexp_replace(col("author"), "[^a-zA-Z0-9 ]", "")) \
    .withColumn(
        "publish_date",
        regexp_replace(col("publish_date"), "[^0-9-]", "")
    )

# keep the most recent record for each book
window_spec = Window.partitionBy("book_id").orderBy(col("scraped_at").desc())
deduped_df = cleaned_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1).drop("row_num")

# Explode Arrays (Genres & Reviews)
genres_df = deduped_df \
    .select(col("book_id"), explode(col("genres")).alias("genre"))
reviews_df = deduped_df \
    .select(col("book_id"), explode(col("reviews")).alias("review_text"))

# convert rating to integer
reviews_df = reviews_df \
    .withColumn(
        "review_rating",
        when(reviews_df["review_rating"] == "Rave", 4)
        .when(reviews_df["review_rating"] == "Positive", 3)
        .when(reviews_df["review_rating"] == "Mixed", 2)
        .when(reviews_df["review_rating"] == "Pan", 1)
        .otherwise(0)
    )

# write to console for testing
deduped_df \
    .select(
        "book_id",
        "title",
        "author",
        "publisher",
        "publish_date",
        "description",
        "link",
        "scraped_at"
    ) \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination() \

genres_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

reviews_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
