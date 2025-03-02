from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, regexp_replace, from_json, row_number, when
)

spark = SparkSession.builder \
    .appName("BookMarksETL") \
    .config("spark.jars", "/app/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/librarian"
properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

raw_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "bookmarks.raw_data") \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .load()

window_spec = Window.partitionBy("book_id").orderBy(col("timestamp").desc())

latest_df = raw_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

book_df = latest_df \
    .select(
        col("book_id"),
        col("title"),
        col("author"),
        col("publisher"),
        col("publish_date"),
        col("description"),
        col("link")
    ) \
    .withColumnRenamed("book_id", "id")

# write to console for testing
book_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
