from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_spark():
    """
    Spark Session create karo
    """
    spark = SparkSession.builder \
        .appName("Bronze_Layer") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_schema():
    """
    Kafka message ka schema define karo
    """
    return StructType([
        StructField("date",         StringType(), True), 
        StructField("symbol",       StringType(), True),
        StructField("open",         DoubleType(), True),
        StructField("high",         DoubleType(), True),
        StructField("low",          DoubleType(), True),
        StructField("close",        DoubleType(), True),
        StructField("volume",       LongType(),   True),
        StructField("processed_at", StringType(), True),
        StructField("source",       StringType(), True)
    ])


def read_from_kafka(spark):
    """
    Kafka se raw data read karo (Bronze Layer)
    No transformations - as-is data
    """
    schema = get_schema()

    df_bronze = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock_prices") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select(
            "data.*",
            "kafka_timestamp"
        )

    return df_bronze


if __name__ == "__main__":
    spark = get_spark()
    df = read_from_kafka(spark)

    query = df.writeStream \
        .format("parquet") \
        .option("path", "/home/mohan/Desktop/stock-analytics/data/bronze/") \
        .option("checkpointLocation", "/home/mohan/Desktop/stock-analytics/data/bronze_checkpoint/") \
        .outputMode("append") \
        .trigger(once=True) \
        .start()

    print("Bronze layer streaming started!")
    query.awaitTermination()