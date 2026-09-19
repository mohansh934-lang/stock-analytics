from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_spark():
    spark = SparkSession.builder \
        .appName("Silver_Layer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_silver(spark):
    # Bronze se read karo
    df_bronze = spark.read.parquet(
        "/home/mohan/Desktop/stock-analytics/data/bronze/"
    )

    # Transformations
    df_silver = df_bronze \
        .dropna() \
        .filter(col("close") > 0) \
        .filter(col("volume") > 0) \
        .withColumn("date", to_date(col("date"))) \
        .withColumn("daily_range", round(col("high") - col("low"), 2)) \
        .withColumn("price_change", round(col("close") - col("open"), 2)) \
        .withColumn("ingested_at", current_timestamp()) \
        .drop("source") \
        .dropDuplicates(["symbol", "date"])

    # Save to Silver
    df_silver.write \
        .mode("overwrite") \
        .parquet("/home/mohan/Desktop/stock-analytics/data/silver/")

    print(f"Silver layer complete! Total rows: {df_silver.count()}")
    df_silver.show(5, truncate=False)

if __name__ == "__main__":
    spark = get_spark()
    process_silver(spark)
    spark.stop()