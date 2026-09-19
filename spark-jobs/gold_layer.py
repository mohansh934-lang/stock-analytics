from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_spark():
    spark = SparkSession.builder \
        .appName("Gold_Layer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_gold_monthly(spark):
    df_silver = spark.read.parquet(
        "/home/mohan/Desktop/stock-analytics/data/silver/"
    )

    df_gold_monthly = df_silver \
        .withColumn("month", date_format(col("date"), "yyyy-MM")) \
        .groupBy("symbol", "month") \
        .agg(
            round(avg("close"), 2).alias("avg_close"),
            round(max("high"), 2).alias("max_high"),
            round(min("low"), 2).alias("min_low"),
            round(avg("volume"), 0).cast("long").alias("avg_volume"),
            round(sum("price_change"), 2).alias("net_price_change"),
            round(avg("daily_range"), 2).alias("avg_daily_range")
        ) \
        .orderBy("symbol", "month")

    df_gold_monthly.write \
        .mode("overwrite") \
        .parquet("/home/mohan/Desktop/stock-analytics/data/gold_monthly/")

    print(f"Gold Monthly complete! Total rows: {df_gold_monthly.count()}")
    df_gold_monthly.show(10, truncate=False)

if __name__ == "__main__":
    spark = get_spark()
    process_gold_monthly(spark)
    spark.stop()