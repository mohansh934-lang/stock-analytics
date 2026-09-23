# Stock Analytics Pipeline

An end-to-end data engineering pipeline that fetches stock market data, streams it through Kafka, processes it with PySpark using a Medallion Architecture (Bronze to Silver to Gold), and loads the final aggregated metrics into PostgreSQL.

## Architecture

Alpha Vantage API -> CSV file (raw) -> Kafka Producer -> Kafka Topic (stock_prices) -> Dead Letter Queue (stocks_dlq) for invalid records -> PySpark Bronze Layer (raw ingestion, Parquet) -> PySpark Silver Layer (cleaned, validated, enriched) -> PySpark Gold Layer (monthly aggregated metrics) -> PostgreSQL (stock_metrics_monthly table)

## Tech Stack

- Data Source: Alpha Vantage API
- Streaming: Apache Kafka (Docker, Confluent images)
- Processing: PySpark (Structured Streaming + batch)
- Storage: Parquet (Bronze/Silver/Gold layers), PostgreSQL (final metrics)
- Language: Python

## Pipeline Details

### 1. Data Ingestion (scripts/api_fetch.py)
Fetches daily OHLCV (Open, High, Low, Close, Volume) data for a set of stock symbols (AAPL, MSFT, GOOG, TSLA) from the Alpha Vantage API and saves it to a CSV file.

### 2. Kafka Producer (kafka-jobs/kafka_producer.py)
- Reads the CSV and publishes each row to the stock_prices Kafka topic
- Validates every record before sending (required fields, data types)
- Routes invalid records to a stocks_dlq (Dead Letter Queue) topic instead of dropping them
- Uses symbol-based partitioning so all records for the same stock land in the same partition (preserves ordering)
- Includes retry logic with exponential backoff for the initial Kafka connection
- Async send with success/error callbacks

### 3. Bronze Layer (spark-jobs/bronze_layer.py)
Reads raw messages from the stock_prices Kafka topic using Spark Structured Streaming and writes them, unmodified, to Parquet. This preserves an exact copy of the raw data for reprocessing/auditing.

### 4. Silver Layer (spark-jobs/silver_layer.py)
Reads from Bronze and applies cleaning:
- Drops nulls and invalid rows (non-positive price/volume)
- Casts date to a proper date type
- Derives daily_range and price_change columns
- Removes duplicate (symbol, date) records

### 5. Gold Layer (spark-jobs/gold_layer.py)
Aggregates Silver data by symbol and month to produce business-ready metrics: average close, monthly high/low, average volume, net price change, and average daily range.

### 6. Load to PostgreSQL (scripts/load_to_postgres.py)
Loads the Gold layer's monthly metrics into a stock_metrics_monthly table in PostgreSQL for querying/reporting.

## Running Locally

1. Start Kafka:
cd kafka-jobs
docker compose up -d

2. Create Kafka topics:
docker exec -it kafka-jobs-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic stock_prices --partitions 4 --replication-factor 1
docker exec -it kafka-jobs-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic stocks_dlq --partitions 1 --replication-factor 1

3. Fetch data and produce to Kafka:
python scripts/api_fetch.py
python kafka-jobs/kafka_producer.py

4. Run the Spark layers in order:
python spark-jobs/bronze_layer.py
python spark-jobs/silver_layer.py
python spark-jobs/gold_layer.py

5. Load results into PostgreSQL:
python scripts/load_to_postgres.py

## Future Improvements

- Orchestration with Airflow: Automate the full pipeline (fetch, produce, Bronze, Silver, Gold, load) on a daily schedule using Apache Airflow, containerized alongside Kafka.
- dbt for transformations: Move some of the Gold-layer aggregation logic into dbt models for better SQL-based lineage and testing.
- Consumer service: Add a dedicated Kafka consumer for real-time monitoring/alerting instead of only batch Spark processing.
- Cloud deployment: Move Kafka/Spark/Postgres to managed cloud services (MSK, EMR, RDS) for a production-grade setup.
