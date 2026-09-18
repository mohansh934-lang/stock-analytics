"""
Stock Data Producer: Reads CSV and streams to Kafka
Industry best practices with simplicity
"""

import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
from typing import Dict, Any, Tuple
from datetime import datetime , timezone
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataProducer:

    def __init__(self, bootstrap_servers: str = None):
        # Get from env var, fallback to default

        if bootstrap_servers is None:
           bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        #Initialize Kafka Producer with production settings
        
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect() #private methodd -- automatic call --

        
    def _connect(self):
        # retryy logic
        retries = 3
        for i in range(retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    
                    # Production settings
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3,   # Retry on failure
                    linger_ms=5,  # Wait up to 5ms for batching
                    batch_size=16384,  # 16KB batch size
                    compression_type=None,  # Compress messages
                    
                    # Timeout settingsdatetime.now(timezone.utc).isoformat()
                    request_timeout_ms=30000,
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
                
            except Exception as e:
                logger.error(f"Connection attempt {i+1} failed: {e}")
                if i < retries - 1:
                    time.sleep(2 ** i)  # Exponential backoff
                else:
                    logger.error("Max retries reached. Exiting.")
                    sys.exit(1)
    
    def validate_stock_data(self, row: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate stock data before sending
        """
        try:
            # Check required fields
            required_fields = ['symbol', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in row:
                    error_msg = f"Missing field: {field}"
                    logger.warning(error_msg)
                    return False, error_msg
            
            # Validate data types
            if not isinstance(row['symbol'], str):
               error_msg = f"Symbol must be string, got {type(row['symbol'])}"
               logger.warning(error_msg)
               return False, error_msg
        
            
            # Convert numeric fields
            row['open'] = float(row['open'])
            row['high'] = float(row['high'])
            row['low'] = float(row['low'])
            row['close'] = float(row['close'])
            row['volume'] = int(float(row['volume']))  # Handle float volume
            
            # Add metadata
            row['processed_at'] =datetime.now(timezone.utc).isoformat()
            row['source'] = 'csv_file'
            
            return True, ""
            
        except (ValueError, TypeError) as e:
            error_msg = f"Data validation failed: {e}"
            logger.warning(error_msg)
            return False, error_msg
    
    def send_to_dead_letter_queue(self, row: Dict, error: str):
        """Send invalid data to DLQ for debugging"""
        try:
            error_row = {
                **row,
                'error': error,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.producer.send('stocks_dlq', value=error_row)
            logger.info(f"Sent to DLQ: {row.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def produce_from_csv(self, csv_path: str, topic: str = 'stock_prices'):
        """
        Read CSV and produce to Kafka topic
        
        Args:
            csv_path: Path to CSV file
            topic: Kafka topic name
        """
        try:
            # Read CSV
            logger.info(f"Reading CSV from {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} rows from CSV")
            
            total_sent = 0
            total_failed = 0
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    row_dict = row.to_dict()
                    
                    # Validate data
                    is_valid, error_msg = self.validate_stock_data(row_dict)
                    if not is_valid:
                        self.send_to_dead_letter_queue(
                            row_dict, 
                            error_msg
                        )
                        total_failed += 1
                        continue
                    
                    # Determine partition based on symbol (for ordering)
                    # Same symbol goes to same partition
                    symbol = row_dict['symbol']
                    partition = hash(symbol) % 4  # 4 partitions
                    
                    # Send to Kafka with callback
                    future = self.producer.send(
                        topic=topic,
                        value=row_dict,
                        partition=partition
                    )
                    
                    # Add callback for async handling
                    future.add_callback(
                        self._on_send_success,
                        symbol=symbol,
                        topic=topic
                    )
                    future.add_errback(
                        self._on_send_error,
                        symbol=symbol,
                        row=row_dict
                    )
                    
                    total_sent += 1
                    
                    # Progress logging
                    if (index + 1) % 10 == 0:
                        logger.info(f"Processed {index + 1}/{len(df)} rows")
                    
                    # Small delay to avoid overwhelming Kafka
                    time.sleep(0.01)
                    
                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}")
                    total_failed += 1
                    self.send_to_dead_letter_queue(row.to_dict(), str(e))
            
            # Wait for all messages to be sent
            self.producer.flush()
            
            # Summary
            logger.info(f"""
            ===== PRODUCTION SUMMARY =====
            Total rows processed: {len(df)}
            Successfully sent: {total_sent}
            Failed: {total_failed}
            Topic: {topic}
            ==============================
            """)
            
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
        except pd.errors.EmptyDataError:
            logger.error("CSV file is empty")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.close()
    
    def _on_send_success(self, record_metadata, symbol: str, topic: str):
        """Callback when message is successfully sent"""
        logger.debug(
            f"Sent {symbol} to {topic} "
            f"[Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}]"
        )
    
    def _on_send_error(self, exception, symbol: str, row: Dict):
        """Callback when message fails to send"""
        logger.error(f"Failed to send {symbol}: {exception}")
        self.send_to_dead_letter_queue(row, str(exception))
    
    def close(self):
        """Clean shutdown"""
        if self.producer:
            self.producer.close(timeout=10)
            logger.info("Kafka producer closed gracefully")

# ========== USAGE ==========
if __name__ == "__main__":
    # Initialize producer
    producer = StockDataProducer(bootstrap_servers='localhost:9092')
    
    # Send CSV data to Kafka
    producer.produce_from_csv(
        csv_path='/home/mohan/Desktop/stock-analytics/data/raw_data/stocks.csv',  # Your CSV path
        topic='stock_prices'  # Kafka topic
    )