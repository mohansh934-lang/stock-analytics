import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host='host.docker.internal',
    database='stock_analytics',
    user='mohan',
    password='mohan123'
)

cursor = conn.cursor()

# Table banao (monthly gold ke schema ke hisaab se)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_metrics_monthly (
        symbol VARCHAR,
        month VARCHAR,
        avg_close FLOAT,
        max_high FLOAT,
        min_low FLOAT,
        avg_volume BIGINT,
        net_price_change FLOAT,
        avg_daily_range FLOAT
    )
""")

# Purana data clear karo (taaki dobara run karne pe duplicate na ho)
cursor.execute("TRUNCATE TABLE stock_metrics_monthly")

# Gold monthly data read karo
df = pd.read_parquet('/home/mohan/Desktop/stock-analytics/data/gold_monthly/')
print(f"Total rows: {len(df)}")

# Insert karo
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO stock_metrics_monthly
        (symbol, month, avg_close, max_high, min_low, avg_volume, net_price_change, avg_daily_range)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['symbol'],
        str(row['month']),
        float(row['avg_close']),
        float(row['max_high']),
        float(row['min_low']),
        int(row['avg_volume']),
        float(row['net_price_change']),
        float(row['avg_daily_range'])
    ))

conn.commit()
print("Data loaded to PostgreSQL!")
cursor.close()
conn.close()