import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import pandas as pd

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

def check_table_exists(table_name: str):
    cur = conn.cursor()
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        order_id VARCHAR(50),
        product_id VARCHAR(50),
        customer_id VARCHAR(50),
        order_date DATE,
        quantity INT,
        unit_price DOUBLE PRECISION,
        total_price DOUBLE PRECISION,
        shipping_street VARCHAR(50),
        shipping_city VARCHAR(50),
        shipping_country VARCHAR(50),
        payment_method VARCHAR(50),
        order_status VARCHAR(50),
        shipping_method VARCHAR(50),
        is_large_order BOOLEAN,
        PRIMARY KEY (order_id, product_id)
        );
    """

    cur.execute(create_table_query)
    conn.commit()

def save_to_postgre(df, table_name):
    cur = None
    table_name = f"orders_schema.{table_name}"
    check_table_exists(table_name) 

    try: 
        cur = conn.cursor()

        columns = list(df.columns)
        cols = ",".join(columns)
        values = [tuple(row) for row in df.to_numpy()]
    
        insert_sql = f"""
            INSERT INTO {table_name} ({cols})
            VALUES %s
        """

        execute_values(cur, insert_sql, values)
        conn.commit()
        print(f"Saved successful {len(df)} rows into table {table_name}")

    except Exception as e:
        if conn:
            conn.rollback()
        print("Lỗi khi lưu dữ liệu:", e)
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()