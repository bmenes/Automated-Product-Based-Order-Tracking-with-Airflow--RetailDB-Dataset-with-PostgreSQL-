from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_status_tracking',
    default_args=default_args,
    description='A DAG to load CSV data into PostgreSQL and create a view',
    schedule_interval='@hourly',
)

def download_csv():
    url_orders = 'https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/orders.csv'
    url_order_items = 'https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/order_items.csv'
    url_products = 'https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/products.csv'

    orders = pd.read_csv(url_orders)
    order_items = pd.read_csv(url_order_items)
    products = pd.read_csv(url_products)

    orders.to_csv('/tmp/orders.csv', index=False)
    order_items.to_csv('/tmp/order_items.csv', index=False)
    products.to_csv('/tmp/products.csv', index=False)

download_task = PythonOperator(
    task_id='download_csv',
    python_callable=download_csv,
    dag=dag,
)

def load_csv_to_postgres(table_name, file_path):
    command = f"psql -U train -d traindb -c \"\\copy {table_name} FROM '{file_path}' DELIMITER ',' CSV HEADER;\""
    subprocess.run(command, shell=True, check=True)

def load_orders():
    load_csv_to_postgres('staging.orders', '/tmp/orders.csv')

def load_order_items():
    load_csv_to_postgres('staging.order_items', '/tmp/order_items.csv')

def load_products():
    load_csv_to_postgres('staging.products', '/tmp/products.csv')

load_orders_task = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)

load_order_items_task = PythonOperator(
    task_id='load_order_items',
    python_callable=load_order_items,
    dag=dag,
)

load_products_task = PythonOperator(
    task_id='load_products',
    python_callable=load_products,
    dag=dag,
)

create_view = PostgresOperator(
    task_id='create_view',
    postgres_conn_id='postgresql_conn',
    sql="""
    CREATE OR REPLACE VIEW serving.v_product_status_track AS
    SELECT
        o.order_id,
        o.order_date,
        o.order_status,
        p.product_name,
        p.product_description,
        oi.order_item_quantity,
        oi.order_item_subtotal,
        oi.order_item_product_price
    FROM
        staging.orders o
    JOIN
        staging.order_items oi ON o.order_id = oi.order_item_order_id
    JOIN
        staging.products p ON oi.order_item_product_id = p.product_id;
    """,
    dag=dag,
)

download_task >> [load_orders_task, load_order_items_task, load_products_task] >> create_view


