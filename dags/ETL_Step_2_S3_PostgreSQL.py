import os
import pandas as pd
from dotenv import load_dotenv
from typing import List, Optional, Tuple
from decimal import Decimal, InvalidOperation

from common.dynamoDB import get_data
from common.s3 import read_csv, _load_to_s3
from common.postgreSQL import save_to_postgre

from airflow import DAG
from datetime import timedelta
from airflow.decorators import task
from airflow.sdk import Param
from airflow.operators.python import get_current_context
from typing import TypedDict

load_dotenv()
table_name = os.getenv("TABLE_NAME")
partition_key = os.getenv("PARTITION_KEY")
bucket = os.getenv("BUCKET_NAME")

"""
Extract data cleansed from S3 
and load into S3 bucket and postgresql

example S3 URI for order_id order_01:
s3://DATA_BUCKET/transformed/order_01/ecommerce_data_order_01.csv
"""

DAG_ID = 'ETL_Step_2_Extract_S3_Load_PostgreSQL'

class ParamsDict(TypedDict):
    order_id: str

with DAG(
    DAG_ID,
    params={
        'order_id': Param(
            type='string',
            description='Order ID to be migrated',
            example='order_example'
        )
    },
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=2)
    },
    description='Extract cleanse data of order from S3, transfrom and load to PostgreSQL',
    catchup=False,
    schedule=None,
    tags=['migration', 'transfrom', 'cleansed', 's3', 'postgresql']
) as dag:
    
    @task(task_id='pre_task')
    def pre_task() -> ParamsDict:
        context = get_current_context()
        dag_run = context['dag_run']
        if(dag_run.conf and 'order_id' in dag_run.conf):
            order_id = dag_run.conf['order_id']

            params = ParamsDict(
                order_id=order_id
            )

            return params
        else:
            raise ValueError("Params 'order_id' must be defined!!!")
        
    @task(task_id='extract_s3')
    def extract(bucket:str, params_task: ParamsDict) -> List:
        print('Start to extract data cleansed from S3 ....')

        key = f'cleansed/{params_task['order_id'].lower()}/ecommerce_data_{params_task['order_id'].lower()}.csv'
        items = read_csv(bucket_name=bucket, key=key)
        return items
    
    @task(task_id='transform')
    def transform(items) -> list:
        print('Start to transform data ......')

        for item in items:
            if 'Shipping_address' in item:
                address = item['Shipping_address'].split(", ")
                item['Shipping_street'] = address[0]
                item['Shipping_city'] = address[1]
                item['Shipping_country'] = address[2]

                item.pop('Shipping_address')
            
            if 'Total_price' in item and item['Total_price'] is not None:
                try:
                    item['Is_large_order'] = True if Decimal(str(item['Total_price'])) >= 1000 else False
                except (InvalidOperation, ValueError):
                    item['Is_large_order'] = False

        print(f'Transform successful {len(items)} rows')
        return items
    
    @task(task_id='load_to_s3')
    def load_to_s3(items: list, bucket_name: str, params_task: ParamsDict):
        print('Start load data into S3 ...')

        output_path=f'transformed/{params_task['order_id'].lower()}/ecommerce_data_{params_task['order_id'].lower()}.csv'
        _load_to_s3(list=items, bucket=bucket_name, output_path=output_path)

    @task(task_id='load_to_postgresql')
    def load_to_postgre(items: list, table_name: str):
        print('Start load data into PostgreSQL ...')

        dataframe = pd.DataFrame(items)
        num_cols = ['Quantity', 'Unit_price', 'Total_price']
        dataframe[num_cols] = dataframe[num_cols].apply(pd.to_numeric, errors='coerce')

        save_to_postgre(dataframe, table_name)

    # Task instances
    params_pre_task = pre_task()
    extract_from_s3 = extract(bucket=bucket,
                              params_task=params_pre_task)
    data_transformed = transform(items=extract_from_s3)
    load_data_to_s3 = load_to_s3(items=data_transformed,
                                 bucket_name=bucket,
                                 params_task=params_pre_task)
    load_data_to_postgre = load_to_postgre(items=data_transformed,
                                           table_name=table_name)
    
    params_pre_task \
        >> extract_from_s3 \
            >> data_transformed \
                >> load_data_to_s3 \
                    >> load_data_to_postgre
                