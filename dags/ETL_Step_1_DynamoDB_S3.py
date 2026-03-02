import os
import pandas as pd
from dotenv import load_dotenv

from common.dynamoDB import get_data
from common.s3 import _load_to_s3

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
Extract data from DynamoDB 
and load into S3 bucket 

example S3 URI for order order_01:
s3://DATA_BUCKET/cleansed/order_01/ecommerce_data_order_01.csv
"""

DAG_ID = 'ETL_Step_1_Extract_Dynamodb'

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
    description='Extract raw and cleanse data of orders from DynamoDB to S3',
    catchup=False,
    schedule=None,
    tags=['migration', 'raw', 'cleansed','dynamodb']
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
        
    @task(task_id='extract_dynamodb')
    def extract(table_name, partition_key, params_task: ParamsDict) -> list:
        print('Start to extract data from DynamoDB ....')
        response = get_data(table_name=table_name, 
                            partition_key=partition_key, 
                            partition_value=params_task['order_id'])
        print(f'Extract successful {len(response)} rows from table {table_name} from DyanmoDB.')
        return response
    
    @task(task_id='load_to_s3')
    def load(items: list, bucket_name: str, params_task: ParamsDict):
        output_path=f'cleansed/{params_task['order_id'].lower()}/ecommerce_data_{params_task['order_id'].lower()}.csv'
        print('Start load data into S3 ...')
        _load_to_s3(list=items, bucket=bucket_name, output_path=output_path)

    # Task instances
    params_pre_task = pre_task()
    extract_from_dynamodb = extract(table_name=table_name,
                                    partition_key=partition_key,
                                    params_task=params_pre_task)
    load_to_s3 = load(items=extract_from_dynamodb,
                      bucket_name=bucket,
                      params_task=params_pre_task) 
    
    params_pre_task >> extract_from_dynamodb >> load_to_s3