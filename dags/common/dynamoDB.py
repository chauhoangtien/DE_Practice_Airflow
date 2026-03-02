import boto3
from boto3.dynamodb.conditions import Key
import csv
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name="us-east-1")

def convert_value(value):
        if value.isdigit():
            return int(value)
        try:
            return Decimal(value)
        except:
            return value
    
def push_data(table_name: str, file_path: str):
    table = dynamodb.Table(table_name)
    with open(file_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        with table.batch_writer() as batch:
            for row in reader:
                item = {k: convert_value(v) for k, v in row.items()}
                batch.put_item(item)

def get_data(table_name: str, partition_key: str, partition_value:str) -> list:
    table = dynamodb.Table(table_name)
    kce = Key(partition_key).eq(partition_value)

    response = table.query(
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression = kce
    )

    return response['Items']
     