import boto3
import pandas as pd
import csv
import io
from typing import List, Optional, Tuple
from botocore.exceptions import ClientError

s3 = boto3.client('s3', region_name="us-east-1")

def ensure_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' does not exist. Creating...")
            s3.create_bucket(Bucket=bucket_name)
        else:
            print(f"Error checking bucket: {e}")
            raise

def _load_to_s3(list: list, bucket:str, output_path: str):
    ensure_bucket_exists(bucket_name=bucket)

    df = pd.DataFrame(list)
    response = s3.put_object(Bucket=bucket,
                             Key=output_path,
                             Body=df.to_csv(index=False).encode('utf-8'))
    
    print('Load object succesfully')
    return response

def list_object_keys(bucket_name:str, prefix: str) -> List[str]:
    keys = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    # for key in keys:
    #     if key.endswith('/'):
    #         keys.remove(key)

    return keys

def delete_objects(bucket_name: str, prefix:str):
    try:
        objects = []

        keys = list_object_keys(bucket_name=bucket_name, 
                                prefix=prefix)
        
        for key in keys:
            objects.append({'Key': key})

        s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': objects
            }
        )
    except Exception as e:
        print(f"Failed delete: {e}")

def read_csv(bucket_name: str, key: str) -> List:
    response = s3.get_object(
        Bucket=bucket_name,
        Key=key
    )
    content = response['Body'].read().decode('utf-8')

    csv_reader = csv.DictReader(io.StringIO(content))
    rows = [row for row in csv_reader]

    return rows