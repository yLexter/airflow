import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import boto3
import pandas as pd
import os
import re
import shutil
import itertools
import uuid
import json

siglas = ['ACF']
anos = [2021]
estados = ['PB']
meses = [f"{i:02}" for i in range(1, 13)]
path_parquets = ''

class BaseDag:

    pattern = r'(?P<sigla>[A-Z]{2,3})(?P<estado>[A-Z]{2})(?P<ano>\d{2})(?P<mes>\d{2}).parquet'

    json_drop_columns = 'dags/jsons/SIA/drop_columns.json'
    json_group_transformation = 'dags/jsons/SIA/group_transformation.json'
    json_rename_columns = 'dags/jsons/SIA/rename_columns.json'

    bucket_bronze = 'bronze'
    base_folder_cache = 'dags/cache'
    minio_client = boto3.client(
           's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
    )


    def read_json(path):
        json = None

        with open(path, 'r') as f:
            json = json.load(f)

        return json

    def get_folder_cache(self):
        return str(uuid.uuid4())[:14]

    def list_folders_minio(self, path):
        folders = set()
        continuation_token = None

        while True:
            list_params = { 
                "Bucket": self.bucket_bronze, 
                "Prefix": path 
            }

            if continuation_token:
                list_params["ContinuationToken"] = continuation_token

            response = self.minio_client.list_objects_v2(**list_params)

            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    parts = key.split("/")[:-1]  

                    for i in range(1, len(parts) + 1):
                        folders.add("/".join(parts[:i]) + "/")

            if response.get("IsTruncated"):
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return sorted(folders)

    def create_filenames(self):
        return [f"{sigla}{estado}{ano % 100}{mes}" for sigla, ano, estado, mes in itertools.product(siglas, anos, estados, meses)]

    def filter_paths(self, paths, codigos):
        paths_filtrados = []
        
        for path in paths:
            path_lower = path.lower()  
            
            for codigo in codigos:
                codigo_lower = codigo.lower()
                if codigo_lower in path_lower:
                    paths_filtrados.append(path)
                    break 

        return paths_filtrados

    def download_parquet(self):
        all_parquets = self.list_folders_minio(path_parquets)
        files = self.create_filenames()

        filtred_parquets = self.filter_paths(all_parquets, files)
        
        for path_parquet_minio in filtred_parquets[:1]:
            
            folder_parquet = re.search(self.pattern, path_parquet_minio).group()
            path_parquet_cache = os.path.join(self.folder_cache_files, folder_parquet)

            if not os.path.exists(path_parquet_cache):
                os.mkdir(path_parquet_cache)
            else:
                shutil.rmtree(path_parquet_cache)
                os.mkdir(path_parquet_cache)
            
            response = self.minio_client.list_objects_v2(Bucket=self.bucket_bronze, Prefix= path_parquet_minio )

            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    file_name = os.path.basename(key)

                    file_path = os.path.join(path_parquet_cache, file_name)
                    
                    self.minio_client.download_file(Bucket=self.bucket_bronze, Key=key, Filename=file_path)
    
    def drop_columns(self, parquet_path):
        
        with open(self.json_file, 'r') as f:
            drop_columns = json.load(f)

        df = pd.read_parquet(parquet_path)
        df = df.drop(columns=drop_columns, errors='ignore')

        df.to_parquet(parquet_path)


    def rename_columns(self, parquet_path):
        json_columns = self.read_json(self.json_drop_columns)

        df = pd.read_parquet(parquet_path)
        df = df.rename(columns=json_columns)

        df.to_parquet(parquet_path)

    
    def group_transformation(self, parquet_path):
        json_columns_mapping = self.read_json(parquet_path)

        df = pd.read_parquet(parquet_path)

        for column_name, mappings in json_columns_mapping.items():
            if column_name in df.columns:
                df[column_name] = df[column_name].map(mappings).fillna(df[column_name])

        df.to_parquet(parquet_path, index=False)

with DAG(
    dag_id="latest_only_with_trigger",
    schedule=datetime.timedelta(hours=4),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example3"],
) as dag:
    pass

  