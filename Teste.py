import datetime
import pendulum

import boto3
import pandas as pd
import os
import re
import itertools
import uuid
import json
import shutil

siglas = ['PS', 'ATD', 'AM']
anos = [2023, 2024]
estados = ['PB']
meses = [f"{i:02}" for i in range(1, 13)]
path_parquets = 'sia-parquet/sia-pb-2023-2024'

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

    def read_json(self, path):
        json_file = None

        with open(path, 'r') as f:
            json_file = json.load(f)

        return json_file

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
        
        drop_columns_json = self.read_json(self.json_drop_columns)

        df = pd.read_parquet(parquet_path)
        df = df.drop(columns=drop_columns_json, errors='ignore')

        df.to_parquet(parquet_path, mode='w')

    def rename_columns(self, parquet_path):
        json_columns = self.read_json(self.json_rename_columns)

        df = pd.read_parquet(parquet_path)
        df = df.rename(columns=json_columns)

        df.to_parquet(parquet_path, mode='w')
    
    def group_transformation(self, parquet_path):
        json_columns_mapping = self.read_json(self.json_group_transformation)

        df = pd.read_parquet(parquet_path)

        for column_name, mappings in json_columns_mapping.items():
            if column_name in df.columns:
                df[column_name] = df[column_name].map(mappings).fillna(df[column_name])

        df.to_parquet(parquet_path, mode='w', index=False)

    def upload_to_silver(self, parquet_path):

        if not os.path.exists(parquet_path):
            print(f"O diretório '{parquet_path}' não existe.")
            return
        
        for root, _, files in os.walk(parquet_path):

            parquet_dir = os.path.basename(root)

            match = re.match(self.pattern, parquet_dir)

            sigla = match.group('sigla')
            estado = match.group('estado')
            ano = match.group('ano')
            mes = match.group('mes')

            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    s3_key = f'SIA/{sigla}/{sigla}{estado}{ano}{mes}.parquet/{file}'

                    self.minio_client.upload_file(Filename=file_path, Bucket='silver', Key=s3_key)

path = 'C:/airflow/dags/cache/AMPB2301.parquet'
base = BaseDag()

base.drop_columns(path)
base.rename_columns(path)
base.group_transformation(path)
base.upload_to_silver(path)