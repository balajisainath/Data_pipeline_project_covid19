from io import BytesIO
import great_expectations as ge
import duckdb
import pandas as pd
from creds import Creds
import boto3
import logging
import os
from paths import path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(path.get_logs_path(), "data_reading.log")
        ),
        logging.StreamHandler(),
    ],
)


class DataReading:
    @staticmethod
    def read_data(file_names: dict):
        ''' takes the fie name and the location of the file to the csv format '''
        data_frames = {}
        for name, file_path in file_names.items():
            data_frames[name] = ge.read_csv(file_path)
        return data_frames

    @staticmethod    
    def read_data_from_db(file_path: str):
        ''' Reads data from duckdb database file and return data frames'''
        try:
            connection = duckdb.connect(file_path)
            tables = connection.execute("SHOW TABLES").fetchall()
            
            data_frames = {}
            for table_name, in tables:
                data_frames[table_name] = ge.read_sql(f"SELECT * FROM {table_name}", connection)
            
            return data_frames
        
        except Exception as e:
            logging.info(f"Error reading data from duckdB: {e}")
            raise
        
        finally:
            if connection:
                connection.close()

    
    @staticmethod
    def read_data_aws(bucket_name):
        
        
        s3 = boto3.client(
            service_name=os.getenv("service_name"),
            region_name=os.getenv("region_name"),
            aws_access_key_id=Creds.get_Access_Key(),
            aws_secret_access_key=Creds.get_Access_Secret_Key()
        )
        
        data_dict = {}

        try:
            response = s3.list_objects_v2(Bucket=bucket_name)
        except Exception as e:
            logging.info(f"Error listing objects in bucket: {e}")
            return data_dict
        
        for obj in response.get('Contents', []):
            file_name = obj['Key']
            file_type = file_name.split('.')[-1].lower()

            try:
                s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
                file_stream = BytesIO(s3_object['Body'].read())
                
                if file_type == 'csv':
                    df = pd.read_csv(file_stream)
                elif file_type == 'parquet':
                    df = pd.read_parquet(file_stream)
                else:
                    continue
                
                data_dict[file_name.lower()] = df
            except Exception as e:
                logging.info(f"Error reading object {file_name}: {e}")
        
        return data_dict
    
    @staticmethod
    def upload_data_aws(output_dir: str, bucket_name: str):
        s3 = boto3.client(
            service_name=os.getenv("service_name"),
            region_name=os.getenv("region_name"),
            aws_access_key_id=Creds.get_Access_Key(),
            aws_secret_access_key=Creds.get_Access_Secret_Key()
        )
        
        try:
           
            for root, dirs, files in os.walk(output_dir):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    
                    try:
                        
                        s3_key = os.path.relpath(file_path, output_dir)
                        s3_key = s3_key.replace(os.path.sep, '/')
                        
                        
                        s3.upload_file(file_path, bucket_name, s3_key)
                        logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name} with key {s3_key}.")
                    
                    except Exception as e:
                        logging.info(f"Error uploading file {file_name}: {e}")
        
        except Exception as e:
            logging.info(f"Error accessing directory {output_dir}: {e}")






