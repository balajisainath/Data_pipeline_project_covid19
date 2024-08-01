import os
import pandas as pd
from paths import path
import string
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(path.get_logs_path(), "data_conversions.log")),
                        logging.StreamHandler()
                    ]
                    )

class DataConvert:
    @staticmethod
    def to_parquet(data_frame: dict, output_dir=None):
        '''
        Convert DataFrames to Parquet format and log the conversion process.

        Args:
            data_frame (dict): Dictionary where keys are names and values are DataFrames to convert.
            output_dir (str, optional): Directory path to save the Parquet files. Defaults to None.

        Returns:
            None

        '''
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        for name, df in data_frame.items():
            output_path = os.path.join(output_dir, f"{name}.parquet")
            if os.path.exists(output_path):
                logging.info(f"File {output_path.split(os.sep)[-2]} already exists and will be overwritten.")
            df.to_parquet(output_path, engine='pyarrow', index=False)
            logging.info(f"Converted {name} to Parquet at {output_path.split(os.sep)[-2]}")

    @staticmethod
    def save_to_csv(non_compliant_df:pd.DataFrame, key: str, intermediate_path:str=path.get_intermediate_path()):
        '''
        Save non-compliant DataFrame to a CSV file at the specified path.

        Args:
            non_compliant_df (pd.DataFrame): DataFrame containing non-compliant data.
            key (str): Name or identifier for the DataFrame.
            intermediate_path (str, optional): Path to store the CSV file. Defaults to path.get_intermediate_path().

        Returns:
            None
        '''
        os.makedirs(intermediate_path, exist_ok=True)
        
        file_path = os.path.join(intermediate_path, f'{key}_non_compliant.csv')
        
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            updated_df = pd.concat([existing_df, non_compliant_df]).drop_duplicates().reset_index(drop=True)
        else:
           updated_df = non_compliant_df

        updated_df.to_csv(file_path, index=False)


class multiple_DataConvert(DataConvert):
    @staticmethod
    def multiple_to_parquet(data_frames: dict, output_dir: str=None):
        '''
        Convert multiple DataFrames to Parquet format and log the conversion process.

        Args:
            data_frames (dict): Dictionary where keys are names and values are DataFrames to convert.
            output_dir (str, optional): Directory path to save the Parquet files. Defaults to None.

        Returns:
            None
        '''
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        for name, df in data_frames.items():
            output_path = os.path.join(output_dir, f"{name}.parquet")
            if os.path.exists(output_path):
                logging.info(f"File {output_path} already exists and will be overwritten.")
            df.to_parquet(output_path, engine='pyarrow', index=False)
            logging.info(f"Converted {name} to Parquet at {output_path.split(os.sep)[-2]}")