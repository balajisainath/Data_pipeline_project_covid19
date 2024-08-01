import os
import logging
import pandas as pd
from paths import path
import great_expectations as ge




# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(path.get_logs_path(), "data_validations.log")),
                        logging.StreamHandler()
                    ]
                    )

class Validate:

    

    @staticmethod
    def fill_null_values(data_frame, columns_list_with_fill: dict):
        """
        Fill null values in specified columns of a DataFrame.

        input:
            data_frame (pd.DataFrame): The DataFrame containing null values.
            columns_list_with_fill (dict): Dictionary where keys are column names and values are fill values.

        output:
            pd.DataFrame: DataFrame with null values filled.

        """

        for column, fill_value in columns_list_with_fill.items():
            if column in data_frame.columns:
                data_frame = data_frame.fillna({column: fill_value})
            else:
                print(f"Column '{column}' not found in DataFrame.")
        
        return data_frame


    @staticmethod
    def column_values_negative(data_frame_dict: dict, Columns: list):
        '''
        Replace negative values with zero in specified columns of DataFrames.

        input:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.
            Columns (list): List of column names to check for negative values.

        output:
            pd.DataFrame: DataFrame with negative values replaced by zero.

        '''
        for key, df in data_frame_dict.items():
            for col in Columns:
                if col in df.columns:
                    condition = df[col] < 0
                
                    if condition.any():
                        logging.info(f"Found negative values in column '{col}'. Setting those values to zero.")
                    
                    # Replace negative values with zero
                        df.loc[condition, col] = 0
            return df

    @staticmethod
    def dimensions(data_frame_dict: dict):
        """
         gives the dimensions (shape) of each DataFrame

        input:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.

        output:
            return the shape of the dataframe
        """
        for key, data_frame in data_frame_dict.items():
            logging.info(f'Size of {key} is {data_frame.shape}')


    @staticmethod
    def check_null_get_col_list(data_frame: pd.DataFrame):
        """
        Get a list of columns with null values in a DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame to check.

        Returns:
            list: List of column names with null values.
        """
        null_dict=dict(data_frame.isnull().sum())
        null_cols=[]
        for name,sum in null_dict.items():
            if sum > 0:
                null_cols.append(name)
        return null_cols
    
    @staticmethod
    def check_null(data_frame_dict: dict):
        """
        Log the number of null values in each DataFrame in a dictionary.

        input:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.

        output:
            returns dictionary of keys as column names and null sum as values

        """

        for key, data_frame in data_frame_dict.items():
            logging.info(f'Null values in {key}:\n{data_frame.isnull().sum()}')

        return {data_frame.isnull().sum()}

    @staticmethod
    def column_value_unique(data_frame_dict: dict, column_name: str):

        """
        Check if all values in a column across multiple DataFrames are unique.

        Args:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.
            column_name (str): Name of the column to check.

        """

        for key, data_frame in data_frame_dict.items():
            is_unique = data_frame[column_name].is_unique
            logging.info(f'Column "{column_name}" in {key} has all unique values: {is_unique}')


    @staticmethod
    def column_values_between(data_frame_dict: dict, column_name: str, min_value: int, max_value: int, intermediate_path: str=None):
        """
         Check if values in a column of DataFrames are within a specified range.
        Non-compliant rows are saved to CSV.

        Args:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.
            column_name (str): Name of the column to check.
            min_value (int): Minimum acceptable value.
            max_value (int): Maximum acceptable value.
            intermediate_path (str, optional): Path to save non-compliant rows.

        Returns:
            pd.DataFrame: DataFrame with compliant rows.
        """

        for key, data_frame in data_frame_dict.items():
            condition = data_frame[column_name].between(min_value, max_value)
            if condition.all():
                logging.info(f'All values in column "{column_name}" of {key} are between {min_value} and {max_value}.')
            else:
                logging.info(f'Not all values in column "{column_name}" of {key} are between {min_value} and {max_value}.')
                non_compliant_df = data_frame[~condition]
                data_frame=data_frame[condition]
                Validate.data_convert.save_to_csv(non_compliant_df, key, intermediate_path)
        
        return data_frame
    

    @staticmethod
    def multiple_column_values_between(data_frame_dict: dict, column_names: list, min_value: int, max_value: int, intermediate_path: str):
        
        """
        Check if values in multiple columns of DataFrames are within a specified range.
        Non-compliant rows are saved to CSV.

        Args:
            data_frame_dict (dict): Dictionary of DataFrames where keys are names and values are DataFrames.
            column_names (list): List of column names to check.
            min_value (int): Minimum acceptable value.
            max_value (int): Maximum acceptable value.
            intermediate_path (str): Path to save non-compliant rows.

        Returns:
            pd.DataFrame: DataFrame with compliant rows.

        """

        for key, data_frame in data_frame_dict.items():
            for column in column_names:
                condition = data_frame[column].between(min_value, max_value)
                if condition.all():
                    logging.info(f'All values in column "{column}" of {key} are between {min_value} and {max_value}.')
                else:
                    logging.info(f'Not all values in column "{column}" of {key} are between {min_value} and {max_value}.')
                    non_compliant_df = data_frame[~condition]
                    data_frame=data_frame[condition]
                    Validate.save_to_csv(non_compliant_df, key, intermediate_path)
            return data_frame
    @staticmethod
    def check_null_sum(data_frame):
        """
        Calculate the total number of null values in a DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame to check.

        Returns:
            int: Total number of null values.

        """

        null_values=data_frame.isnull().sum()
        return null_values.sum()
    
    @staticmethod
    def validate_column_types(data_frame: pd.DataFrame, columns_to_check: list):

        """
        Validate and convert column types in a DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame to validate.
            columns_to_check (list): List of column names to check and potentially convert.

        Returns:
            pd.DataFrame: DataFrame with potentially converted column types.

        """
        ge_df = ge.from_pandas(data_frame)
        updated = False
        for column in columns_to_check:
            result = ge_df.expect_column_values_to_be_in_type_list(column, ['int', 'float', 'str','datetime'])
            if not result['success']:
                logging.info(f'Column "{column}" has incorrect type. Attempting to convert.')
                try:
                    data_frame[column] = pd.to_numeric(data_frame[column], errors='coerce')
                    updated = True
                    logging.info(f'Column "{column}" converted to numeric type.')
                except Exception as e:
                    logging.error(f'Error converting column "{column}": {e}')
        if updated:
            ge_df = ge.from_pandas(data_frame)
            logging.info(f'Updated DataFrame with converted column types.')
        validation_results = ge_df.validate()
        logging.info(validation_results)
        return data_frame
    

   
        

