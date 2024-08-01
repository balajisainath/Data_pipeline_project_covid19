import duckdb
import logging
import os
import glob
from paths import path
import datetime
import shutil

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(path.get_logs_path(), "data_injection.log")),
                        logging.StreamHandler()
                    ])

class DataInjection:
    """A class to handle data injection into a DuckDB database.

    Attributes:
        db_name (str): The name of the database.
        db_folder (str): The folder where the database file is located.
        db_file (str): The full path to the database file.
        connection (duckdb.DuckDBPyConnection): The connection to the DuckDB database.
    """
    def __init__(self, db_name, db_folder):
         
         """Initializes the DataInjection class.

        Args:
            db_name (str): The name of the database.
            db_folder (str): The folder where the database file is located.
        """
         self.db_name = db_name
         self.db_folder = db_folder
         self.db_file = os.path.join(db_folder, f'{db_name}.duckdb')
         self.connection = None
    
    def create_database(self):

        """"
        Creates a database connection and backs up the existing database if it exists.

        Raises:
            Exception: If there is an error connecting to the database or backing it up.

        """
        try:
            if os.path.exists(self.db_file):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_db_file = os.path.join(self.db_folder, f'{self.db_name}_{timestamp}.duckdb')
                shutil.copyfile(self.db_file, backup_db_file)
                logging.info(f"Existing database '{self.db_name}.duckdb' backed up as '{self.db_name}_{timestamp}.duckdb'.")
                #os.remove(self.db_file)
            
            os.makedirs(self.db_folder, exist_ok=True)
            self.connection = duckdb.connect(self.db_file)
            logging.info(f"Connected to {self.db_file} successfully.")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    def inject_data_from_parquet(self):
        """
        Injects data from Parquet files into the database.

        This method reads all Parquet files from the output path and creates tables in the
        database corresponding to each Parquet file.

        Raises:
            Exception: If there is an error injecting data from Parquet files.

        """
        parquet_folder = path.get_output_path()
        
        try:
            parquet_files = glob.glob(os.path.join(parquet_folder, '*.parquet'))
            
            for file_path in parquet_files:
                file_name = os.path.basename(file_path)
                table_name = os.path.splitext(file_name)[0]
                
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
                logging.info(f"Table {table_name} created successfully from {file_path}.")
            
            tables = self.connection.execute("SHOW TABLES").fetchall()
            logging.info(f"Tables in the database: {tables}")
        
        except Exception as e:
            logging.error(f"Error injecting data from Parquet files: {e}")
            raise
        
    def close_connection(self):
        """
        Closes the connection to the database.
        Logs a message indicating that the database connection has been closed.
        """
        
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed.")

# Example usage:
if __name__ == "__main__":
    try:
        data_injector = DataInjection(db_name="covid_19_db",db_folder=path.get_db_files_path())
        data_injector.create_database()
        data_injector.inject_data_from_parquet()
    except Exception as e:
        logging.error(f"An error occurred during data injection: {e}")
    finally:
        if data_injector:
            data_injector.close_connection()
