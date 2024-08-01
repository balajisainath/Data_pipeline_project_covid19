import os
import pytest
import shutil
import glob
import duckdb
from data_injection import DataInjection


project_root=os.path.dirname(os.path.dirname(__file__))
test_db_folder = os.path.join(project_root,'data','test_db_files')
test_parquet_folder = os.path.join(project_root,"data",'test_output','')


@pytest.fixture(scope="module")
def setup_environment():
    os.makedirs(test_db_folder, exist_ok=True)
    os.makedirs(test_parquet_folder, exist_ok=True)
    yield
    shutil.rmtree(test_db_folder, ignore_errors=True)
    shutil.rmtree(test_parquet_folder, ignore_errors=True)

def test_create_database(setup_environment):
    db_name = "test_db"
    injector = DataInjection(db_name)
    
    
    injector.create_database()
    

    assert os.path.exists(injector.db_file)
    
    if os.path.exists(injector.db_file):
        os.remove(injector.db_file)

def test_inject_data_from_parquet(setup_environment):
    db_name = "test_db"
    injector = DataInjection(db_name, )
    injector.create_database()
    
    parquet_file_path = os.path.join(test_parquet_folder, 'table1.parquet')
    with open(parquet_file_path, 'w') as f:
        f.write('dummy data')

   
    injector.inject_data_from_parquet()
    
    if os.path.exists(parquet_file_path):
        os.remove(parquet_file_path)
    injector.close_connection()

def test_close_connection(setup_environment):
    db_name = "test_db"
    injector = DataInjection(db_name)
    injector.create_database()
    injector.close_connection()
    
    assert injector.connection is None
