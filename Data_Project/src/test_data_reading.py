import warnings
import pandas as pd
import pytest
from data_reading import DataReading
import os


@pytest.fixture(scope='module', autouse=True)
def suppress_warnings():
    warnings.filterwarnings('ignore', category=DeprecationWarning, module='botocore.auth')




project_root = os.path.dirname(os.path.dirname(__file__))
MOCK_FILE_PATHS = {
    'file1': os.path.join(project_root, 'data', 'input', 'country_wise_latest.csv'),
    'file2': os.path.join(project_root,'data','input','covid_19_clean_complete.csv'),
    }

def test_read_data_returns_dict():
    result = DataReading.read_data(MOCK_FILE_PATHS)
    assert isinstance(result, dict)

def test_read_data_returns_dataframes():
    result = DataReading.read_data(MOCK_FILE_PATHS)
    for df in result.values():
        assert isinstance(df, pd.DataFrame)

def test_read_from_aws():
    result=DataReading.read_data_aws('dataprojectbuckets')
    for df in result.values():
        assert isinstance(df,pd.DataFrame)



