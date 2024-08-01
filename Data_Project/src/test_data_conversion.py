import os
import pandas as pd
from data_conversion import DataConvert



project_root=os.path.dirname(os.path.dirname(__file__))
Test_output_dir=os.path.join(project_root,'data','test_output')

Mock_Data={
    'test_file': pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
}



def test_to_parquet_creates_file():
    if not os.path.exists(Test_output_dir):
        os.makedirs(Test_output_dir)
    DataConvert.to_parquet(Mock_Data, Test_output_dir)
    output_path = os.path.join(Test_output_dir, 'test_file.parquet')   
    assert os.path.exists(output_path)