import unittest
import pandas as pd
import os
from procces_etl.etl.load import DataFrameSaver

class TestDataFrameSaver(unittest.TestCase):

    def setUp(self):
        self.sample_data = {
            'column1': [1, 2, 3],
            'column2': ['a', 'b', 'c']
        }
        self.dataframe = pd.DataFrame(self.sample_data)
        self.saver = DataFrameSaver(self.dataframe)
        self.test_file_path = '/tmp/test_output.csv'

    def tearDown(self):
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    def test_save_to_csv(self):
        self.saver.save_to_csv(self.test_file_path)
        
        self.assertTrue(os.path.exists(self.test_file_path))
        
        loaded_df = pd.read_csv(self.test_file_path)
        
        pd.testing.assert_frame_equal(loaded_df, self.dataframe)

if __name__ == '__main__':
    unittest.main()