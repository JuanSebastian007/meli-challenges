import unittest
import pandas as pd
from unittest.mock import patch, mock_open
from etl.extract import DataExtractor

class TestDataExtractor(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='col1,col2\nval1,val2')
    @patch('pandas.read_csv')
    def test_extract_csv(self, mock_read_csv, mock_file):
        mock_read_csv.return_value = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
        extractor = DataExtractor('test.csv')
        result = extractor.extract()
        mock_read_csv.assert_called_once_with('test.csv')
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(result.iloc[0]['col1'], 'val1')

    @patch('builtins.open', new_callable=mock_open, read_data='{"col1": "val1", "col2": "val2"}\n')
    @patch('pandas.read_json')
    def test_extract_json(self, mock_read_json, mock_file):
        mock_read_json.return_value = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
        extractor = DataExtractor('test.json')
        result = extractor.extract()
        mock_read_json.assert_called_once_with('test.json', lines=True)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(result.iloc[0]['col1'], 'val1')

    def test_extract_unsupported_format(self):
        extractor = DataExtractor('test.txt')
        with self.assertRaises(ValueError) as context:
            extractor.extract()
        self.assertEqual(str(context.exception), "Unsupported file format")

if __name__ == '__main__':
    unittest.main()