import unittest
import pandas as pd
from datetime import datetime
from etl.transform import ETLTransformer

class TestETLTransformer(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.taps_data = pd.DataFrame({
            'user_id': ['1', '2'],
            'day': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            'event_data': [{'key1': 'value1'}, {'key2': 'value2'}],
            'position': [1, 2],
            'value_prop': ['Prop1', 'Prop2']
        })

        self.prints_data = pd.DataFrame({
            'user_id': ['1', '2'],
            'day': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            'event_data': [{'key1': 'value1'}, {'key2': 'value2'}],
            'position': [1, 2],
            'value_prop': ['Prop1', 'Prop2']
        })

        self.pays_data = pd.DataFrame({
            'user_id': ['1', '2'],
            'pay_date': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            'amount': [100, 200]
        })

        self.transformer = ETLTransformer(self.taps_data, self.prints_data, self.pays_data)

    def test_unnested_columns(self):
        self.transformer.unnested_columns()
        self.assertNotIn('event_data', self.transformer.taps_source.columns)
        self.assertNotIn('event_data', self.transformer.prints_source.columns)
        self.assertIn('key1', self.transformer.taps_source.columns)
        self.assertIn('key2', self.transformer.prints_source.columns)

    def test_convert_dtypes_standarized(self):
        self.transformer.convert_dtypes_standarized()
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.transformer.pays_source['pay_date']))
        self.assertTrue(pd.api.types.is_string_dtype(self.transformer.pays_source['user_id']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.transformer.prints_source['day']))
        self.assertTrue(pd.api.types.is_integer_dtype(self.transformer.prints_source['position']))
        self.assertTrue(pd.api.types.is_string_dtype(self.transformer.prints_source['value_prop']))

    def test_transform_prints_clicked(self):
        self.transformer.unnested_columns()
        self.transformer.convert_dtypes_standarized()
        result = self.transformer.transform_prints_clicked()
        self.assertIn('clicked', result.columns)
        self.assertTrue(result['clicked'].isin([True, False]).all())

if __name__ == '__main__':
    unittest.main()