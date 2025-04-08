import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from mobile_specs import get_mobile_specs_data, merge_with_mobile_specs

class TestMobileSpecs(unittest.TestCase):
    """Tests for mobile specs module."""
    
    def test_get_mobile_specs_data(self):
        """Test that get_mobile_specs_data returns a DataFrame with expected columns."""
        mobile_specs_df = get_mobile_specs_data()
        
        # Check if result is a DataFrame
        self.assertIsInstance(mobile_specs_df, pd.DataFrame)
        
        # Check expected columns
        expected_columns = [
            'Original Model', 'Brand', 'Device Name', 'Release Year', 'Android Version',
            'Fingerprint Sensor', 'Accelerometer', 'Gyro', 'Proximity Sensor',
            'Compass', 'Barometer', 'Background Task Killing Tendency', 'Chipset',
            'RAM', 'Storage', 'Battery (mAh)'
        ]
        
        for column in expected_columns:
            self.assertIn(column, mobile_specs_df.columns)
        
        # Verify it's not empty
        self.assertGreater(len(mobile_specs_df), 0)
    
    def test_merge_with_mobile_specs(self):
        """Test that merge_with_mobile_specs correctly merges data."""
        # Create a test DataFrame
        input_df = pd.DataFrame({
            'model': ['Redmi Note 7', 'SM-A125F', 'INVALID_MODEL'],
            'other_column': [1, 2, 3]
        })
        
        # Merge with mobile specs
        merged_df = merge_with_mobile_specs(input_df)
        
        # Check if result is a DataFrame
        self.assertIsInstance(merged_df, pd.DataFrame)
        
        # Verify the shape - should have original data plus mobile specs columns
        self.assertEqual(len(merged_df), len(input_df))
        self.assertGreater(len(merged_df.columns), len(input_df.columns))
        
        # Verify the 'model' column is still there
        self.assertIn('model', merged_df.columns)
        
        # Verify we have a 'Brand' column from the specs data
        self.assertIn('Brand', merged_df.columns)
        
        # Check data for known models
        xiaomi_row = merged_df[merged_df['model'] == 'Redmi Note 7']
        self.assertEqual(xiaomi_row['Brand'].values[0], 'Xiaomi')
        
        samsung_row = merged_df[merged_df['model'] == 'SM-A125F']
        self.assertEqual(samsung_row['Brand'].values[0], 'Samsung')
        
        # Check default values were applied to unknown model
        unknown_row = merged_df[merged_df['model'] == 'INVALID_MODEL']
        self.assertEqual(unknown_row['Brand'].values[0], 'Unknown')
        self.assertEqual(unknown_row['Device Name'].values[0], 'Unknown Device')

    def test_merge_with_mobile_specs_empty_df(self):
        """Test that merge_with_mobile_specs handles empty DataFrame."""
        # Create an empty DataFrame with 'model' column
        empty_df = pd.DataFrame(columns=['model'])
        
        # Merge with mobile specs
        merged_df = merge_with_mobile_specs(empty_df)
        
        # Check if result is a DataFrame
        self.assertIsInstance(merged_df, pd.DataFrame)
        
        # Verify it's still empty
        self.assertEqual(len(merged_df), 0)
        
        # Verify the columns from specs are there
        self.assertIn('Brand', merged_df.columns)
        self.assertIn('Device Name', merged_df.columns)

if __name__ == '__main__':
    unittest.main() 