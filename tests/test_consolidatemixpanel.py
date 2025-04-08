import unittest
import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import patch, MagicMock
from consolidatemixpanel import consolidate_data

class TestConsolidateMixpanel(unittest.TestCase):
    """Tests for consolidatemixpanel.py module."""
    
    def setUp(self):
        """Set up a temporary directory for test files."""
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.temp_dir, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Create a test input Excel file
        self.input_file = os.path.join(self.temp_dir, "test_input.xlsx")
        self.create_test_input_file()
    
    def tearDown(self):
        """Remove the temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def create_test_input_file(self):
        """Create a test input Excel file."""
        # Create a DataFrame with test data
        data = {
            'tripId': ['12345', '12345', '67890', '67890'],
            'time': pd.to_datetime(['2023-01-01 12:00:00', '2023-01-01 12:30:00', 
                                   '2023-01-02 12:00:00', '2023-01-02 12:30:00']),
            'app_build_number': ['100', '101', '100', '101'],
            'app_version': ['1.0.0', '1.0.1', '1.0.0', '1.0.1'],
            'brand': ['Samsung', 'Samsung', 'Xiaomi', 'Xiaomi'],
            'model': ['SM-A125F', 'SM-A125F', 'Redmi Note 7', 'Redmi Note 7']
        }
        
        df = pd.DataFrame(data)
        df.to_excel(self.input_file, index=False)
    
    @patch('consolidatemixpanel.merge_with_mobile_specs')
    def test_consolidate_data(self, mock_merge):
        """Test that consolidate_data works correctly."""
        # Create a mock DataFrame to be returned by merge_with_mobile_specs
        merged_data = pd.DataFrame({
            'tripId': ['12345', '67890'],
            'time': pd.to_datetime(['2023-01-01 12:30:00', '2023-01-02 12:30:00']),
            'app_build_number': ['101', '101'],
            'app_version': ['1.0.1', '1.0.1'],
            'brand': ['Samsung', 'Xiaomi'],
            'model': ['SM-A125F', 'Redmi Note 7'],
            'Device Name': ['Samsung Galaxy A12', 'Xiaomi Redmi Note 7'],
            'Release Year': [2020, 2019],
            'Android Version': ['10', '9.0 (Pie)']
        })
        mock_merge.return_value = merged_data
        
        # Output paths
        output_dir = os.path.join(self.temp_dir, "output")
        output_file = "consolidated.xlsx"
        
        # Call the function
        result = consolidate_data(self.input_file, output_dir, output_file)
        
        # Assert it returned True for success
        self.assertTrue(result)
        
        # Assert the output file exists
        expected_output_path = os.path.join(output_dir, output_file)
        self.assertTrue(os.path.exists(expected_output_path))
        
        # Verify merge_with_mobile_specs was called
        mock_merge.assert_called_once()
        
        # Load the output file and check it
        output_df = pd.read_excel(expected_output_path)
        self.assertEqual(len(output_df), 2)  # Should have consolidated to 2 rows
    
    def test_consolidate_data_missing_input(self):
        """Test that consolidate_data handles missing input file correctly."""
        # Nonexistent input file
        nonexistent_file = os.path.join(self.temp_dir, "nonexistent.xlsx")
        
        # Output paths
        output_dir = os.path.join(self.temp_dir, "output")
        output_file = "consolidated.xlsx"
        
        # Call the function
        result = consolidate_data(nonexistent_file, output_dir, output_file)
        
        # Assert it returned False for failure
        self.assertFalse(result)
        
        # Assert the output file doesn't exist
        expected_output_path = os.path.join(output_dir, output_file)
        self.assertFalse(os.path.exists(expected_output_path))

if __name__ == '__main__':
    unittest.main() 