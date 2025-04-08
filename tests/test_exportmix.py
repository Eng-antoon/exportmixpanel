import unittest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import pandas as pd
from exportmix import export_data, export_data_for_comparison

class TestExportMix(unittest.TestCase):
    """Tests for exportmix.py module."""
    
    def setUp(self):
        """Set up a temporary directory for test files."""
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.temp_dir, "data")
        self.comparison_dir = os.path.join(self.data_dir, "comparison")
        os.makedirs(self.comparison_dir, exist_ok=True)
    
    def tearDown(self):
        """Remove the temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    @patch('exportmix.requests.get')
    def test_export_data_success(self, mock_get):
        """Test that export_data works correctly when the API call succeeds."""
        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '''{"event":"trip_details_route","properties":{"tripId":"12345","time":1617235200,"app_version":"1.0.0","model":"SM-A125F"}}
{"event":"trip_details_route","properties":{"tripId":"67890","time":1617235300,"app_version":"1.1.0","model":"Redmi Note 7"}}'''
        mock_get.return_value = mock_response
        
        # Test output file path
        output_file = os.path.join(self.temp_dir, "test_export.xlsx")
        
        # Call the function
        result = export_data("2023-01-01", "2023-01-31", output_file)
        
        # Assert it returned True for success
        self.assertTrue(result)
        
        # Assert output file exists
        self.assertTrue(os.path.exists(output_file))
        
        # Verify file content by loading it back
        df = pd.read_excel(output_file)
        self.assertEqual(len(df), 2)
        self.assertIn("tripId", df.columns)
        self.assertIn("time", df.columns)
        self.assertIn("app_version", df.columns)
        self.assertIn("model", df.columns)
    
    @patch('exportmix.requests.get')
    def test_export_data_api_error(self, mock_get):
        """Test that export_data handles API errors correctly."""
        # Mock a failed response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_get.return_value = mock_response
        
        # Test output file path
        output_file = os.path.join(self.temp_dir, "test_export_error.xlsx")
        
        # Call the function
        result = export_data("2023-01-01", "2023-01-31", output_file)
        
        # Assert it returned False for failure
        self.assertFalse(result)
        
        # Assert output file doesn't exist
        self.assertFalse(os.path.exists(output_file))
    
    @patch('exportmix.export_data')
    @patch('os.path.exists')
    def test_export_data_for_comparison_success(self, mock_exists, mock_export_data):
        """Test that export_data_for_comparison works correctly."""
        # Set up mocks to simulate successful export
        mock_export_data.return_value = True
        mock_exists.return_value = True  # Mock that files exist
        
        # Call the function
        base_file, comp_file = export_data_for_comparison(
            "2023-01-01", "2023-01-31", 
            "2023-02-01", "2023-02-28"
        )
        
        # Assert expected file paths
        expected_base_file = os.path.join("data", "comparison", "base_2023-01-01_to_2023-01-31.xlsx")
        expected_comp_file = os.path.join("data", "comparison", "comparison_2023-02-01_to_2023-02-28.xlsx")
        
        self.assertEqual(base_file, expected_base_file)
        self.assertEqual(comp_file, expected_comp_file)
        
        # Verify export_data was called twice with correct parameters
        self.assertEqual(mock_export_data.call_count, 2)
        mock_export_data.assert_any_call("2023-01-01", "2023-01-31", expected_base_file, "trip_details_route")
        mock_export_data.assert_any_call("2023-02-01", "2023-02-28", expected_comp_file, "trip_details_route")
    
    @patch('exportmix.export_data')
    def test_export_data_for_comparison_failure(self, mock_export_data):
        """Test that export_data_for_comparison handles export failures."""
        # Mock export_data to fail
        mock_export_data.return_value = False
        
        # Call the function
        base_file, comp_file = export_data_for_comparison(
            "2023-01-01", "2023-01-31", 
            "2023-02-01", "2023-02-28"
        )
        
        # Assert both files are None on failure
        self.assertIsNone(base_file)
        self.assertIsNone(comp_file)

if __name__ == '__main__':
    unittest.main() 