import unittest
import os
import shutil
import pandas as pd
import consolidatemixpanel


class TestConsolidateMixpanel(unittest.TestCase):
    def setUp(self):
        # Clean up any previous test files
        if os.path.exists('mixpanel_export.xlsx'):
            os.remove('mixpanel_export.xlsx')
        if os.path.exists('data/data.xlsx'):
            os.remove('data/data.xlsx')
        if os.path.exists('data/backup'):
            shutil.rmtree('data/backup')
        if not os.path.exists('data'):
            os.makedirs('data')

    def tearDown(self):
        if os.path.exists('mixpanel_export.xlsx'):
            os.remove('mixpanel_export.xlsx')
        if os.path.exists('data/data.xlsx'):
            os.remove('data/data.xlsx')
        if os.path.exists('data/backup'):
            shutil.rmtree('data/backup')

    def test_consolidate_with_event(self):
        # Create a sample Excel file mimicking the export with an 'event' column
        df = pd.DataFrame({
            'event': ['click', 'view', 'click'],
            'value': [10, 20, 30],
            'time': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'tripId': [1, 2, 3],  # Adding tripId which is needed for the deduplication
            'model': ['SM-A125F', 'SM-A217F', 'M2101K6G']  # Add model values that match the database
        })
        df.to_excel('mixpanel_export.xlsx', index=False)

        # Run the consolidation which groups by event by counting occurrences
        consolidatemixpanel.consolidate_data()

        # Check that the consolidated file is created
        self.assertTrue(os.path.exists('data/data.xlsx'))

        # Read the consolidated file
        df_out = pd.read_excel('data/data.xlsx')

        # For tripId we expect one row per tripId (3 total)
        self.assertEqual(len(df_out), 3)
        
        # Ensure the time and event columns exist
        self.assertIn('time', df_out.columns)
        self.assertIn('event', df_out.columns)
        
        # Check that all tripIds are present
        trip_ids = df_out['tripId'].tolist()
        self.assertEqual(sorted(trip_ids), [1, 2, 3])


if __name__ == '__main__':
    unittest.main() 