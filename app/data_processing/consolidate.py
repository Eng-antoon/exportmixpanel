import pandas as pd
import os
import sys
from app.data_processing.mobile_specs import merge_with_mobile_specs

def consolidate_data(input_file='mixpanel_export.xlsx', output_dir='data', output_file='data.xlsx'):
    """
    Consolidate data from Mixpanel export and combine with mobile specs.
    
    Args:
        input_file: Path to the input Excel file
        output_dir: Directory to store the output file
        output_file: Name of the output file (will be placed in output_dir)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        full_output_path = os.path.join(output_dir, output_file)
        
        # Read input data
        df = pd.read_excel(input_file)
        
        # For this consolidation, we take the exported data and keep only one record per tripId (most recent time)
        df_consolidated = df.sort_values('time', ascending=False).drop_duplicates(subset=['tripId'], keep='first')
        
        # Merge with mobile specs data
        merged_df = merge_with_mobile_specs(df_consolidated)
        
        # Desired column order
        desired_columns = [
            'tripId',
            'time',
            'app_build_number',
            'app_version',
            'brand',
            'carrier',
            'city',
            'has_nfc',
            'lib_version',
            'manufacturer',
            'model',
            'Device Name',
            'Release Year',
            'Android Version',
            'Fingerprint Sensor',
            'Accelerometer',
            'Gyro',
            'Proximity Sensor',
            'Compass',
            'Barometer',
            'Background Task Killing Tendency',
            'Chipset',
            'RAM',
            'Storage',
            'Battery (mAh)',
            'os',
            'os_version',
            'region',
            'user_id',
            'wifi',
            'PhoneNumber',
            'UserId',
            'UserName',
            'event'
        ]
        
        # Filter to only the columns that exist in the merged DataFrame
        final_columns = [col for col in desired_columns if col in merged_df.columns]
        merged_df_final = merged_df[final_columns]
        
        # Save the final data
        merged_df_final.to_excel(full_output_path, index=False)
        print(f"Consolidated file saved as '{full_output_path}'")
        
        return True
        
    except Exception as e:
        print("Failed to consolidate data:", str(e))
        return False 