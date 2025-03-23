#!/usr/bin/env python
import argparse
import requests
import pandas as pd
import json
import os
import concurrent.futures

# Configuration
API_SECRET = '725fc2ea9f36a4b3aec9dcbf1b56556d'
EVENT_NAME = "trip_details_route"

def export_data(start_date, end_date, output_file='mixpanel_export.xlsx', event_name=EVENT_NAME):
    """
    Export data from Mixpanel for a specific date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_file: Path to save the exported data
        event_name: Name of the event to export data for
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Check if file already exists (caching)
    if os.path.exists(output_file):
        print(f"Using cached data from {output_file}")
        return True
    
    # Create parent directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # Mixpanel Export API endpoint
    url = "https://data.mixpanel.com/api/2.0/export/"

    # Query parameters for the API request
    params = {
        'from_date': start_date,
        'to_date': end_date,
        'event': f'["{event_name}"]'
    }

    # Headers: specify that we accept JSON
    headers = {
        'Accept': 'application/json'
    }

    try:
        # Execute the GET request with HTTP Basic Authentication
        response = requests.get(url, auth=(API_SECRET, ''), params=params, headers=headers)

        if response.status_code != 200:
            print("Failed to export data:", response.status_code, response.text)
            return False
            
        records = []
        # Process each newline-delimited JSON record
        for line in response.text.strip().splitlines():
            if line:
                try:
                    record = json.loads(line)
                    # Flatten the JSON: take all keys from "properties" and add the "event" key if needed.
                    if 'properties' in record:
                        data = record['properties']
                        # Optionally include the event name
                        data['event'] = record.get('event', None)
                    else:
                        data = record
                    records.append(data)
                except json.JSONDecodeError:
                    print(f"Warning: Skipping invalid JSON line: {line[:100]}...")
        
        # Create a DataFrame with all records
        df = pd.DataFrame(records)
        
        if df.empty:
            print("Warning: No records found for the given date range")
            df = pd.DataFrame([{"warning": "No records found"}])
        
        # Adjust column headers: remove any leading '$' characters
        df.rename(columns=lambda x: str(x).lstrip('$') if isinstance(x, str) else x, inplace=True)
        
        # Convert the 'time' column from Unix timestamp (seconds) to human-readable dates if present
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='s', errors='coerce')
        
        # Convert the 'mp_api_timestamp_ms' column from Unix timestamp in milliseconds to human-readable dates if present
        if 'mp_api_timestamp_ms' in df.columns:
            df['mp_api_timestamp_ms'] = pd.to_datetime(df['mp_api_timestamp_ms'], unit='ms', errors='coerce')
        
        # Save the entire DataFrame to an Excel file
        df.to_excel(output_file, index=False)
        print(f"Data successfully saved to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error exporting data: {e}")
        return False 

def export_data_for_comparison(base_start_date, base_end_date, comp_start_date, comp_end_date, event_name=EVENT_NAME):
    """
    Export data from Mixpanel for two date ranges for comparison purposes.
    Creates a directory to store comparison data if it doesn't exist.
    
    Args:
        base_start_date: Base period start date in YYYY-MM-DD format
        base_end_date: Base period end date in YYYY-MM-DD format
        comp_start_date: Comparison period start date in YYYY-MM-DD format
        comp_end_date: Comparison period end date in YYYY-MM-DD format
        event_name: Name of the event to export data for
        
    Returns:
        tuple: (base_file_path, comparison_file_path) if successful, (None, None) otherwise
    """
    # Create comparison directory if it doesn't exist
    comparison_dir = os.path.join("data", "comparison")
    os.makedirs(comparison_dir, exist_ok=True)
    
    # Define file paths
    base_file = os.path.join(comparison_dir, f"base_{base_start_date}_to_{base_end_date}.xlsx")
    comp_file = os.path.join(comparison_dir, f"comparison_{comp_start_date}_to_{comp_end_date}.xlsx")
    
    # Check if both files exist already (caching)
    base_exists = os.path.exists(base_file)
    comp_exists = os.path.exists(comp_file)
    
    # If both files exist, return them immediately
    if base_exists and comp_exists:
        print(f"Using cached comparison data for {base_start_date}-{base_end_date} and {comp_start_date}-{comp_end_date}")
        return base_file, comp_file
    
    # Use ThreadPoolExecutor to run only the needed exports in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        
        # Only export if file doesn't exist
        if not base_exists:
            base_future = executor.submit(export_data, base_start_date, base_end_date, base_file, event_name)
            futures.append(("base", base_future))
        
        if not comp_exists:
            comp_future = executor.submit(export_data, comp_start_date, comp_end_date, comp_file, event_name)
            futures.append(("comp", comp_future))
        
        # Check results of any running futures
        for name, future in futures:
            success = future.result()
            if not success:
                print(f"Error exporting {name} data")
                return None, None
    
    # Verify both files exist
    if os.path.exists(base_file) and os.path.exists(comp_file):
        return base_file, comp_file
    else:
        print("Error exporting comparison data: Files not found")
        return None, None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export mixpanel data for a specified date range")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    export_data(args.start_date, args.end_date)
