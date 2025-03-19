import requests
import pandas as pd
import json

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
                record = json.loads(line)
                # Flatten the JSON: take all keys from "properties" and add the "event" key if needed.
                if 'properties' in record:
                    data = record['properties']
                    # Optionally include the event name
                    data['event'] = record.get('event', None)
                else:
                    data = record
                records.append(data)
        
        # Create a DataFrame with all records
        df = pd.DataFrame(records)
        
        # Adjust column headers: remove any leading '$' characters
        df.rename(columns=lambda x: x.lstrip('$'), inplace=True)
        
        # Convert the 'time' column from Unix timestamp (seconds) to human-readable dates if present
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='s')
        
        # Convert the 'mp_api_timestamp_ms' column from Unix timestamp in milliseconds to human-readable dates if present
        if 'mp_api_timestamp_ms' in df.columns:
            df['mp_api_timestamp_ms'] = pd.to_datetime(df['mp_api_timestamp_ms'], unit='ms')
        
        # Save the entire DataFrame to an Excel file
        df.to_excel(output_file, index=False)
        print(f"Data successfully saved to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error exporting data: {e}")
        return False 