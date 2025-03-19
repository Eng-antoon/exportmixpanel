#!/usr/bin/env python
import argparse
from app.data_processing.export import export_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export mixpanel data for a specified date range")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    export_data(args.start_date, args.end_date)
