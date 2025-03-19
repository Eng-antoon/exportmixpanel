#!/usr/bin/env python
import argparse
from app.data_processing.consolidate import consolidate_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolidate Mixpanel export data and combine with mobile specs")
    parser.add_argument("--input-file", default="mixpanel_export.xlsx", help="Input excel file path")
    parser.add_argument("--output-dir", default="data", help="Output directory")
    parser.add_argument("--output-file", default="data.xlsx", help="Output excel filename")
    args = parser.parse_args()
    
    success = consolidate_data(args.input_file, args.output_dir, args.output_file)
    if success:
        print(f"Successfully consolidated data to {args.output_dir}/{args.output_file}")
    else:
        print("Failed to consolidate data")
