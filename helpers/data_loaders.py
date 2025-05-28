import os
import openpyxl
import pandas as pd

def load_excel_data(excel_path):
    if not os.path.exists(excel_path):
        print(f"Excel file not found: {excel_path}. Returning empty data.")
        return []
    try:
        workbook = openpyxl.load_workbook(excel_path)
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return []
    
    sheet = workbook.active
    headers = []
    data = []
    for i, row in enumerate(sheet.iter_rows(values_only=True)):
        if i == 0:
            headers = row
        else:
            # Ensure all row cells are processed, even if shorter than headers
            row_dict = {headers[j]: row[j] for j in range(min(len(headers), len(row)))}
            data.append(row_dict)
    print(f"Loaded {len(data)} rows from Excel: {excel_path}") # Added path to print
    return data

def load_mixpanel_data():
    """
    Load Mixpanel data from Excel file.
    Returns a DataFrame with the data or None if an error occurs.
    """
    try:
        # Standardize mixpanel file name/path
        mixpanel_path = os.path.join("data", "mixpanel_export.xlsx") # Assuming it's in data like other xlsx
        if os.path.exists(mixpanel_path):
            print(f"Loading Mixpanel data from {mixpanel_path}...")
            df_mixpanel = pd.read_excel(mixpanel_path)
            print(f"Successfully loaded Mixpanel data with {len(df_mixpanel)} rows")
            return df_mixpanel
        else:
            # Fallback to root directory if not in data/ (based on original code in automatic_insights)
            mixpanel_path_root = "mixpanel_export.xlsx"
            if os.path.exists(mixpanel_path_root):
                 print(f"Loading Mixpanel data from {mixpanel_path_root}...")
                 df_mixpanel = pd.read_excel(mixpanel_path_root)
                 print(f"Successfully loaded Mixpanel data with {len(df_mixpanel)} rows")
                 return df_mixpanel
            else:
                print(f"Mixpanel data file not found in data/ or root directory: {mixpanel_path} or {mixpanel_path_root}")
                return None
    except Exception as e:
        print(f"Error loading Mixpanel data: {str(e)}")
        return None
