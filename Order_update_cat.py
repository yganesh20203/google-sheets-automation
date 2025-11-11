# Order_update_cat.py - Generates Daily Pivot Report

import os
import json
import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# --- 1. USER CONFIGURATION: You must edit these values ---

# Paste the ID for the folder where ALL your input and output files are located.
# This is the most important setting.
INPUT_OUTPUT_FOLDER_ID = '1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da' # Re-use your original folder ID

# Paste the full URL of your target Google Sheet.
GSHEET_URL = 'https://docs.google.com/spreadsheets/d/1xM7KlPutdAvF_UttWkDBtzfhjBZVw6cWR3B2YE-PjNU/edit?gid=0#gid=0'

# --- End of User Configuration ---

# Define the scopes for the APIs (permissions).
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Helper Functions for Google Drive & Sheets ---

def get_file_id_by_name(service, file_name, folder_id):
    """Finds a file's ID by its name within a specific folder."""
    print(f"Searching for file '{file_name}'...", end='', flush=True)
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    if not files:
        print(" ❌ Not found.")
        return None
    else:
        file_id = files[0].get('id')
        print(f" ✅ Found")
        return file_id

def download_file_from_drive(service, file_id, local_filename):
    """Downloads a file from Google Drive."""
    print(f"Downloading '{os.path.basename(local_filename)}'...", end='', flush=True)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    with open(local_filename, 'wb') as f:
        f.write(fh.read())
    print(" ✅")

def upload_file_to_drive(service, local_path, folder_id):
    """Uploads a file to a specific Google Drive folder, overwriting if it exists."""
    if not os.path.exists(local_path):
        print(f"ℹ️ Skipped uploading '{os.path.basename(local_path)}' as it was not generated.")
        return

    file_metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    print(f"Uploading '{os.path.basename(local_path)}' to Drive...", end='', flush=True)

    # Check if file already exists to overwrite it.
    existing_file_id = get_file_id_by_name(service, os.path.basename(local_path), folder_id)
    if existing_file_id:
        service.files().update(fileId=existing_file_id, media_body=media).execute()
    else:
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(" ✅")

def export_df_to_gsheet(spreadsheet, df_to_export, sheet_name):
    """Exports a Pandas DataFrame to a specific worksheet in a Google Sheet."""
    if df_to_export is None:
        print(f"ℹ️ Skipped exporting '{sheet_name}' as there was no data.")
        return
    try:
        # Reset index if it's not a simple range, (e.g., for pivot tables)
        if not isinstance(df_to_export.index, pd.RangeIndex):
            df_to_export = df_to_export.reset_index()

        export_data = [df_to_export.columns.values.tolist()] + df_to_export.values.tolist()

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
        
        # Clear a reasonable range. Adjust 'Z' if your pivot is wider.
        worksheet.batch_clear(['A:Z']) 
        worksheet.update(export_data, 'A1', value_input_option='USER_ENTERED')
        print(f"✅ Successfully exported to worksheet: '{sheet_name}'")
    except Exception as e:
        print(f"\n❌ An error occurred during the export to '{sheet_name}': {e}")


def main():
    """Main function to run the entire automation process."""
    print("--- 1. Authenticating ---")
    creds_json_str = os.getenv("GCP_SA_KEY")
    if not creds_json_str:
        raise ValueError("GCP_SA_KEY secret not found in environment.")

    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = gspread.authorize(creds)
    print("✅ Authentication successful.")
    print("-" * 30)

    print("--- 2. Finding & Downloading Input File ---")
    local_data_path = 'data'
    os.makedirs(local_data_path, exist_ok=True)

    # We only need 'Capacity_dump.csv' for this specific task
    input_filename = 'Capacity_dump.csv'
    
    file_id = get_file_id_by_name(drive_service, input_filename, INPUT_OUTPUT_FOLDER_ID)
    if not file_id:
        raise FileNotFoundError(f"'{input_filename}' could not be found in the specified Drive folder.")
    
    local_file_path = os.path.join(local_data_path, input_filename)
    download_file_from_drive(drive_service, file_id, local_file_path)
    print("-" * 30)

    print("--- 3. Loading and Processing Data ---")
    df = pd.read_csv(local_file_path, low_memory=False)
    print("✅ Input file loaded into DataFrame.")

    # --- NEW DATA PROCESSING STARTS HERE ---
    
    print("Processing new pivot report...")
    
    # 1. Add new column 'int_order_date' from 'Order Date IST'
    # This splits '10/28/2025 17:36' into '10/28/2025'
    df['int_order_date'] = pd.to_datetime(df['Order Date IST'].astype(str).str.split(' ').str[0], errors='coerce').dt.strftime('%m/%d/%Y')

    # 2. Ensure 'Item Gross Weight' is numeric for sum()
    df['Item Gross Weight'] = pd.to_numeric(df['Item Gross Weight'], errors='coerce')
    
    # 3. Create the pivot table
    print("Creating pivot table...")
    pivot_report_df = df.pivot_table(
        index=['int_order_date', 'Store Code1'], # Rows in tabular form
        values=['Hybris Order Number', 'Item Gross Weight'],
        aggfunc={
            'Hybris Order Number': 'nunique', # Distinct count of Hybris Order Number
            'Item Gross Weight': 'sum'        # Sum of Item Gross Weight
        },
        fill_value=0 # Fill missing values with 0
    )
    
    # Rename columns for clarity in the final report
    pivot_report_df.rename(columns={
        'Hybris Order Number': 'Distinct_Order_Count',
        'Item Gross Weight': 'Total_Item_Gross_Weight'
    }, inplace=True)

    print("✅ Pivot table created successfully.")
    print("-" * tran30)
    
    # --- END OF NEW DATA PROCESSING ---

    # --- 4. Saving & Uploading Output File ---
    print("--- Saving report locally before uploading to Drive ---")

    # Define local output file path
    pivot_output_path = os.path.join(local_data_path, 'daily_order_weight_pivot_report.csv')

    # Save file locally (reset_index so 'int_order_date' and 'Store Code1' are columns)
    pivot_report_df.reset_index().to_csv(pivot_output_path, index=False)
    
    # Upload to Google Drive
    upload_file_to_drive(drive_service, pivot_output_path, INPUT_OUTPUT_FOLDER_ID)
    print("-" * 30)

    # --- 5. Exporting Report to Google Sheets ---
    print("--- Exporting report to Google Sheets ---")
    try:
        spreadsheet = sheets_service.open_by_url(GSHEET_URL)
        # The helper function will reset the index for proper GSheet export
        export_df_to_gsheet(spreadsheet, pivot_report_df, 'Sheet1') 
    except Exception as e:
        print(f"\n❌ An error occurred during the Google Sheets export process: {e}")
    print("-" * 30)

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
