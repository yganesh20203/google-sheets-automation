# Order_update_cat.py - Generates Daily Pivot Reports

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
INPUT_OUTPUT_FOLDER_ID = '1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da' # Re-use your original folder ID

# Paste the full URL of your target Google Sheet.
GSHEET_URL = 'https://docs.google.com/spreadsheets/d/1xM7KlPutdAvF_UttWkDBtzfhjBZVw6cWR3B2YE-PjNU/edit?gid=0#gid=0'

# --- End of User Configuration ---

# Define the scopes for the APIs (permissions).
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Helper Functions for Google Drive & Sheets (Unchanged) ---

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
            print(f"Worksheet '{sheet_name}' not found. Creating it...")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
        
        # Clear a reasonable range. Adjust 'Z' if your pivot is wider.
        print(f"Clearing worksheet '{sheet_name}'...")
        worksheet.batch_clear(['A:Z']) 
        print(f"Updating worksheet '{sheet_name}' with new data...")
        worksheet.update(export_data, 'A1', value_input_option='USER_ENTERED')
        print(f"✅ Successfully exported to worksheet: '{sheet_name}'")
    except Exception as e:
        print(f"\n❌ An error occurred during the export to '{sheet_name}': {e}")


# --- NEW MODULAR FUNCTION ---

# *** Function definition updated to accept drive_service ***
def process_and_upload_pivot_report(df_original, cat_df, sheets_service, drive_service, date_column_name, target_sheet_name, local_data_path):
    """
    Generates a pivot report based on a dynamic date column and uploads it to a specific sheet.
    """
    print("=" * 40)
    print(f"Processing Report for: {target_sheet_name} (using {date_column_name})")
    print("=" * 40)
    
    # --- 3. Loading and Processing Data ---
    # Create a copy to avoid modifying the original dataframe
    df = df_original.copy()
    
    # 1. Merge the dataframes to get the grouping
    print("Merging dataframes and adding 'grouping' column...")
    df = pd.merge(
        df, 
        cat_df[['Subcategory Description', 'Grouping']], 
        on='Subcategory Description', 
        how='left'
    )
    
    # 2. Create the 'grouping' column, filling NaNs with 'Miscellaneous'
    df['grouping'] = df['Grouping'].fillna('Miscellaneous')
    df.drop(columns=['Grouping'], inplace=True) # Drop the original column

    # 3. Convert the specified date column to a datetime object
    # This logic handles "10/28/2025 17:36" or "11/5/2025 10:30"
    print(f"Parsing date column: {date_column_name}")
    df['int_date_dt'] = pd.to_datetime(df[date_column_name].astype(str).str.split(' ').str[0], errors='coerce')

    # 4. Ensure 'Item Gross Weight' is numeric for sum()
    df['Item Gross Weight'] = pd.to_numeric(df['Item Gross Weight'], errors='coerce')
    
    # 5. Define date range for the 5-day window ending yesterday
    today = pd.to_datetime('today').normalize()
    yesterday = today - pd.Timedelta(days=1)
    start_date = yesterday - pd.Timedelta(days=4)
    
    print(f"Filtering for 5-day matured window: {start_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')}")
    
    # 6. Filter the DataFrame for this 5-day window
    df_filtered = df[(df['int_date_dt'] >= start_date) & (df['int_date_dt'] <= yesterday)].copy()
    
    # 7. Create the final string date column, now named 'report_date'
    df_filtered['report_date'] = df_filtered['int_date_dt'].dt.strftime('%m/%d/%Y')
    
    # 8. Create the pivot table
    if df_filtered.empty:
        print(f"❌ No data found for {target_sheet_name} in the 5-day window.")
        pivot_report_df = pd.DataFrame() # No new data
    else:
        print("Creating pivot table...")
        pivot_report_df = df_filtered.pivot_table(
            index=['report_date', 'Store Code1', 'Mode of Fullfillment'], # Rows
            columns=['grouping'],                                         # Columns
            values='Item Gross Weight',                                   # Values
            aggfunc='sum',                                                # Aggregation
            fill_value=0                                                  # Fill missing with 0
        )
    
    print("✅ Pivot table created successfully.")

    # --- 4. Saving & Uploading Output File ---
    print("--- Saving report locally before uploading to Drive ---")
    
    # Create a dynamic filename
    pivot_output_path = os.path.join(local_data_path, f'{target_sheet_name}_pivot_report.csv')

    # Save file locally (reset_index so all index levels are columns)
    pivot_report_df.reset_index().to_csv(pivot_output_path, index=False)
    
    # *** Call updated to use the drive_service variable ***
    upload_file_to_drive(drive_service, pivot_output_path, INPUT_OUTPUT_FOLDER_ID)

    # --- 5. Exporting Report to Google Sheets (with maturation logic) ---
    print(f"--- Reading, Combining, and Exporting Report to {target_sheet_name} ---")
    
    # Prepare the new data
    new_data_df = pivot_report_df.reset_index()

    try:
        spreadsheet = sheets_service.open_by_url(GSHEET_URL)
        
        # Try to read existing data from the target sheet
        try:
            worksheet = spreadsheet.worksheet(target_sheet_name)
            print(f"Reading existing data from '{target_sheet_name}'...")
            existing_data = worksheet.get_all_records()
            existing_df = pd.DataFrame(existing_data)
        except gspread.WorksheetNotFound:
            print(f"'{target_sheet_name}' not found, will create it and paste new data.")
            existing_df = pd.DataFrame()
        except Exception as e:
            print(f"Warning: Could not read existing data from {target_sheet_name}. Will overwrite. Error: {e}")
            existing_df = pd.DataFrame()

        if existing_df.empty:
            print("No existing data found. Pasting newly generated report.")
            final_df_to_export = new_data_df
        else:
            print("Combining old and new data...")
            try:
                # IMPORTANT: Use the generic 'report_date' column for comparison
                existing_df['int_date_dt'] = pd.to_datetime(existing_df['report_date'], format='%m/%d/%Y', errors='coerce')
                
                # `start_date` is the first day of the new report
                # We want to keep all data *before* this date
                old_data_to_keep = existing_df[existing_df['int_date_dt'] < start_date].copy()
                
                # Concatenate the old, untouched data with the new, refreshed data
                final_df_to_export = pd.concat([old_data_to_keep, new_data_df], ignore_index=True)
                
                # Drop the temporary datetime column if it exists
                if 'int_date_dt' in final_df_to_export.columns:
                    final_df_to_export = final_df_to_export.drop(columns=['int_date_dt'])
                
                print(f"Successfully combined {len(old_data_to_keep)} old rows with {len(new_data_df)} new rows for {target_sheet_name}.")
            
            except Exception as e:
                print(f"Error combining data for {target_sheet_name}: {e}. Will just export the new 5-day report.")
                final_df_to_export = new_data_df

        # Export the final combined dataframe
        export_df_to_gsheet(spreadsheet, final_df_to_export, target_sheet_name)
    
    except Exception as e:
        print(f"\n❌ An error occurred during the Google Sheets export process for {target_sheet_name}: {e}")

    print(f"--- Finished processing for {target_sheet_name} ---")


def process_and_upload_sheet3_reports(df_original, sheets_service, drive_service, local_data_path):
    """
    Generates distinct order count reports for Order Date and LR Date
    and uploads them to 'Sheet3' with maturation logic.
    """
    print("=" * 40)
    print("Processing Report for: Sheet3 (Distinct Order Counts)")
    print("=" * 40)

    # --- Define Date Range ---
    today = pd.to_datetime('today').normalize()
    yesterday = today - pd.Timedelta(days=1)
    start_date = yesterday - pd.Timedelta(days=4)
    print(f"Filtering for 5-day matured window: {start_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')}")

    # --- 1. Process Order Date Report ---
    df_ord = df_original.copy()
    df_ord['int_date_dt'] = pd.to_datetime(df_ord['Order Date IST'].astype(str).str.split(' ').str[0], errors='coerce')
    df_ord_filtered = df_ord[(df_ord['int_date_dt'] >= start_date) & (df_ord['int_date_dt'] <= yesterday)].copy()
    
    if not df_ord_filtered.empty:
        df_ord_filtered['report_date'] = df_ord_filtered['int_date_dt'].dt.strftime('%m/%d/%Y')
        order_counts_df = df_ord_filtered.groupby(['report_date', 'Store Code1'])['Hybris Order Number'].nunique().reset_index()
        order_counts_df.rename(columns={'Hybris Order Number': 'Distinct_Order_Count'}, inplace=True)
        order_counts_df['report_type'] = 'Order Date' # Add type identifier
        print("✅ Created Order Date distinct counts.")
    else:
        print("ℹ️ No Order Date data found for the 5-day window.")
        order_counts_df = pd.DataFrame()

    # --- 2. Process LR Date Report ---
    df_lr = df_original.copy()
    df_lr['int_date_dt'] = pd.to_datetime(df_lr['LR Date Time'].astype(str).str.split(' ').str[0], errors='coerce')
    df_lr_filtered = df_lr[(df_lr['int_date_dt'] >= start_date) & (df_lr['int_date_dt'] <= yesterday)].copy()

    if not df_lr_filtered.empty:
        df_lr_filtered['report_date'] = df_lr_filtered['int_date_dt'].dt.strftime('%m/%d/%Y')
        lr_counts_df = df_lr_filtered.groupby(['report_date', 'Store Code1'])['Hybris Order Number'].nunique().reset_index()
        lr_counts_df.rename(columns={'Hybris Order Number': 'Distinct_Order_Count'}, inplace=True)
        lr_counts_df['report_type'] = 'LR Date' # Add type identifier
        print("✅ Created LR Date distinct counts.")
    else:
        print("ℹ️ No LR Date data found for the 5-day window.")
        lr_counts_df = pd.DataFrame()

    # --- 3. Combine and Save New Data ---
    new_data_df = pd.concat([order_counts_df, lr_counts_df], ignore_index=True)
    
    if new_data_df.empty:
        print("❌ No new data generated for Sheet3. Skipping upload.")
        return

    # Save a local copy
    pivot_output_path = os.path.join(local_data_path, 'Sheet3_distinct_counts_report.csv')
    new_data_df.to_csv(pivot_output_path, index=False)
    upload_file_to_drive(drive_service, pivot_output_path, INPUT_OUTPUT_FOLDER_ID)

    # --- 4. Export to Google Sheets (with maturation logic) ---
    print("--- Reading, Combining, and Exporting Report to Sheet3 ---")
    target_sheet_name = 'Sheet3'
    try:
        spreadsheet = sheets_service.open_by_url(GSHEET_URL)
        
        try:
            worksheet = spreadsheet.worksheet(target_sheet_name)
            print(f"Reading existing data from '{target_sheet_name}'...")
            existing_data = worksheet.get_all_records()
            existing_df = pd.DataFrame(existing_data)
        except gspread.WorksheetNotFound:
            print(f"'{target_sheet_name}' not found, will create it and paste new data.")
            existing_df = pd.DataFrame()
        except Exception as e:
            print(f"Warning: Could not read existing data from {target_sheet_name}. Will overwrite. Error: {e}")
            existing_df = pd.DataFrame()

        if existing_df.empty:
            print("No existing data found. Pasting newly generated report.")
            final_df_to_export = new_data_df
        else:
            print("Combining old and new data...")
            try:
                # Use 'report_date' for comparison
                existing_df['int_date_dt'] = pd.to_datetime(existing_df['report_date'], format='%m/%d/%Y', errors='coerce')
                
                # Keep all data *before* this new 5-day window
                old_data_to_keep = existing_df[existing_df['int_date_dt'] < start_date].copy()
                
                # Concatenate the old, untouched data with the new, refreshed data
                final_df_to_export = pd.concat([old_data_to_keep, new_data_df], ignore_index=True)
                
                if 'int_date_dt' in final_df_to_export.columns:
                    final_df_to_export = final_df_to_export.drop(columns=['int_date_dt'])
                
                print(f"Successfully combined {len(old_data_to_keep)} old rows with {len(new_data_df)} new rows for {target_sheet_name}.")
            
            except Exception as e:
                print(f"Error combining data for {target_sheet_name}: {e}. Will just export the new 5-day report.")
                final_df_to_export = new_data_df

        # Export the final combined dataframe
        export_df_to_gsheet(spreadsheet, final_df_to_export, target_sheet_name)
    
    except Exception as e:
        print(f"\n❌ An error occurred during the Google Sheets export process for {target_sheet_name}: {e}")

    print(f"--- Finished processing for {target_sheet_name} ---")


# --- MAIN EXECUTION ---

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
    
    # *** This failing line has been removed ***
    # sheets_service.auth.service = drive_service
    
    print("✅ Authentication successful.")
    print("-" * 30)

    print("--- 2. Finding & Downloading Input Files ---")
    local_data_path = 'data'
    os.makedirs(local_data_path, exist_ok=True)

    # Define all input filenames
    input_filenames = {
        'capacity': 'Capacity_dump.csv',
        'grouping': 'cat_grouping.csv'
    }
    local_file_paths = {}

    # Download all files
    for key, filename in input_filenames.items():
        file_id = get_file_id_by_name(drive_service, filename, INPUT_OUTPUT_FOLDER_ID)
        if not file_id:
            raise FileNotFoundError(f"'{filename}' could not be found in the specified Drive folder.")
        
        local_file_paths[key] = os.path.join(local_data_path, filename)
        download_file_from_drive(drive_service, file_id, local_file_paths[key])
    
    print("-" * 30)

    print("--- 3. Loading Main DataFrames ---")
    df_main = pd.read_csv(local_file_paths['capacity'], low_memory=False)
    cat_df_main = pd.read_csv(local_file_paths['grouping'])
    print("✅ All input files loaded into DataFrames.")
    print("-" * 30)
    
    # --- 4. Process and Upload Reports ---
    
    # Call 1: Process the Order Date report for Sheet1
    process_and_upload_pivot_report(
        df_original=df_main, 
        cat_df=cat_df_main, 
        sheets_service=sheets_service, 
        drive_service=drive_service,
        date_column_name="Order Date IST", 
        target_sheet_name="Sheet1",
        local_data_path=local_data_path
    )
    
    # Call 2: Process the LR Date report for Sheet2
    process_and_upload_pivot_report(
        df_original=df_main, 
        cat_df=cat_df_main, 
        sheets_service=sheets_service, 
        drive_service=drive_service,
        date_column_name="LR Date Time", 
        target_sheet_name="Sheet2",
        local_data_path=local_data_path
    )
    
    # *** THIS IS THE CORRECT LOCATION FOR THE NEW CALL ***
    # Call 3: Process the Distinct Counts report for Sheet3
    process_and_upload_sheet3_reports(
        df_original=df_main,
        sheets_service=sheets_service,
        drive_service=drive_service,
        local_data_path=local_data_path
    )
    
    print("=" * 40)
    print("--- All Reports Finished ---")


if __name__ == "__main__":
    main()

# *** THE CODE BLOCK THAT WAS HERE IS NOW MOVED UP INSIDE main() ***
