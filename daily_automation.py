# Make sure to install the required libraries:
# pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib pandas openpyxl

import datetime
import zipfile
import io
import os
from io import BytesIO
import pandas as pd
import numpy as np
import openpyxl  # Added for .xlsm support
from openpyxl.utils.dataframe import dataframe_to_rows  # Added for .xlsm support
import gc  # <--- ADDED: Import garbage collector

# Google API Libraries for Service Account
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import google.auth  # Import the google.auth library

# --- NEW: Logging Function ---
def log(message):
    """Prints a message with a timestamp."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

# --- Authentication ---

# --- MODIFICATION FOR GITHUB ACTIONS ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
# --- END MODIFICATION FOR GITHUB ACTIONS ---

SCOPES = [
    'https://www.googleapis.com/auth/drive'
]

def authenticate():
    """Authenticates using the service account file."""
    log("Attempting Google Drive authentication...")
    try:
        creds, _ = google.auth.default(scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        log("✅ Google Drive authentication successful (Service Account).")
        return drive_service
    except Exception as e:
        log(f"❌ ERROR: Authentication failed. Details: {e}")
        return None

# --- Configuration ---
SOURCE_FOLDER_ID = '1sern1xXqdDrQQBLXxbANj7LPs3IE1Dzo'
TARGET_FOLDER_ID = '1HTkBss1orVVn1akNygxiuklaAHQnJy8w'
FILE_PREFIXES = ['ArticleSalesReport', 'Overall_Instock']
# ---------------------

# --- Helper Function to Download Helper Files ---

def download_csv_to_df(drive_service, file_name, folder_id):
    """Finds a CSV by name in a folder and loads it into a pandas DataFrame."""
    log(f"  Downloading helper file: {file_name}...")
    try:
        query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        
        if not items:
            log(f"  [ERROR] Helper file '{file_name}' not found in folder.")
            return None
            
        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        file_content_buffer = BytesIO()
        downloader = MediaIoBaseDownload(file_content_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_content_buffer.seek(0)
        # Try to read with utf-8, fall back to latin1 if it fails
        try:
            df = pd.read_csv(file_content_buffer)
        except UnicodeDecodeError:
            file_content_buffer.seek(0)
            df = pd.read_csv(file_content_buffer, encoding='latin1')
            log(f"  [INFO] Read '{file_name}' with 'latin1' encoding.")

        log(f"  ✅ Successfully loaded '{file_name}' into DataFrame.")
        return df
        
    except Exception as e:
        log(f"  [ERROR] Failed to download or read helper file '{file_name}'. Details: {e}")
        return None

# --- Helper Function to load main files (zip or csv) ---

def load_file_to_df(drive_service, file_id, file_name):
    """
    Downloads a file by ID (zip or csv) and loads the first CSV found
    into a pandas DataFrame.
    """
    log(f"  Loading main file: {file_name} (ID: {file_id})...")
    try:
        # Download the file content
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        file_content_buffer = BytesIO()
        downloader = MediaIoBaseDownload(file_content_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        log(f"  Download complete: {file_name}")
        file_content_buffer.seek(0)

        csv_data_bytes = None

        if file_name.endswith('.zip'):
            log(f"    > Unzipping {file_name}...")
            with zipfile.ZipFile(file_content_buffer, 'r') as zf:
                csv_files_in_zip = [name for name in zf.namelist() if name.endswith('.csv') and not name.startswith('__MACOSX')]
                if not csv_files_in_zip:
                    log(f"  [ERROR] No CSV file found inside {file_name}.")
                    return None
                csv_name_in_zip = csv_files_in_zip[0]
                log(f"    > Found '{csv_name_in_zip}' in zip. Reading bytes.")
                csv_data_bytes = zf.read(csv_name_in_zip)
        
        elif file_name.endswith('.csv'):
            log("    > File is a CSV. Reading bytes.")
            csv_data_bytes = file_content_buffer.read()
        
        else:
            log(f"  [ERROR] File {file_name} is not a zip or csv.")
            return None

        # Now, load the bytes into a DataFrame
        if csv_data_bytes:
            log("    > Loading bytes into pandas DataFrame...")
            try:
                df = pd.read_csv(BytesIO(csv_data_bytes))
            except UnicodeDecodeError:
                log("    > Retrying with 'latin1' encoding.")
                df = pd.read_csv(BytesIO(csv_data_bytes), encoding='latin1')
            log(f"  ✅ Successfully loaded '{file_name}' into DataFrame.")
            return df
        else:
            return None
            
    except Exception as e:
        log(f"  [ERROR] Failed to load file {file_name} to DataFrame. Details: {e}")
        return None

# --- Helper Function to upload a DataFrame as a CSV ---

def upload_df_as_csv(drive_service, df, file_name, folder_id):
    """Uploads a pandas DataFrame as a CSV file to Google Drive, overwriting if exists."""
    log(f"  Uploading processed file: {file_name}...")
    if df is None:
        log(f"  [ERROR] DataFrame for {file_name} is empty. Skipping upload.")
        return

    try:
        # Convert DataFrame to CSV in memory
        output_buffer = io.StringIO()
        df.to_csv(output_buffer, index=False)
        output_buffer.seek(0)
        csv_data_bytes = output_buffer.getvalue().encode('utf-8')
        csv_buffer_for_upload = BytesIO(csv_data_bytes)

        # Check if file already exists to overwrite it
        query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        existing_files = results.get('files', [])
        
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media_body = MediaIoBaseUpload(csv_buffer_for_upload, mimetype='text/csv', resumable=True)
        
        if existing_files:
            file_id = existing_files[0]['id']
            log(f"    > Overwriting existing file: {file_name} (ID: {file_id})")
            drive_service.files().update(
                fileId=file_id,
                media_body=media_body,
                supportsAllDrives=True
            ).execute()
        else:
            log(f"    > Creating new file: {file_name}")
            drive_service.files().create(
                body=file_metadata,
                media_body=media_body,
                fields='id',
                supportsAllDrives=True
            ).execute()
            
        log(f"  ✅ Successfully uploaded '{file_name}'.")

    except Exception as e:
        log(f"  [ERROR] Failed to upload DataFrame {file_name}. Details: {e}")


# --- UPDATED: Helper Function to update an existing .xlsm file ---

def update_excel_file(drive_service, df_to_paste, file_name_to_find, sheet_name_to_update, folder_id):
    """
    Finds an .xlsm file in Drive, downloads it, clears a sheet,
    pastes a DataFrame, and re-uploads (updates) the file.
    """
    log(f"\n--- Updating Excel File: {file_name_to_find} ---")
    if df_to_paste is None:
        log(f"  [ERROR] DataFrame is empty. Skipping Excel update.")
        return

    try:
        # 1. Find the .xlsm file
        log(f"  Searching for '{file_name_to_find}' in folder ID: {folder_id}...")
        query = f"'{folder_id}' in parents and name='{file_name_to_find}' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        existing_files = results.get('files', [])
        
        if not existing_files:
            log(f"  [ERROR] Excel file '{file_name_to_find}' not found in target folder.")
            return

        file_id = existing_files[0]['id']
        file_name = existing_files[0]['name']
        log(f"  Found file: {file_name} (ID: {file_id})")

        # 2. Download the file content
        log(f"  Downloading '{file_name}'...")
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        excel_file_buffer = BytesIO()
        downloader = MediaIoBaseDownload(excel_file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        excel_file_buffer.seek(0)
        log("  Download complete.")

        # 3. Load workbook, modify sheet, and save
        log(f"  Opening workbook (keep_vba=True)...")
        # keep_vba=True is ESSENTIAL for .xlsm files
        wb = openpyxl.load_workbook(excel_file_buffer, keep_vba=True)
        
        if sheet_name_to_update not in wb.sheetnames:
            log(f"  [ERROR] Sheet '{sheet_name_to_update}' not found in workbook. Available sheets: {wb.sheetnames}")
            wb.close() # Close workbook on error
            return

        # --- FASTER METHOD: Delete and recreate sheet ---
        log(f"  Removing old sheet: '{sheet_name_to_update}'...")
        sheet_index = wb.sheetnames.index(sheet_name_to_update) # Get index
        ws = wb[sheet_name_to_update]
        wb.remove(ws)
        
        log(f"  Creating new blank sheet: '{sheet_name_to_update}' at index {sheet_index}...")
        ws = wb.create_sheet(title=sheet_name_to_update, index=sheet_index)
        # --- END FASTER METHOD ---

        # Paste the DataFrame (header + data)
        log(f"  Pasting {len(df_to_paste)} rows into sheet...")
        for r in dataframe_to_rows(df_to_paste, index=False, header=True):
            ws.append(r)
        
        # Save the modified workbook to a new in-memory buffer
        log(f"  Saving changes to memory...")
        output_excel_buffer = BytesIO()
        wb.save(output_excel_buffer)
        output_excel_buffer.seek(0)
        wb.close()
        log("  Workbook saved to memory.")

        # 4. Re-upload (update) the file in Google Drive
        log(f"  Uploading updated file back to Drive...")
        # This is the correct MIME type for .xlsm
        media_body = MediaIoBaseUpload(
            output_excel_buffer, 
            mimetype='application/vnd.ms-excel.sheet.macroEnabled.12', 
            resumable=True
        )
        
        drive_service.files().update(
            fileId=file_id,
            media_body=media_body,
            supportsAllDrives=True
        ).execute()
        
        log(f"  ✅ Successfully updated '{file_name_to_find}' in Google Drive.")

    except Exception as e:
        log(f"  [ERROR] Failed to update Excel file '{file_name_to_find}'. Details: {e}")

# --- END OF UPDATED FUNCTION ---


# --- Data Processing Functions ---

def process_overall_instock(df_instock):
    """Adds the 'key' column to the Overall_Instock DataFrame."""
    log("    > Processing Overall_Instock DataFrame...")
    if df_instock is None:
        log("    > [ERROR] Instock DataFrame is empty. Cannot process.")
        return None
    try:
        if 'Store Nbr' in df_instock.columns and 'Old Nbr' in df_instock.columns:
            
            store_nbr_str = df_instock['Store Nbr'].astype(str)
            old_nbr_str = df_instock['Old Nbr'].astype(str)

            # Insert 'key' column at the beginning
            df_instock.insert(
                0,
                'key',
                store_nbr_str + old_nbr_str
            )
            log("      - Added 'key' column.")
        else:
            log("      - [WARN] 'Store Nbr' or 'Old Nbr' not found. Skipping 'key' creation.")
        
        log("    > ✅ Overall_Instock processing complete.")
        return df_instock
    except Exception as e:
        log(f"    > [ERROR] Failed during Overall_Instock processing. Details: {e}")
        return None

def process_article_sales_report(df_article, df_hirarchy, df_division_group, df_instock_processed, df_gst_change_list, df_ytd_sales, day_of_year):
    """Applies all transformations to the Article Sales Report DataFrame."""
    log("    > Processing Article Sales Report DataFrame...")
    if df_article is None:
        log("    > [ERROR] Article DataFrame is empty. Cannot process.")
        return None
        
    try:
        # 1. Add Article UID
        if 'Article No' in df_article.columns and 'Store No' in df_article.columns:
            article_no_col_index = df_article.columns.get_loc('Article No')
            
            store_no_str = df_article['Store No'].astype(str)
            article_no_str = df_article['Article No'].astype(str)

            df_article.insert(
                article_no_col_index + 1,
                'Article UID',
                store_no_str + article_no_str
            )
            log("      - Added 'Article UID' column.")
        else:
            log("      - [WARN] 'Article No' or 'Store No' column not found. Skipping 'Article UID'.")

        # 2. Add Region and Market Manager
        if 'Store No' in df_article.columns and df_hirarchy is not None:
            if 'Location' in df_hirarchy.columns and 'Market' in df_hirarchy.columns and 'Market Manager' in df_hirarchy.columns:
                store_col_name = 'Store No'
                if 'Store' in df_article.columns:
                    store_col_name = 'Store'
                
                if store_col_name in df_article.columns:
                    store_col_index = df_article.columns.get_loc(store_col_name)
                else:
                    log(f"      - [WARN] Neither 'Store' nor 'Store No' found. Appending Region/Manager to the end.")
                    store_col_index = len(df_article.columns) - 1
                
                df_article['Store No'] = df_article['Store No'].astype(str)
                df_hirarchy['Location'] = df_hirarchy['Location'].astype(str)

                df_merged = pd.merge(
                    df_article,
                    df_hirarchy[['Location', 'Market', 'Market Manager']],
                    left_on='Store No',
                    right_on='Location',
                    how='left'
                ).drop('Location', axis=1, errors='ignore')
                
                df_merged = df_merged.rename(columns={'Market': 'Region'})
                
                cols = list(df_article.columns)
                if 'Region' not in cols:
                    cols.insert(store_col_index + 1, 'Region')
                if 'Market Manager' not in cols:
                    cols.insert(store_col_index + 2, 'Market Manager')
                
                final_cols = [c for c in cols if c in df_merged.columns]
                for new_col in ['Region', 'Market Manager']:
                    if new_col in df_merged.columns and new_col not in final_cols:
                        final_cols.append(new_col)
                        
                df_article = df_merged[final_cols]
                
                log("      - Added 'Region' and 'Market Manager' columns.")
            else:
                log("      - [WARN] 'Location', 'Market', or 'Market Manager' not found in hirarchy.csv. Skipping lookup.")
        else:
            log("      - [WARN] 'Store No' column not found or hirarchy.csv failed to load. Skipping Region/Manager lookup.")

        # 3. Add Sub Division_V1
        if 'Sub Division' in df_article.columns and df_division_group is not None:
            if 'Sub Division' in df_division_group.columns and 'Sub Division_V1' in df_division_group.columns:
                sub_div_col_index = df_article.columns.get_loc('Sub Division')
                original_cols = list(df_article.columns)
                
                df_article['Sub Division'] = df_article['Sub Division'].astype(str)
                df_division_group['Sub Division'] = df_division_group['Sub Division'].astype(str)
                
                df_article = pd.merge(
                    df_article,
                    df_division_group[['Sub Division', 'Sub Division_V1']],
                    on='Sub Division',
                    how='left'
                )
                
                if 'Sub Division_V1' not in original_cols:
                    original_cols.insert(sub_div_col_index + 1, 'Sub Division_V1')
                final_cols = [c for c in original_cols if c in df_article.columns]
                if 'Sub Division_V1' in df_article.columns and 'Sub Division_V1' not in final_cols:
                    final_cols.append('Sub Division_V1')
                    
                df_article = df_article[final_cols]
                
                log("      - Added 'Sub Division_V1' column.")
            else:
                log("      - [WARN] 'Sub Division' or 'Sub Division_V1' not found in division_group.csv. Skipping lookup.")
        else:
            log("      - [WARN] 'Sub Division' column not found or division_group.csv failed to load. Skipping Sub Division_V1 lookup.")
            
        # 4. Add KVI columns
        if df_instock_processed is not None and 'Article UID' in df_article.columns:
            kvi_cols = ['key', 'KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']
            if all(col in df_instock_processed.columns for col in kvi_cols):
                
                original_cols = list(df_article.columns)
                
                df_article['Article UID'] = df_article['Article UID'].astype(str)
                df_instock_processed['key'] = df_instock_processed['key'].astype(str)
                
                df_article = pd.merge(
                    df_article,
                    df_instock_processed[['key', 'KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']],
                    left_on='Article UID',
                    right_on='key',
                    how='left'
                ).drop('key', axis=1, errors='ignore')

                kvi_cols_to_add = ['KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']
                if 'VNPK Cost' in original_cols:
                    vnpk_index = original_cols.index('VNPK Cost')
                    for col in reversed(kvi_cols_to_add):
                        if col not in original_cols:
                            original_cols.insert(vnpk_index + 1, col)
                else:
                    log("      - [WARN] 'VNPK Cost' column not found. Appending KVI columns to the end.")
                    for col in kvi_cols_to_add:
                        if col not in original_cols:
                            original_cols.append(col)
                
                final_cols = [c for c in original_cols if c in df_article.columns]
                for new_col in kvi_cols_to_add:
                    if new_col in df_article.columns and new_col not in final_cols:
                        final_cols.append(new_col)
                        
                df_article = df_article[final_cols]
                
                log("      - Added KVI columns (Flag, Allocation, Utilization).")
            else:
                log("      - [WARN] KVI columns not found in processed Instock report. Skipping KVI lookup.")
        else:
            log("      - [WARN] Processed Instock report not available or 'Article UID' missing. Skipping KVI lookup.")

        # 5. Add GST_Change column
        if df_gst_change_list is not None and 'Article UID' in df_article.columns:
            if 'UID' in df_gst_change_list.columns:
                
                df_gst_prepared = df_gst_change_list[['UID']].copy()
                df_gst_prepared['GST_Change'] = 'Yes'
                df_gst_prepared['UID'] = df_gst_prepared['UID'].astype(str)
                
                original_cols = list(df_article.columns)
                df_article['Article UID'] = df_article['Article UID'].astype(str)
                
                df_article = pd.merge(
                    df_article,
                    df_gst_prepared,
                    left_on='Article UID',
                    right_on='UID',
                    how='left'
                ).drop('UID', axis=1, errors='ignore')
                
                df_article['GST_Change'] = df_article['GST_Change'].fillna('')
                
                if 'YTD IM %' in original_cols:
                    ytd_index = original_cols.index('YTD IM %')
                    if 'GST_Change' not in original_cols:
                        original_cols.insert(ytd_index + 1, 'GST_Change')
                else:
                    log("      - [WARN] 'YTD IM %' column not found. Appending 'GST_Change' to the end.")
                    if 'GST_Change' not in original_cols:
                        original_cols.append('GST_Change')
                
                final_cols = [c for c in original_cols if c in df_article.columns]
                if 'GST_Change' in df_article.columns and 'GST_Change' not in final_cols:
                    final_cols.append('GST_Change')
                    
                df_article = df_article[final_cols]
                
                log("      - Added 'GST_Change' column.")
            else:
                log("      - [WARN] 'UID' column not found in gst_change_list.csv. Skipping GST_Change lookup.")
        else:
            log("      - [WARN] 'gst_change_list.csv' not loaded or 'Article UID' missing. Skipping GST_Change lookup.")

        # 6. Add YTD Sales columns (UPDATED FOR 2025)
        # --- ADDED '2025 YTD Sales' to the list below ---
        ytd_cols_to_add = ['2021 YTD Sales', '2022 YTD Sales', '2023 YTD Sales', '2024 YTD Sales', '2025 YTD Sales']
        
        if df_ytd_sales is not None and 'Article UID' in df_article.columns:
            ytd_cols_to_merge = ['Article UID'] + ytd_cols_to_add
            
            # Only proceed if columns exist in the helper file (warn if missing but try to merge what's there)
            available_ytd_cols = [col for col in ytd_cols_to_merge if col in df_ytd_sales.columns]
            
            if 'Article UID' in available_ytd_cols and len(available_ytd_cols) > 1:
                
                original_cols = list(df_article.columns)
                
                df_article['Article UID'] = df_article['Article UID'].astype(str)
                df_ytd_sales['Article UID'] = df_ytd_sales['Article UID'].astype(str)

                df_article = pd.merge(
                    df_article,
                    df_ytd_sales[available_ytd_cols],
                    on='Article UID',
                    how='left'
                )
                
                if 'GST_Change' in original_cols:
                    gst_index = original_cols.index('GST_Change')
                    for col in reversed(ytd_cols_to_add):
                        if col in df_article.columns and col not in original_cols:
                            original_cols.insert(gst_index + 1, col)
                else:
                    log("      - [WARN] 'GST_Change' column not found. Appending YTD Sales columns to the end.")
                    for col in ytd_cols_to_add:
                        if col in df_article.columns and col not in original_cols:
                            original_cols.append(col)
                
                final_cols = [c for c in original_cols if c in df_article.columns]
                for new_col in ytd_cols_to_add:
                    if new_col in df_article.columns and new_col not in final_cols:
                        final_cols.append(new_col)
                        
                df_article = df_article[final_cols]
                log("      - Added YTD Sales columns (2021-2025).")
            else:
                log(f"      - [WARN] Required YTD Sales columns not found in ytd_sales.csv. Available: {available_ytd_cols}")
        else:
            log("      - [WARN] 'ytd_sales.csv' not loaded or 'Article UID' missing. Skipping YTD Sales lookup.")

        # 7. Add Average Sales columns (UPDATED FOR 2025)
        if day_of_year > 0:
            avg_cols_to_add = []
            original_cols = list(df_article.columns)
            
            if 'YTD Sale Amt' in df_article.columns:
                df_article['YTD Avg Sales'] = pd.to_numeric(df_article['YTD Sale Amt'], errors='coerce') / day_of_year
                avg_cols_to_add.append('YTD Avg Sales')
                log("      - Calculated 'YTD Avg Sales'.")
            
            # --- UPDATED LOOP TO INCLUDE 2025 ---
            for year in [2021, 2022, 2023, 2024, 2025]:
                ytd_col = f'{year} YTD Sales'
                avg_col = f'{year} Avg Sales'
                if ytd_col in df_article.columns:
                    df_article[avg_col] = pd.to_numeric(df_article[ytd_col], errors='coerce') / day_of_year
                    avg_cols_to_add.append(avg_col)
                    log(f"      - Calculated '{avg_col}'.")
            
            # --- UPDATED ANCHOR TO 2025 ---
            if '2025 YTD Sales' in original_cols:
                ytd_2025_index = original_cols.index('2025 YTD Sales')
                for col in reversed(avg_cols_to_add):
                    if col not in original_cols:
                        original_cols.insert(ytd_2025_index + 1, col)
            # Fallback to 2024 if 2025 is missing
            elif '2024 YTD Sales' in original_cols:
                ytd_2024_index = original_cols.index('2024 YTD Sales')
                for col in reversed(avg_cols_to_add):
                    if col not in original_cols:
                        original_cols.insert(ytd_2024_index + 1, col)
            else:
                 for col in avg_cols_to_add:
                    if col not in original_cols:
                        original_cols.append(col)

            final_cols = [c for c in original_cols if c in df_article.columns]
            for new_col in avg_cols_to_add:
                if new_col in df_article.columns and new_col not in final_cols:
                    final_cols.append(new_col)
            df_article = df_article[final_cols]
        else:
            log(f"      - [WARN] day_of_year is {day_of_year}. Skipping average sales calculations.")

        # 8. Add Day On Hand (UPDATED FOR 2025 POSITIONING)
        if day_of_year > 0 and 'YTD COST Amt' in df_article.columns and 'On Hand Cost' in df_article.columns:
            original_cols = list(df_article.columns)
            
            avg_daily_cost = pd.to_numeric(df_article['YTD COST Amt'], errors='coerce') / day_of_year
            on_hand_cost = pd.to_numeric(df_article['On Hand Cost'], errors='coerce')
            
            df_article['Day On Hand'] = on_hand_cost / avg_daily_cost
            df_article['Day On Hand'] = df_article['Day On Hand'].replace([np.inf, -np.inf], np.nan)
            
            # --- UPDATED TO LOOK FOR 2025 AVG SALES FIRST ---
            last_avg_col = '2025 Avg Sales'
            if last_avg_col in original_cols:
                last_avg_index = original_cols.index(last_avg_col)
                if 'Day On Hand' not in original_cols:
                    original_cols.insert(last_avg_index + 1, 'Day On Hand')
            elif '2024 Avg Sales' in original_cols:
                last_avg_index = original_cols.index('2024 Avg Sales')
                if 'Day On Hand' not in original_cols:
                    original_cols.insert(last_avg_index + 1, 'Day On Hand')
            else:
                if 'Day On Hand' not in original_cols:
                    original_cols.append('Day On Hand')

            df_article = df_article[[c for c in original_cols if c in df_article.columns]]
            log("      - Calculated 'Day On Hand'.")
        else:
            log("      - [WARN] Skipping 'Day On Hand' calculation (missing columns or day_of_year=0).")
            
        # 9. Add Final Remarks
        if 'Day On Hand' in df_article.columns:
            original_cols = list(df_article.columns)
            
            conditions = [
                (pd.to_numeric(df_article['Day On Hand'], errors='coerce') > 7),
                (pd.to_numeric(df_article['Day On Hand'], errors='coerce') <= 7)
            ]
            choices = [ 'Price Support Required', 'Stock Required' ]
            df_article['Final Remarks'] = np.select(conditions, choices, default='') # Default is blank for NaNs
            
            doh_index = original_cols.index('Day On Hand')
            if 'Final Remarks' not in original_cols:
                original_cols.insert(doh_index + 1, 'Final Remarks')

            df_article = df_article[[c for c in original_cols if c in df_article.columns]]
            log("      - Added 'Final Remarks'.")
        else:
            log("      - [WARN] Skipping 'Final Remarks' (missing 'Day On Hand').")

        # 10. Delete unwanted columns
        cols_to_drop = ['WEEK4_COST', 'WEEK4_QTY', 'WEEEK4_Sales', 'WEEK4_Sales']
        existing_cols_to_drop = [col for col in cols_to_drop if col in df_article.columns]
        if existing_cols_to_drop:
            df_article = df_article.drop(columns=existing_cols_to_drop)
            log(f"      - Removed columns: {', '.join(existing_cols_to_drop)}")
        else:
            log("      - No columns to delete (WEEK4_COST, etc. not found).")
            
        # 11. Filter out unwanted rows
        initial_row_count = len(df_article)
        
        if 'Article Status' in df_article.columns:
            df_article = df_article[df_article['Article Status'].astype(str).str.strip().str.upper() != 'D']
            log(f"      - Filtered rows where Article Status is 'D'.")
        else:
            log("      - [WARN] 'Article Status' column not found. Skipping filter.")
            
        if 'Division' in df_article.columns:
            df_article = df_article[~df_article['Division'].astype(str).str.strip().str.lower().isin(['freebies', 'service article'])]
            log(f"      - Filtered rows where Division is 'Freebies' or 'Service Article'.")
        else:
            log("      - [WARN] 'Division' column not found. Skipping filter.")
            
        if 'Store' in df_article.columns:
            df_article = df_article[df_article['Store'].astype(str).str.strip().str.lower() != 'lucknow fc']
            log(f"      - Filtered rows where Store is 'Lucknow FC'.")
        else:
            log("      - [WARN] 'Store' column not found. Skipping 'Lucknow FC' filter.")
            
        final_row_count = len(df_article)
        log(f"      - Row filtering complete. Removed {initial_row_count - final_row_count} rows.")

        log("    > ✅ Processing complete.")
        return df_article
        
    except Exception as e:
        log(f"    > [ERROR] Failed during data processing. Details: {e}")
        return None

# --- Main Logic (Refactored) ---

def copy_original_file(drive_service, file_id, file_name, target_folder_id):
    """Copies the original file (zip or csv) to the target folder if it doesn't exist."""
    log(f"  Checking if original file '{file_name}' needs to be copied...")
    try:
        # Check if original file already exists
        query = f"'{target_folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        existing_files = results.get('files', [])

        if existing_files:
            log(f"  [SKIP] Original file '{file_name}' already exists in target folder.")
            return

        log(f"  [COPYING] Copying original file '{file_name}' to target folder...")
        drive_service.files().copy(
            fileId=file_id,
            body={'name': file_name, 'parents': [target_folder_id]},
            supportsAllDrives=True
        ).execute()
        log(f"  [SUCCESS] Copied original file '{file_name}'.")

    except Exception as e:
        log(f"  [ERROR] Failed to copy original file '{file_name}'. Details: {e}")

# --- NEW: Helper function to find files for a specific date ---
def find_files_for_date(drive_service, date_str):
    """
    Tries to find all required files for a specific date string.
    Returns a file_info dict if all files are found, else None.
    """
    log(f"  Querying for all files with date: {date_str}")
    file_info = {}
    all_found = True
    
    for prefix in FILE_PREFIXES:
        csv_name = f"{prefix}_{date_str}.csv"
        zip_name = f"{prefix}_{date_str}.zip"
        
        query = f"'{SOURCE_FOLDER_ID}' in parents and (name='{csv_name}' or name='{zip_name}') and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        
        if not items:
            log(f"  [NOT FOUND] No file found for '{prefix}' with date {date_str}.")
            all_found = False
            break  # No need to check for other prefixes if one is missing
        
        found_file = items[0]
        log(f"  [FOUND] Found source file: {found_file['name']} (ID: {found_file['id']})")
        file_info[prefix] = (found_file['id'], found_file['name'])
    
    if all_found:
        log(f"  ✅ Found all required files for {date_str}.")
        return file_info
    else:
        log(f"  ❌ Missing one or more files for {date_str}.")
        return None

# --- Main function, updated with fallback logic ---
def check_and_copy_files(drive_service):
    """Finds, copies, transforms, and extracts files using the Google Drive v3 API."""
    if not drive_service:
        log("Skipping file check as authentication failed.")
        return

    # 1. Download Helper Files
    log("\n--- Downloading Helper Files ---")
    df_hirarchy = download_csv_to_df(drive_service, 'hirarchy.csv', TARGET_FOLDER_ID)
    df_division_group = download_csv_to_df(drive_service, 'division_group.csv', TARGET_FOLDER_ID)
    df_gst_change_list = download_csv_to_df(drive_service, 'gst_change_list.csv', TARGET_FOLDER_ID)
    df_ytd_sales = download_csv_to_df(drive_service, 'ytd_sales.csv', TARGET_FOLDER_ID)
    log("---------------------------------")

    try:
        # --- NEW: Date Fallback Logic ---
        today = datetime.date.today()
        
        # Attempt 1: Try to find files for TODAY
        date_to_process = today
        date_to_find_str = date_to_process.strftime('%Y-%m-%d')
        calc_date = date_to_process - datetime.timedelta(days=1) # Use day before for calcs
        
        log(f"--- Attempt 1: Searching for files for {date_to_find_str} (Today) ---")
        file_info = find_files_for_date(drive_service, date_to_find_str)
        
        # Attempt 2: If no files found, try YESTERDAY
        if file_info is None:
            log(f"Files not found for {date_to_find_str}. Attempting to find yesterday's files.")
            date_to_process = today - datetime.timedelta(days=1)
            date_to_find_str = date_to_process.strftime('%Y-%m-%d')
            calc_date = date_to_process - datetime.timedelta(days=1) # Use day before *that* for calcs
            
            log(f"--- Attempt 2: Searching for files for {date_to_find_str} (Yesterday) ---")
            file_info = find_files_for_date(drive_service, date_to_find_str)

        # If still no files, exit
        if file_info is None:
            log("❌ No complete set of files found for today or yesterday. Exiting process.")
            return
        
        # We have a valid file_info dict, proceed.
        log(f"✅ Processing files for date: {date_to_find_str}")
        day_of_year = calc_date.timetuple().tm_yday
        log(f"Using {calc_date.strftime('%Y-%m-%d')} (Day {day_of_year}) for average calculations.")
        # --- END: Date Fallback Logic ---

        # 3. Load main files into DataFrames
        log("\n--- Loading Main Files ---")
        df_article = None
        df_instock = None

        if 'ArticleSalesReport' in file_info:
            file_id, file_name = file_info['ArticleSalesReport']
            df_article = load_file_to_df(drive_service, file_id, file_name)
        else:
            log("  [ERROR] Cannot proceed without ArticleSalesReport. (This should not happen after find_files_for_date check)")
            return

        if 'Overall_Instock' in file_info:
            file_id, file_name = file_info['Overall_Instock']
            df_instock = load_file_to_df(drive_service, file_id, file_name)
        else:
            log("  [ERROR] Cannot proceed without Overall_Instock. (This should not happen after find_files_for_date check)")
            return
        log("----------------------------")

        # 4. Process DataFrames
        log("\n--- Processing DataFrames ---")
        df_instock_processed = process_overall_instock(df_instock)
        
        # --- OPTIMIZATION: Delete raw df_instock to save memory ---
        if 'df_instock' in locals():
            del df_instock
            gc.collect()
            log("  (Memory optimization: Deleted raw instock df)")
        # --- END OPTIMIZATION ---

        df_article_processed = process_article_sales_report(
            df_article, 
            df_hirarchy, 
            df_division_group, 
            df_instock_processed, 
            df_gst_change_list,
            df_ytd_sales,
            day_of_year
        )
        
        # --- OPTIMIZATION: Delete all helper DFs to save memory ---
        log("  (Memory optimization: Deleting helper dataframes...)")
        if 'df_article' in locals(): del df_article
        if 'df_hirarchy' in locals(): del df_hirarchy
        if 'df_division_group' in locals(): del df_division_group
        # Keep df_instock_processed, it's needed for the upload
        if 'df_gst_change_list' in locals(): del df_gst_change_list
        if 'df_ytd_sales' in locals(): del df_ytd_sales
        gc.collect()
        # --- END OPTIMIZATION ---

        log("-------------------------------")

        # 5. Upload Processed DataFrames
        log("\n--- Uploading Processed Files ---")
        
        if df_article_processed is not None:
            # Call the function to update the .xlsm file
            update_excel_file(
                drive_service=drive_service,
                df_to_paste=df_article_processed,
                file_name_to_find="article_sales_report.xlsm",
                sheet_name_to_update="Sheet1",
                folder_id=TARGET_FOLDER_ID
            )
        else:
            log("  [SKIP] Skipping article_sales_report.xlsm update because processed DataFrame is empty.")
        
        # --- OPTIMIZATION: Delete processed DFs after upload ---
        if 'df_article_processed' in locals():
            del df_article_processed
            gc.collect()
            log("  (Memory optimization: Deleted processed article df)")
        # --- END OPTIMIZATION ---

        if df_instock_processed is not None:
            upload_df_as_csv(
                drive_service, 
                df_instock_processed, 
                f"Overall_Instock_{date_to_find_str}.csv",  # Use the date string of the file we found
                TARGET_FOLDER_ID
            )
            # --- OPTIMIZATION: Delete processed DFs after upload ---
            if 'df_instock_processed' in locals():
                del df_instock_processed
                gc.collect()
                log("  (Memory optimization: Deleted processed instock df)")
            # --- END OPTIMIZATION ---
        else:
            log("  [SKIP] Skipping Overall_Instock CSV upload because processed DataFrame is empty.")
        log("---------------------------------")
        
        # 6. Copy Original Files
        log("\n--- Copying Original Source Files ---")
        if 'ArticleSalesReport' in file_info:
            file_id, file_name = file_info['ArticleSalesReport']
            copy_original_file(drive_service, file_id, file_name, TARGET_FOLDER_ID)

        if 'Overall_Instock' in file_info:
            file_id, file_name = file_info['Overall_Instock']
            copy_original_file(drive_service, file_id, file_name, TARGET_FOLDER_ID)
        log("-----------------------------------")
        
        log("\n--- Daily file check complete. ---")

    except Exception as e:
        log(f"\n[ERROR] An unexpected error occurred during file operations: {e}")
        log("Please ensure the service account has 'Editor' access to both Drive folders.")

# --- Run the main function ---
if __name__ == "__main__":
    log("--- Script execution started ---")
    drive_service_instance = authenticate()
    
    if drive_service_instance:
        check_and_copy_files(drive_service_instance)
    else:
        log("❌ Halting script: Authentication failed.")
    log("--- Script execution finished ---")
