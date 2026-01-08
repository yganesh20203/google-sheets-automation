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
import gc  # Import garbage collector

# Google API Libraries for Service Account
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import google.auth

# --- NEW: Logging Function ---
def log(message):
    """Prints a message with a timestamp."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

# --- Authentication ---
SERVICE_ACCOUNT_FILE = 'credentials.json'

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
    """Downloads a file by ID (zip or csv) and loads the first CSV found into a pandas DataFrame."""
    log(f"  Loading main file: {file_name} (ID: {file_id})...")
    try:
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
    """Uploads a pandas DataFrame as a CSV file to Google Drive."""
    log(f"  Uploading processed file: {file_name}...")
    if df is None:
        log(f"  [ERROR] DataFrame for {file_name} is empty. Skipping upload.")
        return

    try:
        output_buffer = io.StringIO()
        df.to_csv(output_buffer, index=False)
        output_buffer.seek(0)
        csv_data_bytes = output_buffer.getvalue().encode('utf-8')
        csv_buffer_for_upload = BytesIO(csv_data_bytes)

        query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        existing_files = results.get('files', [])
        
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media_body = MediaIoBaseUpload(csv_buffer_for_upload, mimetype='text/csv', resumable=True)
        
        if existing_files:
            file_id = existing_files[0]['id']
            log(f"    > Overwriting existing file: {file_name} (ID: {file_id})")
            drive_service.files().update(fileId=file_id, media_body=media_body, supportsAllDrives=True).execute()
        else:
            log(f"    > Creating new file: {file_name}")
            drive_service.files().create(body=file_metadata, media_body=media_body, fields='id', supportsAllDrives=True).execute()
            
        log(f"  ✅ Successfully uploaded '{file_name}'.")

    except Exception as e:
        log(f"  [ERROR] Failed to upload DataFrame {file_name}. Details: {e}")

# --- Helper Function to update an existing .xlsm file ---

def update_excel_file(drive_service, df_to_paste, file_name_to_find, sheet_name_to_update, folder_id):
    """Finds an .xlsm file, downloads it, clears a sheet, pastes a DataFrame, and re-uploads."""
    log(f"\n--- Updating Excel File: {file_name_to_find} ---")
    if df_to_paste is None:
        log(f"  [ERROR] DataFrame is empty. Skipping Excel update.")
        return

    try:
        # 1. Find the .xlsm file
        log(f"  Searching for '{file_name_to_find}' in folder ID: {folder_id}...")
        query = f"'{folder_id}' in parents and name='{file_name_to_find}' and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
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
        wb = openpyxl.load_workbook(excel_file_buffer, keep_vba=True)
        
        if sheet_name_to_update not in wb.sheetnames:
            log(f"  [ERROR] Sheet '{sheet_name_to_update}' not found in workbook.")
            wb.close()
            return

        # Faster Method: Delete and recreate sheet
        log(f"  Refreshing sheet: '{sheet_name_to_update}'...")
        sheet_index = wb.sheetnames.index(sheet_name_to_update)
        ws = wb[sheet_name_to_update]
        wb.remove(ws)
        ws = wb.create_sheet(title=sheet_name_to_update, index=sheet_index)
        
        log(f"  Pasting {len(df_to_paste)} rows into sheet...")
        for r in dataframe_to_rows(df_to_paste, index=False, header=True):
            ws.append(r)
        
        log(f"  Saving changes to memory...")
        output_excel_buffer = BytesIO()
        wb.save(output_excel_buffer)
        output_excel_buffer.seek(0)
        wb.close()
        log("  Workbook saved to memory.")

        # 4. Re-upload
        log(f"  Uploading updated file back to Drive...")
        media_body = MediaIoBaseUpload(
            output_excel_buffer, 
            mimetype='application/vnd.ms-excel.sheet.macroEnabled.12', 
            resumable=True
        )
        
        drive_service.files().update(
            fileId=file_id, media_body=media_body, supportsAllDrives=True
        ).execute()
        
        log(f"  ✅ Successfully updated '{file_name_to_find}' in Google Drive.")

    except Exception as e:
        log(f"  [ERROR] Failed to update Excel file '{file_name_to_find}'. Details: {e}")

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
            df_instock.insert(0, 'key', store_nbr_str + old_nbr_str)
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
            s_store = pd.to_numeric(df_article['Store No'], errors='coerce').fillna(0).astype('int64').astype(str)
            s_article = pd.to_numeric(df_article['Article No'], errors='coerce').fillna(0).astype('int64').astype(str)
            df_article.insert(article_no_col_index + 1, 'Article UID', s_store + s_article)
            log("      - Added 'Article UID' column.")
        else:
            log("      - [WARN] 'Article No' or 'Store No' column not found.")

        # 2. Add Region and Market Manager
        if 'Store No' in df_article.columns and df_hirarchy is not None:
            if 'Location' in df_hirarchy.columns and 'Market' in df_hirarchy.columns and 'Market Manager' in df_hirarchy.columns:
                store_col_name = 'Store' if 'Store' in df_article.columns else 'Store No'
                store_col_index = df_article.columns.get_loc(store_col_name) if store_col_name in df_article.columns else len(df_article.columns) - 1
                
                df_article['Store No'] = df_article['Store No'].astype(str)
                df_hirarchy['Location'] = df_hirarchy['Location'].astype(str)

                df_merged = pd.merge(
                    df_article,
                    df_hirarchy[['Location', 'Market', 'Market Manager']],
                    left_on='Store No', right_on='Location', how='left'
                ).drop('Location', axis=1, errors='ignore')
                
                df_merged = df_merged.rename(columns={'Market': 'Region'})
                cols = list(df_article.columns)
                if 'Region' not in cols: cols.insert(store_col_index + 1, 'Region')
                if 'Market Manager' not in cols: cols.insert(store_col_index + 2, 'Market Manager')
                
                final_cols = [c for c in cols if c in df_merged.columns]
                for new_col in ['Region', 'Market Manager']:
                    if new_col in df_merged.columns and new_col not in final_cols: final_cols.append(new_col)
                df_article = df_merged[final_cols]
                log("      - Added 'Region' and 'Market Manager'.")

        # 3. Add Sub Division_V1
        if 'Sub Division' in df_article.columns and df_division_group is not None:
            if 'Sub Division' in df_division_group.columns and 'Sub Division_V1' in df_division_group.columns:
                sub_div_col_index = df_article.columns.get_loc('Sub Division')
                original_cols = list(df_article.columns)
                df_article['Sub Division'] = df_article['Sub Division'].astype(str)
                df_division_group['Sub Division'] = df_division_group['Sub Division'].astype(str)
                
                df_article = pd.merge(df_article, df_division_group[['Sub Division', 'Sub Division_V1']], on='Sub Division', how='left')
                if 'Sub Division_V1' not in original_cols: original_cols.insert(sub_div_col_index + 1, 'Sub Division_V1')
                final_cols = [c for c in original_cols if c in df_article.columns]
                if 'Sub Division_V1' in df_article.columns and 'Sub Division_V1' not in final_cols: final_cols.append('Sub Division_V1')
                df_article = df_article[final_cols]
                log("      - Added 'Sub Division_V1'.")

        # 4. Add KVI columns
        if df_instock_processed is not None and 'Article UID' in df_article.columns:
            kvi_cols = ['key', 'KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']
            if all(col in df_instock_processed.columns for col in kvi_cols):
                original_cols = list(df_article.columns)
                df_article['Article UID'] = df_article['Article UID'].astype(str)
                df_instock_processed['key'] = df_instock_processed['key'].astype(str)
                
                df_article = pd.merge(df_article, df_instock_processed[['key', 'KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']], left_on='Article UID', right_on='key', how='left').drop('key', axis=1, errors='ignore')

                kvi_cols_to_add = ['KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']
                vnpk_index = original_cols.index('VNPK Cost') if 'VNPK Cost' in original_cols else len(original_cols)
                for col in reversed(kvi_cols_to_add):
                    if col not in original_cols: original_cols.insert(vnpk_index + 1, col)
                
                final_cols = [c for c in original_cols if c in df_article.columns]
                for new_col in kvi_cols_to_add:
                    if new_col in df_article.columns and new_col not in final_cols: final_cols.append(new_col)
                df_article = df_article[final_cols]
                log("      - Added KVI columns.")

        # 5. Add GST_Change column
        if df_gst_change_list is not None and 'Article UID' in df_article.columns and 'UID' in df_gst_change_list.columns:
            df_gst_prepared = df_gst_change_list[['UID']].copy()
            df_gst_prepared['GST_Change'] = 'Yes'
            df_gst_prepared['UID'] = df_gst_prepared['UID'].astype(str)
            original_cols = list(df_article.columns)
            df_article['Article UID'] = df_article['Article UID'].astype(str)
            
            df_article = pd.merge(df_article, df_gst_prepared, left_on='Article UID', right_on='UID', how='left').drop('UID', axis=1, errors='ignore')
            df_article['GST_Change'] = df_article['GST_Change'].fillna('')
            
            ytd_index = original_cols.index('YTD IM %') if 'YTD IM %' in original_cols else len(original_cols)
            if 'GST_Change' not in original_cols: original_cols.insert(ytd_index + 1, 'GST_Change')
            
            final_cols = [c for c in original_cols if c in df_article.columns]
            if 'GST_Change' in df_article.columns and 'GST_Change' not in final_cols: final_cols.append('GST_Change')
            df_article = df_article[final_cols]
            log("      - Added 'GST_Change'.")

        # 6. Add YTD Sales columns
        ytd_cols_to_add = ['2021 YTD Sales', '2022 YTD Sales', '2023 YTD Sales', '2024 YTD Sales', '2025 YTD Sales']
        if df_ytd_sales is not None and 'Article UID' in df_article.columns:
            available_ytd_cols = [col for col in (['Article UID'] + ytd_cols_to_add) if col in df_ytd_sales.columns]
            if 'Article UID' in available_ytd_cols and len(available_ytd_cols) > 1:
                original_cols = list(df_article.columns)
                df_article['Article UID'] = df_article['Article UID'].astype(str).str.replace(r'\.0$', '', regex=True)
                ytd_uids = pd.to_numeric(df_ytd_sales['Article UID'], errors='coerce').fillna(-1).astype('int64').astype(str)
                df_ytd_sales['Article UID'] = ytd_uids

                df_article = pd.merge(df_article, df_ytd_sales[available_ytd_cols], on='Article UID', how='left')
                
                gst_index = original_cols.index('GST_Change') if 'GST_Change' in original_cols else len(original_cols)
                for col in reversed(ytd_cols_to_add):
                    if col in df_article.columns and col not in original_cols: original_cols.insert(gst_index + 1, col)
                
                final_cols = [c for c in original_cols if c in df_article.columns]
                for new_col in ytd_cols_to_add:
                    if new_col in df_article.columns and new_col not in final_cols: final_cols.append(new_col)
                df_article = df_article[final_cols]
                log("      - Added YTD Sales columns.")

        # 7. Add Average Sales columns
        if day_of_year > 0:
            avg_cols_to_add = []
            original_cols = list(df_article.columns)
            if 'YTD Sale Amt' in df_article.columns:
                df_article['YTD Avg Sales'] = pd.to_numeric(df_article['YTD Sale Amt'], errors='coerce') / day_of_year
                avg_cols_to_add.append('YTD Avg Sales')
            
            for year in [2021, 2022, 2023, 2024, 2025]:
                ytd_col, avg_col = f'{year} YTD Sales', f'{year} Avg Sales'
                if ytd_col in df_article.columns:
                    df_article[avg_col] = pd.to_numeric(df_article[ytd_col], errors='coerce') / day_of_year
                    avg_cols_to_add.append(avg_col)
            
            insert_idx = len(original_cols)
            if '2025 YTD Sales' in original_cols: insert_idx = original_cols.index('2025 YTD Sales') + 1
            elif '2024 YTD Sales' in original_cols: insert_idx = original_cols.index('2024 YTD Sales') + 1

            for col in reversed(avg_cols_to_add):
                if col not in original_cols: original_cols.insert(insert_idx, col)

            final_cols = [c for c in original_cols if c in df_article.columns]
            for new_col in avg_cols_to_add:
                if new_col in df_article.columns and new_col not in final_cols: final_cols.append(new_col)
            df_article = df_article[final_cols]

        # 8. Add Day On Hand
        if day_of_year > 0 and 'YTD COST Amt' in df_article.columns and 'On Hand Cost' in df_article.columns:
            original_cols = list(df_article.columns)
            avg_daily_cost = pd.to_numeric(df_article['YTD COST Amt'], errors='coerce') / day_of_year
            on_hand_cost = pd.to_numeric(df_article['On Hand Cost'], errors='coerce')
            df_article['Day On Hand'] = on_hand_cost / avg_daily_cost
            df_article['Day On Hand'] = df_article['Day On Hand'].replace([np.inf, -np.inf], np.nan)
            
            insert_idx = len(original_cols)
            if '2025 Avg Sales' in original_cols: insert_idx = original_cols.index('2025 Avg Sales') + 1
            elif '2024 Avg Sales' in original_cols: insert_idx = original_cols.index('2024 Avg Sales') + 1
            
            if 'Day On Hand' not in original_cols: original_cols.insert(insert_idx, 'Day On Hand')
            df_article = df_article[[c for c in original_cols if c in df_article.columns]]
            log("      - Calculated 'Day On Hand'.")

        # 9. Add Final Remarks
        if 'Day On Hand' in df_article.columns:
            original_cols = list(df_article.columns)
            conditions = [
                (pd.to_numeric(df_article['Day On Hand'], errors='coerce') > 7),
                (pd.to_numeric(df_article['Day On Hand'], errors='coerce') <= 7)
            ]
            choices = ['Price Support Required', 'Stock Required']
            df_article['Final Remarks'] = np.select(conditions, choices, default='')
            
            doh_index = original_cols.index('Day On Hand')
            if 'Final Remarks' not in original_cols: original_cols.insert(doh_index + 1, 'Final Remarks')
            df_article = df_article[[c for c in original_cols if c in df_article.columns]]
            log("      - Added 'Final Remarks'.")

        # 10. Delete unwanted columns
        cols_to_drop = ['WEEK4_COST', 'WEEK4_QTY', 'WEEEK4_Sales', 'WEEK4_Sales']
        existing_cols_to_drop = [col for col in cols_to_drop if col in df_article.columns]
        if existing_cols_to_drop:
            df_article = df_article.drop(columns=existing_cols_to_drop)

        # 11. Filter out unwanted rows
        if 'Article Status' in df_article.columns:
            df_article = df_article[df_article['Article Status'].astype(str).str.strip().str.upper() != 'D']
        if 'Division' in df_article.columns:
            df_article = df_article[~df_article['Division'].astype(str).str.strip().str.lower().isin(['freebies', 'service article'])]
        if 'Store' in df_article.columns:
            df_article = df_article[df_article['Store'].astype(str).str.strip().str.lower() != 'lucknow fc']

        log(f"      - Row filtering complete. Final rows: {len(df_article)}")
        log("    > ✅ Processing complete.")
        return df_article
        
    except Exception as e:
        log(f"    > [ERROR] Failed during data processing. Details: {e}")
        return None

# --- NEW LOGIC ENGINE: INSIGHTS GENERATION ---

def generate_comprehensive_insights(df, date_str):
    """
    Analyzes the processed DataFrame to build a 'Manage by Exception' text report.
    Returns a string containing the formatted insights.
    """
    log("    > Running Logic Engine to generate comprehensive insights...")
    report = []
    
    # ---------------------------------------------------------
    # 0. Data Preparation & Cleaning (Metric conversions)
    # ---------------------------------------------------------
    df = df.copy() # Work on a copy to not mess up the upload DF
    
    numeric_cols = [
        'YTD Sale Amt', 'YTD COST Amt', 'On Hand Cost', 'YTD Avg Sales', 
        'Day On Hand', 'KVI_Utilization', 'MTD Sale Amt', 'MTD IM %', 'YTD IM %',
        '2024 Avg Sales', 'MTD Qty'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    total_sales = df['YTD Sale Amt'].sum() if 'YTD Sale Amt' in df.columns else 0
    total_inventory = df['On Hand Cost'].sum() if 'On Hand Cost' in df.columns else 0
    
    report.append("================================================================")
    report.append(f"   DAILY BUSINESS INTELLIGENCE REPORT: {date_str}")
    report.append("================================================================\n")
    
    report.append(f"OVERALL HEALTH CHECK:")
    report.append(f"  • Total YTD Sales:   {total_sales:,.0f}")
    report.append(f"  • Total Inventory:   {total_inventory:,.0f}")
    if total_inventory > 0:
        report.append(f"  • Global Efficiency: {total_sales/total_inventory:.2f} (Sales generated per $1 inventory)")
    report.append("\n----------------------------------------------------------------\n")

    # ---------------------------------------------------------
    # 1. REGIONAL & STORE PERFORMANCE (Who is winning?)
    # ---------------------------------------------------------
    report.append("SECTION 1: STORE & REGIONAL ANALYSIS\n")

    # A. Top 5 Stores by Sales Volume
    if 'Store' in df.columns and 'YTD Sale Amt' in df.columns:
        top_stores = df.groupby('Store')['YTD Sale Amt'].sum().nlargest(5)
        report.append("🏆 TOP 5 STORES (YTD Sales Volume):")
        for store, sales in top_stores.items():
            report.append(f"  - {store}: {sales:,.0f}")
        report.append("")

    # B. Regional Capital Efficiency
    if 'Region' in df.columns and 'On Hand Cost' in df.columns and 'YTD Sale Amt' in df.columns:
        region_stats = df.groupby('Region')[['YTD Sale Amt', 'On Hand Cost']].sum()
        region_stats['Efficiency'] = region_stats['YTD Sale Amt'] / region_stats['On Hand Cost']
        # Sort by Efficiency (Descending)
        region_stats = region_stats.sort_values(by='Efficiency', ascending=False)
        
        report.append("📊 REGIONAL CAPITAL EFFICIENCY (Sales per $1 Inventory):")
        report.append("  (High ratio = Efficient stock movement. Low ratio = Stock hoarding)")
        for region, row in region_stats.iterrows():
            report.append(f"  - {region}: {row['Efficiency']:.2f} (Sales: {row['YTD Sale Amt']:,.0f} | Stock: {row['On Hand Cost']:,.0f})")
        report.append("")

    report.append("----------------------------------------------------------------\n")

    # ---------------------------------------------------------
    # 2. CATEGORY & ASSORTMENT HEALTH (What is working?)
    # ---------------------------------------------------------
    report.append("SECTION 2: CATEGORY PERFORMANCE\n")

    # A. Margin Bleed Analysis (MTD vs YTD)
    if 'Sub Division_V1' in df.columns and 'MTD IM %' in df.columns and 'YTD IM %' in df.columns:
        # Weighted Average logic is complex, simple avg for now
        cat_margin = df.groupby('Sub Division_V1')[['MTD IM %', 'YTD IM %']].mean()
        cat_margin['Decline'] = cat_margin['YTD IM %'] - cat_margin['MTD IM %']
        bleeding_cats = cat_margin[cat_margin['Decline'] > 5].sort_values(by='Decline', ascending=False).head(5)
        
        if not bleeding_cats.empty:
            report.append("⚠️ MARGIN ALERTS (Categories where Monthly Margin dropped >5% vs YTD):")
            for cat, row in bleeding_cats.iterrows():
                report.append(f"  - {cat}: MTD {row['MTD IM %']:.1f}% vs YTD {row['YTD IM %']:.1f}% (Drop: {row['Decline']:.1f}%)")
            report.append("")

    # B. Dead Categories (Low Sales, High Stock)
    if 'Sub Division_V1' in df.columns:
        cat_health = df.groupby('Sub Division_V1')[['YTD Sale Amt', 'On Hand Cost']].sum()
        dead_cats = cat_health[ (cat_health['On Hand Cost'] > 50000) & (cat_health['YTD Sale Amt'] < 5000) ].sort_values(by='On Hand Cost', ascending=False).head(5)
        
        if not dead_cats.empty:
            report.append("🛑 STAGNANT CATEGORIES (High Stock >50k, Low Sales <5k):")
            for cat, row in dead_cats.iterrows():
                report.append(f"  - {cat}: Stock {row['On Hand Cost']:,.0f} | Sales {row['YTD Sale Amt']:,.0f}")
            report.append("")

    report.append("----------------------------------------------------------------\n")

    # ---------------------------------------------------------
    # 3. PRODUCT-LEVEL EXCEPTIONS (Actionables)
    # ---------------------------------------------------------
    report.append("SECTION 3: CRITICAL PRODUCT ACTIONABLES\n")

    # A. URGENT STOCK REQUIRED (High Sales, <7 Days Stock)
    if 'Final Remarks' in df.columns and 'YTD Avg Sales' in df.columns:
        urgent = df[ (df['Final Remarks'] == 'Stock Required') & (df['YTD Avg Sales'] > 0) ]
        urgent = urgent.sort_values(by='YTD Avg Sales', ascending=False).head(10)
        
        if not urgent.empty:
            report.append("🔴 URGENT RE-ORDER REQUIRED (Top 10 Fast Movers out of stock):")
            for _, row in urgent.iterrows():
                desc = row.get('Article Description', 'N/A')
                uid = row.get('Article UID', '')
                doh = row.get('Day On Hand', 0)
                sales = row.get('YTD Avg Sales', 0)
                report.append(f"  - {uid} {desc}: {doh:.1f} Days Cover (Sells {sales:.1f}/day)")
            report.append("")

    # B. KVI AVAILABILITY (Strategic Risk)
    if 'KVI_Flag' in df.columns and 'KVI_Utilization' in df.columns:
        kvi_risk = df[ (df['KVI_Flag'].astype(str).str.lower().isin(['yes','1','true'])) & (df['KVI_Utilization'] < 0.85) ]
        kvi_risk = kvi_risk.sort_values(by='KVI_Utilization', ascending=True).head(10)
        
        if not kvi_risk.empty:
            report.append("⭐ KVI AVAILABILITY RISKS (Key Items < 85% Availability):")
            for _, row in kvi_risk.iterrows():
                desc = row.get('Article Description', 'N/A')
                util = row.get('KVI_Utilization', 0) * 100
                report.append(f"  - {desc}: Only {util:.1f}% Availability")
            report.append("")

    # C. CASH TRAPS (Dead Stock)
    if 'Day On Hand' in df.columns and 'On Hand Cost' in df.columns:
        dead_stock = df[ (df['Day On Hand'] > 90) & (df['On Hand Cost'] > 10000) ]
        dead_stock = dead_stock.sort_values(by='On Hand Cost', ascending=False).head(10)
        
        if not dead_stock.empty:
            report.append("💸 CASH TRAPS (Top 10 Items >90 Days Cover & >10k Cost):")
            for _, row in dead_stock.iterrows():
                desc = row.get('Article Description', 'N/A')
                cost = row.get('On Hand Cost', 0)
                doh = row.get('Day On Hand', 0)
                report.append(f"  - {desc}: Frozen Capital {cost:,.0f} ({doh:.0f} Days)")
            report.append("")

    # D. "GHOST" INVENTORY (System says stock, but NO sales)
    if 'On Hand Qty' in df.columns and 'MTD Sale Amt' in df.columns and 'YTD Avg Sales' in df.columns:
        # Logic: Has stock, usually sells well (YTD), but Zero sales this Month
        ghosts = df[ (df['On Hand Qty'] > 10) & (df['YTD Avg Sales'] > 2) & (df['MTD Sale Amt'] == 0) ]
        ghosts = ghosts.sort_values(by='YTD Avg Sales', ascending=False).head(10)
        
        if not ghosts.empty:
            report.append("👻 GHOST INVENTORY (High Stock, Historic Sales, but ZERO MTD Sales):")
            report.append("  (Action: Audit these items. They might be stolen or lost in backroom)")
            for _, row in ghosts.iterrows():
                desc = row.get('Article Description', 'N/A')
                store = row.get('Store', 'N/A')
                stock = row.get('On Hand Qty', 0)
                report.append(f"  - {desc} @ {store}: Shows {stock} qty but 0 sales MTD.")
    
    report.append("\n================================================================")
    report.append("   END OF REPORT")
    report.append("================================================================")
    
    return "\n".join(report)

def upload_text_report(drive_service, report_content, file_name, folder_id):
    """Uploads a text string as a .txt file to Google Drive."""
    log(f"  Uploading Insights Report: {file_name}...")
    try:
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(report_content.encode('utf-8')), mimetype='text/plain', resumable=True)
        
        drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
        log(f"  ✅ Text report uploaded successfully.")
    except Exception as e:
        log(f"  ❌ Failed to upload text report: {e}")

# --- Main Logic (Refactored) ---

def copy_original_file(drive_service, file_id, file_name, target_folder_id):
    """Copies the original file (zip or csv) to the target folder if it doesn't exist."""
    log(f"  Checking if original file '{file_name}' needs to be copied...")
    try:
        query = f"'{target_folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        existing_files = results.get('files', [])

        if existing_files:
            log(f"  [SKIP] Original file '{file_name}' already exists in target folder.")
            return

        log(f"  [COPYING] Copying original file '{file_name}' to target folder...")
        drive_service.files().copy(
            fileId=file_id, body={'name': file_name, 'parents': [target_folder_id]}, supportsAllDrives=True
        ).execute()
        log(f"  [SUCCESS] Copied original file '{file_name}'.")

    except Exception as e:
        log(f"  [ERROR] Failed to copy original file '{file_name}'. Details: {e}")

# --- NEW: Helper function to find files for a specific date ---
def find_files_for_date(drive_service, date_str):
    """Tries to find all required files for a specific date string."""
    log(f"  Querying for all files with date: {date_str}")
    file_info = {}
    all_found = True
    
    for prefix in FILE_PREFIXES:
        csv_name = f"{prefix}_{date_str}.csv"
        zip_name = f"{prefix}_{date_str}.zip"
        
        query = f"'{SOURCE_FOLDER_ID}' in parents and (name='{csv_name}' or name='{zip_name}') and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        
        if not items:
            log(f"  [NOT FOUND] No file found for '{prefix}' with date {date_str}.")
            all_found = False
            break
        
        found_file = items[0]
        log(f"  [FOUND] Found source file: {found_file['name']} (ID: {found_file['id']})")
        file_info[prefix] = (found_file['id'], found_file['name'])
    
    if all_found:
        log(f"  ✅ Found all required files for {date_str}.")
        return file_info
    else:
        log(f"  ❌ Missing one or more files for {date_str}.")
        return None

# --- Main function ---
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
        # Date Fallback Logic
        today = datetime.date.today()
        date_to_process = today
        date_to_find_str = date_to_process.strftime('%Y-%m-%d')
        calc_date = date_to_process - datetime.timedelta(days=1)
        
        log(f"--- Attempt 1: Searching for files for {date_to_find_str} (Today) ---")
        file_info = find_files_for_date(drive_service, date_to_find_str)
        
        if file_info is None:
            log(f"Files not found for {date_to_find_str}. Attempting to find yesterday's files.")
            date_to_process = today - datetime.timedelta(days=1)
            date_to_find_str = date_to_process.strftime('%Y-%m-%d')
            calc_date = date_to_process - datetime.timedelta(days=1)
            
            log(f"--- Attempt 2: Searching for files for {date_to_find_str} (Yesterday) ---")
            file_info = find_files_for_date(drive_service, date_to_find_str)

        if file_info is None:
            log("❌ No complete set of files found for today or yesterday. Exiting process.")
            return
        
        log(f"✅ Processing files for date: {date_to_find_str}")
        day_of_year = calc_date.timetuple().tm_yday
        log(f"Using {calc_date.strftime('%Y-%m-%d')} (Day {day_of_year}) for average calculations.")

        # 3. Load main files
        log("\n--- Loading Main Files ---")
        if 'ArticleSalesReport' in file_info:
            file_id, file_name = file_info['ArticleSalesReport']
            df_article = load_file_to_df(drive_service, file_id, file_name)
        else: return

        if 'Overall_Instock' in file_info:
            file_id, file_name = file_info['Overall_Instock']
            df_instock = load_file_to_df(drive_service, file_id, file_name)
        else: return
        log("----------------------------")

        # 4. Process DataFrames
        log("\n--- Processing DataFrames ---")
        df_instock_processed = process_overall_instock(df_instock)
        
        if 'df_instock' in locals():
            del df_instock
            gc.collect()
            log("  (Memory optimization: Deleted raw instock df)")

        df_article_processed = process_article_sales_report(
            df_article, df_hirarchy, df_division_group, df_instock_processed, 
            df_gst_change_list, df_ytd_sales, day_of_year
        )
        
        # Cleanup helpers
        log("  (Memory optimization: Deleting helper dataframes...)")
        if 'df_article' in locals(): del df_article
        if 'df_hirarchy' in locals(): del df_hirarchy
        if 'df_division_group' in locals(): del df_division_group
        if 'df_gst_change_list' in locals(): del df_gst_change_list
        if 'df_ytd_sales' in locals(): del df_ytd_sales
        gc.collect()
        log("-------------------------------")

        # --- 4.5 Generate Insights (THE NEW PART) ---
        if df_article_processed is not None:
            log("\n--- Generating Actionable Insights ---")
            insights_text = generate_comprehensive_insights(df_article_processed, date_to_find_str)
            
            # Print to console for Logs
            print(insights_text)
            
            # Upload to Drive
            upload_text_report(
                drive_service, 
                insights_text, 
                f"Daily_Business_Briefing_{date_to_find_str}.txt", 
                TARGET_FOLDER_ID
            )
        # ---------------------------------------------

        # 5. Upload Processed DataFrames
        log("\n--- Uploading Processed Files ---")
        if df_article_processed is not None:
            update_excel_file(
                drive_service=drive_service,
                df_to_paste=df_article_processed,
                file_name_to_find="article_sales_report.xlsm",
                sheet_name_to_update="Sheet1",
                folder_id=TARGET_FOLDER_ID
            )
        else:
            log("  [SKIP] Skipping article_sales_report.xlsm update (Empty DF).")
        
        if 'df_article_processed' in locals():
            del df_article_processed
            gc.collect()
            log("  (Memory optimization: Deleted processed article df)")

        if df_instock_processed is not None:
            upload_df_as_csv(
                drive_service, df_instock_processed, 
                f"Overall_Instock_{date_to_find_str}.csv", TARGET_FOLDER_ID
            )
            if 'df_instock_processed' in locals():
                del df_instock_processed
                gc.collect()
                log("  (Memory optimization: Deleted processed instock df)")
        else:
            log("  [SKIP] Skipping Overall_Instock CSV upload (Empty DF).")
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
        log(f"\n[ERROR] An unexpected error occurred: {e}")
        log("Please ensure the service account has 'Editor' access.")

# --- Run the main function ---
if __name__ == "__main__":
    log("--- Script execution started ---")
    drive_service_instance = authenticate()
    if drive_service_instance:
        check_and_copy_files(drive_service_instance)
    else:
        log("❌ Halting script: Authentication failed.")
    log("--- Script execution finished ---")
