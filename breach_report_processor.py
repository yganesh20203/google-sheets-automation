import pandas as pd
from datetime import datetime, timedelta
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import numpy as np
import warnings
from googleapiclient.discovery import build
import io
from googleapiclient.http import MediaIoBaseDownload
import os
import json

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# --- Configuration ---
G_SHEET_URL = "https://docs.google.com/spreadsheets/d/1AHRKubx_Q-K8DR-86xVnEC_dUD94kcGwB1l3LguEYfk/edit?usp=sharing"
RAW_DATA_SHEET_NAME = "raw_data"
PIVOT_SHEET_NAME = "Pivot"
VIEW_SHEET_NAME = "View"
DRIVE_FOLDER_ID = "1mBCJJ_7kTSMlNDj7mMxZ33hNeMykqKyR"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

print("--- Script Started ---")

def get_todays_folder_name():
    """Generates the folder name for the current date (e.g., '15th October')."""
    today = datetime.now()
    day = today.day
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]
    return f"{day}{suffix} {today.strftime('%B')}"

def download_csv_from_drive(service, folder_id, folder_name, file_name):
    """Downloads a CSV file from a specific folder in Google Drive."""
    # Search for the folder that CONTAINS the date string
    print(f"Searching for folder containing '{folder_name}'...")
    query = f"name contains '{folder_name}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, spaces='drive', fields='nextPageToken, files(id, name)').execute()
    items = results.get('files', [])

    if not items:
        print(f"ERROR: Folder containing '{folder_name}' not found.")
        return None
    
    found_folder_name = items[0]['name']
    todays_folder_id = items[0]['id']
    print(f"Found folder: '{found_folder_name}' with ID: {todays_folder_id}")

    # Search for the file in the found folder
    query = f"name='{file_name}' and '{todays_folder_id}' in parents"
    results = service.files().list(q=query, spaces='drive', fields='nextPageToken, files(id, name)').execute()
    items = results.get('files', [])

    if not items:
        print(f"ERROR: File '{file_name}' not found in folder '{found_folder_name}'.")
        return None

    file_id = items[0]['id']
    print(f"Found file: '{file_name}' with ID: {file_id}")

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")
    
    fh.seek(0)
    return pd.read_csv(fh, encoding='utf-8', low_memory=False)

# --- Main Script ---
try:
    print("Step 1: Authenticating and setting up services...")
    creds_json_str = os.getenv("GCP_SA_KEY")
    if not creds_json_str:
        raise ValueError("GCP_SA_KEY secret not found. Please check GitHub Actions secrets.")

    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    spreadsheet = gc.open_by_url(G_SHEET_URL)
    print("Successfully authenticated and set up services.")
    
    print("Step 2: Loading data from Google Drive...")
    todays_folder = get_todays_folder_name()
    df = download_csv_from_drive(drive_service, DRIVE_FOLDER_ID, todays_folder, "Merged_Breach_Report.csv")

    if df is None:
        exit()
    print("Data loaded successfully from Google Drive.")

    print("Step 3: Filtering data for the last 4 days...")
    df['Int_Order_Date'] = df['Int_Order_Date'].replace('Not Available', np.nan)
    df.dropna(subset=['Int_Order_Date'], inplace=True)
    df['Int_Order_Date'] = pd.to_datetime(df['Int_Order_Date'], errors='coerce')
    df.dropna(subset=['Int_Order_Date'], inplace=True)

    today = datetime.now().date()
    four_days_ago = today - timedelta(days=3)
    df_last_4_days = df[df['Int_Order_Date'].dt.date >= four_days_ago].copy()
    print(f"Found {len(df_last_4_days)} rows from the last 4 days.")

    print("Step 4: Extracting order hour from 'CT_Order_Date_PBI'...")
    df_last_4_days['CT_Order_Date_PBI'] = df_last_4_days['CT_Order_Date_PBI'].replace('Not Available', np.nan)
    df_last_4_days['CT_Order_Date_PBI'] = pd.to_datetime(df_last_4_days['CT_Order_Date_PBI'], errors='coerce')
    df_last_4_days['order hour'] = df_last_4_days['CT_Order_Date_PBI'].dt.hour
    print("'order hour' column has been added.")

    print("Step 5: Applying filters for the pivot table...")
    if not df['Int_Order_Date'].dropna().empty:
        latest_date_in_data = df['Int_Order_Date'].dt.date.max()
        yesterday = latest_date_in_data - timedelta(days=1)
        print(f"Latest date found in data: {latest_date_in_data}. Using {yesterday} as yesterday's date for pivot filter.")
    else:
        yesterday = datetime.now().date() - timedelta(days=1)
        print(f"Warning: No valid dates found in 'Int_Order_Date'. Using system's yesterday: {yesterday}")

    modes_to_keep = ['DSD', 'ISP']
    statuses_to_exclude = [
        'HANDOVER OF GOODS BACK TO STORE',
        'ORDER CANCELLED',
        'RETURNS COMPLETE',
        'PICK COMPLETED & ORDER CANCELLED',
        'PENDING FOR HANDOVER BY AP POST 3PL REJECTION'
    ]
    df_for_pivot = df_last_4_days[
        (df_last_4_days['Int_Order_Date'].dt.date == yesterday) &
        (df_last_4_days['Mode_PBI'].isin(modes_to_keep)) &
        (~df_last_4_days['PBI_Status'].isin(statuses_to_exclude))
    ].copy()
    df_for_pivot.dropna(subset=['order hour'], inplace=True)
    df_for_pivot['order hour'] = df_for_pivot['order hour'].astype(int)
    print(f"Found {len(df_for_pivot)} rows matching pivot criteria for yesterday's date ({yesterday}).")

    print("Step 6: Creating the pivot table...")
    if not df_for_pivot.empty:
        pivot_table = pd.pivot_table(
            df_for_pivot,
            index='Store_Name_PBI',
            columns='order hour',
            values='Order_ID',
            aggfunc='count',
            fill_value=0
        )
        pivot_table_to_export = pivot_table.reset_index()
        pivot_table_to_export.insert(0, 'Date', yesterday.strftime('%Y-%m-%d'))
        print("Pivot table created successfully and date column added.")
    else:
        print("Warning: No data available for pivot table after applying filters. An empty table will be uploaded.")
        pivot_table_to_export = pd.DataFrame(columns=['Date', 'Store_Name_PBI'])

    print("Step 7: Uploading data to Google Sheets...")
    print(f"Writing raw data to '{RAW_DATA_SHEET_NAME}' sheet...")
    raw_data_ws = spreadsheet.worksheet(RAW_DATA_SHEET_NAME)
    raw_data_ws.clear()
    df_to_upload = df_last_4_days.copy()
    for col in df_to_upload.select_dtypes(include=['datetime64[ns]']).columns:
        df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    set_with_dataframe(raw_data_ws, df_to_upload, include_index=False, allow_formulas=False)
    print("Raw data written successfully.")

    print(f"Appending pivot table data to '{PIVOT_SHEET_NAME}' sheet...")
    pivot_ws = spreadsheet.worksheet(PIVOT_SHEET_NAME)
    existing_values = pivot_ws.get_all_values()
    next_row_to_write = len(existing_values) + 1
    write_header = next_row_to_write == 1
    set_with_dataframe(
        worksheet=pivot_ws,
        dataframe=pivot_table_to_export,
        row=next_row_to_write,
        include_column_header=write_header,
        include_index=False,
        allow_formulas=False
    )
    print("Pivot table data appended successfully.")

    print("Step 8: Updating the 'View' sheet with the last updated timestamp...")
    view_ws = spreadsheet.worksheet(VIEW_SHEET_NAME)
    # Using Asia/Kolkata for accurate timezone display
    from datetime import timezone
    update_time = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime('%Y-%m-%d %H:%M:%S IST')
    view_ws.update_acell('B1', f"Last updated: {update_time}")
    print("Timestamp updated successfully.")

except gspread.exceptions.SpreadsheetNotFound:
    print(f"ERROR: The Google Sheet was not found at the URL provided. Please check the link.")
except gspread.exceptions.WorksheetNotFound as e:
    print(f"ERROR: A required worksheet was not found: {e}. Please ensure the sheet names are correct.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("\n--- Script Finished ---")
