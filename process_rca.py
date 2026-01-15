import os
import json
import duckdb
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import io
import time

# ==========================================
# 1. SETUP & AUTHENTICATION
# ==========================================
creds_info = json.loads(os.environ['GCP_SA_KEY'])
creds = service_account.Credentials.from_service_account_info(creds_info)

# Initialize BOTH Drive and Sheets APIs
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ENV VARIABLES
REQUEST_ID = os.environ.get('REQUEST_ID', 'unknown_id')
INPUT_FILE_ID = os.environ.get('INPUT_FILE_ID', '')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '') # <--- NEW VARIABLE
FOLDER_ID = "1pjsuzA9bmQdltnvf21vZ0U4bZ75fUyWt"
RESULTS_TAB_NAME = "Analysis_Results"
QUEUE_TAB_NAME = "Request_Queue"

def download_file(file_id, output_path):
    try:
        if not file_id: return False
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        return True
    except Exception as e:
        print(f"Error downloading {file_id}: {e}")
        return False

def write_to_google_sheet(rows, rca_text):
    """Writes analysis results directly to the Google Sheet"""
    try:
        if not SPREADSHEET_ID:
            print("Error: SPREADSHEET_ID is missing.")
            return

        print(f"Writing {len(rows)} rows to Sheet ID: {SPREADSHEET_ID}...")

        # 1. Prepare Data Payload for 'Analysis_Results'
        # Columns: [Request ID, Found In File, Store NBR, Mem NBR, Mem Name, Status, RCA Text]
        values = []
        for row in rows:
            values.append([
                REQUEST_ID,
                row.get('Found_In_File', 'N/A'),
                str(row.get('store_nbr', '')),
                str(row.get('Membership_NBR', '') or row.get('mem_nbr', '')),
                row.get('mem_name', ''),
                "MATCH FOUND",
                rca_text
            ])
        
        # If no matches, at least write one row with the RCA text
        if not values:
            values.append([REQUEST_ID, "N/A", "", "", "", "COMPLETED", rca_text])

        # 2. Append to 'Analysis_Results'
        body = {'values': values}
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RESULTS_TAB_NAME}!A:A",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        print("Successfully appended rows to Analysis_Results.")

        # 3. Update Status in 'Request_Queue' to COMPLETED
        # We need to find the row index first.
        # Ideally, we read the sheet, find the ID, and update that specific cell.
        # For simplicity/speed in this script, we assume the GAS script might handle status,
        # BUT since you want Python to do it, here is the robust way:
        
        # A. Read Column B (Request IDs) from Request_Queue
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!B:B").execute()
        rows_in_queue = result.get('values', [])
        
        # B. Find the Index
        row_index = -1
        for i, row_data in enumerate(rows_in_queue):
            if row_data and row_data[0] == REQUEST_ID:
                row_index = i + 1 # Sheets are 1-indexed
                break
        
        if row_index != -1:
            # C. Update Status (Col C) and RCA (Col G)
            # Col C is index 3, Col G is index 7
            update_body = {
                "values": [["COMPLETED", "", "", "", rca_text]] 
            }
            # This targets Range C{row}:G{row} (Status...RCA)
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{QUEUE_TAB_NAME}!C{row_index}:G{row_index}",
                valueInputOption="USER_ENTERED",
                body=update_body
            ).execute()
            print(f"Updated Request_Queue status for Row {row_index}")
        else:
            print(f"Warning: Request ID {REQUEST_ID} not found in Queue to update status.")

    except HttpError as e:
        print(f"Google Sheets API Error: {e}")

# ==========================================
# 2. MAIN LOGIC
# ==========================================
con = duckdb.connect(database=':memory:')
print(f"Processing Request: {REQUEST_ID}")

# --- A. LOAD USER INPUT ---
print(f"Downloading Input File ID: {INPUT_FILE_ID}...")
if download_file(INPUT_FILE_ID, 'user_input.json'):
    try:
        with open('user_input.json', 'r') as f:
            raw = json.load(f)
        if isinstance(raw, dict): raw = [raw]
        df = pd.DataFrame(raw)
        df.columns = [x.strip().lower() for x in df.columns]
        con.register('user_data', df)
    except Exception as e:
        print(f"Input Error: {e}")
        con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")
else:
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")

# --- B. LOAD MASTER FILES ---
print("Looking for CSV files in folder...")
all_master_files = [] 
try:
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
        fields="files(id, name)").execute()
    files = results.get('files', [])
except: files = []

if not files:
    print("WARNING: No CSV files found.")
else:
    print(f"Found {len(files)} CSV files. Downloading...")
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        all_master_files.append(safe_name)
        # Optimization: Check if file exists to avoid re-downloading on re-runs if caching enabled
        if not os.path.exists(safe_name):
            download_file(item['id'], safe_name)

# ==========================================
# 3. ANALYSIS
# ==========================================
print("Running Analysis...")

sql_query = """
    SELECT 
        u.mem_nbr AS User_ID,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
"""

rca_text = ""
matches = []

try:
    if files:
        matches_df = con.execute(sql_query).fetchdf()
        matches = matches_df.to_dict(orient='records')
        
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
        else:
            found_files = []
            
        total_files_count = len(all_master_files)
        found_files_count = len(found_files)
        
        if found_files_count == 0:
            rca_text = "Member not found in any file."
        elif found_files_count == total_files_count:
            rca_text = "Found in all files."
        else:
            missing_files = list(set(all_master_files) - set(found_files))
            missing_files.sort()
            rca_text = f"Member removed from {len(missing_files)} file(s): {', '.join(missing_files)}"
    else:
        rca_text = "Analysis Failed: No Master CSV files found."

except Exception as e:
    rca_text = f"Error during query: {str(e)}"
    print(rca_text)

# ==========================================
# 4. WRITE DIRECTLY TO SHEETS
# ==========================================
print(f"RCA Result: {rca_text}")
write_to_google_sheet(matches, rca_text)
print("Done.")
