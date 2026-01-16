import os
import json
import duckdb
import pandas as pd
import re
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import io

# ==========================================
# 1. SETUP & AUTHENTICATION
# ==========================================
creds_info = json.loads(os.environ['GCP_SA_KEY'])
creds = service_account.Credentials.from_service_account_info(creds_info)

# Initialize APIs
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ENV VARIABLES
REQUEST_ID = os.environ.get('REQUEST_ID', 'unknown_id')
INPUT_FILE_ID = os.environ.get('INPUT_FILE_ID', '')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '') 
FOLDER_ID = "1pjsuzA9bmQdltnvf21vZ0U4bZ75fUyWt"
RESULTS_TAB_NAME = "Analysis_Results"
QUEUE_TAB_NAME = "Request_Queue"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_file_date(filename):
    """
    Parses 'Jan_25' from filename to a real date for sorting.
    Example: 'BP..._Jan_25...' -> Date(2025, 1, 1)
    """
    try:
        # Regex looks for: underscore + 3 letters + underscore + 2 digits (e.g., _Jan_25)
        match = re.search(r'_([A-Za-z]{3})_(\d{2})', filename)
        if match:
            month_str = match.group(1)
            year_str = match.group(2)
            # Parse into a datetime object
            return datetime.strptime(f"{month_str} {year_str}", "%b %y")
    except:
        pass
    return datetime.min # Fallback for files without dates

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
    """Writes results to Sheets without erasing input columns"""
    try:
        if not SPREADSHEET_ID: return

        print(f"Writing result to Sheet...")

        # 1. Append to Analysis_Results
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
        
        if not values:
            values.append([REQUEST_ID, "N/A", "", "", "", "COMPLETED", rca_text])

        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RESULTS_TAB_NAME}!A:A",
            valueInputOption="USER_ENTERED",
            body={'values': values}
        ).execute()

        # 2. Update Request_Queue Status (Surgically)
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!B:B").execute()
        rows_in_queue = result.get('values', [])
        
        row_index = -1
        for i, row_data in enumerate(rows_in_queue):
            if row_data and row_data[0] == REQUEST_ID:
                row_index = i + 1
                break
        
        if row_index != -1:
            # Update Status (Col C)
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!C{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [["COMPLETED"]]}
            ).execute()
            # Update RCA (Col G)
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!G{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [[rca_text]]}
            ).execute()
            print("Sheet updated successfully.")

    except HttpError as e:
        print(f"Sheets API Error: {e}")

# ==========================================
# 3. MAIN LOGIC
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
    except:
        con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")
else:
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")

# --- B. LOAD & SORT MASTER FILES ---
print("Looking for CSV files...")
all_master_files = [] 
try:
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
        fields="files(id, name)").execute()
    files = results.get('files', [])
except: files = []

if files:
    print(f"Found {len(files)} CSV files. Downloading...")
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        all_master_files.append(safe_name)
        if not os.path.exists(safe_name):
            download_file(item['id'], safe_name)
            
    # CRITICAL STEP: Sort files by Date (Jan < Feb < Mar)
    # This ensures 'latest' really means latest in time
    all_master_files.sort(key=get_file_date)

# ==========================================
# 4. ANALYSIS (UPDATED)
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
        
        found_files = []
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
            
        total_files_count = len(all_master_files)
        found_files_count = len(found_files)
        
        # --- NEW LOGIC START ---
        if found_files_count == 0:
            rca_text = "Member not found in any file."
        elif found_files_count == total_files_count:
            rca_text = "Found in all files."
        else:
            # Calculate missing files
            missing_files = list(set(all_master_files) - set(found_files))
            
            # Sort missing files by Date to find the LATEST one
            missing_files.sort(key=get_file_date)
            
            # Grab the last one in the sorted list (The latest month)
            latest_missing_file = missing_files[-1]
            
            rca_text = f"Member not found in {latest_missing_file}"
        # --- NEW LOGIC END ---

    else:
        rca_text = "Analysis Failed: No Master CSV files found."

except Exception as e:
    rca_text = f"Error during query: {str(e)}"
    print(rca_text)

# ==========================================
# 5. WRITE RESULT
# ==========================================
print(f"RCA Result: {rca_text}")
write_to_google_sheet(matches, rca_text)
print("Done.")
