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
    """Parses '_Jan_25' to datetime for sorting."""
    try:
        match = re.search(r'_([A-Za-z]{3})_(\d{2})', filename)
        if match:
            return datetime.strptime(f"{match.group(1)} {match.group(2)}", "%b %y")
    except: pass
    return datetime.min

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

def write_values_to_sheet(values):
    """Writes pre-calculated rows to Sheets"""
    try:
        if not SPREADSHEET_ID or not values: return

        print(f"Writing {len(values)} rows to Sheet...")

        # 1. Append to Analysis_Results
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RESULTS_TAB_NAME}!A:A",
            valueInputOption="USER_ENTERED",
            body={'values': values}
        ).execute()

        # 2. Update Request_Queue Status
        # We assume the overall status is COMPLETED if we finished analysis
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!B:B").execute()
        rows_in_queue = result.get('values', [])
        
        row_index = -1
        for i, row_data in enumerate(rows_in_queue):
            if row_data and row_data[0] == REQUEST_ID:
                row_index = i + 1
                break
        
        if row_index != -1:
            # Update Status
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!C{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [["COMPLETED"]]}
            ).execute()
            
            # Update RCA Column with a summary or the first user's result
            # (Since multiple users have different RCAs, we usually put "Processed" or the first error)
            summary_rca = values[0][6] if values else "Analysis Complete"
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!G{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [[summary_rca]]}
            ).execute()

    except HttpError as e:
        print(f"Sheets API Error: {e}")

# ==========================================
# 3. MAIN LOGIC
# ==========================================
con = duckdb.connect(database=':memory:')
print(f"Processing Request: {REQUEST_ID}")

# --- A. LOAD INPUT ---
input_members = []
if download_file(INPUT_FILE_ID, 'user_input.json'):
    try:
        with open('user_input.json', 'r') as f:
            raw = json.load(f)
        if isinstance(raw, dict): raw = [raw]
        
        # Normalize and store input for iteration later
        df_input = pd.DataFrame(raw)
        df_input.columns = [x.strip().lower() for x in df_input.columns]
        con.register('user_data', df_input)
        
        # Extract list of mem_nbrs to iterate over
        if 'mem_nbr' in df_input.columns:
            input_members = df_input['mem_nbr'].astype(str).unique().tolist()
            
    except: con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")
else:
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")

# --- B. LOAD MASTER FILES ---
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
            
    # Sort files chronologically
    all_master_files.sort(key=get_file_date)

# ==========================================
# 4. ANALYSIS (PER MEMBER)
# ==========================================
print("Running Analysis...")

# This query gets ALL matches for ALL users at once (Efficiency)
sql_query = """
    SELECT 
        CAST(u.mem_nbr AS VARCHAR) AS User_ID,
        u.mem_name AS User_Name,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
"""

final_rows_to_write = []

try:
    if files:
        # 1. Get all matches
        matches_df = con.execute(sql_query).fetchdf()
        
        # 2. Iterate per INPUT MEMBER (to handle missing logic individually)
        for mem_id in input_members:
            mem_id_str = str(mem_id)
            
            # Filter matches just for this user
            user_matches = matches_df[matches_df['User_ID'] == mem_id_str]
            
            # Identify found files for this specific user
            found_files = []
            if not user_matches.empty:
                found_files = user_matches['Found_In_File'].unique().tolist()
            
            # Determine RCA Logic
            total_files = len(all_master_files)
            found_count = len(found_files)
            rca_text = ""
            
            if found_count == 0:
                rca_text = "Member not found in any file."
                # Add a dummy row so they appear in output even if not found
                final_rows_to_write.append([
                    REQUEST_ID, "N/A", "", mem_id_str, "", "COMPLETED", rca_text
                ])
                
            elif found_count == total_files:
                rca_text = "Found in all files."
                # Add all found rows
                for _, row in user_matches.iterrows():
                    final_rows_to_write.append([
                        REQUEST_ID,
                        row['Found_In_File'],
                        str(row.get('store_nbr', '')),
                        mem_id_str,
                        row.get('User_Name', '') or row.get('mem_name', ''),
                        "MATCH FOUND",
                        rca_text
                    ])
                    
            else:
                # Partial Match - Find latest missing
                missing_files = list(set(all_master_files) - set(found_files))
                missing_files.sort(key=get_file_date)
                latest_missing = missing_files[-1]
                rca_text = f"Member not found in {latest_missing}"
                
                # Add all found rows
                for _, row in user_matches.iterrows():
                    final_rows_to_write.append([
                        REQUEST_ID,
                        row['Found_In_File'],
                        str(row.get('store_nbr', '')),
                        mem_id_str,
                        row.get('User_Name', '') or row.get('mem_name', ''),
                        "MATCH FOUND",
                        rca_text
                    ])

    else:
        # Fallback if no CSVs exist
        final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", "No Master Files Found"])

except Exception as e:
    print(f"Error: {e}")
    final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", str(e)])

# ==========================================
# 5. WRITE RESULT
# ==========================================
write_values_to_sheet(final_rows_to_write)
print("Done.")
