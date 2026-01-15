import os
import json
import duckdb
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

# ==========================================
# 1. SETUP & AUTHENTICATION
# ==========================================
creds_info = json.loads(os.environ['GCP_SA_KEY'])
creds = service_account.Credentials.from_service_account_info(creds_info)
drive_service = build('drive', 'v3', credentials=creds)

REQUEST_ID = os.environ.get('REQUEST_ID', 'unknown_id')
INPUT_FILE_ID = os.environ.get('INPUT_FILE_ID', '')
FOLDER_ID = "1pjsuzA9bmQdltnvf21vZ0U4bZ75fUyWt"

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

def upload_file(filename, content_dict):
    try:
        file_metadata = {'name': filename, 'parents': [FOLDER_ID]}
        media = MediaIoBaseUpload(io.BytesIO(json.dumps(content_dict).encode('utf-8')), mimetype='application/json')
        drive_service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        print(f"Upload failed: {e}")

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
all_master_files = [] # Track all filenames
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
        all_master_files.append(safe_name) # Add to master list
        download_file(item['id'], safe_name)

# ==========================================
# 3. ANALYSIS (NEW LOGIC)
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
summary_data = []

try:
    if files:
        # Run Query to find matches
        matches_df = con.execute(sql_query).fetchdf()
        summary_data = matches_df.to_dict(orient='records')
        
        # 1. Identify where member WAS found
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
        else:
            found_files = []
            
        # 2. Compare against ALL files
        total_files_count = len(all_master_files)
        found_files_count = len(found_files)
        
        # 3. Apply Logic
        if found_files_count == 0:
            # Case: Not found anywhere
            rca_text = "Member not found in any file."
            
        elif found_files_count == total_files_count:
            # Case: Found everywhere
            rca_text = "Found in all files."
            
        else:
            # Case: Found in some, missing in others (Removed)
            # Find the difference: All Files - Found Files
            missing_files = list(set(all_master_files) - set(found_files))
            missing_files.sort() # Sort alphabetically to look cleaner
            
            rca_text = f"Member removed/missing from {len(missing_files)} file(s): {', '.join(missing_files)}"

    else:
        rca_text = "Analysis Failed: No Master CSV files found."

except Exception as e:
    rca_text = f"Error during query: {str(e)}"
    print(rca_text)

# ==========================================
# 4. UPLOAD RESULT
# ==========================================
result_payload = {"request_id": REQUEST_ID, "rca_text": rca_text, "summary": summary_data}
upload_file(f"result_{REQUEST_ID}.json", result_payload)
print(f"Done. RCA: {rca_text}")
