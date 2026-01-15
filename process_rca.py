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

# ENV VARIABLES
REQUEST_ID = os.environ.get('REQUEST_ID', 'unknown_id')
INPUT_FILE_ID = os.environ.get('INPUT_FILE_ID', '')
# HARDCODE YOUR FOLDER ID HERE
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

# --- A. LOAD USER INPUT (Using Pandas for Safety) ---
print(f"Downloading Input File ID: {INPUT_FILE_ID}...")
if download_file(INPUT_FILE_ID, 'user_input.json'):
    try:
        # Load JSON with Python first to handle formatting issues
        with open('user_input.json', 'r') as f:
            raw = json.load(f)
        
        # Ensure it's a list
        if isinstance(raw, dict): raw = [raw]
            
        # Create DataFrame and Register to DuckDB
        df = pd.DataFrame(raw)
        # Normalize column names (lowercase, strip spaces) to avoid matching errors
        df.columns = [x.strip().lower() for x in df.columns]
        
        print(f"User Data Loaded. Columns: {list(df.columns)}")
        con.register('user_data', df)
    except Exception as e:
        print(f"Input Error: {e}")
        con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)") # Fallback
else:
    print("Input download failed.")
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)") # Fallback

# --- B. LOAD MASTER FILES (Using DuckDB for Speed) ---
print("Looking for CSV files in folder...")
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
        download_file(item['id'], safe_name)

# ==========================================
# 3. ANALYSIS QUERY
# ==========================================
print("Running Analysis...")

# SQL: Join Pandas Table (user_data) with DuckDB CSVs
# Note: I used 'LOWER' to ensure case-insensitive matching on the Header
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
        # Check if 'mem_nbr' exists in input (case insensitive check)
        # We forced columns to lower case earlier in Pandas step
        matches_df = con.execute(sql_query).fetchdf()
        
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
            rca_text = f"Success! Found in: {', '.join(found_files)}"
        else:
            rca_text = "Member ID not found in any master file."
        
        summary_data = matches_df.to_dict(orient='records')
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
print("Done.")
