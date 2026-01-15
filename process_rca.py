import os
import json
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
import io

# ==========================================
# 1. SETUP & AUTHENTICATION
# ==========================================
creds_info = json.loads(os.environ['GCP_SA_KEY'])
creds = service_account.Credentials.from_service_account_info(creds_info)
drive_service = build('drive', 'v3', credentials=creds)

# ENV VARIABLES
REQUEST_ID = os.environ['REQUEST_ID']
INPUT_FILE_ID = os.environ['INPUT_FILE_ID']
FOLDER_ID = "1pjsuzA9bmQdltnvf21vZ0U4bZ75fUyWt" 

def download_file(file_id, output_path):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
    except HttpError as e:
        print(f"Error downloading {file_id}: {e}")
        if 'fileNotDownloadable' in str(e):
             print("Skipping file (Google Sheet/Doc).")

def upload_file(filename, content_dict):
    file_metadata = {'name': filename, 'parents': [FOLDER_ID]}
    media = MediaIoBaseUpload(io.BytesIO(json.dumps(content_dict).encode('utf-8')),
                              mimetype='application/json')
    drive_service.files().create(body=file_metadata, media_body=media).execute()

# ==========================================
# 2. MAIN LOGIC
# ==========================================
con = duckdb.connect(database=':memory:')
print(f"Processing Request: {REQUEST_ID}")

# A. Download and Sanitize User Input
print(f"Downloading Input File ID: {INPUT_FILE_ID}...")
download_file(INPUT_FILE_ID, 'raw_input.json')

# SANITIZATION STEP:
# We read the file in Python first to ensure it's a clean list of objects
# This fixes issues where GAS might add invisible characters or formatting
try:
    with open('raw_input.json', 'r') as f:
        raw_data = json.load(f)
    
    # Ensure it's a list (if it's a single object, wrap it)
    if isinstance(raw_data, dict):
        raw_data = [raw_data]
        
    # Write back a perfectly clean JSON file for DuckDB
    with open('user_input.json', 'w') as f:
        json.dump(raw_data, f)
        
except Exception as e:
    print(f"JSON Parsing Error: {e}")
    # Create empty file to prevent crash
    with open('user_input.json', 'w') as f: f.write('[]')

# B. Load into DuckDB (STRICT MODE)
# We use format='array' to force DuckDB to unpack the columns
try:
    con.execute("CREATE TABLE user_data AS SELECT * FROM read_json('user_input.json', format='array')")
    
    # DEBUG: Print columns to log to be sure
    print("User Data Columns Found:", con.execute("DESCRIBE user_data").fetchall())
    
except Exception as e:
    print(f"DuckDB Loading Error: {e}")
    # Fallback: Create empty table with expected column if loading failed
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")

# C. Download Master Files
print("Looking for CSV files in folder...")
results = drive_service.files().list(
    q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
    fields="files(id, name)").execute()

files = results.get('files', [])

if not files:
    print("WARNING: No CSV files found!")
else:
    print(f"Found {len(files)} CSV files. Downloading...")
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        print(f"Downloading {safe_name}...")
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

try:
    if files:
        matches_df = con.execute(sql_query).fetchdf()
        
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
            rca_text = f"Found Member ID in: {', '.join(found_files)}"
        else:
            rca_text = "Member ID not found in any CSV file."
        
        summary_data = matches_df.to_dict(orient='records')
    else:
        rca_text = "Error: No CSV master files found."
        summary_data = []

except Exception as e:
    # Improved Error Logging
    rca_text = f"Error during query: {str(e)}"
    print("Detailed Error:", rca_text)
    
    # Check if 'mem_nbr' exists in user_data to give a better hint
    try:
        cols = [x[0] for x in con.execute("DESCRIBE user_data").fetchall()]
        if 'mem_nbr' not in cols:
            rca_text += f" (Debug: user_data table only has columns: {cols})"
    except:
        pass
        
    summary_data = []

# ==========================================
# 4. UPLOAD RESULT
# ==========================================
result_payload = {
    "request_id": REQUEST_ID,
    "rca_text": rca_text,
    "summary": summary_data
}

output_filename = f"result_{REQUEST_ID}.json"
print(f"Uploading {output_filename}...")
upload_file(output_filename, result_payload)
print("Done.")
