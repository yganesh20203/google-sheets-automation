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

# *** CRITICAL FIX: HARDCODED FOLDER ID ***
# This is the ID from your screenshot URL
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
        # If it's a Google Doc/Sheet, we can't download it as binary.
        if 'fileNotDownloadable' in str(e):
             print("Skipping file (It might be a Google Sheet/Doc, not a CSV/JSON).")

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

# A. Download User Input (The JSON file)
print(f"Downloading Input File ID: {INPUT_FILE_ID}...")
download_file(INPUT_FILE_ID, 'user_input.json')

# Check if file exists before proceeding
if not os.path.exists('user_input.json'):
    # Create a dummy file if download failed just to prevent crash, but log error
    print("CRITICAL ERROR: Input file failed to download. Check the File ID.")
    with open('user_input.json', 'w') as f: f.write('[]')

try:
    con.execute("CREATE TABLE user_data AS SELECT * FROM read_json_auto('user_input.json')")
except Exception as e:
    print(f"Error reading input JSON: {e}")

# B. Download Master Files (CSVs)
print("Looking for CSV files in folder...")
results = drive_service.files().list(
    q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
    fields="files(id, name)").execute()

files = results.get('files', [])

if not files:
    print("WARNING: No CSV files found! (If your files are .xlsb, please convert at least one to CSV for testing)")
else:
    print(f"Found {len(files)} CSV files. Downloading...")
    for item in files:
        # Replace spaces to prevent SQL errors
        safe_name = item['name'].replace(" ", "_")
        print(f"Downloading {safe_name}...")
        download_file(item['id'], safe_name)

# ==========================================
# 3. ANALYSIS
# ==========================================
print("Running Analysis...")

# This query looks for the 'mem_nbr' from user_input inside ALL downloaded CSVs
# Note: Ensure your CSV column header matches 'Membership_NBR' exactly.
sql_query = """
    SELECT 
        u.mem_nbr AS User_ID,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
"""

try:
    # Only run if we actually downloaded CSVs
    if files:
        matches_df = con.execute(sql_query).fetchdf()
        
        if len(matches_df) > 0:
            found_files = matches_df['Found_In_File'].unique().tolist()
            rca_text = f"Found Member ID in: {', '.join(found_files)}"
        else:
            rca_text = "Member ID not found in any CSV file."
        
        summary_data = matches_df.to_dict(orient='records')
    else:
        rca_text = "Error: No CSV master files were found to check against."
        summary_data = []

except Exception as e:
    rca_text = f"Error during query: {str(e)}"
    summary_data = []
    print(rca_text)

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
