import os
import json
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

# ==========================================
# 1. SETUP & AUTHENTICATION
# ==========================================
# Load Service Account Credentials from GitHub Secret
creds_info = json.loads(os.environ['GCP_SA_KEY'])
creds = service_account.Credentials.from_service_account_info(creds_info)
drive_service = build('drive', 'v3', credentials=creds)

# Get Environment Variables
REQUEST_ID = os.environ['REQUEST_ID']
INPUT_FILE_ID = os.environ['INPUT_FILE_ID']
FOLDER_ID = "YOUR_DRIVE_FOLDER_ID"  # <--- MAKE SURE THIS IS UPDATED

def download_file(file_id, output_path):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(output_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

def upload_file(filename, content_dict):
    file_metadata = {'name': filename, 'parents': [FOLDER_ID]}
    media = MediaIoBaseUpload(io.BytesIO(json.dumps(content_dict).encode('utf-8')),
                              mimetype='application/json')
    drive_service.files().create(body=file_metadata, media_body=media).execute()

# ==========================================
# 2. DATA PREPARATION
# ==========================================
# Initialize DuckDB
con = duckdb.connect(database=':memory:')

print(f"Processing Request: {REQUEST_ID}")

# A. Download the User's Input (The rows they typed in the web app)
download_file(INPUT_FILE_ID, 'user_input.json')

# Load User Input into DuckDB
# We assume the column name from your frontend is "mem_nbr" based on your previous code
con.execute("CREATE TABLE user_data AS SELECT * FROM read_json_auto('user_input.json')")

# B. Download ALL Master Files from Drive
print("Downloading Master Files...")
results = drive_service.files().list(
    q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
    fields="files(id, name)").execute()

files = results.get('files', [])
if not files:
    print("Warning: No CSV files found in the Drive folder.")

# Download each CSV
for item in files:
    print(f"Downloading {item['name']}...")
    # Sanitize filename (remove spaces just in case)
    safe_name = item['name'].replace(" ", "_")
    download_file(item['id'], safe_name)

# ==========================================
# 3. THE "SEARCH EVERYWHERE" QUERY
# ==========================================
print("Running Member Search...")

# We use read_csv_auto('*.csv', filename=true) 
# This magic command reads ALL CSVs at once and adds a 'filename' column
# We join this with the user_data to find matches.

# Note: Ensure the CSV column header matches 'Membership_NBR' or whatever is in your files.
# I am assuming the CSV header is "Membership_NBR" and User input is "mem_nbr"

sql_query = """
    SELECT 
        u.mem_nbr AS User_Requested_ID,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
"""

try:
    # Run the search
    matches_df = con.execute(sql_query).fetchdf()
    
    # Create the text summary
    if len(matches_df) > 0:
        found_files = matches_df['Found_In_File'].unique().tolist()
        rca_text = f"Success! Found Member ID in {len(found_files)} file(s): {', '.join(found_files)}"
    else:
        rca_text = "Member ID not found in any of the 20 master files."
        
    summary_data = matches_df.to_dict(orient='records')

except Exception as e:
    rca_text = f"Error during query: {str(e)}"
    summary_data = []
    print(rca_text)

# ==========================================
# 4. UPLOAD RESULTS
# ==========================================
result_payload = {
    "request_id": REQUEST_ID,
    "rca_text": rca_text,
    "summary": summary_data, # This contains the full row data + filename
    "metrics": [] 
}

print("Uploading Results to Drive...")
upload_file(f"result_{REQUEST_ID}.json", result_payload)
print("Process Complete.")
