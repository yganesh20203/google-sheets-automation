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
        match = re.search(r'_([A-Za-z]{3})_(\d{2,4})', filename)
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
    """Writes consolidated rows to Analysis_Results"""
    try:
        if not SPREADSHEET_ID or not values: return

        print(f"Writing {len(values)} rows to Sheet...")

        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RESULTS_TAB_NAME}!A:A",
            valueInputOption="USER_ENTERED",
            body={'values': values}
        ).execute()

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!B:B").execute()
        rows_in_queue = result.get('values', [])
        
        row_index = -1
        for i, row_data in enumerate(rows_in_queue):
            if row_data and row_data[0] == REQUEST_ID:
                row_index = i + 1
                break
        
        if row_index != -1:
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!C{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [["COMPLETED"]]}
            ).execute()
            
            summary_msg = "Analysis Complete. Check 'Analysis_Results' tab."
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!G{row_index}",
                valueInputOption="USER_ENTERED", body={"values": [[summary_msg]]}
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
        
        df_input = pd.DataFrame(raw)
        df_input.columns = [x.strip().lower() for x in df_input.columns]
        
        # SMART FIX: Swap Name/ID if ID is missing but Name is numeric
        if 'mem_nbr' in df_input.columns and 'mem_name' in df_input.columns:
            def fix_id(row):
                val_id = str(row['mem_nbr']).strip()
                val_name = str(row['mem_name']).strip()
                if (val_id == '' or val_id == 'nan' or val_id == 'None') and val_name.isdigit():
                    return val_name 
                return val_id
            df_input['mem_nbr'] = df_input.apply(fix_id, axis=1)

        con.register('user_data', df_input)
        
        if 'mem_nbr' in df_input.columns:
            input_members = [x for x in df_input['mem_nbr'].astype(str).unique().tolist() if x and x != 'nan' and x != 'None']
            
    except Exception as e:
        print(f"Input Parsing Error: {e}")
        con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")
else:
    con.execute("CREATE TABLE user_data (mem_nbr VARCHAR)")

# --- B. LOAD MASTER FILES (ORDER: GUARDRAIL -> SAVEEASY -> MONTHLY) ---
all_master_files = [] 
has_save_easy = False
has_store_guardrail = False 

try:
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
        fields="files(id, name)").execute()
    files = results.get('files', [])
except: files = []

if files:
    print(f"Found {len(files)} CSV files. Starting Sequence...")
    
    # 1. PROCESS STORE GUARDRAIL FIRST
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        if "Store_Guardrail" in safe_name:
            print(">>> Loading Store_Guardrail...")
            if download_file(item['id'], safe_name):
                try:
                    # Load as ALL VARCHAR to protect the Codes
                    con.execute(f"CREATE OR REPLACE TABLE store_guardrail AS SELECT * FROM read_csv_auto('{safe_name}', all_varchar=true, union_by_name=true)")
                    has_store_guardrail = True
                except Exception as e: print(f"Failed to load Guardrail: {e}")
    
    # 2. PROCESS SAVEEASY SECOND
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        if "SaveEasy" in safe_name:
            print(">>> Loading SaveEasy...")
            if download_file(item['id'], safe_name):
                try:
                    con.execute(f"CREATE OR REPLACE TABLE save_easy AS SELECT * FROM read_csv_auto('{safe_name}', union_by_name=true)")
                    has_save_easy = True
                except Exception as e: print(f"Failed to load SaveEasy: {e}")

    # 3. PROCESS MONTHLY FILES LAST
    print(">>> Loading Monthly Files...")
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        
        # Skip the special files we just handled
        if "Store_Guardrail" in safe_name or "SaveEasy" in safe_name:
            continue
            
        all_master_files.append(safe_name)
        if not os.path.exists(safe_name):
            download_file(item['id'], safe_name)
            
    all_master_files.sort(key=get_file_date)

# ==========================================
# 4. ANALYSIS
# ==========================================
print("Running Analysis...")

sql_query = """
    SELECT 
        CAST(u.mem_nbr AS VARCHAR) AS User_ID,
        u.mem_name AS User_Name,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
    WHERE m.filename NOT LIKE '%SaveEasy%' AND m.filename NOT LIKE '%Store_Guardrail%'
"""

final_rows_to_write = []

try:
    if files:
        matches_df = con.execute(sql_query).fetchdf()
        
        for mem_id in input_members:
            mem_id_str = str(mem_id)
            user_matches = matches_df[matches_df['User_ID'] == mem_id_str]
            
            # --- 1. Monthly File Logic ---
            found_files = []
            if not user_matches.empty:
                found_files = user_matches['Found_In_File'].unique().tolist()
            
            total_files = len(all_master_files)
            found_count = len(found_files)
            
            found_location = "N/A"
            store_val = ""
            name_val = ""
            rca_text = ""
            status = "COMPLETED"

            # --- SMART STORE NUMBER RETRIEVAL ---
            input_store_val = ""
            try:
                input_row = con.execute(f"SELECT store_nbr FROM user_data WHERE CAST(mem_nbr AS VARCHAR) = '{mem_id_str}'").fetchone()
                if input_row and input_row[0]:
                    input_store_val = str(input_row[0]).replace('.0', '').strip()
            except: pass

            matched_store_val = ""
            if not user_matches.empty:
                first_record = user_matches.iloc[0]
                for col in ['store_nbr', 'Store_NBR', 'Store', 'StoreId']:
                    if col in user_matches.columns:
                        val = first_record.get(col)
                        if val and str(val).lower() != 'nan':
                            matched_store_val = str(val).replace('.0', '').strip()
                            break
                
                name_val = first_record.get('User_Name', '') or first_record.get('mem_name', '')
                status = "MATCH FOUND"

            # Priority: Monthly File Store > Input Store
            store_val = matched_store_val if matched_store_val else input_store_val

            if found_count == 0:
                rca_text = "Member not found in any monthly file."
                found_location = "N/A"
            elif found_count == total_files:
                rca_text = "Found in all monthly files."
                found_location = "All Monthly Files"
            else:
                missing_files = list(set(all_master_files) - set(found_files))
                missing_files.sort(key=get_file_date)
                latest_missing = missing_files[-1]
                rca_text = f"Member not found in {latest_missing}"
                found_location = "Multiple Files"

            # --- 2. SaveEasy Logic ---
            if has_save_easy:
                try:
                    check_se = con.execute(f"SELECT COUNT(*) FROM save_easy WHERE CAST(MembershipNBR AS VARCHAR) = '{mem_id_str}'").fetchone()
                    if check_se[0] > 0:
                        rca_text += " - SaveEasy Member" 
                        if status == "COMPLETED": status = "MATCH FOUND (SaveEasy)"
                except: pass

            # --- 3. Store Guardrail Logic ---
            if has_store_guardrail and store_val and mem_id_str:
                combined_code = f"{store_val}{mem_id_str}"
                try:
                    # Check Column A ('Code')
                    check_sg = con.execute(f"SELECT COUNT(*) FROM store_guardrail WHERE TRIM(CAST(Code AS VARCHAR)) = '{combined_code}'").fetchone()
                    if check_sg[0] > 0:
                        rca_text += " - Member in Store Guardrail list"
                except Exception as e:
                    print(f"Error checking guardrail: {e}")

            # --- 4. Final Row ---
            final_rows_to_write.append([
                REQUEST_ID,
                found_location,
                store_val,
                mem_id_str,
                name_val,
                status,
                rca_text
            ])

    else:
        final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", "No Master Files Found"])

except Exception as e:
    print(f"Error: {e}")
    final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", str(e)])

# ==========================================
# 5. WRITE RESULT
# ==========================================
write_values_to_sheet(final_rows_to_write)
print("Done.")
