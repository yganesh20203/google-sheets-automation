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

# CONFIG: NSU Sheets
NSU_SPREADSHEET_ID = "1CHBcnNoVhW025l486C004VU7xHWgEQj6Xs7wkZXeelA"
NSU_CONFIG = {
    931173305: 'D',   # BDA ShortID
    1119970190: 'M',  # BDA Short ID
    899736083: 'G'    # Supervisor Short ID
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_file_date(filename):
    """Parses '_Jan_26' to datetime for sorting."""
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

def get_sales_team_ids():
    """Fetches valid IDs from specific columns in the 3 Google Sheet tabs."""
    valid_ids = set()
    print(">>> Fetching Sales Team Data from Google Sheets...")
    try:
        meta = sheets_service.spreadsheets().get(spreadsheetId=NSU_SPREADSHEET_ID).execute()
        sheets_info = meta.get('sheets', [])
        for sheet in sheets_info:
            gid = sheet['properties']['sheetId']
            title = sheet['properties']['title']
            if gid in NSU_CONFIG:
                col_letter = NSU_CONFIG[gid]
                range_name = f"'{title}'!{col_letter}:{col_letter}"
                try:
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=NSU_SPREADSHEET_ID, range=range_name).execute()
                    rows = result.get('values', [])
                    for row in rows:
                        if row:
                            val = str(row[0]).strip()
                            if val and val.lower() not in ['bda shortid', 'bda short id', 'supervisor short id']:
                                valid_ids.add(val)
                except Exception as e: print(f"Error reading {range_name}: {e}")
        print(f"Loaded {len(valid_ids)} unique Sales Team IDs.")
        return valid_ids
    except Exception as e:
        print(f"Failed to fetch Sales Team Sheets: {e}")
        return set()

def write_values_to_sheet(values):
    """Writes consolidated rows to Analysis_Results"""
    try:
        if not SPREADSHEET_ID or not values: return
        print(f"Writing {len(values)} rows to Sheet...")
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=f"{RESULTS_TAB_NAME}!A:A",
            valueInputOption="USER_ENTERED", body={'values': values}
        ).execute()

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{QUEUE_TAB_NAME}!B:B").execute()
        rows_in_queue = result.get('values', [])
        
        row_index = -1
        for i, row_data in enumerate(rows_in_queue):
            if row_data and row_data[0] == REQUEST_ID:
                row_index = i + 1; break
        
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
    except HttpError as e: print(f"Sheets API Error: {e}")

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
        
        # SMART FIX: Swap Name/ID
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

# --- B. LOAD EXTERNAL DATA ---
sales_team_ids = get_sales_team_ids()

# --- C. LOAD MASTER FILES ---
all_master_files = [] 
has_save_easy = False; has_store_guardrail = False; has_pan_india = False
has_4r_extraction = False; has_member_sales = False

try:
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false",
        fields="files(id, name)").execute()
    files = results.get('files', [])
except: files = []

if files:
    print(f"Found {len(files)} CSV files. Loading...")
    
    # 1. SPECIAL FILES
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        if "4RExtraction" in safe_name: 
            if download_file(item['id'], safe_name):
                try: con.execute(f"CREATE OR REPLACE TABLE extraction_4r AS SELECT * FROM read_csv_auto('{safe_name}', all_varchar=true, union_by_name=true)"); has_4r_extraction = True
                except: pass
        elif "Memberwise_sales" in safe_name:
            if download_file(item['id'], safe_name):
                try: con.execute(f"CREATE OR REPLACE TABLE member_sales AS SELECT * FROM read_csv_auto('{safe_name}', union_by_name=true)"); has_member_sales = True
                except: pass
        elif "Store_Guardrail" in safe_name and "Pan_india" not in safe_name:
            if download_file(item['id'], safe_name):
                try: con.execute(f"CREATE OR REPLACE TABLE store_guardrail AS SELECT * FROM read_csv_auto('{safe_name}', all_varchar=true, union_by_name=true)"); has_store_guardrail = True
                except: pass
        elif "Pan_india" in safe_name:
            if download_file(item['id'], safe_name):
                try: con.execute(f"CREATE OR REPLACE TABLE guardrail_pan_india AS SELECT * FROM read_csv_auto('{safe_name}', all_varchar=true, union_by_name=true)"); has_pan_india = True
                except: pass
        elif "SaveEasy" in safe_name:
            if download_file(item['id'], safe_name):
                try: con.execute(f"CREATE OR REPLACE TABLE save_easy AS SELECT * FROM read_csv_auto('{safe_name}', union_by_name=true)"); has_save_easy = True
                except: pass

    # 2. MONTHLY FILES
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        if any(x in safe_name for x in ["Store_Guardrail", "SaveEasy", "Pan_india", "4RExtraction", "Memberwise_sales"]): continue
        all_master_files.append(safe_name)
        if not os.path.exists(safe_name): download_file(item['id'], safe_name)
    all_master_files.sort(key=get_file_date)

    # IDENTIFY CURRENT MONTH FILE (The latest one, e.g., Jan_26)
    current_month_file = all_master_files[-1] if all_master_files else None
    print(f"Current Month File identified as: {current_month_file}")

# ==========================================
# 4. ANALYSIS
# ==========================================
print("Running Analysis...")

# Union of all monthly files (including current month)
sql_query = """
    SELECT 
        CAST(u.mem_nbr AS VARCHAR) AS User_ID,
        u.mem_name AS User_Name,
        m.filename AS Found_In_File,
        m.* FROM user_data u
    JOIN read_csv_auto('*.csv', filename=true, union_by_name=true) m 
      ON CAST(u.mem_nbr AS VARCHAR) = CAST(m."Membership_NBR" AS VARCHAR)
    WHERE m.filename NOT LIKE '%SaveEasy%' 
      AND m.filename NOT LIKE '%Store_Guardrail%'
      AND m.filename NOT LIKE '%Pan_india%'
      AND m.filename NOT LIKE '%4RExtraction%'
      AND m.filename NOT LIKE '%Memberwise_sales%'
"""

final_rows_to_write = []

try:
    if files:
        matches_df = con.execute(sql_query).fetchdf()
        
        for mem_id in input_members:
            mem_id_str = str(mem_id)
            user_matches = matches_df[matches_df['User_ID'] == mem_id_str]
            
            # --- PREPARE BASIC DETAILS ---
            # Retrieve Store, Name, SubCat from Input
            input_details = con.execute(f"SELECT store_nbr, mem_name, sub_cat_name FROM user_data WHERE CAST(mem_nbr AS VARCHAR) = '{mem_id_str}'").fetchone()
            input_store = str(input_details[0] if input_details else "").replace('.0', '').strip()
            input_name = input_details[1] if input_details else ""
            input_sub_cat = str(input_details[2] if input_details and input_details[2] else "").strip().lower()

            # Retrieve from Matches (Fallback)
            matched_store = ""
            matched_name = ""
            if not user_matches.empty:
                first = user_matches.iloc[0]
                matched_name = first.get('User_Name', '') or first.get('mem_name', '')
                for col in ['store_nbr', 'Store_NBR', 'Store']:
                    if col in user_matches.columns:
                        val = first.get(col)
                        if val and str(val).lower() != 'nan': matched_store = str(val).replace('.0', '').strip(); break
            
            final_store = input_store if input_store else matched_store
            final_name = input_name if input_name else matched_name

            # ==========================================================
            # PRIORITY 1: CURRENT MONTH SUB-CAT CHECK
            # ==========================================================
            current_month_status = None
            if current_month_file:
                # Check if member exists in the Current Month File specifically
                current_match = user_matches[user_matches['Found_In_File'] == current_month_file]
                
                if not current_match.empty:
                    # Member Found in Current Month -> Check Sub Category
                    # We look for common Sub Cat column names
                    file_sub_cat = ""
                    current_row = current_match.iloc[0]
                    for col in ['Sub Cat Name', 'Sub_Cat_Name', 'Sub Category', 'Sub_Category', 'sub_cat_name']:
                        if col in current_match.columns:
                            val = current_row.get(col)
                            if val: file_sub_cat = str(val).strip().lower(); break
                    
                    if file_sub_cat and input_sub_cat:
                        if file_sub_cat == input_sub_cat:
                            current_month_status = "Member already present in beat"
                        else:
                            current_month_status = f"Member already in different BU ({file_sub_cat})"
                    elif not input_sub_cat:
                        # Fallback if user didn't provide sub cat but member found
                        current_month_status = "Member already present in beat (No Input Sub-Cat)"

            # ==========================================================
            # DECISION LOGIC
            # ==========================================================
            
            final_rca = ""
            final_status = "COMPLETED"

            if current_month_status:
                # STOP Analysis here if found in current month
                final_rca = current_month_status
                final_status = "MATCH FOUND"
            else:
                # PROCEED with Full Analysis (NSU -> History -> Guardrails)
                
                # 1. NSU Check
                nsu_text = ""
                if has_4r_extraction:
                    try:
                        qc_query = f"SELECT \"QC User ID\" FROM extraction_4r WHERE CAST(\"Membership Nbr\" AS VARCHAR) = '{mem_id_str}' LIMIT 1"
                        try: qc_res = con.execute(qc_query).fetchone()
                        except: qc_res = con.execute(f"SELECT QC_User_ID FROM extraction_4r WHERE CAST(Membership_Nbr AS VARCHAR) = '{mem_id_str}' LIMIT 1").fetchone()

                        if qc_res:
                            if str(qc_res[0]).strip() in sales_team_ids: nsu_text = "found match, NSU member onboarded by sales team"
                            else: nsu_text = "NSU member not onboarded by sales team"
                        else: nsu_text = "not NSU member"
                    except: nsu_text = "Error checking NSU status"
                else: nsu_text = "4RExtraction.csv not found"

                # 2. Monthly History
                found_files = []
                if not user_matches.empty: found_files = user_matches['Found_In_File'].unique().tolist()
                
                found_count = len(found_files)
                total_files = len(all_master_files)
                
                monthly_text = ""
                found_loc = "N/A"
                if found_count > 0: final_status = "MATCH FOUND"

                if found_count == 0:
                    monthly_text = "Member not found in any monthly file."
                elif found_count == total_files:
                    monthly_text = "Found in all monthly files."
                    found_loc = "All Monthly Files"
                else:
                    missing_files = list(set(all_master_files) - set(found_files))
                    missing_files.sort(key=get_file_date)
                    latest_missing = missing_files[-1]
                    monthly_text = f"Member not found in {latest_missing}"
                    found_loc = "Multiple Files"

                # 3. Guardrails & Ecom
                guardrail_text = ""
                if has_save_easy:
                    try:
                        if con.execute(f"SELECT COUNT(*) FROM save_easy WHERE CAST(MembershipNBR AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                            guardrail_text += " - SaveEasy Member"
                    except: pass
                
                if has_store_guardrail and final_store and mem_id_str:
                    try:
                        if con.execute(f"SELECT COUNT(*) FROM store_guardrail WHERE TRIM(CAST(Code AS VARCHAR)) = '{final_store}{mem_id_str}'").fetchone()[0] > 0:
                            guardrail_text += " - Member in Store Guardrail list"
                    except: pass

                if has_pan_india:
                    try:
                        if con.execute(f"SELECT COUNT(*) FROM guardrail_pan_india WHERE CAST(\"Membership no.\" AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                            guardrail_text += " - member in Guardrail_Pan_india_list"
                    except: 
                        try:
                             if con.execute(f"SELECT COUNT(*) FROM guardrail_pan_india WHERE CAST(Membership_no_ AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                                guardrail_text += " - member in Guardrail_Pan_india_list"
                        except: pass

                if has_member_sales:
                    try:
                        sales_res = con.execute(f"SELECT SUM(Current_Month + Month_Minus_1 + Month_Minus_2 + Month_Minus_3 + Month_Minus_4 + Month_Minus_5 + Month_Minus_6) FROM member_sales WHERE CAST(MEMBERSHIP_NBR AS VARCHAR) = '{mem_id_str}' AND CHANNEL_TYPE = 'ZECM'").fetchone()
                        if sales_res and sales_res[0] and sales_res[0] > 0:
                            guardrail_text += " - Ecom member need approval from Ecom team"
                    except: pass

                final_rca = f"{nsu_text} - {monthly_text}{guardrail_text}"

            # --- WRITE ROW ---
            found_location_val = "Current Beat" if current_month_status else (found_loc if 'found_loc' in locals() else "N/A")
            
            final_rows_to_write.append([
                REQUEST_ID,
                found_location_val,
                final_store,
                mem_id_str,
                final_name,
                final_status,
                final_rca
            ])
    else:
        final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", "No Master Files Found"])

except Exception as e:
    print(f"Error: {e}")
    final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", str(e)])

write_values_to_sheet(final_rows_to_write)
print("Done.")
