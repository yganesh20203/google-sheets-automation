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
try:
    creds_info = json.loads(os.environ['GCP_SA_KEY'])
    creds = service_account.Credentials.from_service_account_info(creds_info)
except:
    print("Error loading credentials. Ensure GCP_SA_KEY env var is set.")
    exit(1)

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
def normalize_store_id(val):
    """Safely converts 4729.0, '4729', or 4729 to string '4729'."""
    if not val: return ""
    return str(val).replace('.0', '').strip()

def get_col_value(row, possible_names):
    """Case-insensitive search for a column value."""
    # Convert row keys to lower case for comparison
    row_lower = {k.lower(): v for k, v in row.items()}
    for name in possible_names:
        if name.lower() in row_lower:
            return row_lower[name.lower()]
    return None

def get_file_date(filename):
    """Parses '_Jan_26' or 'Jan 26' to datetime for sorting."""
    try:
        # Match Month (3 chars) and Year (2 or 4 digits)
        match = re.search(r'([A-Za-z]{3})[ _-]?(\d{2,4})', filename)
        if match:
            date_str = f"{match.group(1)} {match.group(2)}"
            # Handle 2 digit year vs 4 digit year
            fmt = "%b %y" if len(match.group(2)) == 2 else "%b %Y"
            return datetime.strptime(date_str, fmt)
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
    """Writes consolidated rows to Analysis_Results and updates Queue"""
    try:
        if not SPREADSHEET_ID or not values: return
        print(f"Writing {len(values)} rows to Sheet...")
        
        # 1. Append Data to Analysis_Results (Cols A-H)
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=f"{RESULTS_TAB_NAME}!A:H",
            valueInputOption="USER_ENTERED", body={'values': values}
        ).execute()

        # 2. Update Request Queue Status to COMPLETED
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
        
        # Fix: Swap Name/ID if user pasted incorrectly
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
    
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        # 1. SPECIAL FILES
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

    # 2. MONTHLY BEAT FILES
    for item in files:
        safe_name = item['name'].replace(" ", "_")
        if any(x in safe_name for x in ["Store_Guardrail", "SaveEasy", "Pan_india", "4RExtraction", "Memberwise_sales"]): continue
        all_master_files.append(safe_name)
        if not os.path.exists(safe_name): download_file(item['id'], safe_name)
    all_master_files.sort(key=get_file_date)

    current_month_file = all_master_files[-1] if all_master_files else None
    print(f"Current Month File identified as: {current_month_file}")

# ==========================================
# 4. ANALYSIS & LOGIC
# ==========================================
print("Running Analysis...")

# SQL to find member in ANY loaded CSV (except special ones)
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
    # Pre-fetch matches
    matches_df = con.execute(sql_query).fetchdf() if files else pd.DataFrame()
    
    for mem_id in input_members:
        mem_id_str = str(mem_id)
        user_matches = matches_df[matches_df['User_ID'] == mem_id_str] if not matches_df.empty else pd.DataFrame()
        
        # --- 1. GATHER INPUT DETAILS ---
        input_details = con.execute(f"SELECT store_nbr, mem_name, sub_cat_name FROM user_data WHERE CAST(mem_nbr AS VARCHAR) = '{mem_id_str}'").fetchone()
        
        input_store_clean = normalize_store_id(input_details[0]) if input_details else ""
        input_name = input_details[1] if input_details else ""
        input_sub_cat = str(input_details[2] if input_details and input_details[2] else "").strip().lower()

        matched_store_clean = ""
        matched_name = ""
        
        if not user_matches.empty:
            # We convert row to dict to safely look for columns
            first_row = user_matches.iloc[0].to_dict()
            matched_name = first_row.get('User_Name', '') or first_row.get('mem_name', '')
            
            # Find Store in matches
            raw_matched_store = get_col_value(first_row, ['store_nbr', 'Store_NBR', 'Store', 'Store NBR'])
            matched_store_clean = normalize_store_id(raw_matched_store)
        
        final_store_display = input_store_clean if input_store_clean else matched_store_clean
        final_name_display = input_name if input_name else matched_name

        # --- 2. FETCH SALES DATA FOR EMAIL ---
        sales_info_str = "No Data"
        if has_member_sales:
            try:
                sales_q = f"""
                    SELECT Current_Month, Month_Minus_1, Month_Minus_2, 
                           Month_Minus_3, Month_Minus_4, Month_Minus_5, Month_Minus_6
                    FROM member_sales 
                    WHERE CAST(MEMBERSHIP_NBR AS VARCHAR) = '{mem_id_str}'
                    ORDER BY Current_Month DESC LIMIT 1
                """
                sales_row = con.execute(sales_q).fetchone()
                if sales_row:
                    sales_info_str = f"Curr: {sales_row[0]} | M-1: {sales_row[1]} | M-2: {sales_row[2]} | M-3: {sales_row[3]} | M-4: {sales_row[4]} | M-5: {sales_row[5]} | M-6: {sales_row[6]}"
            except: pass

        # --- 3. SET FLAGS (FACT GATHERING) ---
        flag_in_current_beat = False
        current_beat_store_clean = ""
        current_beat_sub_cat = ""
        
        # A. Current Beat Check
        if current_month_file and not user_matches.empty:
            current_match = user_matches[user_matches['Found_In_File'] == current_month_file]
            if not current_match.empty:
                flag_in_current_beat = True
                curr_row = current_match.iloc[0].to_dict()
                
                # Get Store from File
                raw_store = get_col_value(curr_row, ['store_nbr', 'Store_NBR', 'Store', 'Store NBR'])
                current_beat_store_clean = normalize_store_id(raw_store)
                
                # Get Sub Cat from File
                raw_cat = get_col_value(curr_row, ['Sub Cat Name', 'Sub_Cat_Name', 'Sub Category', 'Sub_Category', 'sub_cat_name'])
                if raw_cat: current_beat_sub_cat = str(raw_cat).strip().lower()

        # B. NSU Check
        flag_is_nsu = False; flag_nsu_sales_team = False 
        if has_4r_extraction:
            try:
                qc_res = con.execute(f"SELECT QC_User_ID FROM extraction_4r WHERE CAST(Membership_Nbr AS VARCHAR) = '{mem_id_str}' LIMIT 1").fetchone()
                # Try quote version if first fails
                if not qc_res: qc_res = con.execute(f"SELECT \"QC User ID\" FROM extraction_4r WHERE CAST(\"Membership Nbr\" AS VARCHAR) = '{mem_id_str}' LIMIT 1").fetchone()
                
                if qc_res:
                    flag_is_nsu = True
                    if str(qc_res[0]).strip() in sales_team_ids: flag_nsu_sales_team = True
            except: pass

        # C. SaveEasy Check
        flag_save_easy = False
        if has_save_easy:
            try:
                if con.execute(f"SELECT COUNT(*) FROM save_easy WHERE CAST(MembershipNBR AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                    flag_save_easy = True
            except: pass

        # D. Store Guardrail & ZBDA Check
        flag_store_guard = False; zbda_sales_val = 0; zbda_store_val = ""
        if has_store_guardrail and final_store_display and mem_id_str:
            try:
                if con.execute(f"SELECT COUNT(*) FROM store_guardrail WHERE TRIM(CAST(Code AS VARCHAR)) = '{final_store_display}{mem_id_str}'").fetchone()[0] > 0:
                    flag_store_guard = True
                    if has_member_sales:
                        zbda_query = f"""
                            SELECT CAST(STORE_NUMBER AS VARCHAR), (Current_Month + Month_Minus_1 + Month_Minus_2 + Month_Minus_3 + Month_Minus_4 + Month_Minus_5 + Month_Minus_6)
                            FROM member_sales WHERE CAST(MEMBERSHIP_NBR AS VARCHAR) = '{mem_id_str}' AND CHANNEL_TYPE = 'ZBDA' LIMIT 1
                        """
                        zbda_res = con.execute(zbda_query).fetchone()
                        if zbda_res:
                            zbda_store_val = normalize_store_id(zbda_res[0])
                            zbda_sales_val = zbda_res[1] if zbda_res[1] is not None else 0
            except: pass

        # E. Pan India Check
        flag_pan_india = False
        if has_pan_india:
            try:
                if con.execute(f"SELECT COUNT(*) FROM guardrail_pan_india WHERE CAST(\"Membership no.\" AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                    flag_pan_india = True
            except:
                try: 
                    if con.execute(f"SELECT COUNT(*) FROM guardrail_pan_india WHERE CAST(Membership_no_ AS VARCHAR) = '{mem_id_str}'").fetchone()[0] > 0:
                        flag_pan_india = True
                except: pass

        # F. E-Commerce (ZECM) Check
        flag_zecm_active = False
        if has_member_sales:
            try:
                sales_res = con.execute(f"SELECT SUM(Current_Month + Month_Minus_1 + Month_Minus_2 + Month_Minus_3 + Month_Minus_4 + Month_Minus_5 + Month_Minus_6) FROM member_sales WHERE CAST(MEMBERSHIP_NBR AS VARCHAR) = '{mem_id_str}' AND CHANNEL_TYPE = 'ZECM'").fetchone()
                if sales_res and sales_res[0] and sales_res[0] > 0:
                    flag_zecm_active = True
            except: pass

        # G. History (Last Active)
        latest_missing_month = "N/A"
        found_files = []
        if not user_matches.empty: found_files = user_matches['Found_In_File'].unique().tolist()
        if len(found_files) > 0 and len(all_master_files) > len(found_files):
            missing_files = list(set(all_master_files) - set(found_files))
            missing_files.sort(key=get_file_date)
            latest_missing_month = missing_files[-1]

        # ==========================================================
        # 4. DECISION TREE (THE LOGIC CORE)
        # ==========================================================
        
        final_display_rca = ""
        final_status = "COMPLETED"

        # 1. SaveEasy (Highest Priority Reject)
        if flag_save_easy:
            final_display_rca = "REJECT: SaveEasy active member cannot be added to the Beat"
            final_status = "MATCH FOUND (SaveEasy)"

        # 2. Current Beat Plan
        elif flag_in_current_beat:
            # --- STRONG LOGIC: SAME STORE CHECK ---
            # If input store matches the store where member is found -> REJECT
            if input_store_clean and current_beat_store_clean and (input_store_clean == current_beat_store_clean):
                 final_display_rca = "REJECT: Member is already in the beat of the same store"
                 final_status = "MATCH FOUND (Same Store)"
            
            # If Member is in Beat but DIFFERENT Store (or store not defined) -> check BU
            elif input_sub_cat and current_beat_sub_cat:
                if input_sub_cat == current_beat_sub_cat:
                    final_display_rca = f"REJECT: Member already in Beat (Store {current_beat_store_clean})"
                else:
                    prof_keywords = ['o&i corp', 'horeca', 'kam']
                    groc_keywords = ['grocery common + gm kirana', 'gm common', 'grocery-kam']
                    
                    if current_beat_sub_cat in prof_keywords:
                        final_display_rca = "ACTION: Member is present under professional BU get Insti. team approval to add into beat"
                    elif current_beat_sub_cat in groc_keywords:
                        target_store = current_beat_store_clean if current_beat_store_clean else final_store_display
                        final_display_rca = f"ACTION: Member is present under grocery BU get store manager approval from {target_store} approval to add into beat"
                    else:
                        final_display_rca = f"REJECT: Member in different BU ({current_beat_sub_cat})"
            else:
                final_display_rca = f"REJECT: Member already in Beat (Store {current_beat_store_clean})"
            
            if final_status == "COMPLETED": final_status = "MATCH FOUND"

        # 3. Store Guardrail
        elif flag_store_guard:
            if zbda_sales_val > 0:
                final_display_rca = "REJECT: Member already in Store gradrail list cannot be added in beat"
            else:
                target_store = zbda_store_val if zbda_store_val else final_store_display
                final_display_rca = f"ACTION: Member in Store gradrail list get permission by {target_store} store manager"
            final_status = "MATCH FOUND (Guardrail)"

        # 4. Pan India
        elif flag_pan_india:
            final_display_rca = "ACTION: Member already in Pan bharat file get market manager approval to add in beat"
            final_status = "MATCH FOUND (Pan India)"

        # 5. E-Commerce
        elif flag_zecm_active:
            final_display_rca = "ACTION: Ecom member get approval from Ecom team"

        # 6. NSU Logic
        elif flag_is_nsu:
            if flag_nsu_sales_team:
                final_display_rca = "PROCEED: NSU member will be added to the beat"
            else:
                final_display_rca = "ACTION: NSU member onboarded by store team get store manger approval to add in beat"

        # 7. Clean Case / Unknown
        else:
            if latest_missing_month != "N/A":
                # Re-activation
                final_display_rca = f"PROCEED: Member will be added in beat (Last excluded: {latest_missing_month})"
            else:
                # Unknown / Not Found in ANY file
                final_display_rca = "REJECT: Given member not found please check the member nbr and reenter it"
                final_status = "ERROR"

        # --- WRITE ROW ---
        found_location_val = "Current Beat" if flag_in_current_beat else ("Historical Files" if len(found_files)>0 else "N/A")
        
        final_rows_to_write.append([
            REQUEST_ID,
            found_location_val,
            final_store_display,
            mem_id_str,
            final_name_display,
            final_status,
            final_display_rca,
            sales_info_str 
        ])

except Exception as e:
    print(f"Error: {e}")
    final_rows_to_write.append([REQUEST_ID, "N/A", "", "", "", "ERROR", str(e), ""])

write_values_to_sheet(final_rows_to_write)
print("Done.")
