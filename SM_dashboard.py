import os
import io
import pandas as pd
import numpy as np
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import time

# Scopes for Drive (downloading .xlsb) and Sheets (updating the dashboard)
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

STORE_MAPPING = {
    "4702": "Amritsar", "4703": "Zirakpur", "4706": "Jalandhar", "4712": "Bhopal-1",
    "4713": "Kota", "4716": "Raipur", "4717": "Ludhiana-1", "4719": "Jammu",
    "4720": "Meerut", "4721": "Vijayawada", "4723": "Agra-1", "4724": "Aurangabad",
    "4725": "Guntur", "4727": "Lucknow", "4729": "Indore-1", "4734": "Hyderabad",
    "4742": "Rajahmundry", "4744": "Amravati", "4760": "Bhopal-2", "4797": "Agra-2",
    "4799": "Visakhapatnam", "4801": "Ludhiana-3", "4803": "Indore-2", "4805": "Tirupathi",
    "4813": "Karimnagar", "4814": "Kurnool"
}

def authenticate_service_account():
    print("Authenticating with Service Account...")
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        return creds
    except Exception as e:
        print(f"Authentication failed. Check your credentials.json file. Error: {e}")
        raise

def download_from_drive(drive_service, filename='Daily_KPI_Processing.xlsb'):
    """Finds and downloads the file from Google Drive."""
    print(f"\n--- Processing Sales KPI Data ---")
    print(f"Searching Drive for {filename}...")
    results = drive_service.files().list(
        q=f"name='{filename}' and trashed=false",
        fields="files(id, name)"
    ).execute()
    
    items = results.get('files', [])
    if not items:
        print("No file found in Drive. Ensure the Apps Script ran successfully.")
        return None
        
    file_id = items[0]['id']
    print(f"Downloading file ID: {file_id}...")
    
    request = drive_service.files().get_media(fileId=file_id)
    file_path = 'temp_downloaded.xlsb'
    
    with io.FileIO(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
    return file_path

def process_and_update_sheet(creds, xlsb_path):
    """Processes the .xlsb and updates both FTD and MTD in the Master Google Sheet."""
    print("Reading .xlsb data...")
    df = pd.read_excel(xlsb_path, sheet_name='Store Wise Raw Working', engine='pyxlsb', header=None)
    
    # 1. Force Store Code (Index 3 / Col D) to string
    df[3] = df[3].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # ==========================================
    # 2. UPGRADED MAPPING DICTIONARY (FTD & MTD)
    # Format: "Metric Name": {"FTD": xlsb_col_index, "MTD": xlsb_col_index}
    # IMPORTANT: Update the MTD numbers below to match your actual .xlsb columns!
    # ==========================================
    metric_mapping = {
        "Sales Tgt": {"FTD": 8,  "MTD": 7},    # FTD = Col I, MTD = Col J (Example)
        "Sales Ach": {"FTD": 15, "MTD": 14},   # FTD = Col P, MTD = Col Q (Example)
        "MAC Plan":  {"FTD": 23, "MTD": 22},   
        "MAC Actual":{"FTD": 29, "MTD": 28},
        "Lines Plan":{"FTD": 32, "MTD": 31},
        "Lines Act": {"FTD": 37, "MTD": 36},
        "OTGS Plan": {"FTD": 16, "MTD": 17},
        "OTGS Act":  {"FTD": 18, "MTD": 19},
        "Txns":      {"FTD": 41, "MTD": 40}
    }
    
    # 3. Extract all unique column indices we need to process
    cols_to_sum = []
    for mapping in metric_mapping.values():
        cols_to_sum.extend([mapping["FTD"], mapping["MTD"]])
    cols_to_sum = list(set(cols_to_sum)) # Remove duplicates
    
    # Clean and convert ALL mapped columns to numeric at once
    for col_idx in cols_to_sum:
        df[col_idx] = pd.to_numeric(df[col_idx], errors='coerce').fillna(0)
    
    # 4. Group by Store Code and sum ALL target columns simultaneously
    grouped_data = df.groupby(3)[cols_to_sum].sum().to_dict('index')
    
    print("Connecting to target Google Sheet for Sales KPIs...")
    gc = gspread.authorize(creds)
    sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    worksheet = gc.open_by_key(sheet_id).worksheet('Store_Data') 
    
    target_data = worksheet.get_all_values()
    cells_to_update = []
    
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    # 5. Loop through the Google Sheet and update FTD, MTD, and Last Updated
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if store_code in grouped_data and cell_type in metric_mapping:
                
                # Get the specific column indexes for this metric
                ftd_col_index = metric_mapping[cell_type]["FTD"]
                mtd_col_index = metric_mapping[cell_type]["MTD"]
                
                # Extract the summed values
                ftd_val = grouped_data[store_code][ftd_col_index]
                mtd_val = grouped_data[store_code][mtd_col_index]
                
                # Queue FTD Update (Column C / col=3)
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                
                # Queue MTD Update (Column D / col=4)
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                
                # Queue Timestamp Update (Column E / col=5)
                # (We only need to push the timestamp once per row, so doing it here is perfect)
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        # Divide by 3 because we are pushing 3 cells per row (FTD, MTD, Timestamp)
        print(f"Updating {len(cells_to_update)//3} Sales records (FTD & MTD) in Google Sheets...")
        worksheet.update_cells(cells_to_update)
        print("Sales KPI Update complete!")
    else:
        print("No matching rows found to update for Sales KPIs.")

def update_damage_metric(creds):
    """Fetches Damage data and updates dashboard ONLY if data has changed."""
    print(f"\n--- Processing Secondary Google Sheet Data (FTD & MTD) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1b1JvfLQPhqp160-FA0TCI-10kmnWlDgfY1RM1YR4Qa4'
    try:
        source_ws = gc.open_by_key(source_sheet_id).sheet1 
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open source sheet. Error: {e}")
        return
        
    df_source = pd.DataFrame(source_data)
    if df_source.empty or len(df_source.columns) < 5:
        print("Damage source sheet is empty or doesn't have enough columns.")
        return
        
    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Calculating FTD for {yesterday} and MTD from {start_of_month} to {yesterday}")
    
    # --- Date Fixes ---
    pd_yesterday = pd.to_datetime(yesterday)
    pd_start_of_month = pd.to_datetime(start_of_month)
    
    df_source[0] = pd.to_datetime(df_source[0], errors='coerce').dt.normalize()
    df_source = df_source.dropna(subset=[0])
    
    # Create Filtered DataFrames
    df_ftd = df_source[df_source[0] == pd_yesterday].copy()
    df_mtd = df_source[(df_source[0] >= pd_start_of_month) & (df_source[0] <= pd_yesterday)].copy()
    
    if df_mtd.empty:
        print("No MTD data found for this month.")
        return
        
    # 3. Clean and Map Columns
    sheet_metric_mapping = {
        "DT(Damage)": 4,    # Column E
        "DD(Expiry)": 5,    # Column F 
        "CO(shrink)": 6     # Column G 
    }
    cols_to_sum = list(sheet_metric_mapping.values())
    
    # Force Numeric
    for df in [df_ftd, df_mtd]:
        if not df.empty:
            df[1] = df[1].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            for col_idx in cols_to_sum:
                if col_idx in df.columns: 
                    df[col_idx] = pd.to_numeric(df[col_idx], errors='coerce').fillna(0)
    
    # Group by Store Code
    grouped_ftd = df_ftd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_ftd.empty else {}
    grouped_mtd = df_mtd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_mtd.empty else {}
    
    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Check if metric is in our mapping AND the store has at least some MTD data
            if cell_type in sheet_metric_mapping and store_code in grouped_mtd:
                col_index = sheet_metric_mapping[cell_type]
                
                # A. Get NEW calculated values
                new_ftd = float(grouped_ftd.get(store_code, {}).get(col_index, 0))
                new_mtd = float(grouped_mtd.get(store_code, {}).get(col_index, 0))
                
                # B. Get OLD values from sheet (Columns C and D)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    # Update Timestamp ONLY if numbers changed
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Secondary Sheet Update complete!")
    else:
        print("No changes detected in Damage/Expiry/Shrink data.")

def update_third_metric(creds):
    """Fetches T-2 data, calculates FTD/MTD, and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Third Google Sheet (T-2 Data) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1Zg01KzKUefdKvONNmed7PRL7WU95BpAuvouo-nKk1kw'
    try:
        source_ws = gc.open_by_key(source_sheet_id).sheet1 
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open third source sheet. Error: {e}")
        return
        
    df_source = pd.DataFrame(source_data)
    if df_source.empty or len(df_source.columns) < 9: # Needs at least up to Column I (index 8)
        print("Third source sheet is empty or doesn't have enough columns.")
        return
        
    # 2. Determine Dates (T-2 and Start of Month)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(ist_timezone).date()
    
    # FTD target is Today - 2 days
    target_date = today - timedelta(days=2)
    # MTD start is the 1st of the month that the target_date belongs to
    start_of_month = target_date.replace(day=1) 
    
    print(f"Calculating FTD for {target_date} and MTD from {start_of_month} to {target_date}")
    
    # --- Date Fix: Convert Python dates to Pandas Timestamps ---
    pd_target_date = pd.to_datetime(target_date)
    pd_start_of_month = pd.to_datetime(start_of_month)
    
    # Parse Date Column D (Index 3)
    df_source[3] = pd.to_datetime(df_source[3], errors='coerce').dt.normalize()
    df_source = df_source.dropna(subset=[3]) # Remove rows with invalid dates
    
    # Filter DataFrames for FTD and MTD using Timestamp comparison
    df_ftd = df_source[df_source[3] == pd_target_date].copy()
    df_mtd = df_source[(df_source[3] >= pd_start_of_month) & (df_source[3] <= pd_target_date)].copy()
    
    if df_mtd.empty:
        print("No MTD data found for this period.")
        return
        
    # 3. MAPPING DICTIONARY
    sheet_metric_mapping = {
        "OFR - No of Orders Effected": 8  # 8 is Column I
    }
    cols_to_sum = list(sheet_metric_mapping.values())
    
    # Clean Store Code (Col B / Index 1) and mapped metrics
    for df in [df_ftd, df_mtd]:
        if not df.empty:
            df[1] = df[1].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            for col_idx in cols_to_sum:
                if col_idx in df.columns: 
                    df[col_idx] = pd.to_numeric(df[col_idx], errors='coerce').fillna(0)
    
    # Group by Store Code and sum
    grouped_ftd = df_ftd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_ftd.empty else {}
    grouped_mtd = df_mtd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_mtd.empty else {}
    
    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in sheet_metric_mapping and store_code in grouped_mtd:
                col_index = sheet_metric_mapping[cell_type]
                
                # A. Get NEW values
                new_ftd = float(grouped_ftd.get(store_code, {}).get(col_index, 0))
                new_mtd = float(grouped_mtd.get(store_code, {}).get(col_index, 0))
                
                # B. Get OLD values from Google Sheet
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Update FTD (Col C)
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    # Update MTD (Col D)
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    # Update Timestamp (Col E) - ONLY if changed
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Third Sheet Update complete!")
    else:
        print("No changes detected in T-2 data.")


def update_fourth_metric(creds):
    """Fetches FTD/MTD data from 4th Sheet and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Fourth Google Sheet (Store Name Mapping) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '14Dpes2Cs2TXVj6bmu8TbKwvHbbekK72fuW9LhjhdyPc'
    try:
        # GID 63795866 is targeted specifically
        source_ws = gc.open_by_key(source_sheet_id).get_worksheet_by_id(63795866) 
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open fourth source sheet. Error: {e}")
        return

    # Helper: Convert sheet values (like "2.4%") to numbers (0.024)
    # We will use this for BOTH the source data AND checking the old data!
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']:
            return 0.0
            
        is_percent = False
        if val_str.endswith('%'):
            val_str = val_str[:-1] # Remove the '%' symbol
            is_percent = True
            
        try:
            num = float(val_str)
            # If it was a percentage, divide by 100
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    # 2. Extract FTD Data (Rows 33 to 64 -> Python Index 32 to 64)
    ftd_data = {}
    for row in source_data[32:64]:
        if len(row) > 0: 
            # Store names are in Column A (Index 0)
            store_name = str(row[0]).strip()
            if store_name:
                ftd_data[store_name] = {i: safe_float(val) for i, val in enumerate(row)}

    # Extract MTD Data (Rows 72 to 102 -> Python Index 71 to 102)
    mtd_data = {}
    for row in source_data[71:102]:
        if len(row) > 0: 
            store_name = str(row[0]).strip()
            if store_name:
                mtd_data[store_name] = {i: safe_float(val) for i, val in enumerate(row)}

    # ==========================================
    # 3. MAPPING TO DASHBOARD ROWS
    # ==========================================
    fourth_sheet_mapping = {
        "Canc%":   {"FTD": 2,  "MTD": 2}, # C mapped to C
        "RTO%":    {"FTD": 3,  "MTD": 3}, # D mapped to D
        "D1":      {"FTD": 9,  "MTD": 8}, # J mapped to I
        "D2":      {"FTD": 10, "MTD": 9},
        "OFR %":   {"FTD": 12, "MTD": 11} 
    }

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Ensure STORE_MAPPING is defined in your main script!
            if cell_type in fourth_sheet_mapping and store_code in STORE_MAPPING:
                
                target_store_name = STORE_MAPPING[store_code]
                
                ftd_col_idx = fourth_sheet_mapping[cell_type]["FTD"]
                mtd_col_idx = fourth_sheet_mapping[cell_type]["MTD"]
                
                # A. Get NEW values
                new_ftd = ftd_data.get(target_store_name, {}).get(ftd_col_idx, 0)
                new_mtd = mtd_data.get(target_store_name, {}).get(mtd_col_idx, 0)
                
                # B. Get OLD values (Using safe_float to handle existing % signs correctly)
                old_ftd = safe_float(row[2]) if len(row) > 2 else 0.0
                old_mtd = safe_float(row[3]) if len(row) > 3 else 0.0
                
                # C. Compare (Change Detection)
                # We use a small epsilon (1e-9) because floating point math is weird
                if abs(new_ftd - old_ftd) > 0.000001 or abs(new_mtd - old_mtd) > 0.000001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    # Update Timestamp ONLY if changed
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Fourth Sheet Update complete!")
    else:
        print("No changes detected in Fourth Sheet data.")


def update_fifth_metric(creds):
    """Fetches 'AR' data, calculates averages, and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Fifth Google Sheet (AR Metric) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1NiQ-PpFDgruxsSmW-jfLSEgdejQq5EaJXHFJ5zwJn30'
    try:
        # Targeting GID 1023010443
        source_ws = gc.open_by_key(source_sheet_id).get_worksheet_by_id(1023010443) 
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open fifth source sheet. Error: {e}")
        return

    if len(source_data) < 3:
        print("Fifth sheet does not have enough rows for data extraction.")
        return

    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    current_year = yesterday.year

    print(f"Finding FTD for {yesterday} and MTD Average from {start_of_month} to {yesterday}")

    # 3. Dynamically find the Column Indexes in Row 2 (Python Index 1)
    date_row = source_data[1] 
    ftd_col_idx = -1
    mtd_col_indices = []

    for idx, cell_val in enumerate(date_row):
        val_str = str(cell_val).strip().rstrip(',')
        if not val_str: 
            continue
            
        try:
            # Safely append the year so Pandas parses it perfectly without warnings
            if str(current_year) not in val_str:
                val_str = f"{val_str} {current_year}"
                
            parsed_date = pd.to_datetime(val_str).date()
            
            # Map the exact columns
            if parsed_date == yesterday:
                ftd_col_idx = idx
            if start_of_month <= parsed_date <= yesterday:
                mtd_col_indices.append(idx)
        except Exception:
            pass # Ignore cells that aren't dates

    if ftd_col_idx == -1 and not mtd_col_indices:
        print("Could not find yesterday's date or current month dates in Row 2.")
        return

    # Helper function to clean numbers (handles percentages if they exist)
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        
        is_percent = False
        if val_str.endswith('%'):
            val_str = val_str[:-1]
            is_percent = True
            
        try:
            num = float(val_str)
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    # 4. Extract Data from Row 3 onwards (Python Index 2+)
    ftd_data = {}
    mtd_data = {}

    for row in source_data[2:]:
        if len(row) > 1:
            # Store names are in Column B (Index 1)
            store_name = str(row[1]).strip() 
            if store_name:
                
                # Extract FTD
                if ftd_col_idx != -1 and ftd_col_idx < len(row):
                    ftd_data[store_name] = safe_float(row[ftd_col_idx])
                else:
                    ftd_data[store_name] = 0.0
                
                # Extract MTD and calculate AVERAGE
                if mtd_col_indices:
                    # Get all valid numeric values for the month up to yesterday
                    mtd_vals = [safe_float(row[i]) for i in mtd_col_indices if i < len(row)]
                    # Average them out (Sum / Count)
                    mtd_data[store_name] = sum(mtd_vals) / len(mtd_col_indices) if len(mtd_col_indices) > 0 else 0.0
                else:
                    mtd_data[store_name] = 0.0

    # 5. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Match strictly against "AR" and our Store Mapping
            if cell_type == "AR" and store_code in STORE_MAPPING:
                
                target_store_name = STORE_MAPPING[store_code]
                
                # A. Get NEW values
                new_ftd = ftd_data.get(target_store_name, 0.0)
                new_mtd = mtd_data.get(target_store_name, 0.0)
                
                # B. Get OLD values (safely handle existing data)
                old_ftd = safe_float(row[2]) if len(row) > 2 else 0.0
                old_mtd = safe_float(row[3]) if len(row) > 3 else 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.000001 or abs(new_mtd - old_mtd) > 0.000001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    # Update Timestamp ONLY if changed
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Fifth Sheet (AR) Update complete!")
    else:
        print("No changes detected in AR metric.")



def calculate_derived_metrics(creds):
    """Calculates metrics and inherits the Last Updated time from the base metric."""
    print("\n--- Calculating Derived Metrics (Timestamp Aware) ---")
    gc = gspread.authorize(creds)
    
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    worksheet = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = worksheet.get_all_values()
    
    # store_data structure: 
    # {'4702': {'Sales Ach': {'FTD': 100, 'MTD': 500, 'TIME': '12-Feb...'}, ...}}
    store_data = {}
    row_mappings = {} 
    
    # --- FIRST PASS: Collect all base metrics, values, AND TIMESTAMPS ---
    for idx, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            if not store_code: continue
            if store_code not in store_data: store_data[store_code] = {}
            
            # Safely convert FTD and MTD to floats
            try:
                ftd = float(str(row[2]).replace(',', '').replace('%', '').strip() or 0)
            except: ftd = 0.0
            try:
                mtd = float(str(row[3]).replace(',', '').replace('%', '').strip() or 0)
            except: mtd = 0.0
            
            # --- CAPTURE TIMESTAMP (Column E / Index 4) ---
            # If the row is short, default to empty string
            timestamp = str(row[4]) if len(row) > 4 else ""
            
            store_data[store_code][cell_type] = {'FTD': ftd, 'MTD': mtd, 'TIME': timestamp}
            
            # Smart context for repeated "Vs Plan" rows
            if cell_type == "Vs Plan":
                prev_type = str(target_data[idx-1][1]).strip() if idx > 0 else ""
                
                if prev_type == "Sales Ach": unique_key = "Sales Vs Plan"
                elif prev_type == "MAC Actual": unique_key = "MAC Vs Plan"
                elif prev_type == "Lines Act": unique_key = "Lines Vs Plan"
                elif prev_type == "OTGS Act": unique_key = "OTGS Sales Vs Plan"
                else: unique_key = f"Unknown_Vs_Plan_{idx}"
                
                row_mappings[(store_code, unique_key)] = idx + 1 # 1-indexed for gspread
            else:
                row_mappings[(store_code, cell_type)] = idx + 1

    cells_to_update = []
    
    # Fallback time if base metric has no timestamp (rare)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time_fallback = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    def safe_div(num, den):
        return num / den if den else 0.0

    # --- SECOND PASS: Calculate and Queue Updates ---
    for store_code, metrics in store_data.items():
        
        # Helper to grab the timestamp from the "Actual" metric
        def get_time(metric_name):
            return metrics.get(metric_name, {}).get("TIME", current_time_fallback)
        
        # Calculate derived metrics + Attach the Inherited Time
        derived_calcs = {
            "Sales Vs Plan": {
                "FTD": safe_div(metrics.get("Sales Ach", {}).get("FTD", 0), metrics.get("Sales Tgt", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Sales Ach", {}).get("MTD", 0), metrics.get("Sales Tgt", {}).get("MTD", 0)),
                "TIME": get_time("Sales Ach") # Inherit from Sales Ach
            },
            "MAC Vs Plan": {
                "FTD": safe_div(metrics.get("MAC Actual", {}).get("FTD", 0), metrics.get("MAC Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("MAC Actual", {}).get("MTD", 0), metrics.get("MAC Plan", {}).get("MTD", 0)),
                "TIME": get_time("MAC Actual")
            },
            "Lines Vs Plan": {
                "FTD": safe_div(metrics.get("Lines Act", {}).get("FTD", 0), metrics.get("Lines Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Lines Act", {}).get("MTD", 0), metrics.get("Lines Plan", {}).get("MTD", 0)),
                "TIME": get_time("Lines Act")
            },
            "LPB": {
                "FTD": safe_div(metrics.get("Lines Act", {}).get("FTD", 0), metrics.get("Txns", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Lines Act", {}).get("MTD", 0), metrics.get("Txns", {}).get("MTD", 0)),
                "TIME": get_time("Lines Act")
            },
            "OTGS Sales Vs Plan": {
                "FTD": safe_div(metrics.get("OTGS Act", {}).get("FTD", 0), metrics.get("OTGS Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("OTGS Act", {}).get("MTD", 0), metrics.get("OTGS Plan", {}).get("MTD", 0)),
                "TIME": get_time("OTGS Act")
            },
            "ABV": {
                "FTD": safe_div(metrics.get("Sales Ach", {}).get("FTD", 0), metrics.get("Txns", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Sales Ach", {}).get("MTD", 0), metrics.get("Txns", {}).get("MTD", 0)),
                "TIME": get_time("Sales Ach")
            },
            "TPC": {
                "FTD": safe_div(metrics.get("Txns", {}).get("FTD", 0), metrics.get("MAC Actual", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Txns", {}).get("MTD", 0), metrics.get("MAC Actual", {}).get("MTD", 0)),
                "TIME": get_time("Txns")
            },
            "DT%": {
                "FTD": safe_div(metrics.get("DT(Damage)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("DT(Damage)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0)),
                "TIME": get_time("DT(Damage)") # Inherit from Damage Metric
            },
            "DD%": {
                "FTD": safe_div(metrics.get("DD(Expiry)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("DD(Expiry)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0)),
                "TIME": get_time("DD(Expiry)")
            },
            "CO%": {
                "FTD": safe_div(metrics.get("CO(shrink)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("CO(shrink)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0)),
                "TIME": get_time("CO(shrink)")
            }
        }
        
        # Build the batch update payload
        for calc_name, vals in derived_calcs.items():
            if (store_code, calc_name) in row_mappings:
                row_idx = row_mappings[(store_code, calc_name)]
                cells_to_update.append(gspread.Cell(row=row_idx, col=3, value=vals["FTD"]))
                cells_to_update.append(gspread.Cell(row=row_idx, col=4, value=vals["MTD"]))
                # Push the INHERITED timestamp
                cells_to_update.append(gspread.Cell(row=row_idx, col=5, value=vals["TIME"]))

    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} derived metric records in Google Sheets...")
        worksheet.update_cells(cells_to_update)
        print("Derived Metrics Update complete!")
    else:
        print("No derived metrics to update.")


def update_seventh_metric(creds):
    """Fetches Tonnage & Order Plans and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Seventh Google Sheet (Tonnage & Order Plans) ---")
    gc = gspread.authorize(creds)
    
    source_sheet_id = '1dgmZmhcmJrSd2QzjyzGtk37w3dgHs2-qm_2FnsXwaoY'
    
    # Configuration for the two tabs
    metric_configs = {
        "Tonnage Plan": 292200791,
        "Order Plan": 53822165
    }

    # Reverse global STORE_MAPPING so we can look up "Amritsar" and get "4702"
    # Ensure STORE_MAPPING is defined in your global scope!
    REVERSE_STORE_MAPPING = {v.lower().strip(): k for k, v in STORE_MAPPING.items()}

    # Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    current_year = yesterday.year

    print(f"Finding FTD for {yesterday} and MTD Sum from {start_of_month} to {yesterday}")

    # Helper for floats
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    # 1. HARVEST DATA: Build a dictionary of { (StoreCode, MetricName): {FTD: val, MTD: val} }
    calculated_data = {}

    for metric_name, gid in metric_configs.items():
        try:
            source_ws = gc.open_by_key(source_sheet_id).get_worksheet_by_id(gid) 
            source_data = source_ws.get_all_values()
        except Exception as e:
            print(f"Failed to open {metric_name} sheet. Error: {e}")
            continue

        if len(source_data) < 3: continue

        # Dynamically find Date Columns
        date_row = source_data[1] 
        ftd_col_idx = -1
        mtd_col_indices = []

        for idx, cell_val in enumerate(date_row):
            val_str = str(cell_val).strip()
            if not val_str: continue
            
            try:
                # Format "1-Feb" -> "1-Feb-2026"
                if str(current_year) not in val_str:
                    val_str = f"{val_str}-{current_year}"
                    
                parsed_date = pd.to_datetime(val_str).date()
                
                if parsed_date == yesterday:
                    ftd_col_idx = idx
                if start_of_month <= parsed_date <= yesterday:
                    mtd_col_indices.append(idx)
            except Exception:
                pass 

        if ftd_col_idx == -1 and not mtd_col_indices:
            print(f"Could not find valid dates for {metric_name}.")
            continue

        # Extract Data
        for row in source_data[2:]:
            if len(row) > 0:
                store_name = str(row[0]).strip().lower() # Col A
                
                if store_name in REVERSE_STORE_MAPPING:
                    store_code = REVERSE_STORE_MAPPING[store_name]
                    
                    # Calculate FTD
                    ftd_val = safe_float(row[ftd_col_idx]) if ftd_col_idx != -1 and ftd_col_idx < len(row) else 0.0
                    
                    # Calculate MTD Sum
                    mtd_vals = [safe_float(row[i]) for i in mtd_col_indices if i < len(row)]
                    mtd_val = sum(mtd_vals)
                    
                    # Store in our lookup dictionary
                    calculated_data[(store_code, metric_name)] = {'FTD': ftd_val, 'MTD': mtd_val}

    # 2. UPDATE TARGET SHEET with Change Detection
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0

    for index, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            # Check if we have calculated data for this specific row
            if (store_code, cell_type) in calculated_data:
                
                # A. New Values
                new_vals = calculated_data[(store_code, cell_type)]
                new_ftd = new_vals['FTD']
                new_mtd = new_vals['MTD']
                
                # B. Old Values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Seventh Sheet Update complete!")
    else:
        print("No changes detected for Tonnage & Order Plans.")


def update_sixth_metric(creds):
    """Fetches '>50 Lines Invoices' data and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Sixth Google Sheet (>50 Lines Invoices) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1bFVs4weqrMWiBwZVPtlVt-NCtKAzK3Kus0kgWcSn-d4'
    try:
        source_doc = gc.open_by_key(source_sheet_id)
    except Exception as e:
        print(f"Failed to open sixth source sheet. Error: {e}")
        return

    # 2. Determine Yesterday's Date (Tab Name format: yyyy-mm-dd)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    target_tab_name = yesterday.strftime('%Y-%m-%d')
    print(f"Looking for tab named: {target_tab_name}")

    try:
        # Dynamically grab the specific worksheet matching yesterday's date
        source_ws = source_doc.worksheet(target_tab_name)
        source_data = source_ws.get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        print(f"Tab '{target_tab_name}' not found in the sixth sheet. Has it been created yet?")
        return
    except Exception as e:
        print(f"Error reading tab '{target_tab_name}': {e}")
        return

    # Helper function to safely convert sheet values
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']:
            return 0.0
        is_percent = False
        if val_str.endswith('%'):
            val_str = val_str[:-1]
            is_percent = True
        try:
            num = float(val_str)
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    # 3. Extract FTD and MTD Data
    # Column C (Index 2) = Store Code (Site)
    # Column E (Index 4) = FTD (Order Count)
    # Column G (Index 6) = MTD (MTD Order Count)
    extracted_data = {}
    
    # Skip the header row (index 0) and loop through the rest
    for row in source_data[1:]:
        if len(row) >= 7: 
            store_code = str(row[2]).strip()
            
            # Check if this is a valid store code from our global mapping
            if store_code in STORE_MAPPING: 
                ftd_val = safe_float(row[4])  # Column E
                mtd_val = safe_float(row[6])  # Column G
                extracted_data[store_code] = {"FTD": ftd_val, "MTD": mtd_val}

    if not extracted_data:
        print(f"No valid store data found in tab '{target_tab_name}'.")
        return

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Target specifically the row for ">50 Lines Invoices"
            if cell_type == ">50 Lines Invoices" and store_code in extracted_data:
                
                # A. Get NEW values
                new_ftd = extracted_data[store_code]["FTD"]
                new_mtd = extracted_data[store_code]["MTD"]
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue updates for FTD, MTD, and Timestamp
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Sixth Sheet (>50 Lines) Update complete!")
    else:
        print("No changes detected in >50 Lines Invoices.")
        

def update_eighth_metric(creds):
    """Fetches 'Tonnage attainment' and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Eighth Google Sheet (Tonnage Attainment) ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1U6y-mEgv7WoqdsmqESM3bhZqV-3d0ApgSY5G7MCbvaI'
    try:
        # Targeting GID 1747919180 specifically
        source_ws = gc.open_by_key(source_sheet_id).get_worksheet_by_id(1747919180) 
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open eighth source sheet. Error: {e}")
        return

    if len(source_data) < 3:
        print("Eighth sheet does not have enough rows for data extraction.")
        return

    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)

    print(f"Finding FTD for {yesterday} and MTD Average from {start_of_month} to {yesterday}")

    # 3. Dynamically find the Column Indexes in Row 1 (Python Index 0)
    # Since it's a merged cell, the date is in the first cell. 
    # The target "Attainment" is the 4th column of the merge (Index + 3).
    date_row = source_data[0] 
    ftd_col_idx = -1
    mtd_col_indices = []

    for idx, cell_val in enumerate(date_row):
        val_str = str(cell_val).strip()
        if not val_str: 
            continue
            
        try:
            parsed_date = pd.to_datetime(val_str).date()
            
            # If the date matches, shift +3 indexes to grab the 4th column
            if parsed_date == yesterday:
                ftd_col_idx = idx + 3 
            if start_of_month <= parsed_date <= yesterday:
                mtd_col_indices.append(idx + 3)
        except Exception:
            pass # Ignore cells that aren't valid dates

    if ftd_col_idx == -1 and not mtd_col_indices:
        print("Could not find valid dates in Row 1 for Tonnage attainment.")
        return

    # Helper function to safely convert sheet percentages (e.g., "145%") to floats (1.45)
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        is_percent = False
        if val_str.endswith('%'):
            val_str = val_str[:-1]
            is_percent = True
        try:
            num = float(val_str)
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    # 4. Extract Data from Row 3 onwards (Python Index 2+)
    extracted_data = {}
    
    for row in source_data[2:]:
        if len(row) > 0:
            store_code = str(row[0]).strip() # Column A
            
            if store_code in STORE_MAPPING:
                
                # Extract FTD
                if ftd_col_idx != -1 and ftd_col_idx < len(row):
                    ftd_val = safe_float(row[ftd_col_idx])
                else:
                    ftd_val = 0.0
                
                # Extract MTD and calculate AVERAGE (Since attainment is a percentage)
                if mtd_col_indices:
                    mtd_vals = [safe_float(row[i]) for i in mtd_col_indices if i < len(row)]
                    mtd_val = sum(mtd_vals) / len(mtd_vals) if mtd_vals else 0.0
                else:
                    mtd_val = 0.0
                    
                extracted_data[store_code] = {"FTD": ftd_val, "MTD": mtd_val}

    # 5. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Match strictly against "Tonnage attainment"
            if cell_type == "Tonnage attainment" and store_code in extracted_data:
                
                # A. Get NEW values
                new_ftd = extracted_data[store_code]["FTD"]
                new_mtd = extracted_data[store_code]["MTD"]
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Use a small tolerance for float comparison
                if abs(new_ftd - old_ftd) > 0.000001 or abs(new_mtd - old_mtd) > 0.000001:
                    updates_count += 1
                    
                    # Queue updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Eighth Sheet (Tonnage Attainment) Update complete!")
    else:
        print("No changes detected in Tonnage attainment.")


def process_vehicle_stats(creds, file_path):
    """Reads vehicle_stats.XLSX, calculates metrics, and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Vehicle Stats (.XLSX) ---")
    
    # Read the Excel file
    try:
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Failed to read vehicle stats file. Error: {e}")
        return

    # 1. Column Mapping (0-based index)
    # B=1 (Store), E=4 (Date), G=6 (Vendor), N=13 (Type), P=15 (Time), Q=16 (Cases)
    
    # 2. Clean Store Code
    df[1] = df[1].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 3. Parse Date (Column E / Index 4)
    df[4] = pd.to_datetime(df[4], format='%m/%d/%Y', errors='coerce').dt.normalize()
    
    # 4. Filter Out Invalid Times (Column P / Index 15)
    def is_valid_time(val):
        s = str(val).strip().upper()
        return s != "12:00:00 AM" and s != "00:00:00" and s != "NAN" and s != "NAT"
    
    # Apply filter safely
    df = df[df[15].apply(is_valid_time)].copy()
    
    # 5. Define Dates (Yesterday & MTD)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Vehicle Stats: Filtering for FTD ({yesterday}) and MTD ({start_of_month} to {yesterday})")
    
    # Convert comparison dates to Pandas Timestamp to avoid errors
    pd_yesterday = pd.to_datetime(yesterday)
    pd_start_of_month = pd.to_datetime(start_of_month)

    # 6. Filter DataFrames for FTD and MTD
    df_ftd = df[df[4] == pd_yesterday].copy()
    df_mtd = df[(df[4] >= pd_start_of_month) & (df[4] <= pd_yesterday)].copy()
    
    if df_mtd.empty:
        print("No valid vehicle data found for this period.")
        return

    # Helper to calculate metrics for a specific DataFrame
    def calc_metrics(subset_df):
        if subset_df.empty: 
            return {
                "#Of Vehicle Received": {}, "#Of Cases": {}, "#Of couriers": {}, "#Of HUL Vehicle": {}
            }
        
        # Metric 1: #Of Vehicle Received (Count of rows)
        vehicle_count = subset_df.groupby(1).size().to_dict()
        
        # Metric 2: #Of Cases (Sum of Column Q / Index 16)
        subset_df[16] = pd.to_numeric(subset_df[16], errors='coerce').fillna(0)
        cases_sum = subset_df.groupby(1)[16].sum().to_dict()
        
        # Metric 3: #Of couriers (Count where Col N / Index 13 == 'Courier')
        courier_df = subset_df[subset_df[13].astype(str).str.strip().str.title() == "Courier"]
        courier_count = courier_df.groupby(1).size().to_dict()
        
        # Metric 4: #Of HUL Vehicle (Count where Col G / Index 6 matches list)
        hul_vendors = ["HINDUSTAN UNILEVER LIMITED 01", "HINDUSTAN UNILEVER LIMITED 02"]
        hul_df = subset_df[subset_df[6].astype(str).str.strip().isin(hul_vendors)]
        hul_count = hul_df.groupby(1).size().to_dict()
        
        return {
            "#Of Vehicle Received": vehicle_count,
            "#Of Cases": cases_sum,
            "#Of couriers": courier_count,
            "#Of HUL Vehicle": hul_count
        }

    # Calculate metrics for both time periods
    ftd_data = calc_metrics(df_ftd)
    mtd_data = calc_metrics(df_mtd)
    
    # 7. Update Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    metrics_list = ["#Of Vehicle Received", "#Of Cases", "#Of couriers", "#Of HUL Vehicle"]
    
    for index, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            if cell_type in metrics_list:
                # A. Get NEW values
                new_ftd = float(ftd_data.get(cell_type, {}).get(store_code, 0))
                new_mtd = float(mtd_data.get(cell_type, {}).get(store_code, 0))
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Use epsilon for float comparison
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Vehicle Stats Update complete!")
    else:
        print("No changes detected in Vehicle Stats.")



def update_expense_metrics(creds):
    """Fetches expenses and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Expense Metrics (Targeting 'Sheet1') ---")
    gc = gspread.authorize(creds)
    
    source_sheet_id = '1B7sKYLDr0KPA8tCMDUHgYo-fO4yBDnj6K3ubXCC8gfs'
    try:
        source_ws = gc.open_by_key(source_sheet_id).worksheet('Sheet1')
        source_data = source_ws.get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        print("Error: Could not find a tab named 'Sheet1'. Please check the tab name at the bottom of your Google Sheet.")
        return
    except Exception as e:
        print(f"Failed to open expense source sheet. Error: {e}")
        return

    # 1. FIND THE HEADER ROW AUTOMATICALLY
    header_index = -1
    # Scan first 10 rows for the real header
    for i, row in enumerate(source_data[:10]): 
        row_str = [str(x).strip().lower() for x in row]
        # We look for "store code" and "account head" to confirm it's the right row
        if "store code" in row_str and "account head" in row_str:
            header_index = i
            print(f"Found Data Headers at Row {i+1}. Processing rows below it...")
            break
            
    if header_index == -1:
        print("CRITICAL ERROR: Could not find 'Store Code' header in 'Sheet1'.")
        print(f"Top row of Sheet1 looks like: {source_data[0]}")
        return

    # 2. Setup Processing
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Expenses: FTD for {yesterday} | MTD from {start_of_month}")

    target_categories = [
        "Stationery Expenses", 
        "Associate Relations", 
        "Member Satisfaction & Business Development", 
        "Repair and Maintenance"
    ]

    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    ftd_data = {} 
    mtd_data = {} 
    rows_processed = 0

    # 3. Process Data starting AFTER the header row
    for row in source_data[header_index + 1:]:
        if len(row) < 4: continue # Skip empty rows
        
        # Pad row if short to avoid index errors
        if len(row) <= 20:
            row += [''] * (21 - len(row))

        status = str(row[1]).strip().lower()
        if status == "cancelled": continue 

        # Store Code (Col D / Index 3)
        store_code = str(row[3]).replace('.0', '').strip()
        
        # Category (Col G / Index 6)
        category = str(row[6]).strip()

        try:
            # Date (Col F / Index 5)
            row_date = pd.to_datetime(row[5], errors='coerce').date()
            if pd.isna(row_date): continue
        except:
            continue

        if category in target_categories:
            rows_processed += 1
            
            # TRY COLUMN U (Settlement - Index 20) FIRST
            val = safe_float(row[20])
            
            # FALLBACK: If Col U is 0, use Col L (Invoice Amt - Index 11)
            if val == 0:
                val = safe_float(row[11])

            if store_code not in ftd_data: ftd_data[store_code] = {}
            if store_code not in mtd_data: mtd_data[store_code] = {}

            if start_of_month <= row_date <= yesterday:
                mtd_data[store_code][category] = mtd_data[store_code].get(category, 0) + val
            
            if row_date == yesterday:
                ftd_data[store_code][category] = ftd_data[store_code].get(category, 0) + val

    print(f"Processed {rows_processed} valid expense rows.")

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_ws = gc.open_by_key('1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34').worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in target_categories:
                # A. Get NEW values
                new_ftd = ftd_data.get(store_code, {}).get(cell_type, 0)
                new_mtd = mtd_data.get(store_code, {}).get(cell_type, 0)
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Use epsilon for float comparison
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Expense Metrics Update complete!")
    else:
        print("No changes detected in Expense Metrics.")

def update_osa_metric(creds):
    """Fetches 'OSA %' and updates dashboard ONLY if changed."""
    print(f"\n--- Processing OSA % Metric ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1zSjGXEmGdM0sOVzOfweyu4KPkmp0qjohe9R0Zl7rRhk'
    try:
        source_ws = gc.open_by_key(source_sheet_id).worksheet('View')
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open OSA source sheet. Error: {e}")
        return

    if len(source_data) < 2: return

    # 2. Prepare Mappings
    # Ensure STORE_MAPPING is defined in your global scope
    REVERSE_STORE_MAPPING = {v.lower().strip(): k for k, v in STORE_MAPPING.items()}

    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        is_percent = False
        if val_str.endswith('%'):
            val_str = val_str[:-1]
            is_percent = True
        try:
            num = float(val_str)
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    ftd_data = {} 
    mtd_data = {} 

    # 3. Extract Data
    for row in source_data:
        # --- Process FTD Part ---
        if len(row) > 6:
            store_name_ftd = str(row[1]).strip().lower()
            if store_name_ftd in REVERSE_STORE_MAPPING:
                store_code = REVERSE_STORE_MAPPING[store_name_ftd]
                val = safe_float(row[6])
                ftd_data[store_code] = val
        
        # --- Process MTD Part ---
        if len(row) > 13:
            store_name_mtd = str(row[8]).strip().lower()
            if store_name_mtd in REVERSE_STORE_MAPPING:
                store_code = REVERSE_STORE_MAPPING[store_name_mtd]
                val = safe_float(row[13])
                mtd_data[store_code] = val

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Match strictly against "OSA %"
            if cell_type == "OSA %":
                
                # A. Get NEW values
                new_ftd = ftd_data.get(store_code, 0.0)
                new_mtd = mtd_data.get(store_code, 0.0)
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Use epsilon for float comparison
                if abs(new_ftd - old_ftd) > 0.000001 or abs(new_mtd - old_mtd) > 0.000001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("OSA % Update complete!")
    else:
        print("No changes detected in OSA %.")


def update_qc_tpv_vd_metrics(creds):
    """Fetches QC, TPV, VD metrics and updates dashboard ONLY if changed."""
    print(f"\n--- Processing QC Orders, TPV, and VD Metrics ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '16Syh3BNGT9tOdklfNO2VObYxn_7vMxZXmh_Z_qIIHQM'
    try:
        source_ws = gc.open_by_key(source_sheet_id).worksheet('Sheet1')
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open QC/TPV source sheet. Error: {e}")
        return

    if len(source_data) < 2: return

    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Metrics: FTD for {yesterday} | MTD from {start_of_month}")

    # 3. Helper to clean numbers
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A', '']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    # 4. Initialize Data Structures
    ftd_data = {}
    mtd_data = {}

    # 5. Process Rows
    REVERSE_STORE_MAPPING = {v.lower().strip(): k for k, v in STORE_MAPPING.items()}

    for row in source_data[1:]: # Skip header
        if len(row) > 62: # Ensure row is long enough to reach Col BK (Index 62)
            
            # Parse Date
            try:
                row_date = pd.to_datetime(row[1], format='%m-%d-%Y', errors='coerce').date()
                if pd.isna(row_date): continue
            except:
                continue
            
            # Match Store
            store_name_raw = str(row[3]).strip().lower()
            if store_name_raw in REVERSE_STORE_MAPPING:
                store_code = REVERSE_STORE_MAPPING[store_name_raw]
                
                # Extract Values
                qc_val = safe_float(row[10])      # Col K
                
                tpv_num = safe_float(row[58])     # Col BG
                tpv_den = safe_float(row[60])     # Col BI
                
                vd_num = safe_float(row[62])      # Col BK
                vd_den = safe_float(row[61])      # Col BJ
                
                # Init Dictionaries
                if store_code not in ftd_data: 
                    ftd_data[store_code] = {'QC': 0.0, 'TPV_Num': 0.0, 'TPV_Den': 0.0, 'VD_Num': 0.0, 'VD_Den': 0.0}
                if store_code not in mtd_data: 
                    mtd_data[store_code] = {'QC': 0.0, 'TPV_Num': 0.0, 'TPV_Den': 0.0, 'VD_Num': 0.0, 'VD_Den': 0.0}
                
                # Accumulate MTD
                if start_of_month <= row_date <= yesterday:
                    mtd_data[store_code]['QC'] += qc_val
                    mtd_data[store_code]['TPV_Num'] += tpv_num
                    mtd_data[store_code]['TPV_Den'] += tpv_den
                    mtd_data[store_code]['VD_Num'] += vd_num
                    mtd_data[store_code]['VD_Den'] += vd_den
                
                # Accumulate FTD
                if row_date == yesterday:
                    ftd_data[store_code]['QC'] += qc_val
                    ftd_data[store_code]['TPV_Num'] += tpv_num
                    ftd_data[store_code]['TPV_Den'] += tpv_den
                    ftd_data[store_code]['VD_Num'] += vd_num
                    ftd_data[store_code]['VD_Den'] += vd_den

    # 6. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    targets = ["QC Orders", "TPV", "VD"]

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in targets and store_code in mtd_data:
                
                ftd_vals = ftd_data.get(store_code, {'QC': 0, 'TPV_Num': 0, 'TPV_Den': 0, 'VD_Num': 0, 'VD_Den': 0})
                mtd_vals = mtd_data.get(store_code, {'QC': 0, 'TPV_Num': 0, 'TPV_Den': 0, 'VD_Num': 0, 'VD_Den': 0})
                
                # A. Calculate NEW Values
                new_ftd = 0.0
                new_mtd = 0.0
                
                if cell_type == "QC Orders":
                    new_ftd = ftd_vals['QC']
                    new_mtd = mtd_vals['QC']
                    
                elif cell_type == "TPV":
                    new_ftd = ftd_vals['TPV_Num'] / ftd_vals['TPV_Den'] if ftd_vals['TPV_Den'] else 0.0
                    new_mtd = mtd_vals['TPV_Num'] / mtd_vals['TPV_Den'] if mtd_vals['TPV_Den'] else 0.0
                    
                elif cell_type == "VD":
                    new_ftd = ftd_vals['VD_Num'] / ftd_vals['VD_Den'] if ftd_vals['VD_Den'] else 0.0
                    new_mtd = mtd_vals['VD_Num'] / mtd_vals['VD_Den'] if mtd_vals['VD_Den'] else 0.0

                # B. Get OLD Values
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0

                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.000001 or abs(new_mtd - old_mtd) > 0.000001:
                    updates_count += 1
                    
                    # Queue updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("QC/TPV/VD Metrics Update complete!")
    else:
        print("No changes detected in QC/TPV/VD Metrics.")



def process_price_override(creds, file_path):
    """Reads PriceOverride.csv, filters for Code 5, and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Price Override (Code 2 Items/Value) ---")
    
    try:
        # Read CSV with latin1 and low_memory=False to ensure it reads safely
        # header=None so we can target by exact index (0-based)
        df = pd.read_csv(file_path, encoding='latin1', low_memory=False, header=None)
    except Exception as e:
        print(f"Failed to read PriceOverride.csv. Error: {e}")
        return

    # 1. Standardize Columns 
    # Col A (Idx 0) = Store Code
    # Col L (Idx 11) = Reason Code (Targeting 5)
    # Col H (Idx 7) = Qty (For "Code 2 Items")
    # Col K (Idx 10) = Value (For "Code 2 Value")

    if df.shape[1] <= 11:
        print(f"Error: CSV only has {df.shape[1]} columns, expected at least 12.")
        return

    # 2. Clean Store Code (Col A / Idx 0)
    df[0] = df[0].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 3. Clean Reason Code (Col L / Idx 11) -> Force to pure numbers to match 5 safely
    df[11] = pd.to_numeric(df[11], errors='coerce').fillna(-1)
    
    # 4. Filter for Reason Code 5
    df_code5 = df[df[11] == 5].copy()
    
    # Even if empty, we might need to overwrite old data with 0s, so we don't return early here
    # unless we are sure no stores had overrides. But safest is to proceed.
    
    # 5. Convert Numeric Columns safely (Col H and Col K)
    if not df_code5.empty:
        df_code5[7] = pd.to_numeric(df_code5[7], errors='coerce').fillna(0.0)
        df_code5[10] = pd.to_numeric(df_code5[10], errors='coerce').fillna(0.0)
        
        # 6. Group by Store and Sum
        dict_items = df_code5.groupby(0)[7].sum().to_dict()  # Items
        dict_value = df_code5.groupby(0)[10].sum().to_dict() # Value
    else:
        dict_items = {}
        dict_value = {}
    
    # Combine into a unified, lowercase dictionary for safe lookup
    grouped = {}
    all_stores = set(dict_items.keys()).union(set(dict_value.keys()))
    
    for store in all_stores:
        if store == 'nan' or not store: continue
        grouped[store] = {
            "code 2 items": dict_items.get(store, 0.0),
            "code 2 value": dict_value.get(store, 0.0)
        }

    # 7. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    valid_metrics = ["code 2 items", "code 2 value"]
    
    for index, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            
            # Clean the cell text from the sheet (lowercase, remove double spaces)
            cell_type_clean = " ".join(str(row[1]).lower().split())
            
            # Check if this row is one of our metrics
            if cell_type_clean in valid_metrics:
                
                # A. Get NEW Value (Default to 0 if store not in CSV)
                new_val = 0.0
                if store_code in grouped:
                    new_val = float(grouped[store_code].get(cell_type_clean, 0.0))
                
                # B. Get OLD Values (Safely)
                # Note: For this metric, FTD (Col 3) and MTD (Col 4) are often the same value
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # We update if EITHER FTD or MTD doesn't match the new value
                if abs(new_val - old_ftd) > 0.001 or abs(new_val - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val)) # FTD
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_val)) # MTD
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Price Override Update complete!")
    else:
        print("No changes detected in Price Override metrics.")

def process_article_sales_report(creds, file_path):
    """Reads ArticleSalesReport.csv and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Article Sales Report (<5, CWO, NOH) ---")
    
    try:
        # Read CSV with latin1 to handle potential special characters
        df = pd.read_csv(file_path, encoding='latin1', header=None, low_memory=False)
    except Exception as e:
        print(f"Failed to read ArticleSalesReport.csv. Error: {e}")
        return

    # 1. Define Column Indices (0-based)
    STORE_COL = 1        # Column B (Store Code)
    CWO_STATUS_COL = 9   # Column J (Status)
    NOH_AND_QTY_COL = 27 # Column AB (Used for both <5 count and NOH sum)
    CWO_VAL_COL = 28     # Column AC (Value to sum for CWO)

    # 2. Clean Store Code (Column B / Index 1)
    df[STORE_COL] = df[STORE_COL].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    def to_float(val):
        try:
            return float(str(val).replace(',', '').strip())
        except:
            return 0.0

    # 3. Calculate Metrics
    # Structure: {StoreCode: {'<5': 0, 'CWO': 0.0, 'NOH': 0.0}}
    metrics = {} 

    for index, row in df.iterrows():
        store_code = str(row[STORE_COL]).strip()
        
        # Skip headers or empty store codes
        if not store_code or store_code == 'nan' or store_code.lower() in ['store', 'store code', 'site']: 
            continue
        
        if store_code not in metrics:
            metrics[store_code] = {'<5': 0, 'CWO': 0.0, 'NOH': 0.0}

        # --- Metric 1: <5 (Count rows where 0 < Col AB < 5) ---
        # --- Metric 3: NOH (If Col AB < 0, Sum Col AB) ---
        try:
            if len(row) > NOH_AND_QTY_COL:
                val_ab = to_float(row[NOH_AND_QTY_COL])
                
                # Check for <5 Metric
                if 0 < val_ab < 5:
                    metrics[store_code]['<5'] += 1
                    
                # Check for NOH Metric
                if val_ab < 0:
                    metrics[store_code]['NOH'] += val_ab
        except: pass

        # --- Metric 2: CWO (If Col J == 'C', Sum Col AC) ---
        try:
            if len(row) > CWO_VAL_COL:
                status_j = str(row[CWO_STATUS_COL]).strip().upper()
                if status_j == 'C':
                    metrics[store_code]['CWO'] += to_float(row[CWO_VAL_COL])
        except: pass

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    target_map = {
        "<5": "<5",
        "CWO": "CWO",
        "NOH": "NOH"
    }

    for index, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            if cell_type in target_map:
                
                # A. Get NEW values (Default to 0 if store not in CSV)
                new_val = 0.0
                if store_code in metrics:
                    metric_key = target_map[cell_type]
                    new_val = float(metrics[store_code][metric_key])
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Note: FTD and MTD get the same value in this report logic
                if abs(new_val - old_ftd) > 0.001 or abs(new_val - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val)) # FTD
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_val)) # MTD
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Article Sales Update complete!")
    else:
        print("No changes detected in Article Sales metrics.")


def update_vehicle_count_metric(creds):
    """Fetches '#Of Vehicles' and updates dashboard ONLY if changed."""
    print(f"\n--- Processing #Of Vehicles Metric ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1lYB9VXpKd60nl52_shy5F9UFemUy7V-HpKoz68XHLf4'
    try:
        # User specified sheet name "Feb 26"
        source_ws = gc.open_by_key(source_sheet_id).worksheet('Feb 26')
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open #Of Vehicles source sheet. Error: {e}")
        return

    if len(source_data) < 2: return

    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    # Format dates to match sheet header (e.g., "11-Feb-26" or "1-Feb-26")
    yesterday_str = yesterday.strftime("%d-%b-%y").lstrip("0") 
    print(f"Looking for header: {yesterday_str} for FTD")

    # 3. Analyze Header Row (Row 1 / Index 0)
    header_row = source_data[0]
    
    ftd_col_idx = -1
    mtd_col_indices = [] # Tuples of (index+1, index+3)

    for i, cell_val in enumerate(header_row):
        val_str = str(cell_val).strip()
        if not val_str: continue
        
        try:
            # Parse date from header
            header_date = datetime.strptime(val_str, "%d-%b-%y").date()
            
            # Check if date is in MTD range
            if start_of_month <= header_date <= yesterday:
                # Store the offsets for this date: (2nd col, 4th col) -> (idx+1, idx+3)
                mtd_col_indices.append((i + 1, i + 3))
            
            # Check if date is FTD
            if header_date == yesterday:
                ftd_col_idx = i
        except ValueError:
            continue # Not a date column

    if ftd_col_idx == -1:
        print(f"Could not find header '{yesterday_str}' in Row 1.")
        # We continue to process MTD if possible, or exit? 
        if not mtd_col_indices: return

    # 4. Helper to clean numbers
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    # 5. Process Rows
    ftd_data = {} # {StoreCode: "Actual (Plan)"}
    mtd_data = {} # {StoreCode: Total_Actual}

    for row in source_data[2:]: # Assuming data starts from Row 3 (Index 2)
        if len(row) < 4: continue
        
        store_code = str(row[0]).strip() # Column A
        
        # Get Plan from Column D (Index 3)
        plan_val = safe_float(row[3]) 
        
        # --- Calculate FTD ---
        ftd_actual = 0.0
        if ftd_col_idx != -1 and len(row) > ftd_col_idx + 3:
            val1 = safe_float(row[ftd_col_idx + 1]) # 2nd column
            val2 = safe_float(row[ftd_col_idx + 3]) # 4th column
            ftd_actual = val1 + val2
            
        # Format: "Actual (Plan)" -> e.g. "5 (12)"
        ftd_str = f"{int(ftd_actual)} ({int(plan_val)})"
        ftd_data[store_code] = ftd_str
        
        # --- Calculate MTD ---
        mtd_total = 0.0
        for (idx1, idx2) in mtd_col_indices:
            if len(row) > idx2:
                v1 = safe_float(row[idx1])
                v2 = safe_float(row[idx2])
                mtd_total += (v1 + v2)
        
        mtd_data[store_code] = mtd_total

    # 6. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type == "#Of Vehicles":
                
                # A. Get NEW Values
                new_ftd_str = ftd_data.get(store_code, "0 (0)")
                new_mtd_val = float(mtd_data.get(store_code, 0))
                
                # B. Get OLD Values
                old_ftd_str = str(row[2]).strip() if len(row) > 2 else ""
                try:
                    old_mtd_val = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd_val = 0.0
                
                # C. Compare (Change Detection)
                # String comparison for FTD ("5 (10)" vs "5 (10)")
                # Float comparison for MTD
                ftd_changed = new_ftd_str != old_ftd_str
                mtd_changed = abs(new_mtd_val - old_mtd_val) > 0.001
                
                if ftd_changed or mtd_changed:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd_str))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd_val))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("#Of Vehicles Update complete!")
    else:
        print("No changes detected in #Of Vehicles.")

def process_mb51_report(creds, file_path):
    """Reads mb_51.xlsx and updates dashboard ONLY if changed."""
    print(f"\n--- Processing MB51 Report (GRN, Return, Refund) ---")
    
    try:
        # Read Excel file (Assuming no header or standard header, accessing by index is safest)
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Failed to read mb_51.xlsx. Error: {e}")
        return

    # 1. Column Mapping (0-based Index)
    # Col B (Idx 1) = Movement Type (101, 653, 252)
    # Col F (Idx 5) = Amount / Value
    # Col G (Idx 6) = Date (mm/dd/yyyy)
    # Col P (Idx 15) = Store Code
    
    # 2. Clean Store Code (Col P / Idx 15)
    # Force to string and remove decimals (e.g. 4702.0 -> "4702")
    df[15] = df[15].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 3. Parse Date (Col G / Idx 6)
    df[6] = pd.to_datetime(df[6], format='%m/%d/%Y', errors='coerce').dt.normalize()
    
    # 4. Clean Movement Type (Col B / Idx 1)
    df[1] = df[1].astype(str).str.strip()

    # 5. Clean Value (Col F / Idx 5)
    df[5] = pd.to_numeric(df[5], errors='coerce').fillna(0.0)

    # 6. Define Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"MB51: FTD for {yesterday} | MTD from {start_of_month}")

    # 7. Initialize Data Structure
    # {StoreCode: {'101': {'FTD': 0, 'MTD': 0}, '653': {...}, '252': {...}}}
    store_metrics = {}

    target_codes = ['101', '653', '252']

    # 8. Process Rows
    for index, row in df.iterrows():
        # Skip rows with invalid dates
        if pd.isna(row[6]): continue
        
        row_date = row[6].date()
        store_code = row[15]
        mvmt_type = row[1]
        val = row[5]
        
        if mvmt_type in target_codes:
            if store_code not in store_metrics:
                store_metrics[store_code] = {
                    '101': {'FTD': 0.0, 'MTD': 0.0},
                    '653': {'FTD': 0.0, 'MTD': 0.0},
                    '252': {'FTD': 0.0, 'MTD': 0.0}
                }
            
            # MTD Calculation
            if start_of_month <= row_date <= yesterday:
                store_metrics[store_code][mvmt_type]['MTD'] += val
            
            # FTD Calculation
            if row_date == yesterday:
                store_metrics[store_code][mvmt_type]['FTD'] += val

    # 9. Update Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    # Map Dashboard Metric Names to Movement Types
    metric_map = {
        "GRN Value": '101',
        "Return Value - 653": '653',
        "Refund Value - 252": '252'
    }

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in metric_map:
                code_key = metric_map[cell_type]
                
                # A. Get NEW Values (Default 0 if not in CSV)
                new_ftd = 0.0
                new_mtd = 0.0
                if store_code in store_metrics:
                    new_ftd = store_metrics[store_code][code_key]['FTD']
                    new_mtd = store_metrics[store_code][code_key]['MTD']
                
                # B. Get OLD Values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("MB51 Update complete!")
    else:
        print("No changes detected in MB51 metrics.")


def process_near_expiry_report(creds, file_path):
    """Reads near_expiry.XLSX and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Near Expiry Report ---")
    
    try:
        # Read Excel file (no header assumed to access by index safely)
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Failed to read near_expiry.XLSX. Error: {e}")
        return

    # 1. Column Mapping (0-based Index)
    # Col E (Idx 4) = Short Expire Items (Qty)
    # Col F (Idx 5) = Short Expire Value (Amt)
    # Col G (Idx 6) = Date (mm/dd/yyyy)
    # Col P (Idx 15) = Store Code
    
    # 2. Clean Store Code (Col P / Idx 15)
    df[15] = df[15].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 3. Parse Date (Col G / Idx 6)
    df[6] = pd.to_datetime(df[6], format='%m/%d/%Y', errors='coerce').dt.normalize()
    
    # 4. Clean Numeric Columns
    df[4] = pd.to_numeric(df[4], errors='coerce').fillna(0.0) # Items
    df[5] = pd.to_numeric(df[5], errors='coerce').fillna(0.0) # Value

    # 5. Define Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Near Expiry: FTD for {yesterday} | MTD from {start_of_month}")

    # 6. Initialize Data Structure
    store_metrics = {}

    # 7. Process Rows
    for index, row in df.iterrows():
        if pd.isna(row[6]): continue
        
        row_date = row[6].date()
        store_code = row[15]
        
        qty = row[4]
        val = row[5]
        
        if store_code not in store_metrics:
            store_metrics[store_code] = {
                'Value': {'FTD': 0.0, 'MTD': 0.0},
                'Items': {'FTD': 0.0, 'MTD': 0.0}
            }
        
        # MTD Calculation
        if start_of_month <= row_date <= yesterday:
            store_metrics[store_code]['Value']['MTD'] += val
            store_metrics[store_code]['Items']['MTD'] += qty
        
        # FTD Calculation
        if row_date == yesterday:
            store_metrics[store_code]['Value']['FTD'] += val
            store_metrics[store_code]['Items']['FTD'] += qty

    # 8. Update Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    # Map Dashboard Metric Names to our internal keys
    metric_map = {
        "Short Expire Value": 'Value',
        "Short expire items": 'Items'
    }

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in metric_map:
                key = metric_map[cell_type]
                
                # A. Get NEW values
                new_ftd = 0.0
                new_mtd = 0.0
                if store_code in store_metrics:
                    new_ftd = store_metrics[store_code][key]['FTD']
                    new_mtd = store_metrics[store_code][key]['MTD']
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Near Expiry Update complete!")
    else:
        print("No changes detected in Near Expiry metrics.")


def process_near_expiry_report(creds, file_path):
    """Reads near_expiry.XLSX and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Near Expiry Report ---")
    
    try:
        # Read Excel file (no header assumed to access by index safely)
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Failed to read near_expiry.XLSX. Error: {e}")
        return

    # 1. Column Mapping (0-based Index)
    # Col E (Idx 4) = Short Expire Items (Qty)
    # Col F (Idx 5) = Short Expire Value (Amt)
    # Col G (Idx 6) = Date (mm/dd/yyyy)
    # Col P (Idx 15) = Store Code
    
    # 2. Clean Store Code (Col P / Idx 15)
    df[15] = df[15].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 3. Parse Date (Col G / Idx 6)
    df[6] = pd.to_datetime(df[6], format='%m/%d/%Y', errors='coerce').dt.normalize()
    
    # 4. Clean Numeric Columns
    df[4] = pd.to_numeric(df[4], errors='coerce').fillna(0.0) # Items
    df[5] = pd.to_numeric(df[5], errors='coerce').fillna(0.0) # Value

    # 5. Define Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Near Expiry: FTD for {yesterday} | MTD from {start_of_month}")

    # 6. Initialize Data Structure
    store_metrics = {}

    # 7. Process Rows
    for index, row in df.iterrows():
        if pd.isna(row[6]): continue
        
        row_date = row[6].date()
        store_code = row[15]
        
        qty = row[4]
        val = row[5]
        
        if store_code not in store_metrics:
            store_metrics[store_code] = {
                'Value': {'FTD': 0.0, 'MTD': 0.0},
                'Items': {'FTD': 0.0, 'MTD': 0.0}
            }
        
        # MTD Calculation
        if start_of_month <= row_date <= yesterday:
            store_metrics[store_code]['Value']['MTD'] += val
            store_metrics[store_code]['Items']['MTD'] += qty
        
        # FTD Calculation
        if row_date == yesterday:
            store_metrics[store_code]['Value']['FTD'] += val
            store_metrics[store_code]['Items']['FTD'] += qty

    # 8. Update Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    # Map Dashboard Metric Names to our internal keys
    metric_map = {
        "Short Expire Value": 'Value',
        "Short expire items": 'Items'
    }

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in metric_map:
                key = metric_map[cell_type]
                
                # A. Get NEW values
                new_ftd = 0.0
                new_mtd = 0.0
                if store_code in store_metrics:
                    new_ftd = store_metrics[store_code][key]['FTD']
                    new_mtd = store_metrics[store_code][key]['MTD']
                
                # B. Get OLD values (Safely)
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Near Expiry Update complete!")
    else:
        print("No changes detected in Near Expiry metrics.")

def update_veh_released_metric(creds):
    """Fetches '#Of Veh Released <10am' and updates dashboard ONLY if changed."""
    print(f"\n--- Processing #Of Veh Released <10am ---")
    gc = gspread.authorize(creds)
    
    # 1. Open Source Sheet
    source_sheet_id = '1eYd1U5ooBjdgjwL3g4wWnUocNgnhaH6defIccVtx4CI'
    try:
        source_ws = gc.open_by_key(source_sheet_id).worksheet('Feb 26')
        source_data = source_ws.get_all_values()
    except Exception as e:
        print(f"Failed to open source sheet. Error: {e}")
        return

    if len(source_data) < 2: return

    # 2. Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    # Match header format: "1-Feb-26" or "11-Feb-26"
    yesterday_str = yesterday.strftime("%d-%b-%y").lstrip("0") 
    print(f"Looking for header: {yesterday_str}")

    # 3. Analyze Header Row
    header_row = source_data[0]
    
    ftd_col_idx = -1
    mtd_col_indices = [] # List of tuples: (col_1_idx, col_2_idx)

    for i, cell_val in enumerate(header_row):
        val_str = str(cell_val).strip()
        if not val_str: continue
        
        try:
            header_date = datetime.strptime(val_str, "%d-%b-%y").date()
            
            # Check if date is in MTD range
            if start_of_month <= header_date <= yesterday:
                mtd_col_indices.append((i, i + 1))
            
            # Check if date is FTD
            if header_date == yesterday:
                ftd_col_idx = i
        except ValueError:
            continue 

    if ftd_col_idx == -1:
        print(f"Could not find header '{yesterday_str}' in Row 1.")

    # 4. Helper to clean numbers
    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    # 5. Process Rows
    ftd_data = {} 
    mtd_data = {} 

    for row in source_data[2:]: 
        if len(row) < 1: continue
        
        store_code = str(row[0]).strip() # Column A
        if not store_code: continue
        
        # --- Calculate FTD ---
        ftd_val = 0.0
        if ftd_col_idx != -1 and len(row) > ftd_col_idx + 1:
            val1 = safe_float(row[ftd_col_idx])     # 1st col
            val2 = safe_float(row[ftd_col_idx + 1]) # 2nd col
            ftd_val = val1 + val2
            
        ftd_data[store_code] = ftd_val
        
        # --- Calculate MTD ---
        mtd_total = 0.0
        for (idx1, idx2) in mtd_col_indices:
            if len(row) > idx2:
                v1 = safe_float(row[idx1])
                v2 = safe_float(row[idx2])
                mtd_total += (v1 + v2)
        
        mtd_data[store_code] = mtd_total

    # 6. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    metric_name = "#Of Veh Released <10am"

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type == metric_name:
                
                # A. Get NEW values
                new_ftd = ftd_data.get(store_code, 0.0)
                new_mtd = mtd_data.get(store_code, 0.0)
                
                # B. Get OLD values
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                if abs(new_ftd - old_ftd) > 0.001 or abs(new_mtd - old_mtd) > 0.001:
                    updates_count += 1
                    
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_ftd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_mtd))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Update complete!")
    else:
        print(f"No changes detected in {metric_name}.")

def process_inventory_ageing(creds, file_path):
    """Reads Inventory_Aeging.csv and updates dashboard ONLY if changed."""
    print(f"\n--- Processing Inventory Ageing Report ---")
    
    try:
        df = pd.read_csv(file_path, encoding='latin1', low_memory=False, header=None)
    except Exception as e:
        print(f"Failed to read Inventory_Aeging.csv. Error: {e}")
        return

    if df.shape[1] <= 33:
        print(f"Error: CSV only has {df.shape[1]} columns, expected at least 34 to reach Column AH.")
        return

    # 1. Clean Store Code (Col O / Idx 14)
    df[14] = df[14].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 2. Convert Column P, AC, AF, AH to pure numbers
    df[15] = pd.to_numeric(df[15], errors='coerce').fillna(-1) 
    df[28] = pd.to_numeric(df[28], errors='coerce').fillna(0.0)
    df[31] = pd.to_numeric(df[31], errors='coerce').fillna(0.0)
    df[33] = pd.to_numeric(df[33], errors='coerce').fillna(0.0)

    # 3. Calculate Metrics
    
    # Metric 1: Claims Regular Value
    df_regular = df[df[15] == 7]
    dict_regular = df_regular.groupby(14)[31].sum().to_dict()

    # Metric 2: Claims RTV Value
    df_rtv = df[df[15] == 8]
    dict_rtv = df_rtv.groupby(14)[31].sum().to_dict()

    # Metric 3: >180 Days Claims Inventory
    df_180 = df[(df[15].isin([7, 8])) & (df[28] > 180)]
    dict_180 = df_180.groupby(14)[31].sum().to_dict()

    # Metric 4: Dual MRP Value
    df_12 = df[df[15] == 12]
    dict_12 = df_12.groupby(14)[33].sum().to_dict()

    # Combine into unified dictionary
    store_metrics = {}
    all_stores = set(dict_regular.keys()).union(set(dict_rtv.keys())).union(set(dict_180.keys())).union(set(dict_12.keys()))
    
    for store in all_stores:
        if store == 'nan' or not store: continue
        store_metrics[store] = {
            "claims regular value": dict_regular.get(store, 0.0),
            "claims rtv value": dict_rtv.get(store, 0.0),
            ">180 days claims inventory": dict_180.get(store, 0.0),
            "dual mrp value": dict_12.get(store, 0.0) 
        }

    # 4. Update Target Master Sheet (With Change Detection)
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    updates_count = 0
    
    valid_metrics = ["claims regular value", "claims rtv value", ">180 days claims inventory", "dual mrp value"]

    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type_clean = " ".join(str(row[1]).lower().split())
            
            if cell_type_clean in valid_metrics:
                
                # A. Get NEW Value (Default 0 if not found)
                new_val = 0.0
                if store_code in store_metrics:
                    new_val = float(store_metrics[store_code].get(cell_type_clean, 0.0))
                
                # B. Get OLD Values
                try:
                    old_ftd = float(str(row[2]).replace(',', '').strip() or 0)
                except: old_ftd = 0.0
                try:
                    old_mtd = float(str(row[3]).replace(',', '').strip() or 0)
                except: old_mtd = 0.0
                
                # C. Compare (Change Detection)
                # Note: FTD and MTD are typically the same for inventory snapshot reports
                if abs(new_val - old_ftd) > 0.001 or abs(new_val - old_mtd) > 0.001:
                    updates_count += 1
                    
                    # Queue Updates
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val))
                    cells_to_update.append(gspread.Cell(row=index+1, col=4, value=new_val))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Detected changes in {updates_count} rows. Updating Google Sheets...")
        target_ws.update_cells(cells_to_update)
        print("Inventory Ageing Update complete!")
    else:
        print("No changes detected in Inventory Ageing metrics.")


def main():
    creds = authenticate_service_account()
    drive_service = build('drive', 'v3', credentials=creds)

    print("--- Starting Dashboard Update Pipeline ---")

    # --- HELPER 1: Run standard sheet functions safely with a pause ---
    def run_safely(func, *args, name="Task"):
        try:
            print(f"\nStarting {name}...")
            func(*args)
        except Exception as e:
            print(f"Error in {name}: {e}")
        finally:
            print("Pausing for 15 seconds to respect API quota...")
            time.sleep(15)

    # 1. Pull data from all standard Google Sheets
    run_safely(update_damage_metric, creds, name="Damage Metric")
    run_safely(update_third_metric, creds, name="Third Metric")
    run_safely(update_fourth_metric, creds, name="Fourth Metric")
    run_safely(update_fifth_metric, creds, name="Fifth Metric")
    run_safely(update_sixth_metric, creds, name="Sixth Metric")
    run_safely(update_seventh_metric, creds, name="Seventh Metric")
    run_safely(update_eighth_metric, creds, name="Eighth Metric")
    run_safely(update_expense_metrics, creds, name="Expense Metrics")
    run_safely(update_osa_metric, creds, name="OSA Metric")
    run_safely(update_qc_tpv_vd_metrics, creds, name="QC/TPV/VD Metrics")
    run_safely(update_vehicle_count_metric, creds, name="Vehicle Count Metric")
    run_safely(update_ninth_metric, creds, name="Adhoc Vehicle Metric")
    run_safely(update_veh_released_metric, creds, name="Veh Released <10am Metric")

    # --- HELPER 2: Download, Process, and Clean Up Drive Files ---
    files_to_process = [
        ('PriceOverride.csv', process_price_override, "Price Override"),
        ('ArticleSalesReport.csv', process_article_sales_report, "Article Sales Report"),
        ('mb_51.xlsx', process_mb51_report, "MB51 Report"),
        ('near_expiry.XLSX', process_near_expiry_report, "Near Expiry Report"),
        ('Inventory_Aeging.csv', process_inventory_ageing, "Inventory Ageing"),
        ('vehicle_stats.XLSX', process_vehicle_stats, "Vehicle Stats"),
        ('Daily_KPI_Processing.xlsb', process_and_update_sheet, "Sales KPI")
    ]

    # 2. Process all Drive Files sequentially
    for filename, func, name in files_to_process:
        downloaded_file = None
        try:
            print(f"\nDownloading and starting {name}...")
            downloaded_file = download_from_drive(drive_service, filename=filename)
            
            if downloaded_file:
                func(creds, downloaded_file)
            else:
                print(f"Could not find or download {filename}")
                
        except Exception as e:
            print(f"Error in {name}: {e}")
        finally:
            # Delete the file to save local space
            if downloaded_file and os.path.exists(downloaded_file):
                os.remove(downloaded_file)
            print("Pausing for 15 seconds to respect API quota...")
            time.sleep(15)

    # 3. Calculate Derived Metrics (No pause needed after this, it's the last step!)
    try:
        print("\nCalculating Derived Metrics...")
        calculate_derived_metrics(creds)
    except Exception as e:
        print(f"Error in Derived Metrics: {e}")

    print("\n--- Pipeline Complete! ---")

if __name__ == '__main__':
    main()
