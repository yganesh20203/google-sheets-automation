import os
import io
import pandas as pd
import numpy as np
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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
    """Fetches Damage data, calculates FTD and MTD, and updates the dashboard."""
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
        
    # 2. Determine Dates (Yesterday and Start of Month)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1) # 1st day of the current month
    
    print(f"Calculating FTD for {yesterday} and MTD from {start_of_month} to {yesterday}")
    
    # Normalize the date column (Column A / Index 0)
    df_source[0] = pd.to_datetime(df_source[0], errors='coerce').dt.normalize()
    
    # Create two filtered DataFrames: One for FTD, one for MTD
    df_ftd = df_source[df_source[0].dt.date == yesterday].copy()
    df_mtd = df_source[(df_source[0].dt.date >= start_of_month) & (df_source[0].dt.date <= yesterday)].copy()
    
    if df_mtd.empty:
        print("No MTD data found for this month.")
        return
        
    # 3. Clean and Mapped Columns
    sheet_metric_mapping = {
        "DT(Damage)": 4,    # Column E
        "DD(Expiry)": 5,    # Column F (Example - Update if different)
        "CO(shrink)": 6     # Column G (Example - Update if different)
    }
    cols_to_sum = list(sheet_metric_mapping.values())
    
    # Clean Store Codes and force metrics to numeric for BOTH DataFrames
    for df in [df_ftd, df_mtd]:
        if not df.empty:
            df[1] = df[1].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            for col_idx in cols_to_sum:
                if col_idx in df.columns: 
                    df[col_idx] = pd.to_numeric(df[col_idx], errors='coerce').fillna(0)
    
    # Group by Store Code and sum
    grouped_ftd = df_ftd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_ftd.empty else {}
    grouped_mtd = df_mtd.groupby(1)[cols_to_sum].sum().to_dict('index') if not df_mtd.empty else {}
    
    # 4. Update Target Master Sheet
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Check if metric is in our mapping AND the store has at least some MTD data
            if cell_type in sheet_metric_mapping and store_code in grouped_mtd:
                col_index = sheet_metric_mapping[cell_type]
                
                # Extract values (Default to 0 if the store had no data for that specific bucket)
                ftd_val = grouped_ftd.get(store_code, {}).get(col_index, 0)
                mtd_val = grouped_mtd.get(store_code, {}).get(col_index, 0)
                
                # Update FTD_Value (Col C)
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                # Update MTD_Value (Col D)
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                # Update Last_Updated (Col E)
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records (FTD & MTD) from the secondary sheet...")
        target_ws.update_cells(cells_to_update)
        print("Secondary Sheet Update complete!")
    else:
        print("No matching rows found to update for secondary sheet.")


def update_third_metric(creds):
    """Fetches T-2 data from the third Google Sheet, calculates FTD and MTD."""
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
    
    # Parse Date Column D (Index 3). Format is mm-dd-yyyy
    df_source[3] = pd.to_datetime(df_source[3], errors='coerce').dt.normalize()
    
    # Filter DataFrames for FTD and MTD
    df_ftd = df_source[df_source[3].dt.date == target_date].copy()
    df_mtd = df_source[(df_source[3].dt.date >= start_of_month) & (df_source[3].dt.date <= target_date)].copy()
    
    if df_mtd.empty:
        print("No MTD data found for this period.")
        return
        
    # ==========================================
    # 3. CREATE YOUR MAPPING DICTIONARY
    # Because you didn't mention WHICH row this updates in the Master Sheet,
    # replace "YOUR_METRIC_NAME_HERE" with the exact name from Column B (e.g., "OFR %")
    # ==========================================
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
    
    # 4. Update Target Master Sheet
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in sheet_metric_mapping and store_code in grouped_mtd:
                col_index = sheet_metric_mapping[cell_type]
                
                ftd_val = grouped_ftd.get(store_code, {}).get(col_index, 0)
                mtd_val = grouped_mtd.get(store_code, {}).get(col_index, 0)
                
                # Update FTD (Col C), MTD (Col D), and Last_Updated (Col E)
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for the third sheet...")
        target_ws.update_cells(cells_to_update)
        print("Third Sheet Update complete!")
    else:
        print("No matching rows found to update for the third sheet.")

def update_fourth_metric(creds):
    """Fetches FTD and MTD data from specific rows in the fourth Google Sheet using the Global Store Mapping."""
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

    # Helper function to safely convert sheet values (like "2.4%") to numbers (0.024)
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
            # If it was a percentage, divide by 100 so Google Sheets reads it correctly
            return num / 100.0 if is_percent else num
        except ValueError:
            return 0.0

    # 2. Extract FTD Data (Rows 33 to 64 -> Python Index 32 to 64)
    ftd_data = {}
    for row in source_data[32:64]:
        if len(row) > 0: 
            # CRITICAL FIX: Store names are in Column A (Index 0), NOT Column B!
            store_name = str(row[0]).strip()
            if store_name:
                # Dynamically extract ALL columns in the row
                ftd_data[store_name] = {i: safe_float(val) for i, val in enumerate(row)}

    # Extract MTD Data (Rows 72 to 102 -> Python Index 71 to 102)
    mtd_data = {}
    for row in source_data[71:102]:
        if len(row) > 0: 
            # CRITICAL FIX: Store names are in Column A (Index 0), NOT Column B!
            store_name = str(row[0]).strip()
            if store_name:
                mtd_data[store_name] = {i: safe_float(val) for i, val in enumerate(row)}

    # ==========================================
    # 3. MAPPING TO DASHBOARD ROWS
    # ==========================================
    fourth_sheet_mapping = {
        "Canc%":   {"FTD": 2,  "MTD": 2}, # C mapped to C
        "RTO%":   {"FTD": 3,  "MTD": 3}, # D mapped to D
        "D1": {"FTD": 9,  "MTD": 8}, # J mapped to I
        "D2": {"FTD": 10, "MTD": 9},
        "OFR %": {"FTD": 12, "MTD": 11} # Mapped perfectly based on your previous edit
    }

    # 4. Update Target Master Sheet
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # If the metric is mapped AND the store_code exists in our global dictionary
            if cell_type in fourth_sheet_mapping and store_code in STORE_MAPPING:
                
                # Get the translated store name (e.g., "4702" -> "Amritsar")
                target_store_name = STORE_MAPPING[store_code]
                
                ftd_col_idx = fourth_sheet_mapping[cell_type]["FTD"]
                mtd_col_idx = fourth_sheet_mapping[cell_type]["MTD"]
                
                # Pull values using the translated store name. Default to 0 if missing.
                ftd_val = ftd_data.get(target_store_name, {}).get(ftd_col_idx, 0)
                mtd_val = mtd_data.get(target_store_name, {}).get(mtd_col_idx, 0)
                
                # Queue updates
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for the fourth sheet...")
        target_ws.update_cells(cells_to_update)
        print("Fourth Sheet Update complete!")
    else:
        print("No matching rows found to update for the fourth sheet.")

def update_fifth_metric(creds):
    """Fetches 'AR' data, dynamically finding the date columns, and averages MTD."""
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
        # Clean the string (e.g., "Tue, Feb 3," -> "Tue, Feb 3")
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

    # 5. Update Target Master Sheet
    print("Connecting to target Google Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Match strictly against "AR" and our Store Mapping
            if cell_type == "AR" and store_code in STORE_MAPPING:
                
                target_store_name = STORE_MAPPING[store_code]
                
                ftd_val = ftd_data.get(target_store_name, 0.0)
                mtd_val = mtd_data.get(target_store_name, 0.0)
                
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for the fifth sheet...")
        target_ws.update_cells(cells_to_update)
        print("Fifth Sheet (AR) Update complete!")
    else:
        print("No matching rows found to update for AR metric.")

def calculate_derived_metrics(creds):
    """Calculates all percentages and ratios from the Master Sheet's updated values."""
    print("\n--- Calculating Derived Metrics (%, LPB, ABV, TPC) ---")
    gc = gspread.authorize(creds)
    
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    worksheet = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = worksheet.get_all_values()
    
    # store_data will hold the base numbers: {'4702': {'Sales Ach': {'FTD': 100, 'MTD': 500}, ...}}
    store_data = {}
    
    # row_mappings tracks the exact row index to push the calculated data back into
    row_mappings = {} 
    
    # --- FIRST PASS: Collect all base metrics and map the rows ---
    for idx, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            if not store_code: continue
            if store_code not in store_data: store_data[store_code] = {}
            
            # Safely convert FTD and MTD to floats
            try:
                ftd = float(str(row[2]).replace(',', '').strip() or 0)
            except: ftd = 0.0
            try:
                mtd = float(str(row[3]).replace(',', '').strip() or 0)
            except: mtd = 0.0
            
            store_data[store_code][cell_type] = {'FTD': ftd, 'MTD': mtd}
            
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
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    # Helper function for division to prevent ZeroDivisionError (returns 0.0 if denominator is 0)
    def safe_div(num, den):
        return num / den if den else 0.0

    # --- SECOND PASS: Calculate and queue updates ---
    for store_code, metrics in store_data.items():
        
        # Calculate all required derived metrics simultaneously
        derived_calcs = {
            "Sales Vs Plan": {
                "FTD": safe_div(metrics.get("Sales Ach", {}).get("FTD", 0), metrics.get("Sales Tgt", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Sales Ach", {}).get("MTD", 0), metrics.get("Sales Tgt", {}).get("MTD", 0))
            },
            "MAC Vs Plan": {
                "FTD": safe_div(metrics.get("MAC Actual", {}).get("FTD", 0), metrics.get("MAC Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("MAC Actual", {}).get("MTD", 0), metrics.get("MAC Plan", {}).get("MTD", 0))
            },
            "Lines Vs Plan": {
                "FTD": safe_div(metrics.get("Lines Act", {}).get("FTD", 0), metrics.get("Lines Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Lines Act", {}).get("MTD", 0), metrics.get("Lines Plan", {}).get("MTD", 0))
            },
            "LPB": {
                "FTD": safe_div(metrics.get("Lines Act", {}).get("FTD", 0), metrics.get("Txns", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Lines Act", {}).get("MTD", 0), metrics.get("Txns", {}).get("MTD", 0))
            },
            "OTGS Sales Vs Plan": {
                "FTD": safe_div(metrics.get("OTGS Act", {}).get("FTD", 0), metrics.get("OTGS Plan", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("OTGS Act", {}).get("MTD", 0), metrics.get("OTGS Plan", {}).get("MTD", 0))
            },
            "ABV": {
                "FTD": safe_div(metrics.get("Sales Ach", {}).get("FTD", 0), metrics.get("Txns", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Sales Ach", {}).get("MTD", 0), metrics.get("Txns", {}).get("MTD", 0))
            },
            "TPC": {
                "FTD": safe_div(metrics.get("Txns", {}).get("FTD", 0), metrics.get("MAC Actual", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("Txns", {}).get("MTD", 0), metrics.get("MAC Actual", {}).get("MTD", 0))
            },
            "DT%": {
                "FTD": safe_div(metrics.get("DT(Damage)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("DT(Damage)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0))
            },
            "DD%": {
                "FTD": safe_div(metrics.get("DD(Expiry)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("DD(Expiry)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0))
            },
            "CO%": {
                "FTD": safe_div(metrics.get("CO(shrink)", {}).get("FTD", 0), metrics.get("Sales Ach", {}).get("FTD", 0)),
                "MTD": safe_div(metrics.get("CO(shrink)", {}).get("MTD", 0), metrics.get("Sales Ach", {}).get("MTD", 0))
            }
        }
        
        # Build the batch update payload
        for calc_name, vals in derived_calcs.items():
            if (store_code, calc_name) in row_mappings:
                row_idx = row_mappings[(store_code, calc_name)]
                cells_to_update.append(gspread.Cell(row=row_idx, col=3, value=vals["FTD"]))
                cells_to_update.append(gspread.Cell(row=row_idx, col=4, value=vals["MTD"]))
                cells_to_update.append(gspread.Cell(row=row_idx, col=5, value=current_time))

    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} derived metric records in Google Sheets...")
        worksheet.update_cells(cells_to_update)
        print("Derived Metrics Update complete!")
    else:
        print("No derived metrics to update.")

def update_seventh_metric(creds):
    """Fetches 'Tonnage Plan' and 'Order Plan' data, matching dates like '1-Feb'."""
    print(f"\n--- Processing Seventh Google Sheet (Tonnage & Order Plans) ---")
    gc = gspread.authorize(creds)
    
    source_sheet_id = '1dgmZmhcmJrSd2QzjyzGtk37w3dgHs2-qm_2FnsXwaoY'
    
    # Configuration for the two tabs
    # Format: {"Exact Dashboard Metric Name": Worksheet_GID}
    metric_configs = {
        "Tonnage Plan": 292200791,
        "Order Plan": 53822165
    }

    # Reverse our global STORE_MAPPING so we can look up "Amritsar" and get "4702"
    REVERSE_STORE_MAPPING = {v.lower().strip(): k for k, v in STORE_MAPPING.items()}

    # Determine Dates
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    current_year = yesterday.year

    print(f"Finding FTD for {yesterday} and MTD Sum from {start_of_month} to {yesterday}")

    # Connect to Target Dashboard
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")

    def safe_float(val):
        val_str = str(val).replace(',', '').strip()
        if not val_str or val_str in ['-', 'NA', '#DIV/0!', '#N/A']: return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0

    # Process each tab one by one
    for metric_name, gid in metric_configs.items():
        print(f"Processing tab for: {metric_name}...")
        try:
            source_ws = gc.open_by_key(source_sheet_id).get_worksheet_by_id(gid) 
            source_data = source_ws.get_all_values()
        except Exception as e:
            print(f"Failed to open {metric_name} sheet. Error: {e}")
            continue

        if len(source_data) < 3: continue

        # Dynamically find the Date Column Indexes in Row 2 (Python Index 1)
        date_row = source_data[1] 
        ftd_col_idx = -1
        mtd_col_indices = []

        for idx, cell_val in enumerate(date_row):
            val_str = str(cell_val).strip()
            if not val_str: continue
            
            try:
                # Format "1-Feb" into "1-Feb-2026" so Pandas parses it accurately
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

        # Extract Data from Row 3 onwards (Python Index 2+)
        for row in source_data[2:]:
            if len(row) > 0:
                store_name = str(row[0]).strip().lower() # Store name is in Column A
                
                # Check if this name exists in our reverse mapping dict
                if store_name in REVERSE_STORE_MAPPING:
                    store_code = REVERSE_STORE_MAPPING[store_name]
                    
                    ftd_val = safe_float(row[ftd_col_idx]) if ftd_col_idx != -1 and ftd_col_idx < len(row) else 0.0
                    
                    # Sum all the days up to yesterday for MTD
                    mtd_vals = [safe_float(row[i]) for i in mtd_col_indices if i < len(row)]
                    mtd_val = sum(mtd_vals)
                    
                    # Queue the update by matching store code and metric name in Master Sheet
                    for t_idx, t_row in enumerate(target_data):
                        if len(t_row) >= 2:
                            if str(t_row[0]).strip() == store_code and str(t_row[1]).strip() == metric_name:
                                cells_to_update.append(gspread.Cell(row=t_idx+1, col=3, value=ftd_val))
                                cells_to_update.append(gspread.Cell(row=t_idx+1, col=4, value=mtd_val))
                                cells_to_update.append(gspread.Cell(row=t_idx+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for Tonnage & Order Plans...")
        target_ws.update_cells(cells_to_update)
        print("Seventh Sheet Update complete!")
    else:
        print("No matching rows found to update for Tonnage & Order Plans.")
        
def update_sixth_metric(creds):
    """Fetches '>50 Lines Invoices' data from a dynamically named tab (yyyy-mm-dd)."""
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
    # Column G (Index 6) = MTD (MTD Order Count)  <-- FIXED THIS based on screenshot
    extracted_data = {}
    
    # Skip the header row (index 0) and loop through the rest
    for row in source_data[1:]:
        # We only need the row to be at least 7 columns long to reach Column G
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

    # 4. Update Target Master Sheet
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Target specifically the row for ">50 Lines Invoices"
            if cell_type == ">50 Lines Invoices" and store_code in extracted_data:
                
                ftd_val = extracted_data[store_code]["FTD"]
                mtd_val = extracted_data[store_code]["MTD"]
                
                # Queue updates for FTD, MTD, and Timestamp
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for '>50 Lines Invoices'...")
        target_ws.update_cells(cells_to_update)
        print("Sixth Sheet Update complete!")
    else:
        print("No matching rows found to update for '>50 Lines Invoices'.")


def update_eighth_metric(creds):
    """Fetches 'Tonnage attainment' data from merged date headers."""
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

    # 5. Update Target Master Sheet
    print("Connecting to target Master Sheet...")
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            # Match strictly against "Tonnage attainment"
            if cell_type == "Tonnage attainment" and store_code in extracted_data:
                
                ftd_val = extracted_data[store_code]["FTD"]
                mtd_val = extracted_data[store_code]["MTD"]
                
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for 'Tonnage attainment'...")
        target_ws.update_cells(cells_to_update)
        print("Eighth Sheet (Tonnage Attainment) Update complete!")
    else:
        print("No matching rows found to update for Tonnage attainment.")


def process_vehicle_stats(creds, file_path):
    """Reads vehicle_stats.XLSX, filters invalid times, and calculates 4 key metrics."""
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
    
    # 3. Parse Date (Column E / Index 4) - Format is mm/dd/yyyy
    df[4] = pd.to_datetime(df[4], format='%m/%d/%Y', errors='coerce').dt.normalize()
    
    # 4. Filter Out Invalid Times (Column P / Index 15)
    # We remove rows where Time is "12:00:00 AM" or empty
    def is_valid_time(val):
        s = str(val).strip().upper()
        return s != "12:00:00 AM" and s != "00:00:00" and s != "NAN"
    
    df = df[df[15].apply(is_valid_time)].copy()
    
    # 5. Define Dates (Yesterday & MTD)
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    start_of_month = yesterday.replace(day=1)
    
    print(f"Vehicle Stats: Filtering for FTD ({yesterday}) and MTD ({start_of_month} to {yesterday})")
    
    # 6. Filter DataFrames for FTD and MTD
    df_ftd = df[df[4].dt.date == yesterday].copy()
    df_mtd = df[(df[4].dt.date >= start_of_month) & (df[4].dt.date <= yesterday)].copy()
    
    if df_mtd.empty:
        print("No valid vehicle data found for this period.")
        return

    # Helper to calculate metrics for a specific DataFrame
    def calc_metrics(subset_df):
        if subset_df.empty: return {}
        
        # Metric 1: #Of Vehicle Received (Count of rows)
        vehicle_count = subset_df.groupby(1).size().to_dict()
        
        # Metric 2: #Of Cases (Sum of Column Q / Index 16)
        subset_df[16] = pd.to_numeric(subset_df[16], errors='coerce').fillna(0)
        cases_sum = subset_df.groupby(1)[16].sum().to_dict()
        
        # Metric 3: #Of couriers (Count where Col N / Index 13 == 'Courier')
        # Normalize text to handle "Courier", "courier ", etc.
        courier_df = subset_df[subset_df[13].astype(str).str.strip().str.title() == "Courier"]
        courier_count = courier_df.groupby(1).size().to_dict()
        
        # Metric 4: #Of HUL Vehicle (Count where Col G / Index 6 in HUL list)
        hul_vendors = ["HINDUSTAN UNILEVER LIMITED 01", "HINDUSTAN UNILEVER LIMITED 02"]
        # Normalize text to handle formatting issues
        hul_df = subset_df[subset_df[6].astype(str).str.strip().isin(hul_vendors)]
        hul_count = hul_df.groupby(1).size().to_dict()
        
        return {
            "#Of Vehicle Received": vehicle_count,
            "#Of Cases": cases_sum,
            "#Of couriers": courier_count,
            "#Of HUL Vehicle": hul_count
        }

    # Calculate metrics for both time periods
    ftd_metrics = calc_metrics(df_ftd)
    mtd_metrics = calc_metrics(df_mtd)
    
    # 7. Update Master Sheet
    print("Connecting to target Master Sheet...")
    gc = gspread.authorize(creds)
    target_sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    target_ws = gc.open_by_key(target_sheet_id).worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    metrics_list = ["#Of Vehicle Received", "#Of Cases", "#Of couriers", "#Of HUL Vehicle"]
    
    for index, row in enumerate(target_data):
        if len(row) >= 2:
            store_code = str(row[0]).strip()
            cell_type = str(row[1]).strip()
            
            if cell_type in metrics_list:
                # Get FTD Value (Default to 0)
                ftd_val = ftd_metrics.get(cell_type, {}).get(store_code, 0)
                
                # Get MTD Value (Default to 0)
                mtd_val = mtd_metrics.get(cell_type, {}).get(store_code, 0)
                
                # Queue Update
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))

    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} records for Vehicle Stats...")
        target_ws.update_cells(cells_to_update)
        print("Vehicle Stats Update complete!")
    else:
        print("No matching rows found to update for Vehicle Stats.")


def update_expense_metrics(creds):
    """Fetches expenses from the tab named 'Sheet1' (ignoring the Dashboard tab)."""
    print(f"\n--- Processing Expense Metrics (Targeting 'Sheet1') ---")
    gc = gspread.authorize(creds)
    
    source_sheet_id = '1B7sKYLDr0KPA8tCMDUHgYo-fO4yBDnj6K3ubXCC8gfs'
    try:
        # FIX: Open the specific tab named 'Sheet1' instead of the first tab
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
        # Diagnostic print to help if it fails again
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
    # Based on your screenshot, the columns should be:
    # Col D (Index 3): Store Code
    # Col F (Index 5): Date
    # Col G (Index 6): Account Head (Category)
    # Col L (Index 11): Invoice Amount
    # Col U (Index 20): Settlement Amount
    
    for row in source_data[header_index + 1:]:
        if len(row) < 4: continue # Skip empty rows
        
        # Pad row if short
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
            
            # TRY COLUMN U (Settlement) FIRST
            val = safe_float(row[20])
            
            # FALLBACK: If Col U is 0, use Col L (Invoice Amt - Index 11)
            # This captures "Submitted" expenses that aren't settled yet
            if val == 0:
                val = safe_float(row[11])

            if store_code not in ftd_data: ftd_data[store_code] = {}
            if store_code not in mtd_data: mtd_data[store_code] = {}

            if start_of_month <= row_date <= yesterday:
                mtd_data[store_code][category] = mtd_data[store_code].get(category, 0) + val
            
            if row_date == yesterday:
                ftd_data[store_code][category] = ftd_data[store_code].get(category, 0) + val

    print(f"Processed {rows_processed} valid expense rows.")

    # 4. Update Target Master Sheet
    print("Connecting to target Master Sheet...")
    target_ws = gc.open_by_key('1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34').worksheet('Store_Data') 
    target_data = target_ws.get_all_values()
    
    cells_to_update = []
    current_time = datetime.now(ist_timezone).strftime("%d-%b-%Y %I:%M %p")
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if cell_type in target_categories:
                ftd_val = ftd_data.get(store_code, {}).get(cell_type, 0)
                mtd_val = mtd_data.get(store_code, {}).get(cell_type, 0)
                
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=ftd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=4, value=mtd_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//3} expense records...")
        target_ws.update_cells(cells_to_update)
        print("Expense Metrics Update complete!")
    else:
        print("No matching expense rows found to update.")
        

        
def main():
    creds = authenticate_service_account()
    drive_service = build('drive', 'v3', credentials=creds)

    # 1. Pull data from all secondary APIs
    update_damage_metric(creds)
    update_third_metric(creds)
    update_fourth_metric(creds)
    update_fifth_metric(creds)
    update_sixth_metric(creds)
    update_seventh_metric(creds)
    update_eighth_metric(creds)
    update_expense_metrics(creds)
    
    # 2. Process Vehicle Stats (.XLSX) - NEW STEP
    # Reuse the download function but pass the specific filename
    vehicle_file = download_from_drive(drive_service, filename='vehicle_stats.XLSX')
    if vehicle_file:
        try:
            process_vehicle_stats(creds, vehicle_file)
        finally:
            if os.path.exists(vehicle_file):
                os.remove(vehicle_file)

    # 3. Process Sales KPI (.xlsb)
    # Note: Using default filename 'Daily_KPI_Processing.xlsb'
    kpi_file = download_from_drive(drive_service) 
    if kpi_file:
        try:
            process_and_update_sheet(creds, kpi_file)
        finally:
            if os.path.exists(kpi_file):
                os.remove(kpi_file)
                
    # 4. CALCULATE ALL DERIVED METRICS LAST!
    calculate_derived_metrics(creds)

if __name__ == '__main__':
    main()
