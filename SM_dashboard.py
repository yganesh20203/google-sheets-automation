import os
import io
import pandas as pd
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

def authenticate_service_account():
    """Authenticates using a Service Account JSON file for GitHub Actions."""
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


def main():
    creds = authenticate_service_account()
    
    # --- STEP 1: Process the Damage Sheet (T-1) ---
    update_damage_metric(creds)
    
    # --- STEP 2: Process the New T-2 Sheet ---
    update_third_metric(creds)
    
    # --- STEP 3: Process the Sales .xlsb File from Drive ---
    drive_service = build('drive', 'v3', credentials=creds)
    file_path = download_from_drive(drive_service)
    
    if file_path:
        try:
            process_and_update_sheet(creds, file_path)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

if __name__ == '__main__':
    main()
