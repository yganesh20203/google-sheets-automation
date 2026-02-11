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
    """Processes the .xlsb and updates the Master Google Sheet."""
    print("Reading .xlsb data...")
    df = pd.read_excel(xlsb_path, sheet_name='Store Wise Raw Working', engine='pyxlsb', header=None)
    
    # 1. Force Store Code (Index 3 / Col D) to string
    df[3] = df[3].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 2. CREATE A MAPPING DICTIONARY 
    metric_mapping = {
        "Sales Tgt": 8,       # Column I
        "Sales Ach": 15,      # Column P
        "MAC Plan": 23,       # Column X
        "MAC Actual": 29,     # Column AD
        "Lines Plan": 32,     # Column AG
        "Lines Act": 37,      # Column AL
        "OTGS Plan": 16,      # Column Q
        "OTGS Act": 18,       # Column S
        "Txns": 41            # Column AP
    }
    
    # 3. Clean and convert ALL mapped columns to numeric at once
    cols_to_sum = list(metric_mapping.values())
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
    
    # 5. Loop through the Google Sheet and update based on the mapping
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = str(row[0]).strip() 
            cell_type = str(row[1]).strip()
            
            if store_code in grouped_data and cell_type in metric_mapping:
                xlsb_col_index = metric_mapping[cell_type]
                new_val = grouped_data[store_code][xlsb_col_index]
                
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val))
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//2} Sales records in Google Sheets...")
        worksheet.update_cells(cells_to_update)
        print("Sales KPI Update complete!")
    else:
        print("No matching rows found to update for Sales KPIs.")
def update_damage_metric(creds):
    """Fetches multiple metrics from a separate Google Sheet and updates the dashboard."""
    print(f"\n--- Processing Secondary Google Sheet Data ---")
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
    if df_source.empty:
        return
        
    # 2. Filter for Yesterday's Date (Column A / Index 0)
    df_source[0] = pd.to_datetime(df_source[0], errors='coerce').dt.normalize()
    ist_timezone = timezone(timedelta(hours=5, minutes=30))
    yesterday = (datetime.now(ist_timezone) - timedelta(days=1)).date()
    
    df_filtered = df_source[df_source[0].dt.date == yesterday].copy()
    if df_filtered.empty:
        print(f"No data found for yesterday ({yesterday.strftime('%m/%d/%Y')}).")
        return
        
    # Clean Store Code (Col B / Index 1)
    df_filtered[1] = df_filtered[1].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # ==========================================
    # 3. CREATE YOUR MAPPING DICTIONARY HERE
    # Format: {"Exact Name in Master Sheet": source_sheet_column_index}
    # Index guide: A=0, B=1, C=2, D=3, E=4, F=5, G=6...
    # ==========================================
    sheet_metric_mapping = {
        "DT(Damage)": 4,    # Column E
        "DD(Expiry)": 5,    # Assuming this is in Column F. Change as needed!
        "CO(shrink)": 6     # Assuming this is in Column G. Change as needed!
    }
    
    # Clean and convert all mapped columns to numbers
    cols_to_sum = list(sheet_metric_mapping.values())
    for col_idx in cols_to_sum:
        # Ensures that if a column is missing in the sheet, it doesn't crash
        if col_idx in df_filtered.columns: 
            df_filtered[col_idx] = pd.to_numeric(df_filtered[col_idx], errors='coerce').fillna(0)
    
    # Group by Store Code and sum ALL mapped columns simultaneously
    grouped_data = df_filtered.groupby(1)[cols_to_sum].sum().to_dict('index')
    
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
            
            # If store code is found AND the row name matches our dictionary
            if store_code in grouped_data and cell_type in sheet_metric_mapping:
                # Get the correct column index from the dictionary
                col_index = sheet_metric_mapping[cell_type]
                
                # Check if that data exists for the store
                if col_index in grouped_data[store_code]:
                    new_val = grouped_data[store_code][col_index]
                    
                    # Update FTD_Value (Col C) & Last_Updated (Col E)
                    cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val))
                    cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)//2} records from the secondary sheet...")
        target_ws.update_cells(cells_to_update)
        print("Secondary Sheet Update complete!")
    else:
        print("No matching rows found to update for secondary sheet.")

def main():
    creds = authenticate_service_account()
    
    # --- STEP 1: Process the Damage Sheet ---
    update_damage_metric(creds)
    
    # --- STEP 2: Process the Sales .xlsb File from Drive ---
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
