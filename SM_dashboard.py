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
    # Format: {"Exact Name in Google Sheet Col B": pandas_column_index}
    # Note: Pandas is 0-indexed (A=0, B=1... I=8, P=15, X=23, etc.)
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
    # This creates a nested dictionary: {'4702': {8: 1000, 15: 950, 23: 500...}}
    grouped_data = df.groupby(3)[cols_to_sum].sum().to_dict('index')
    
    print("Connecting to target Google Sheet...")
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
            
            # If the store code exists in our data AND the metric is in our dictionary
            if store_code in grouped_data and cell_type in metric_mapping:
                
                # Get the correct column index for this specific metric
                xlsb_col_index = metric_mapping[cell_type]
                
                # Extract the summed value
                new_val = grouped_data[store_code][xlsb_col_index]
                
                # Queue the FTD_Value update (Column C / col=3)
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val))
                # Queue the Last_Updated update (Column E / col=5)
                cells_to_update.append(gspread.Cell(row=index+1, col=5, value=current_time))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)} cells in Google Sheets...")
        worksheet.update_cells(cells_to_update)
        print("Update complete!")
    else:
        print("No matching rows found to update.")

def main():
    creds = authenticate_service_account()
    drive_service = build('drive', 'v3', credentials=creds)
    
    # 1. Pull the file from Google Drive
    file_path = download_from_drive(drive_service)
    
    if file_path:
        try:
            # 2. Process locally and push to Google Sheets
            process_and_update_sheet(creds, file_path)
        finally:
            # Clean up the local temporary file
            if os.path.exists(file_path):
                os.remove(file_path)

if __name__ == '__main__':
    main()
