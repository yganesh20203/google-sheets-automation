import os
import io
import pandas as pd
import gspread
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
    # Read using pyxlsb. header=None to use integer index
    df = pd.read_excel(xlsb_path, sheet_name='Store Wise Raw Working', engine='pyxlsb', header=None)
    
    # Col D is index 3, Col I is index 8. Force Col 8 to numeric, handle errors, fill NaNs.
    df[8] = pd.to_numeric(df[8], errors='coerce').fillna(0)
    
    # Group by Store Code and sum the values
    grouped_data = df.groupby(3)[8].sum().to_dict()
    
    print("Connecting to target Google Sheet...")
    gc = gspread.authorize(creds)
    sheet_id = '1BTy6r3ep-NhUQ1iCFGM2VWqKXPysyfnoiTJdUZzzl34'
    worksheet = gc.open_by_key(sheet_id).sheet1 
    
    target_data = worksheet.get_all_values()
    cells_to_update = []
    
    for index, row in enumerate(target_data):
        if len(row) >= 2: 
            store_code = row[0] # Column A
            cell_type = row[1]  # Column B
            
            if store_code in grouped_data and cell_type == "Sales Tgt":
                # gspread is 1-indexed. row=index+1, col=3 (Column C)
                new_val = grouped_data[store_code]
                cells_to_update.append(gspread.Cell(row=index+1, col=3, value=new_val))
                
    if cells_to_update:
        print(f"Updating {len(cells_to_update)} records in Google Sheets...")
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
