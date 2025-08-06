import pandas as pd
import os
import numpy as np
import io

# Google API Client libraries are required for this version.
# Make sure to update your requirements/workflow file.
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- Configuration ---
# ID of the Google Drive folder containing the files.
# This is the string of characters at the end of your folder's URL.
DRIVE_FOLDER_ID = "1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da"

# Filenames to search for in the Google Drive folder
DRIVE_FILENAMES = {
    "capacity_dump": "Capacity_dump.csv",
    "merged_breach": "Merged_breach_report.csv",
    "vd_raw_file": "VD_raw_file.txt"
}

# Name for the temporary file to hold service account credentials
SERVICE_ACCOUNT_FILE = 'gcp_service_account_key.json'

def setup_drive_service():
    """Sets up the Google Drive API service using service account credentials."""
    print("--- Setting up Google Drive Service ---")
    # The service account key is passed as a GitHub secret and written to a file
    gcp_sa_key_json = os.getenv('GCP_SA_KEY')
    if not gcp_sa_key_json:
        print("ERROR: GCP_SA_KEY environment variable not found.")
        print("Please ensure you have set this secret in your GitHub repository settings.")
        return None

    try:
        # Write the key to a temporary file for the Credentials object to read
        with open(SERVICE_ACCOUNT_FILE, 'w') as f:
            f.write(gcp_sa_key_json)

        # Define the required scopes for Google Drive API
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("Google Drive service created successfully.")
        return service
    except Exception as e:
        print(f"ERROR: Failed to create Google Drive service: {e}")
        return None
    finally:
        # Clean up the temporary credentials file
        if os.path.exists(SERVICE_ACCOUNT_FILE):
            os.remove(SERVICE_ACCOUNT_FILE)

def find_and_download_file(service, folder_id, file_name):
    """Searches for a file by name in a specific folder and downloads its content."""
    print(f"Searching for '{file_name}' in Google Drive...")
    try:
        # Construct the query to search by name and parent folder, excluding trashed files
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])

        if not files:
            print(f"WARNING: File '{file_name}' not found in the specified folder.")
            return None, None  # Return None for both content and file ID

        # Assume the first file found is the correct one
        file_id = files[0].get('id')
        print(f"Found '{file_name}' with ID: {file_id}. Downloading...")

        request = service.files().get_media(fileId=file_id)
        # Use io.BytesIO to handle the downloaded content in memory
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%.")
        
        # Reset the buffer's position to the beginning before reading
        file_content.seek(0)
        print("Download complete.")
        return file_content, file_id  # Return the in-memory content and the file's ID

    except Exception as e:
        print(f"An error occurred while downloading '{file_name}': {e}")
        return None, None

def upload_file_to_drive(service, folder_id, data_to_upload, file_name, mime_type, existing_file_id=None):
    """Uploads or updates a file in Google Drive."""
    print(f"Uploading '{file_name}' to Google Drive...")
    
    # Create an in-memory file-like object from the string data
    media_body = io.BytesIO(data_to_upload.encode('utf-8'))
    media = MediaFileUpload(None, mimetype=mime_type, chunksize=1024*1024, resumable=True)
    media.stream = lambda: media_body

    try:
        file_metadata = {'name': file_name}

        if existing_file_id:
            # Update the existing file
            print(f"Updating existing file with ID: {existing_file_id}")
            file = service.files().update(fileId=existing_file_id, body=file_metadata, media_body=media, fields='id').execute()
        else:
            # Create a new file in the specified folder
            print("Creating new file...")
            file_metadata['parents'] = [folder_id]
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        print(f"Upload successful. New File ID: {file.get('id')}")
    except Exception as e:
        print(f"An error occurred during upload: {e}")


def process_int_order(row):
    """Calculates the value for the 'Int_order' column."""
    order_num_str = str(row['Hybris Order Number'])
    length = len(order_num_str)
    try:
        if length == 10:
            return int(order_num_str)
        elif length == 16:
            return int(order_num_str.replace('"', ''))
        else:
            return 0
    except (ValueError, TypeError):
        return 0

def main():
    """Main function to run the entire data processing pipeline."""
    drive_service = setup_drive_service()
    if not drive_service:
        return

    # Download source files from Google Drive
    print("\n--- Downloading Source Files ---")
    capacity_content, _ = find_and_download_file(drive_service, DRIVE_FOLDER_ID, DRIVE_FILENAMES["capacity_dump"])
    merged_breach_content, _ = find_and_download_file(drive_service, DRIVE_FOLDER_ID, DRIVE_FILENAMES["merged_breach"])

    if not capacity_content or not merged_breach_content:
        print("ERROR: Could not download one or more essential source files. Exiting.")
        return

    # Load data into pandas from in-memory content
    print("\n--- Loading data into pandas DataFrames ---")
    capacity_df = pd.read_csv(capacity_content, low_memory=False)
    merged_breach_df = pd.read_csv(merged_breach_content, low_memory=False)

    # --- Data Processing ---
    print("\n--- Starting data processing on Capacity_dump.csv ---")
    capacity_df['Length'] = capacity_df['Hybris Order Number'].astype(str).str.len()
    capacity_df['Int_order'] = capacity_df.apply(process_int_order, axis=1)
    
    breach_lookup = merged_breach_df[['Order_ID', 'Int_Delivery_Date']].copy()

    # FIX: Ensure both merge keys are the same data type (int64) to prevent ValueError.
    # 'coerce' will turn any non-numeric values into NaN, which won't match and will be ignored.
    capacity_df['Int_order'] = pd.to_numeric(capacity_df['Int_order'], errors='coerce').astype('Int64')
    breach_lookup['Order_ID'] = pd.to_numeric(breach_lookup['Order_ID'], errors='coerce').astype('Int64')

    # Now the merge can be performed safely.
    capacity_df = pd.merge(capacity_df, breach_lookup, left_on='Int_order', right_on='Order_ID', how='left')
    
    capacity_df.rename(columns={'Int_Delivery_Date': 'Merged_match'}, inplace=True)
    capacity_df.drop('Order_ID', axis=1, inplace=True, errors='ignore')
    capacity_df['Int_delivery_date'] = capacity_df['Delivery Success Timestamp'].astype(str).str[:11].replace('nan', np.nan)
    capacity_df['Final Delivery date'] = capacity_df['Int_delivery_date'].fillna(capacity_df['Merged_match'])
    print("--- Processing complete. ---\n")

    # Filter for rows with a valid final delivery date
    valid_dates_df = capacity_df.dropna(subset=['Final Delivery date'])
    valid_dates_df = valid_dates_df[pd.to_datetime(valid_dates_df['Final Delivery date'], errors='coerce').notna()]
    print(f"Found {len(valid_dates_df)} new rows with valid delivery dates.")

    # Handle duplicates by downloading and merging with the existing VD_raw_file.txt
    print("\n--- Handling duplicates with existing VD_raw_file.txt ---")
    vd_raw_content, vd_raw_file_id = find_and_download_file(drive_service, DRIVE_FOLDER_ID, DRIVE_FILENAMES["vd_raw_file"])
    
    if vd_raw_content and vd_raw_content.getbuffer().nbytes > 0:
        print("Reading existing data to handle duplicates.")
        existing_df = pd.read_csv(vd_raw_content, sep='\t')
        combined_df = pd.concat([existing_df, valid_dates_df], ignore_index=True)
    else:
        print(f"'{DRIVE_FILENAMES['vd_raw_file']}' not found or is empty. A new file will be created/overwritten.")
        combined_df = valid_dates_df

    # Remove duplicate rows, keeping the last (most recent) entry
    final_df = combined_df.drop_duplicates(keep='last')
    print(f"Combined data has {len(final_df)} unique rows after deduplication.")

    # Convert final DataFrame to a string to be uploaded
    final_data_string = final_df.to_csv(sep='\t', index=False)

    # Upload the processed data back to Google Drive
    upload_file_to_drive(
        service=drive_service,
        folder_id=DRIVE_FOLDER_ID,
        data_to_upload=final_data_string,
        file_name=DRIVE_FILENAMES["vd_raw_file"],
        mime_type='text/plain',
        existing_file_id=vd_raw_file_id
    )
    
    print(f"\nProcess finished successfully!")
    print(f"'{DRIVE_FILENAMES['vd_raw_file']}' has been updated in Google Drive.")

if __name__ == '__main__':
    main()
