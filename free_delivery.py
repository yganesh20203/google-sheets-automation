# -*- coding: utf-8 -*-
"""
This script automates the processing of sales data from Google Drive using a
Google Cloud Service Account for authentication, making it suitable for
non-interactive environments like GitHub Actions.

It performs the following steps:
1.  Authenticates with Google Drive API using a service account key.
2.  Finds and reads the required source files from a specified Google Drive folder.
3.  Processes the main data file ('Capacity_dump.csv').
4.  Merges data from multiple lookup files.
5.  Saves the final data as 'free_delivery_data.csv'.
6.  Uploads the resulting file back to the Google Drive folder.
"""

# -- 1. Import necessary libraries --
import os
import json
import pandas as pd
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# -- 2. Authenticate and Authorize using Service Account --
# This section handles authentication by reading a JSON service account key
# from an environment variable. This is a secure method for server-side apps.

print("Authenticating with Google using Service Account...")

# Get the service account key from the environment variable set in GitHub Actions
GCP_SA_KEY_STRING = os.getenv('GCP_SA_KEY')
if not GCP_SA_KEY_STRING:
    print("Error: GCP_SA_KEY environment variable is not set.")
    exit()

try:
    # The key is a JSON string, so it needs to be loaded into a dictionary
    creds_json = json.loads(GCP_SA_KEY_STRING)
    
    # Define the necessary scope to access Google Drive
    scopes = ['https://www.googleapis.com/auth/drive']
    
    # Create credentials from the service account info
    credentials = service_account.Credentials.from_service_account_info(
        creds_json,
        scopes=scopes
    )
    
    # Build the Google Drive service client
    drive_service = build('drive', 'v3', credentials=credentials)
    print("Authentication successful.")

except (json.JSONDecodeError, KeyError) as e:
    print(f"Authentication failed. Error with service account key: {e}")
    exit()


# -- 3. Define File Names and Folder ID --
DRIVE_FOLDER_ID = '1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da'
CAPACITY_DUMP_FILE = 'Capacity_dump.csv'
STORE_MASTER_FILE = 'CT_Master_Store_Code.xlsx'
FREE_ARTICLES_FILE = 'Free Delivery Articles.xlsx'
FREE_PINCODES_FILE = 'free_delivery_pincodes.xlsx'
OUTPUT_FILE_NAME = 'free_delivery_data.csv'

# -- 4. Function to Find and Read Files from Drive --
def get_file_from_drive(file_name, folder_id):
    """
    Finds a file by name within a specific Google Drive folder and returns its content.
    """
    print(f"Searching for '{file_name}' in Google Drive...")
    try:
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed=false"
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])

        if not files:
            print(f"Error: File '{file_name}' not found in the specified Drive folder.")
            return None

        file_id = files[0].get('id')
        print(f"Found '{file_name}' with ID: {file_id}. Reading file...")
        
        request = drive_service.files().get_media(fileId=file_id)
        file_content = BytesIO(request.execute())
        print(f"Successfully read '{file_name}'.")
        return file_content

    except Exception as e:
        print(f"An error occurred while accessing '{file_name}': {e}")
        return None

# -- 5. Load All Source Files into Pandas DataFrames --
capacity_dump_content = get_file_from_drive(CAPACITY_DUMP_FILE, DRIVE_FOLDER_ID)
store_master_content = get_file_from_drive(STORE_MASTER_FILE, DRIVE_FOLDER_ID)
free_articles_content = get_file_from_drive(FREE_ARTICLES_FILE, DRIVE_FOLDER_ID)
free_pincodes_content = get_file_from_drive(FREE_PINCODES_FILE, DRIVE_FOLDER_ID)

if not all([capacity_dump_content, store_master_content, free_articles_content, free_pincodes_content]):
    print("\nCould not load all necessary files. Exiting.")
    exit()

df_capacity = pd.read_csv(capacity_dump_content)
df_store_master = pd.read_excel(store_master_content)
df_free_articles = pd.read_excel(free_articles_content)
df_free_pincodes = pd.read_excel(free_pincodes_content)

print("\nAll source files have been loaded into DataFrames.")

# -- 6. Process the Main DataFrame ('Capacity Dump') --
print("\nStarting data processing and transformation...")

df_capacity['Int_order_date'] = pd.to_datetime(df_capacity['Order Date IST'], errors='coerce').dt.date
df_capacity['int_store_code'] = pd.to_numeric(df_capacity['Store Code1'], errors='coerce').fillna(0).astype(int)
df_capacity['int_pincode'] = pd.to_numeric(df_capacity['ShipToPincode'], errors='coerce').fillna(0).astype(int)
df_capacity['Key'] = (df_capacity['int_store_code'].astype(str) + df_capacity['int_pincode'].astype(str)).astype(int)
df_capacity['Int_article_num'] = pd.to_numeric(df_capacity['Item'], errors='coerce').fillna(0).astype(int)

print("Initial column transformations complete.")

# -- 7. Perform Lookups (Merges) --
print("Performing lookups from master files...")

df_final = pd.merge(
    df_capacity,
    df_store_master[['Store_Code', 'Store_Name_PBI']],
    left_on='int_store_code',
    right_on='Store_Code',
    how='left'
)
df_final.rename(columns={'Store_Name_PBI': 'Store_name'}, inplace=True)
df_final.drop(columns=['Store_Code'], inplace=True)
print("Store name lookup complete.")

df_free_articles.columns = df_free_articles.columns.str.strip().str.lower().str.replace(' ', '_')
df_final = pd.merge(
    df_final,
    df_free_articles[['int_article']],
    left_on='Int_article_num',
    right_on='int_article',
    how='left'
)
df_final.rename(columns={'int_article': 'target_articles'}, inplace=True)
print("Target articles lookup complete.")

df_free_pincodes.columns = df_free_pincodes.columns.str.strip().str.lower().str.replace(' ', '_')
df_final = pd.merge(
    df_final,
    df_free_pincodes[['free_delivery_pincode']],
    left_on='Key',
    right_on='free_delivery_pincode',
    how='left'
)
print("Free delivery pincode lookup complete.")

# -- 8. Save and Upload the Final DataFrame --
print("\nProcessing complete. Preparing to save and upload the output file.")

output_csv_string = df_final.to_csv(index=False)

# Create a temporary local file to upload
with open(OUTPUT_FILE_NAME, 'w') as f:
    f.write(output_csv_string)
    
media = MediaFileUpload(OUTPUT_FILE_NAME, mimetype='text/csv', resumable=True)

query = f"name = '{OUTPUT_FILE_NAME}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
response = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
existing_files = response.get('files', [])

try:
    if existing_files:
        file_id = existing_files[0]['id']
        print(f"Output file '{OUTPUT_FILE_NAME}' already exists. Updating it...")
        drive_service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Successfully updated '{OUTPUT_FILE_NAME}' in Google Drive.")
    else:
        file_metadata = {'name': OUTPUT_FILE_NAME, 'parents': [DRIVE_FOLDER_ID]}
        print(f"Output file '{OUTPUT_FILE_NAME}' not found. Creating a new one...")
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Successfully uploaded '{OUTPUT_FILE_NAME}' to Google Drive.")
except Exception as e:
    print(f"An error occurred during file upload: {e}")

print("\nScript finished successfully!")
