import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import warnings
import os
import json

# ==============================================================================
# SECTION 1: CONFIGURATION
# ==============================================================================
# --- Google Drive Folder ID (from your URL) ---
GDRIVE_FOLDER_ID = '1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da'

# --- Google Sheet URL ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1WvupGx_L3qpQf4JpDVmVKWwEPmXsT-9a_8kJQOYOpEk"

# --- Define local temporary file paths for processing ---
LOCAL_TEMP_DIR = 'temp_data'
if not os.path.exists(LOCAL_TEMP_DIR):
    os.makedirs(LOCAL_TEMP_DIR)

# --- Suppress FutureWarnings for a cleaner output ---
warnings.simplefilter(action='ignore', category=FutureWarning)

# ==============================================================================
# SECTION 2: AUTHENTICATION & GOOGLE DRIVE SETUP (WITH FIX)
# ==============================================================================
print("--- Authenticating with Google Services ---")

try:
    # Get the credentials from the environment variable
    gcp_sa_key_str = os.environ.get('GCP_SA_KEY')
    if not gcp_sa_key_str:
        raise ValueError("GCP_SA_KEY environment variable not found.")
    
    # Load the credentials from the JSON string
    creds_dict = json.loads(gcp_sa_key_str)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    
    # --- Authorize gspread (This method works well for it) ---
    creds_gspread = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gs_client = gspread.authorize(creds_gspread)

    # --- Authorize pydrive2 (THE FIX: Use its dedicated service auth method) ---
    settings = {
        "client_config_backend": "service",
        "service_config": {
            "client_json_dict": creds_dict
        }
    }
    gauth = GoogleAuth(settings=settings)
    gauth.ServiceAuth() # Authenticate using the service account dictionary
    drive = GoogleDrive(gauth)

    print("✅ Authentication successful for both services.")

except Exception as e:
    print(f"❌ Authentication failed: {e}")
    exit()


# --- Function to download a file from Google Drive ---
def download_file_from_drive(folder_id, filename):
    """Finds a file by name in a specific folder and downloads it."""
    local_path = os.path.join(LOCAL_TEMP_DIR, filename)
    try:
        query = f"'{folder_id}' in parents and trashed=false and title='{filename}'"
        file_list = drive.ListFile({'q': query}).GetList()
        if not file_list:
            print(f"⚠️  File not found in Google Drive: {filename}")
            return None
        
        print(f"Downloading '{filename}'...")
        file_drive = file_list[0]
        file_drive.GetContentFile(local_path)
        print(f"✅ Downloaded '{filename}' successfully.")
        return local_path
    except Exception as e:
        print(f"❌ Error downloading '{filename}': {e}")
        return None

# --- Function to upload a file to Google Drive ---
def upload_file_to_drive(folder_id, local_path):
    """Uploads a local file to a specific Google Drive folder."""
    filename = os.path.basename(local_path)
    try:
        # Check if file already exists to overwrite it
        query = f"'{folder_id}' in parents and trashed=false and title='{filename}'"
        file_list = drive.ListFile({'q': query}).GetList()
        
        if file_list:
            drive_file = file_list[0]
            drive_file.SetContentFile(local_path)
            print(f"Overwriting '{filename}' in Google Drive...")
        else:
            drive_file = drive.CreateFile({'title': filename, 'parents': [{'id': folder_id}]})
            drive_file.SetContentFile(local_path)
            print(f"Uploading new file '{filename}' to Google Drive...")
        
        drive_file.Upload()
        print(f"✅ Uploaded '{filename}' successfully.")
    except Exception as e:
        print(f"❌ Error uploading '{filename}': {e}")


# --- Download all required files ---
print("\n--- Downloading files from Google Drive ---")
base_file_name = 'base_data_open_orders.xlsx'
latest_file_name = 'fareye_report.csv'
store_master_name = 'CT_Master_Store_Code.xlsx'
updated_final_name = 'updated_base_data_final.xlsx'
new_base_name_csv = 'base_data_open_orders.csv'

latest_file_path = download_file_from_drive(GDRIVE_FOLDER_ID, latest_file_name)
store_master_path = download_file_from_drive(GDRIVE_FOLDER_ID, store_master_name)
updated_final_path = download_file_from_drive(GDRIVE_FOLDER_ID, updated_final_name)
new_base_path_csv = os.path.join(LOCAL_TEMP_DIR, new_base_name_csv)

# ==============================================================================
# SECTION 3: PRE-PROCESSING & FILE ROTATION
# ==============================================================================
print("\n--- Starting Pre-processing & File Rotation ---")

if updated_final_path and os.path.exists(updated_final_path):
    print(f"Found '{updated_final_name}'. Processing it to create the new base file.")
    try:
        prev_run_df = pd.read_excel(updated_final_path)
        cols_to_delete = ['int_hybris', 'Reference Number', 'store name', 'age', 'aging bucket', 'aging column detailed']
        prev_run_df.drop(columns=cols_to_delete, inplace=True, errors='ignore')
        prev_run_df.to_csv(new_base_path_csv, index=False)
        print(f"Successfully created new local base file: '{new_base_name_csv}'")
        upload_file_to_drive(GDRIVE_FOLDER_ID, new_base_path_csv)
        base_file_path = new_base_path_csv
    except Exception as e:
        print(f"Error processing '{updated_final_name}': {e}. Halting script.")
        exit()
else:
    print(f"'{updated_final_name}' not found. Using original '{base_file_name}' as base.")
    base_file_path = download_file_from_drive(GDRIVE_FOLDER_ID, base_file_name)

if not base_file_path or not os.path.exists(base_file_path):
     print("❌ Critical error: No base file available to process. Halting.")
     exit()

print("\nCleaning 'fareye_report.csv'...")
try:
    latest_df = pd.read_csv(latest_file_path, dtype={28: str})
    
    if 'Store Code1' in latest_df.columns:
        latest_df['Store Code1'] = pd.to_numeric(
            latest_df['Store Code1'].astype(str).str.extract(r'(\d+)').iloc[:, 0], errors='coerce'
        ).astype('Int64')
        print("Cleaned 'Store Code1' column.")

    if 'Invoice Value' in latest_df.columns and 'Invoice Value Without Tax' in latest_df.columns:
        latest_df['Invoice Value'] = latest_df['Invoice Value Without Tax']
        print("Updated 'Invoice Value' column.")
except Exception as e:
    print(f"❌ An error occurred while cleaning the fareye report: {e}. Halting.")
    exit()

# ==============================================================================
# SECTION 4: MAIN SCRIPT LOGIC
# ==============================================================================
print("\n--- Starting Main Data Processing ---")

try:
    if base_file_path.endswith('.csv'):
        base_df = pd.read_csv(base_file_path)
    else:
        base_df = pd.read_excel(base_file_path)
    store_master_df = pd.read_excel(store_master_path)
    print("✅ All source files loaded successfully.")
except Exception as e:
    print(f"❌ Error loading dataframes: {e}. Halting.")
    exit()

## Step 1: Clean and Convert Key Columns
print("\n--- Step 1: Cleaning and Converting Key Columns ---")
base_df['int_hybris'] = base_df.iloc[:, 9].astype(str).str.extract(r'(\d+)').iloc[:, 0]
latest_df['int_hybris'] = latest_df.iloc[:, 9].astype(str).str.extract(r'(\d+)').iloc[:, 0]
base_df['int_hybris'] = pd.to_numeric(base_df['int_hybris'], errors='coerce').astype('Int64')
latest_df['int_hybris'] = pd.to_numeric(latest_df['int_hybris'], errors='coerce').astype('Int64')
if 'Store Code1' in base_df.columns:
    base_df['Store Code1'] = pd.to_numeric(base_df['Store Code1'], errors='coerce').astype('Int64')

## Step 2: Initial Deletion of 'end' flow
if 'Current Flow' in base_df.columns:
    base_df = base_df[base_df['Current Flow'] != 'end'].copy()

## Step 3: Update & Identify Common/New Orders
base_hybris_set = set(base_df['int_hybris'].dropna())
latest_hybris_set = set(latest_df['int_hybris'].dropna())
common_orders = base_hybris_set.intersection(latest_hybris_set)
new_orders = latest_hybris_set.difference(base_hybris_set)

base_df.drop_duplicates(subset=['int_hybris'], keep='last', inplace=True)
latest_df.drop_duplicates(subset=['int_hybris'], keep='last', inplace=True)
base_df.set_index('int_hybris', inplace=True)
latest_df.set_index('int_hybris', inplace=True)

column_p_name, column_b_name = base_df.columns[15], base_df.columns[1]
latest_column_p_name, latest_column_b_name = latest_df.columns[15], latest_df.columns[1]
base_df.loc[list(common_orders), [column_p_name, column_b_name]] = latest_df.loc[list(common_orders), [latest_column_p_name, latest_column_b_name]]
base_df.reset_index(inplace=True)

## Step 4: Append New Orders
print("\n--- Step 4: Appending New Orders ---")
new_orders_df = latest_df.loc[list(new_orders)].reset_index()
all_columns = pd.Index(base_df.columns.union(new_orders_df.columns))
base_df = base_df.reindex(columns=all_columns)
new_orders_df = new_orders_df.reindex(columns=all_columns)
updated_df = pd.concat([base_df, new_orders_df], ignore_index=True)
print(f"Total rows after append: {updated_df.shape[0]}")

## Step 5: Delete Ended Orders
print("\n--- Step 5: Deleting Ended Orders ---")
rows_before = len(updated_df)
if 'Current Flow' in updated_df.columns:
    updated_df = updated_df[updated_df['Current Flow'] != 'End']
    print(f"Deleted {rows_before - len(updated_df)} 'End' rows.")

## Step 6: Enrich Data
print("\n--- Step 6: Enriching Data ---")
if 'Store Code1' in updated_df.columns:
    store_master_df.rename(columns={store_master_df.columns[0]: 'Store Code Master', store_master_df.columns[1]: 'store name'}, inplace=True)
    updated_df = pd.merge(updated_df, store_master_df[['Store Code Master', 'store name']], left_on='Store Code1', right_on='Store Code Master', how='left').drop(columns=['Store Code Master'])
    print("Added 'store name'.")

if 'POS Invoice Date' in updated_df.columns:
    updated_df['POS Invoice Date'] = pd.to_datetime(updated_df['POS Invoice Date'], errors='coerce')
    updated_df['age'] = (pd.Timestamp.now().normalize() - updated_df['POS Invoice Date']).dt.days
    print("Added 'age'.")

if 'age' in updated_df.columns:
    bins = [-float('inf'), 2, 4, 7, 15, 30, float('inf')]
    labels = ['0-2 days', '3-4 days', '5-7 days', '8-15 days', '15-30 days', 'more than 30 days']
    detailed_labels = ['1.0-2 days', '2.3-4 days', '4.5-7 days', '5.8-15 days', '6.15-30 days', '7.more than 30 days']
    updated_df['aging bucket'] = pd.cut(updated_df['age'], bins=bins, labels=labels)
    updated_df['aging column detailed'] = pd.cut(updated_df['age'], bins=bins, labels=detailed_labels)
    print("Added 'aging bucket' and 'aging column detailed'.")

## Step 7: Save and Upload Final File
print(f"\n--- Step 7: Saving and Uploading Final File ---")
final_output_path = os.path.join(LOCAL_TEMP_DIR, updated_final_name)
updated_df.to_excel(final_output_path, index=False)
upload_file_to_drive(GDRIVE_FOLDER_ID, final_output_path)


## Step 8: Send Data to Google Sheets
print("\n--- Step 8: Sending Data to Google Sheets ---")
try:
    spreadsheet = gs_client.open_by_url(SHEET_URL)
    worksheet = spreadsheet.get_worksheet(0)
    
    df_for_gsheet = updated_df.copy()
    for col in df_for_gsheet.columns:
        if pd.api.types.is_datetime64_any_dtype(df_for_gsheet[col]):
            df_for_gsheet[col] = df_for_gsheet[col].dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', '')
        else:
            df_for_gsheet[col] = df_for_gsheet[col].astype(str).replace('<NA>', '').replace('nan', '')
    df_for_gsheet.fillna('', inplace=True)
    
    print("Clearing existing data from the sheet...")
    worksheet.clear()
    print("Writing new data to the sheet...")
    set_with_dataframe(worksheet, df_for_gsheet, resize=True)
    print("✅✅ Successfully updated the Google Sheet! ✅✅")

except Exception as e:
    print(f"❌ An error occurred with Google Sheets: {e}")
