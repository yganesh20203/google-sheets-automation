# -*- coding: utf-8 -*-
"""
This script automates the processing of sales data from Google Drive.

It performs the following steps:
1.  Authenticates with Google APIs (Drive and Sheets).
2.  Finds and reads the required source files from a specified Google Drive folder.
3.  Processes the main data file ('Capacity_dump.csv') by:
    - Cleaning and transforming existing columns.
    - Creating new columns based on calculations and lookups.
4.  Merges data from multiple lookup files to enrich the main dataset.
5.  Saves the final, processed data as 'free_delivery_data.csv'.
6.  Uploads the resulting file back to the original Google Drive folder.
"""

# -- 1. Import necessary libraries --
# The 'google-colab' library is used for Google Colab environments to handle authentication.
# 'gspread' is for interacting with Google Sheets.
# 'oauth2client' is for Google API authentication.
# 'pandas' is essential for data manipulation and analysis.
# 'openpyxl' is required by pandas to work with .xlsx files.

try:
    from google.colab import auth
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    import gspread
    from oauth2client.client import GoogleCredentials
    import pandas as pd
except ImportError:
    print("Some libraries are not installed. Please run:")
    print("!pip install google-colab gspread oauth2client pandas openpyxl")
    exit()

# -- 2. Authenticate and Authorize --
# This section handles the authentication with Google's services, allowing the script
# to access your Drive and Sheets files securely.

print("Authenticating with Google...")
try:
    auth.authenticate_user()
    gc = gspread.authorize(GoogleCredentials.get_application_default())
    drive_service = build('drive', 'v3')
    print("Authentication successful.")
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()

# -- 3. Define File Names and Folder ID --
# Specify the Google Drive folder ID where your files are located.
# This ID is the last part of the folder's URL.
# Also, define the names of your source and output files.

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

    Args:
        file_name (str): The name of the file to find.
        folder_id (str): The ID of the Google Drive folder to search in.

    Returns:
        A file-like object (BytesIO) containing the file's content, or None if not found.
    """
    print(f"Searching for '{file_name}' in Google Drive...")
    try:
        # Query to search for the file by name and parent folder
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed=false"
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])

        if not files:
            print(f"Error: File '{file_name}' not found in the specified Drive folder.")
            return None

        file_id = files[0].get('id')
        print(f"Found '{file_name}' with ID: {file_id}. Reading file...")
        
        # Download the file content
        request = drive_service.files().get_media(fileId=file_id)
        
        # Use a BytesIO object to handle the downloaded binary data
        from io import BytesIO
        file_content = BytesIO(request.execute())
        print(f"Successfully read '{file_name}'.")
        return file_content

    except Exception as e:
        print(f"An error occurred while accessing '{file_name}': {e}")
        return None

# -- 5. Load All Source Files into Pandas DataFrames --
# Using the helper function, we load each required file into a pandas DataFrame.
# Error handling is included to stop the script if a file cannot be loaded.

capacity_dump_content = get_file_from_drive(CAPACITY_DUMP_FILE, DRIVE_FOLDER_ID)
store_master_content = get_file_from_drive(STORE_MASTER_FILE, DRIVE_FOLDER_ID)
free_articles_content = get_file_from_drive(FREE_ARTICLES_FILE, DRIVE_FOLDER_ID)
free_pincodes_content = get_file_from_drive(FREE_PINCODES_FILE, DRIVE_FOLDER_ID)

# Exit if any of the essential files are missing
if not all([capacity_dump_content, store_master_content, free_articles_content, free_pincodes_content]):
    print("\nCould not load all necessary files. Please check file names and permissions. Exiting.")
    exit()

# Read the file content into pandas DataFrames
df_capacity = pd.read_csv(capacity_dump_content)
df_store_master = pd.read_excel(store_master_content)
df_free_articles = pd.read_excel(free_articles_content)
df_free_pincodes = pd.read_excel(free_pincodes_content)

print("\nAll source files have been loaded into DataFrames.")

# -- 6. Process the Main DataFrame ('Capacity Dump') --
print("\nStarting data processing and transformation...")

# Create 'Int_order_date'
# Convert 'Order Date IST' to datetime objects, coercing errors to NaT (Not a Time)
# Then extract just the date part.
df_capacity['Int_order_date'] = pd.to_datetime(df_capacity['Order Date IST'], errors='coerce').dt.date

# Create 'int_store_code'
# Convert 'Store Code1' to a numeric type, coercing errors into NaN (Not a Number)
# Then fill any missing values with 0 and convert to integer.
df_capacity['int_store_code'] = pd.to_numeric(df_capacity['Store Code1'], errors='coerce').fillna(0).astype(int)

# Create 'int_pincode'
df_capacity['int_pincode'] = pd.to_numeric(df_capacity['ShipToPincode'], errors='coerce').fillna(0).astype(int)

# Create 'Key'
# Concatenate the string representations of the two integer columns
# Then convert the resulting string back to an integer.
df_capacity['Key'] = (df_capacity['int_store_code'].astype(str) + df_capacity['int_pincode'].astype(str)).astype(int)

# Create 'Int_article_num'
df_capacity['Int_article_num'] = pd.to_numeric(df_capacity['Item'], errors='coerce').fillna(0).astype(int)

print("Initial column transformations complete.")

# -- 7. Perform Lookups (Merges) --
print("Performing lookups from master files...")

# Lookup 'Store_name'
# Merge the main DataFrame with the store master data.
# We use a 'left' merge to keep all rows from the main DataFrame.
df_final = pd.merge(
    df_capacity,
    df_store_master[['Store_Code', 'Store_Name_PBI']],
    left_on='int_store_code',
    right_on='Store_Code',
    how='left'
)
# Rename the resulting column for clarity and drop the redundant key column.
df_final.rename(columns={'Store_Name_PBI': 'Store_name'}, inplace=True)
df_final.drop(columns=['Store_Code'], inplace=True)
print("Store name lookup complete.")

# Lookup 'target_articles'
# Clean up the column names in the lookup file for consistency.
df_free_articles.columns = df_free_articles.columns.str.strip().str.lower().str.replace(' ', '_')
df_final = pd.merge(
    df_final,
    df_free_articles[['int_article']],
    left_on='Int_article_num',
    right_on='int_article',
    how='left'
)
# Rename the column to the desired 'target_articles'.
df_final.rename(columns={'int_article': 'target_articles'}, inplace=True)
print("Target articles lookup complete.")

# Lookup 'free_delivery_pincode'
# Clean up column names for consistency.
df_free_pincodes.columns = df_free_pincodes.columns.str.strip().str.lower().str.replace(' ', '_')
df_final = pd.merge(
    df_final,
    df_free_pincodes[['free_delivery_pincode']],
    left_on='Key',
    right_on='free_delivery_pincode',
    how='left'
)
# The column name is already correct, so no rename is needed.
print("Free delivery pincode lookup complete.")


# -- 8. Save and Upload the Final DataFrame --
print("\nProcessing complete. Preparing to save and upload the output file.")

# Convert the final DataFrame to a CSV string
output_csv_string = df_final.to_csv(index=False)

# Define file metadata for Google Drive upload
file_metadata = {
    'name': OUTPUT_FILE_NAME,
    'parents': [DRIVE_FOLDER_ID],
    'mimeType': 'text/csv'
}

# Create a media object for the upload
media = MediaFileUpload(
    OUTPUT_FILE_NAME,
    mimetype='text/csv',
    resumable=True
)
# Write the CSV string to the local file that will be uploaded
with open(OUTPUT_FILE_NAME, 'w') as f:
    f.write(output_csv_string)

# Check if the file already exists to decide whether to create or update
query = f"name = '{OUTPUT_FILE_NAME}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
response = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
existing_files = response.get('files', [])

try:
    if existing_files:
        # Update existing file
        file_id = existing_files[0]['id']
        print(f"Output file '{OUTPUT_FILE_NAME}' already exists. Updating it...")
        drive_service.files().update(
            fileId=file_id,
            media_body=media,
            fields='id'
        ).execute()
        print(f"Successfully updated '{OUTPUT_FILE_NAME}' in Google Drive.")
    else:
        # Create new file
        print(f"Output file '{OUTPUT_FILE_NAME}' not found. Creating a new one...")
        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"Successfully uploaded '{OUTPUT_FILE_NAME}' to Google Drive.")
except Exception as e:
    print(f"An error occurred during file upload: {e}")

print("\nScript finished successfully!")
