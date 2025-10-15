import pandas as pd
import os
import json
import io
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import zipfile

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- 1. USER CONFIGURATION ---

# The ID of the top-level folder containing the daily dated folders (e.g., "15th October...")
DRIVE_PARENT_FOLDER_ID = '1mBCJJ_7kTSMlNDj7mMxZ33hNeMykqKyR'

# **NEW**: Create a folder in Google Drive for the output reports and paste its ID here.
# Make sure your service account has "Editor" access to this folder.
DRIVE_OUTPUT_FOLDER_ID = '1zgkXizA77RfJ8XIAGYXcZa9BpvXPo0F0'

# The email address to send the final report to.
RECIPIENT_EMAIL = 'kanakala.yoganand@flipkart.com' 

# --- End of User Configuration ---

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_todays_date_string():
    """Generates the date string for folder searching (e.g., '15th October')."""
    today = datetime.now()
    day = today.day
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]
    return f"{day}{suffix} {today.strftime('%B')}"

def download_ap_files(service):
    """Finds today's folder, then the 'AP files' subfolder, and downloads all XLSX files."""
    date_str = get_todays_date_string()
    print(f"Searching for parent folder containing '{date_str}'...")

    query = f"name contains '{date_str}' and '{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    daily_folders = results.get('files', [])

    if not daily_folders:
        print(f"❌ ERROR: No daily folder found containing '{date_str}'.")
        return []
    
    daily_folder_id = daily_folders[0]['id']
    daily_folder_name = daily_folders[0]['name']
    print(f"✅ Found daily folder: '{daily_folder_name}'")

    print("Searching for 'AP files' subfolder...")
    query = f"name = 'AP files' and '{daily_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    ap_folders = results.get('files', [])

    if not ap_folders:
        print(f"❌ ERROR: 'AP files' subfolder not found inside '{daily_folder_name}'.")
        return []

    ap_folder_id = ap_folders[0]['id']
    print("✅ Found 'AP files' subfolder.")

    print("Listing and downloading all .xlsx files...")
    query = f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and '{ap_folder_id}' in parents"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    xlsx_files = results.get('files', [])

    if not xlsx_files:
        print("⚠️ Warning: No .xlsx files found in the 'AP files' subfolder.")
        return []
        
    all_dataframes = []
    for file in xlsx_files:
        try:
            print(f"  - Downloading '{file['name']}'...")
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            fh.seek(0)
            df = pd.read_excel(fh)
            df['Source_File'] = file['name']
            all_dataframes.append(df)
            print(f"    ... Success.")
        except Exception as e:
            print(f"    ... ⚠️ Error reading {file['name']}: {e}")

    return all_dataframes

def upload_file_and_get_link(service, local_path, folder_id):
    """Uploads a file to Drive, makes it public, and returns the link."""
    print(f"Uploading '{os.path.basename(local_path)}' to Google Drive...")
    file_metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    
    # Check if file already exists to overwrite it.
    query = f"name = '{os.path.basename(local_path)}' and '{folder_id}' in parents and trashed=false"
    response = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
    files = response.get('files', [])

    if files:
        file_id = files[0].get('id')
        updated_file = service.files().update(fileId=file_id, media_body=media, fields='id, webViewLink').execute()
        print("✅ File updated.")
        file_link = updated_file.get('webViewLink')
    else:
        file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        file_id = file.get('id')
        print("✅ File uploaded.")
        file_link = file.get('webViewLink')

    print("Setting public permissions...")
    permission = {'type': 'anyone', 'role': 'reader'}
    service.permissions().create(fileId=file_id, body=permission).execute()
    print("✅ Permissions set. Anyone with the link can view.")
    return file_link

def send_email_notification(sender_email, sender_password, recipient_email, subject, body):
    """Sends an email notification without an attachment."""
    print("Preparing to send email notification...")
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print(f"✅ Email notification sent successfully to {recipient_email}")
    except Exception as e:
        print(f"❌ FAILED to send email: {e}")

def main():
    """Main function to run the entire process."""
    if DRIVE_OUTPUT_FOLDER_ID == 'YOUR_OUTPUT_FOLDER_ID_HERE':
        raise ValueError("Please update the 'DRIVE_OUTPUT_FOLDER_ID' in the script configuration.")

    print("--- 1. Authenticating with Google ---")
    creds_json_str = os.getenv("GCP_SA_KEY")
    if not creds_json_str:
        raise ValueError("GCP_SA_KEY secret not found. Please check GitHub Actions secrets.")

    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    print("✅ Authentication successful.")
    print("-" * 50)

    print("--- 2. Finding and Merging AP Files ---")
    list_of_dfs = download_ap_files(drive_service)

    if not list_of_dfs:
        print("❌ No files were downloaded or processed. Aborting script.")
        return

    merged_df = pd.concat(list_of_dfs, ignore_index=True)
    print(f"✨ Merge complete! Total rows in raw data: {len(merged_df)}")
    print("-" * 50)

    print("--- 3. Creating Pivot Tables ---")
    try:
        merged_df['Invoice Scan Date'] = pd.to_datetime(merged_df['Invoice Scan Date'].astype(str).str.strip(), format='%d/%m/%Y', errors='coerce')
        yesterday = datetime.now().date() - timedelta(days=1)
        
        pivot_source_df = merged_df[
            (merged_df['Invoice Scan Date'].dt.date == yesterday) &
            (merged_df['Hold Reason'] != 'Ok')
        ].dropna(subset=['Invoice Scan Date']).copy()

        if pivot_source_df.empty:
            print("⚠️ Warning: No data found for yesterday's date after filtering. Pivots will be empty.")
        
        pivot1 = pd.pivot_table(pivot_source_df, index='Store Code', values='Billing No', aggfunc=pd.Series.nunique)
        pivot1.rename(columns={'Billing No': 'Distinct Count of Billing No'}, inplace=True)
        print("✅ Pivot Table 1 (by Store Code) created.")

        pivot2 = pd.pivot_table(pivot_source_df, index=['Store Code', 'Description'], values='Billing No', aggfunc=pd.Series.nunique)
        pivot2.rename(columns={'Billing No': 'Distinct Count of Billing No'}, inplace=True)
        print("✅ Pivot Table 2 (by Store Code and Description) created.")

        today_str = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'AP_Merged_Report_{today_str}.xlsx'
        
        with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
            merged_df.to_excel(writer, sheet_name='Merged_Data', index=False)
            pivot1.to_excel(writer, sheet_name='Pivot_By_Store')
            pivot2.to_excel(writer, sheet_name='Pivot_By_Store_Description')
        
        print(f"✅ Successfully created report file: {output_filename}")
        print("-" * 50)

        zip_filename = f'AP_Merged_Report_{today_str}.zip'
        print(f"Compressing report into '{zip_filename}'...")
        with zipfile.ZipFile(zip_filename, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(output_filename, arcname=os.path.basename(output_filename))
        print("✅ Compression successful.")
        print("-" * 50)
        
        print("--- 4. Uploading Report to Google Drive ---")
        file_link = upload_file_and_get_link(drive_service, zip_filename, DRIVE_OUTPUT_FOLDER_ID)
        print("-" * 50)

        print("--- 5. Emailing Report Link ---")
        sender_email = os.getenv("SENDER_EMAIL")
        sender_password = os.getenv("SENDER_APP_PASSWORD")

        if not sender_email or not sender_password:
            raise ValueError("SENDER_EMAIL or SENDER_APP_PASSWORD secrets not found.")

        ist = timezone(timedelta(hours=5, minutes=30))
        email_timestamp = datetime.now(ist).strftime('%d-%b-%Y %I:%M %p')
        email_subject = f"Daily AP Merged Report - {email_timestamp}"
        email_body = (
            f"The daily AP Merged Report has been generated.\n\n"
            f"You can download it using the following link:\n{file_link}\n\n"
            "This email was sent automatically by a GitHub Actions script."
        )

        send_email_notification(sender_email, sender_password, RECIPIENT_EMAIL, email_subject, email_body)

    except KeyError as e:
        print(f"❌ CRITICAL ERROR: A required column ({e}) was not found. Aborting pivot creation.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

