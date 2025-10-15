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

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. USER CONFIGURATION ---

# The ID of the top-level folder containing the daily dated folders (e.g., "15th October...")
DRIVE_PARENT_FOLDER_ID = '1mBCJJ_7kTSMlNDj7mMxZ33hNeMykqKyR'

# The email address to send the final report to.
RECIPIENT_EMAIL = 'y.ganesh@flipkart.com' 

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

    # 1. Find the daily parent folder
    query = f"name contains '{date_str}' and '{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    daily_folders = results.get('files', [])

    if not daily_folders:
        print(f"❌ ERROR: No daily folder found containing '{date_str}'.")
        return []
    
    daily_folder_id = daily_folders[0]['id']
    daily_folder_name = daily_folders[0]['name']
    print(f"✅ Found daily folder: '{daily_folder_name}'")

    # 2. Find the 'AP files' subfolder within the daily folder
    print("Searching for 'AP files' subfolder...")
    query = f"'name' = 'AP files' and '{daily_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    ap_folders = results.get('files', [])

    if not ap_folders:
        print(f"❌ ERROR: 'AP files' subfolder not found inside '{daily_folder_name}'.")
        return []

    ap_folder_id = ap_folders[0]['id']
    print("✅ Found 'AP files' subfolder.")

    # 3. List and download all XLSX files from the 'AP files' subfolder
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

def send_email_with_attachment(sender_email, sender_password, recipient_email, subject, body, file_path):
    """Sends an email with a file attachment using SMTP."""
    print("Preparing to send email report...")
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file_path)}")
        msg.attach(part)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print(f"✅ Email sent successfully to {recipient_email}")
    except Exception as e:
        print(f"❌ FAILED to send email: {e}")

def main():
    """Main function to run the entire process."""
    # --- 1. Authentication ---
    print("--- 1. Authenticating with Google ---")
    creds_json_str = os.getenv("GCP_SA_KEY")
    if not creds_json_str:
        raise ValueError("GCP_SA_KEY secret not found. Please check GitHub Actions secrets.")

    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    print("✅ Authentication successful.")
    print("-" * 50)

    # --- 2. Download and Merge Files ---
    print("--- 2. Finding and Merging AP Files ---")
    list_of_dfs = download_ap_files(drive_service)

    if not list_of_dfs:
        print("❌ No files were downloaded or processed. Aborting script.")
        return

    merged_df = pd.concat(list_of_dfs, ignore_index=True)
    print(f"✨ Merge complete! Total rows in raw data: {len(merged_df)}")
    print("-" * 50)

    # --- 3. Pivot Table Creation ---
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

        # --- 4. Save to Excel File ---
        today_str = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'AP_Merged_Report_{today_str}.xlsx'
        
        with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
            merged_df.to_excel(writer, sheet_name='Merged_Data', index=False)
            pivot1.to_excel(writer, sheet_name='Pivot_By_Store')
            pivot2.to_excel(writer, sheet_name='Pivot_By_Store_Description')
        
        print(f"✅ Successfully created report file: {output_filename}")
        print("-" * 50)

        # --- 5. Email the Report ---
        print("--- 4. Emailing Report ---")
        sender_email = os.getenv("SENDER_EMAIL")
        sender_password = os.getenv("SENDER_APP_PASSWORD")

        if not sender_email or not sender_password:
            raise ValueError("SENDER_EMAIL or SENDER_APP_PASSWORD secrets not found.")

        # Using a timezone-aware timestamp for the email subject
        ist = timezone(timedelta(hours=5, minutes=30))
        email_timestamp = datetime.now(ist).strftime('%d-%b-%Y %I:%M %p')
        email_subject = f"Daily AP Merged Report - {email_timestamp}"
        email_body = "Please find the attached daily AP Merged Report.\n\nThis email was sent automatically by a GitHub Actions script."

        send_email_with_attachment(sender_email, sender_password, RECIPIENT_EMAIL, email_subject, email_body, output_filename)

    except KeyError as e:
        print(f"❌ CRITICAL ERROR: A required column ({e}) was not found. Aborting pivot creation.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
