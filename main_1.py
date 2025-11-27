# main.py - Final Version for GitHub Actions

import os
import json
import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# --- 1. USER CONFIGURATION: You must edit these values ---

# Paste the ID for the folder where ALL your input and output files are located.
INPUT_OUTPUT_FOLDER_ID = '1a7BSDVcQOXon5jP2CoGSpodnv7Ggj9Da'

# Paste the full URL of your target Google Sheet.
GSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dgmZmhcmJrSd2QzjyzGtk37w3dgHs2-qm_2FnsXwaoY/edit?gid=973578648#gid=973578648'

# --- End of User Configuration ---

# Define the scopes for the APIs (permissions).
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Helper Functions for Google Drive & Sheets ---

def get_file_id_by_name(service, file_name, folder_id):
    """Finds a file's ID by its name within a specific folder."""
    print(f"Searching for file '{file_name}'...", end='', flush=True)
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    if not files:
        print(" ❌ Not found.")
        return None
    else:
        file_id = files[0].get('id')
        print(f" ✅ Found")
        return file_id

def download_file_from_drive(service, file_id, local_filename):
    """Downloads a file from Google Drive."""
    print(f"Downloading '{os.path.basename(local_filename)}'...", end='', flush=True)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    with open(local_filename, 'wb') as f:
        f.write(fh.read())
    print(" ✅")

def upload_file_to_drive(service, local_path, folder_id):
    """Uploads a file to a specific Google Drive folder, overwriting if it exists."""
    if not os.path.exists(local_path):
        print(f"ℹ️ Skipped uploading '{os.path.basename(local_path)}' as it was not generated.")
        return

    file_metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    print(f"Uploading '{os.path.basename(local_path)}' to Drive...", end='', flush=True)

    # Check if file already exists to overwrite it.
    existing_file_id = get_file_id_by_name(service, os.path.basename(local_path), folder_id)
    if existing_file_id:
        service.files().update(fileId=existing_file_id, media_body=media).execute()
    else:
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(" ✅")

def export_df_to_gsheet(spreadsheet, df_to_export, sheet_name):
    """Exports a Pandas DataFrame to a specific worksheet in a Google Sheet."""
    if df_to_export is None:
        print(f"ℹ️ Skipped exporting '{sheet_name}' as there was no data.")
        return
    try:
        if not isinstance(df_to_export.index, pd.RangeIndex):
            df_to_export = df_to_export.reset_index()

        # Handle NaN/Inf for JSON serialization
        df_to_export = df_to_export.fillna('')

        export_data = [df_to_export.columns.values.tolist()] + df_to_export.values.tolist()

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")

        worksheet.batch_clear(['A:M'])
        worksheet.update(export_data, 'A1', value_input_option='USER_ENTERED')
        print(f"✅ Successfully exported to worksheet: '{sheet_name}'")
    except Exception as e:
        print(f"\n❌ An error occurred during the export to '{sheet_name}': {e}")


def main():
    """Main function to run the entire automation process."""
    print("--- 1. Authenticating ---")
    creds_json_str = os.getenv("GCP_SA_KEY")
    if not creds_json_str:
        raise ValueError("GCP_SA_KEY secret not found in environment.")

    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = gspread.authorize(creds)
    print("✅ Authentication successful.")
    print("-" * 30)

    print("--- 2. Finding & Downloading Input Files ---")
    local_data_path = 'data'
    os.makedirs(local_data_path, exist_ok=True)

    input_filenames = [
        'Capacity_dump.csv', 'CT_Master_Store_Code.xlsx', 'Pincode_distance.xlsx',
        'Xd_store.xlsx', 'Free_delivery_list.xlsx'
    ]

    for filename in input_filenames:
        file_id = get_file_id_by_name(drive_service, filename, INPUT_OUTPUT_FOLDER_ID)
        if not file_id:
            raise FileNotFoundError(f"'{filename}' could not be found in the specified Drive folder. Please check the name and location.")
        download_file_from_drive(drive_service, file_id, os.path.join(local_data_path, filename))
    print("-" * 30)

    print("--- 3. Loading and Processing Data ---")
    main_file = os.path.join(local_data_path, 'Capacity_dump.csv')
    ct_master_file = os.path.join(local_data_path, 'CT_Master_Store_Code.xlsx')
    pincode_file = os.path.join(local_data_path, 'Pincode_distance.xlsx')
    xd_store_file = os.path.join(local_data_path, 'Xd_store.xlsx')
    free_delivery_file = os.path.join(local_data_path, 'Free_delivery_list.xlsx')

    df = pd.read_csv(main_file, low_memory=False)
    ct_master_df = pd.read_excel(ct_master_file)
    pincode_df = pd.read_excel(pincode_file)
    xd_store_df = pd.read_excel(xd_store_file)
    free_delivery_df = pd.read_excel(free_delivery_file)
    print("✅ All input files loaded into DataFrames.")
    print("-" * 30)

    # --- DATA PROCESSING AND REPORT GENERATION STARTS HERE ---

    print("--- Processing and Enriching Data ---")
    df['Int_pincode'] = pd.to_numeric(df['ShipToPincode'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce').fillna(0).astype(int)
    df['Int_article'] = pd.to_numeric(df['Item'], errors='coerce').fillna(0).astype(int)
    df['Int_storecode'] = pd.to_numeric(df['Store Code1'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce').fillna(0).astype(int)
    df['Key'] = pd.to_numeric(df['Int_storecode'].astype(str) + df['Int_pincode'].astype(str), errors='coerce').fillna(0).astype(int)
    df['Int_order_date'] = pd.to_datetime(df['Order Date IST'].astype(str).str.split(' ').str[0], errors='coerce').dt.strftime('%m/%d/%Y')
    df['Int_delivery_date'] = pd.to_datetime(df['Delivery Success Timestamp'].astype(str).str.split(' ').str[0], errors='coerce').dt.strftime('%Y-%m-%d')
    df['Int_LR_date'] = pd.to_datetime(df['LR Date Time'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['UPI ID'] = pd.to_numeric(df['upiTransactionId'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce').fillna(0).astype('int64')
    df['M track'] = pd.to_numeric(df['Member Id'].astype(str).str.extract(r'(\d+)', expand=False).str.slice(-8), errors='coerce').fillna(0).astype(int)
    df['Gross Weight'] = pd.to_numeric(df['Gross Weight'], errors='coerce')
    df['Item Gross Weight'] = pd.to_numeric(df['Item Gross Weight'], errors='coerce')
    df['Net_Amount'] = pd.to_numeric(df['Net_Amount'], errors='coerce')
    df['Invoice Value'] = pd.to_numeric(df['Invoice Value'], errors='coerce')
    df['Item Freight'] = pd.to_numeric(df['Item Freight'], errors='coerce')

    df = pd.merge(df, ct_master_df[['Store_Code', 'Store_Name_PBI']], left_on='Int_storecode', right_on='Store_Code', how='left').rename(columns={'Store_Name_PBI': 'Store_Name'}).drop(columns=['Store_Code'])
    df = pd.merge(df, pincode_df[['Concat', 'Distance']], left_on='Key', right_on='Concat', how='left').rename(columns={'Distance': 'distance'}).drop(columns=['Concat'])
    df = pd.merge(df, xd_store_df[['Pincode', 'Cross_dock_name']], left_on='Int_pincode', right_on='Pincode', how='left').rename(columns={'Cross_dock_name': 'X_doc'}).drop(columns=['Pincode'])
    free_delivery_df['match_indicator'] = 'Yes'
    df = pd.merge(df, free_delivery_df[['Membership Nbr', 'match_indicator']], left_on='M track', right_on='Membership Nbr', how='left').rename(columns={'match_indicator': 'Cheque'}).fillna({'Cheque': 'No'}).drop(columns=['Membership Nbr'])

    df['Free_Delivery'] = np.select([(df['Mode of Fullfillment'] == 'DSD'), (df['Mode of Fullfillment'].isin(['ISP', 'Walkin'])) & (df['Cheque'] == 'Yes')], ['Yes', 'Yes'], default='No')
    df['Considered'] = np.select([(df['Mode of Fullfillment'].isin(['DSD','ISP'])) & (df['Free_Delivery'] == 'Yes')], ['Yes'], default='No')

    df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
    df['Load'] = np.select([(df['distance'].isna()) & (df['Key'] != 0), (df['Gross Weight'] > 3000), (df['distance'] > 100)], ['>100', 'Bulk', '>100'], default='Normal')
    print("✅ Data processing and enrichment complete.")

    # --- Deduplication ---
    initial_rows = df.shape[0]
    df.drop_duplicates(inplace=True)
    print(f"🧹 De-duplicated main data, removed {initial_rows - df.shape[0]} rows.")

    print("--- Creating Store Summary Report ---")
    today_str_format1 = pd.to_datetime('today').strftime('%m/%d/%Y')
    summary_df = df[df['Current Flow'].isin(['Dummy Task', 'Reattempt', 'Transportation'])].pivot_table(index='Store_Name', columns='Current Flow', values='Item Gross Weight', aggfunc='sum', fill_value=0)
    summary_df = summary_df.reindex(columns=['Dummy Task', 'Reattempt', 'Transportation'], fill_value=0)
    summary_df['Grand Total'] = summary_df.sum(axis=1)
    statuses_to_keep = ['Dummy Task','Reattempt', 'Transportation']
    summary_df['Critical order tonnage to be cleared'] = summary_df.index.map(df[(df['Int_order_date'] != today_str_format1) & (df['Current Flow'].isin(statuses_to_keep))].groupby('Store_Name')['Item Gross Weight'].sum()).fillna(0)
    summary_df['Critical order tonnage to be cleared DSD'] = summary_df.index.map(df[(df['Int_order_date'] != today_str_format1) & (df['Considered'] == 'Yes') & (~df['Load'].isin(['>100', 'Bulk'])) & (df['Current Flow'].isin(statuses_to_keep))].groupby('Store_Name')['Item Gross Weight'].sum()).fillna(0)
    three_days_ago = pd.to_datetime('today').normalize() - pd.Timedelta(days=3)
    df['temp_order_date_dt'] = pd.to_datetime(df['Int_order_date'], format='%m/%d/%Y', errors='coerce')
    statuses_to_exclude =['End', 'Delivery Field', 'Manager Verification','COD Reconcilation','SAP Order  Status Success','Pending to Update End KM','Trip Confirmation']
    old_orders_df = df[(df['temp_order_date_dt'] < three_days_ago) & (~df['Current Flow'].isin(statuses_to_exclude)) & (df['Mode of Fullfillment'] == 'DSD')]
    old_orders_count = old_orders_df.groupby('Store_Name')['Reference Number'].nunique()
    summary_df['Orders Older Than 3 Days'] = summary_df.index.map(old_orders_count).fillna(0).astype(int)
    df.drop(columns=['temp_order_date_dt'], inplace=True)
    df['Int_LR_date_dt'] = pd.to_datetime(df['Int_LR_date'], errors='coerce')
    if 'Vehicle Model' in df.columns:
        vehicle_model_list_lower = [v.lower() for v in ['TATA 207/PICK UP', 'TATA ACE', 'PICKUP', 'APE / AUTO / 3 WHEELER', 'AUTO', 'ACE', 'DOST', 'Bolero']]
        three_days_ago_lr = pd.to_datetime('today').normalize() - pd.Timedelta(days=2)
        filtered_avg_df = df[(df['Current Flow'].isin(['End', 'Delivery Field', 'SAP Order Status Success'])) & (df.get('Prev Status') != 'handover_to_member') & (df['Vehicle Model'].str.lower().fillna('').isin(vehicle_model_list_lower)) & (df['Int_LR_date_dt'] >= three_days_ago_lr)]
        avg_tonnage = filtered_avg_df.groupby('Store_Name')['Item Gross Weight'].sum() / 3
        summary_df['Avg Specific Tonnage (Last 3 Days)'] = summary_df.index.map(avg_tonnage).fillna(0)
    else:
        summary_df['Avg Specific Tonnage (Last 3 Days)'] = 0
    summary_df = summary_df.reset_index()
    print("✅ Store Summary Report data created.")

    print("--- Creating Order Attainment Report for Yesterday ---")
    yesterday_str_format1 = (pd.to_datetime('today').normalize() - pd.Timedelta(days=1)).strftime('%m/%d/%Y')
    yesterday_df = df[df['Int_order_date'] == yesterday_str_format1]
    if yesterday_df.empty:
        overall_pivot, yes_pivot = None, None
    else:
        overall_pivot = yesterday_df.pivot_table(index='Store_Name', columns='Load', values='Item Gross Weight', aggfunc='sum', fill_value=0, margins=True, margins_name='Grand Total')
        considered_yes_df = yesterday_df[yesterday_df['Considered'] == 'Yes']
        if considered_yes_df.empty:
            yes_pivot = pd.DataFrame(data={'Message': ["No 'Considered=Yes' data found for yesterday."]})
        else:
            yes_pivot = considered_yes_df.pivot_table(index='Store_Name', columns='Load', values='Item Gross Weight', aggfunc='sum', fill_value=0, margins=True, margins_name='Grand Total')
    print("✅ Order Attainment Report data created.")

    print("--- Creating Capacity Summary Report for Yesterday ---")
    yesterday_lr_str = (pd.to_datetime('today').normalize() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    lr_yesterday_df = df[df['Int_LR_date'] == yesterday_lr_str]
    if lr_yesterday_df.empty:
        final_capacity_summary = None
    else:
        specific_vehicle_models = ['TATA 207/PICK UP', 'TATA ACE', 'PICKUP', 'APE / AUTO / 3 WHEELER', 'AUTO', 'ACE', 'DOST', 'Bolero', 'EICHER']
        contractual_df = lr_yesterday_df[(lr_yesterday_df['Vehicle type'] != 'adhoc') & (lr_yesterday_df['Vehicle Model'].isin(specific_vehicle_models))]
        adhoc_df = lr_yesterday_df[(lr_yesterday_df['Vehicle type'] == 'adhoc') & (lr_yesterday_df['Vehicle Model'].isin(specific_vehicle_models))]
        contractual_summary = contractual_df.groupby('Store_Name').agg(Contractual_Weight=('Item Gross Weight', 'sum'), Contractual_Vehicle_Count=('Vehicle Number1', 'nunique')) if not contractual_df.empty else pd.DataFrame(columns=['Contractual_Weight', 'Contractual_Vehicle_Count'])
        adhoc_summary = adhoc_df.groupby('Store_Name').agg(Adhoc_Weight=('Item Gross Weight', 'sum'), Adhoc_Vehicle_Count=('Vehicle Number1', 'nunique')) if not adhoc_df.empty else pd.DataFrame(columns=['Adhoc_Weight', 'Adhoc_Vehicle_Count'])
        final_capacity_summary = pd.concat([contractual_summary, adhoc_summary], axis=1).fillna(0)
        int_cols = ['Contractual_Vehicle_Count', 'Adhoc_Vehicle_Count']
        for col in int_cols:
            if col in final_capacity_summary.columns:
                final_capacity_summary[col] = final_capacity_summary[col].astype(int)
        final_capacity_summary.reset_index(inplace=True)
    print("✅ Capacity Summary Report data created.")

    print("--- Creating UPI Summary Report for Yesterday ---")
    yesterday_delivery_str = (pd.to_datetime('today').normalize() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    upi_df_base = df[(df['Payment Mode'] == 'Cash on Del Store') & (df['Int_delivery_date'] == yesterday_delivery_str)].copy()
    if upi_df_base.empty:
        upi_summary = None
    else:
        upi_df_base['UPI_Amount'] = np.where(upi_df_base['upiTransactionId'].notna(), upi_df_base['Net_Amount'], 0)
        upi_summary = upi_df_base.groupby('Store_Name').agg(Unique_upiTransactionId=('upiTransactionId', 'nunique'), Unique_Reference_Number=('Reference Number', 'nunique'), Total_UPI_Amount=('UPI_Amount', 'sum'), Total_COD_Amount=('Net_Amount', 'sum')).reset_index()
    print("✅ UPI Summary Report data created.")

    print("--- Creating Free Delivery Non-Adherence Report for Yesterday ---")
    vehicle_model_list_non_adherence = ['TATA 207/PICK UP', 'TATA ACE', 'PICKUP', 'APE / AUTO / 3 WHEELER', 'AUTO', 'ACE', 'DOST', 'Bolero', 'EICHER']
    non_adherence_raw_df = df[(df['Int_LR_date'] == yesterday_lr_str) & (df['Free_Delivery'] != 'Yes') & (df['Vehicle Model'].isin(vehicle_model_list_non_adherence)) & (df['Invoice Value'] < 750000) & (df['Load'].isin(['Normal']))].copy()
    if non_adherence_raw_df.empty:
        non_adherence_summary = None
    else:
        non_adherence_summary = non_adherence_raw_df.groupby('Store_Name').agg(Reference_Number_Count=('Reference Number', 'nunique'), Total_Item_weight=('Item Gross Weight', 'sum'), Total_Net_Amount=('Net_Amount', 'sum')).reset_index()
    print("✅ Non-Adherence summary created.")

    print("--- Creating Cross Dock Attainment Summary for Yesterday ---")
    cross_dock_filtered_df = df[(df['Considered'] == 'Yes') & (df['Int_order_date'] == yesterday_str_format1) & (df['Prev Status'] != 'handover_to_member')].copy()
    if cross_dock_filtered_df.empty:
        cross_dock_summary = None
    else:
        cross_dock_summary = pd.pivot_table(cross_dock_filtered_df, index=['Store_Name', 'X_doc'], values='Item Gross Weight', aggfunc='sum', fill_value=0).reset_index()
    print("✅ Cross Dock Attainment summary created.")

    print("--- Creating Dispatch Summary (TripSheet) Report for Yesterday ---")
    # 1. Define yesterday in YYYY-MM-DD format
    yesterday_iso = (pd.to_datetime('today').normalize() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 2. Filter rows where 'TripSheet Number' is not null
    dispatch_df = df[df['TripSheet Number'].notna()].copy()

    # 3. Extract parts: "RJ32GD9054 9752826416_bpl 2025-11-27 12:04"
    split_data = dispatch_df['TripSheet Number'].astype(str).str.split(expand=True)
    
    if split_data.shape[1] >= 4:
        dispatch_df['Extracted_Vehicle'] = split_data[0]
        dispatch_df['Extracted_Date'] = split_data[2]
        dispatch_df['Extracted_Time'] = split_data[3]
        
        # 4. Filter for yesterday's date
        dispatch_df = dispatch_df[dispatch_df['Extracted_Date'] == yesterday_iso]

        if dispatch_df.empty:
             dispatch_summary_final = None
             print("ℹ️ No TripSheet data found for yesterday.")
        else:
            # 5. Categorize Time Buckets
            dispatch_df['Hour'] = pd.to_numeric(dispatch_df['Extracted_Time'].str.split(':').str[0], errors='coerce')
            
            conditions = [
                dispatch_df['Hour'] < 9,
                (dispatch_df['Hour'] >= 9) & (dispatch_df['Hour'] < 10),
                (dispatch_df['Hour'] >= 10) & (dispatch_df['Hour'] < 11),
                dispatch_df['Hour'] >= 11
            ]
            choices = ['Till 9 Am', '9 Am to 10 Am', '10 Am to 11 Am', 'After 11 Am']
            dispatch_df['Time_Bucket'] = np.select(conditions, choices, default='Unknown')

            # 6. Pivot 1: TOTAL TRIP COUNT (TripSheet Number Count) per Time Bucket
            # CHANGED: from nunique to count to match list length
            time_pivot_counts = dispatch_df.pivot_table(
                index='Store Code1', 
                columns='Time_Bucket', 
                values='TripSheet Number', 
                aggfunc='count', 
                fill_value=0
            )
            
            # 7. Pivot 2: LIST of TripSheets per Time Bucket
            def join_tripsheets(x):
                return ", ".join(sorted(set(str(s) for s in x if s)))

            time_pivot_lists = dispatch_df.pivot_table(
                index='Store Code1',
                columns='Time_Bucket',
                values='TripSheet Number',
                aggfunc=join_tripsheets,
                fill_value=''
            )

            # 8. Ensure all columns exist and rename list columns
            required_cols = ['Till 9 Am', '9 Am to 10 Am', '10 Am to 11 Am', 'After 11 Am']
            
            # Add missing count columns
            for col in required_cols:
                if col not in time_pivot_counts.columns:
                    time_pivot_counts[col] = 0
            
            # Add suffix to list columns and ensure they exist
            time_pivot_lists = time_pivot_lists.add_suffix('_TripSheets')
            required_cols_lists = [c + '_TripSheets' for c in required_cols]
            for col in required_cols_lists:
                if col not in time_pivot_lists.columns:
                    time_pivot_lists[col] = ''

            # 9. Pivot 3: Total Unique Vehicle Count per Store (Stays UNIQUE)
            vehicle_pivot = dispatch_df.groupby('Store Code1')['Extracted_Vehicle'].nunique().to_frame(name='Unique_Vehicle_Count')

            # 10. Merge all pivots
            dispatch_summary_final = pd.concat([time_pivot_counts, time_pivot_lists, vehicle_pivot], axis=1).fillna(0)

            # 11. Reorder columns for readability (Count followed by List)
            final_col_order = []
            for col in required_cols:
                final_col_order.append(col)
                final_col_order.append(col + '_TripSheets')
            final_col_order.append('Unique_Vehicle_Count')
            
            # Apply reordering and reset index
            dispatch_summary_final = dispatch_summary_final[final_col_order]
            
            # Clean up: Ensure list columns that were NaN (now 0 from concat/fillna) are empty strings
            dispatch_summary_final[required_cols_lists] = dispatch_summary_final[required_cols_lists].replace(0, '')
            
            dispatch_summary_final.reset_index(inplace=True)
            print("✅ Dispatch Summary Report data created.")

    else:
        dispatch_summary_final = None
        print("❌ 'TripSheet Number' format unexpected or empty.")
    print("-" * 30)

    # --- 4. Saving & Uploading Output Files ---
    print("--- Saving reports locally before uploading to Drive ---")

    # Define local output file paths
    output_file_path = os.path.join(local_data_path, 'Capacity_dump_updated.csv')
    summary_output_path = os.path.join(local_data_path, 'Store_Summary_Report.csv')
    order_attainment_path = os.path.join(local_data_path, 'Order_attainment_summary_report.xlsx')
    capacity_summary_path = os.path.join(local_data_path, 'Capacity_Summary_Report.xlsx')
    upi_summary_path = os.path.join(local_data_path, 'UPI_Summary_Report.xlsx')
    non_adherence_report_path = os.path.join(local_data_path, 'Free_Delivery_Non_Adherence_Report.xlsx')
    cross_dock_report_path = os.path.join(local_data_path, 'Cross_Dock_Attainment_Summary.xlsx')
    dispatch_report_path = os.path.join(local_data_path, 'Dispatch_Summary_Report.xlsx')

    # Save files locally
    df.drop(columns=['Int_LR_date_dt'], inplace=True, errors='ignore')
    df.to_csv(output_file_path, index=False)

    summary_df.to_csv(summary_output_path, index=False)

    if overall_pivot is not None:
        with pd.ExcelWriter(order_attainment_path, engine='openpyxl') as writer:
            overall_pivot.to_excel(writer, sheet_name='Overall Attainment Report')
            yes_pivot.to_excel(writer, sheet_name='Considered Yes Attainment')

    if final_capacity_summary is not None:
        final_capacity_summary.to_excel(capacity_summary_path, index=False, sheet_name='Capacity_Summary')

    if upi_summary is not None:
        upi_summary.to_excel(upi_summary_path, index=False, sheet_name='UPI_Summary')

    if non_adherence_summary is not None:
        with pd.ExcelWriter(non_adherence_report_path, engine='openpyxl') as writer:
            non_adherence_summary.to_excel(writer, sheet_name='Summary', index=False)
            non_adherence_raw_df.to_excel(writer, sheet_name='Raw_Data', index=False)

    if cross_dock_summary is not None:
        cross_dock_summary.to_excel(cross_dock_report_path, index=False, sheet_name='Cross_Dock_Summary')

    if dispatch_summary_final is not None:
        dispatch_summary_final.to_excel(dispatch_report_path, index=False, sheet_name='Dispatch_summary')

    # Upload all generated files to Google Drive
    files_to_upload = [
        output_file_path, summary_output_path, order_attainment_path, 
        capacity_summary_path, upi_summary_path, non_adherence_report_path, 
        cross_dock_report_path, dispatch_report_path
    ]

    for path in files_to_upload:
        upload_file_to_drive(drive_service, path, INPUT_OUTPUT_FOLDER_ID)
    print("-" * 30)

    # --- 5. Exporting Reports to Google Sheets ---
    print("--- Exporting reports to Google Sheets ---")
    try:
        spreadsheet = sheets_service.open_by_url(GSHEET_URL)
        export_df_to_gsheet(spreadsheet, summary_df, 'Store_Summary_Report')
        export_df_to_gsheet(spreadsheet, overall_pivot, 'Overall Attainment Report')
        export_df_to_gsheet(spreadsheet, yes_pivot, 'Considered Yes Attainment')
        export_df_to_gsheet(spreadsheet, final_capacity_summary, 'Capacity_Summary_Report')
        export_df_to_gsheet(spreadsheet, upi_summary, 'UPI_Summary_Report')
        export_df_to_gsheet(spreadsheet, non_adherence_summary, 'Free_Delivery_Non_Adherence_Summ')
        export_df_to_gsheet(spreadsheet, non_adherence_raw_df, 'Free_Delivery_Non_Adherence_Raw')
        export_df_to_gsheet(spreadsheet, cross_dock_summary, 'Cross_Dock_Attainment_Summary')
        export_df_to_gsheet(spreadsheet, dispatch_summary_final, 'Dispatch_summary')
    except Exception as e:
        print(f"\n❌ An error occurred during the Google Sheets export process: {e}")
    print("-" * 30)

    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
