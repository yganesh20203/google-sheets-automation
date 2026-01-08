# ==============================================================================
# AUTOMATED BUSINESS INTELLIGENCE ENGINE (GOOGLE DRIVE + ADVANCED EXCEL REPORTING)
# ==============================================================================

import datetime
import zipfile
import io
import os
import gc
from io import BytesIO

# Data Handling Libraries
import pandas as pd
import numpy as np
import openpyxl 
from openpyxl.utils.dataframe import dataframe_to_rows 
import xlsxwriter  # CRITICAL: Engine for creating Excel Graphs & Dashboards

# Google API Libraries
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import google.auth

# --- LOGGING UTILITY ---
def log(message):
    """Prints a message with a timestamp for debugging/logs."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

# --- AUTHENTICATION & CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'credentials.json' # Ensure this file exists in your repo
SCOPES = ['https://www.googleapis.com/auth/drive']

# Folder IDs
SOURCE_FOLDER_ID = '1sern1xXqdDrQQBLXxbANj7LPs3IE1Dzo'
TARGET_FOLDER_ID = '1HTkBss1orVVn1akNygxiuklaAHQnJy8w'
FILE_PREFIXES = ['ArticleSalesReport', 'Overall_Instock']

def authenticate():
    """Authenticates using the Service Account and returns the Drive Service."""
    log("Attempting Google Drive authentication...")
    try:
        creds, _ = google.auth.default(scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        log("✅ Google Drive authentication successful.")
        return drive_service
    except Exception as e:
        log(f"❌ ERROR: Authentication failed. Details: {e}")
        return None

# ==============================================================================
# SECTION 1: HELPER FUNCTIONS (DOWNLOAD / UPLOAD)
# ==============================================================================

def download_csv_to_df(drive_service, file_name, folder_id):
    """Finds a CSV helper file by name and loads it into a Pandas DataFrame."""
    log(f"  Downloading helper file: {file_name}...")
    try:
        query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        
        if not items:
            log(f"  [ERROR] Helper file '{file_name}' not found.")
            return None
            
        request = drive_service.files().get_media(fileId=items[0]['id'], supportsAllDrives=True)
        file_buffer = BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done: _, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        try:
            return pd.read_csv(file_buffer)
        except UnicodeDecodeError:
            file_buffer.seek(0)
            log(f"  [INFO] Reading '{file_name}' with latin1 encoding.")
            return pd.read_csv(file_buffer, encoding='latin1')

    except Exception as e:
        log(f"  [ERROR] Failed to download '{file_name}': {e}")
        return None

def load_file_to_df(drive_service, file_id, file_name):
    """Downloads a generic file (CSV or ZIP containing CSV) into a DataFrame."""
    log(f"  Loading main file: {file_name} (ID: {file_id})...")
    try:
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        file_buffer = BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done: _, done = downloader.next_chunk()
        file_buffer.seek(0)

        csv_bytes = None
        if file_name.endswith('.zip'):
            log(f"    > Unzipping {file_name}...")
            with zipfile.ZipFile(file_buffer, 'r') as zf:
                # Find first CSV ignoring MACOSX artifacts
                csv_names = [n for n in zf.namelist() if n.endswith('.csv') and not n.startswith('__MACOSX')]
                if not csv_names:
                    log("    [ERROR] No CSV found inside zip.")
                    return None
                csv_bytes = zf.read(csv_names[0])
        elif file_name.endswith('.csv'):
            csv_bytes = file_buffer.read()
        else:
            log(f"    [ERROR] Unsupported file format: {file_name}")
            return None

        if csv_bytes:
            try:
                return pd.read_csv(BytesIO(csv_bytes))
            except UnicodeDecodeError:
                return pd.read_csv(BytesIO(csv_bytes), encoding='latin1')
        return None

    except Exception as e:
        log(f"  [ERROR] Failed to load main file: {e}")
        return None

def upload_excel_report(drive_service, excel_buffer, file_name, folder_id):
    """Uploads the generated Excel Report (with graphs) to Drive."""
    log(f"  Uploading Insights Report: {file_name}...")
    try:
        # Check if file exists to overwrite
        query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        existing_files = results.get('files', [])

        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaIoBaseUpload(
            excel_buffer, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
            resumable=True
        )
        
        if existing_files:
            file_id = existing_files[0]['id']
            log(f"    > Overwriting existing report (ID: {file_id})")
            drive_service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        else:
            drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            
        log(f"  ✅ Advanced Excel Report uploaded successfully.")
    except Exception as e:
        log(f"  ❌ Failed to upload report: {e}")

def update_xlsm_data_sheet(drive_service, df_to_paste, file_name_to_find, sheet_name_to_update, folder_id):
    """Updates the raw data sheet in the macro-enabled .xlsm file."""
    log(f"\n--- Updating Raw Data in Excel: {file_name_to_find} ---")
    if df_to_paste is None: return

    try:
        # Find File
        query = f"'{folder_id}' in parents and name='{file_name_to_find}' and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        items = results.get('files', [])
        if not items:
            log(f"  [ERROR] {file_name_to_find} not found.")
            return
        
        file_id = items[0]['id']
        
        # Download
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        buffer.seek(0)
        
        # Modify with OpenPyXL
        wb = openpyxl.load_workbook(buffer, keep_vba=True)
        if sheet_name_to_update in wb.sheetnames:
            idx = wb.sheetnames.index(sheet_name_to_update)
            wb.remove(wb[sheet_name_to_update])
            ws = wb.create_sheet(sheet_name_to_update, index=idx)
        else:
            ws = wb.create_sheet(sheet_name_to_update)
            
        for r in dataframe_to_rows(df_to_paste, index=False, header=True):
            ws.append(r)
            
        out_buffer = BytesIO()
        wb.save(out_buffer)
        out_buffer.seek(0)
        wb.close()
        
        # Upload
        media = MediaIoBaseUpload(out_buffer, mimetype='application/vnd.ms-excel.sheet.macroEnabled.12', resumable=True)
        drive_service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        log("  ✅ XLSM updated successfully.")

    except Exception as e:
        log(f"  [ERROR] Updating XLSM failed: {e}")

def upload_df_as_csv(drive_service, df, file_name, folder_id):
    """Simple helper to upload a DataFrame as a CSV."""
    if df is None: return
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        media = MediaIoBaseUpload(BytesIO(buffer.getvalue().encode('utf-8')), mimetype='text/csv', resumable=True)
        
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        drive_service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
        log(f"  ✅ CSV Uploaded: {file_name}")
    except Exception as e:
        log(f"  [ERROR] CSV Upload failed: {e}")

# ==============================================================================
# SECTION 2: DATA TRANSFORMATION LOGIC
# ==============================================================================

def process_overall_instock(df):
    """Adds primary keys to Instock Report."""
    log("    > Processing Overall_Instock...")
    if df is None: return None
    try:
        if 'Store Nbr' in df.columns and 'Old Nbr' in df.columns:
            df.insert(0, 'key', df['Store Nbr'].astype(str) + df['Old Nbr'].astype(str))
        return df
    except Exception as e:
        log(f"    [ERROR] Instock processing failed: {e}")
        return None

def process_article_sales_report(df, df_hirarchy, df_div, df_instock, df_gst, df_ytd, day_of_year):
    """The Master Transformation Function (11-Step Logic)."""
    log("    > Processing Article Sales Report (Transformation Pipeline)...")
    if df is None: return None
    
    try:
        # 1. Generate Article UID (Store + Article No)
        if 'Article No' in df.columns and 'Store No' in df.columns:
            # Safe conversion removing decimals
            s_store = pd.to_numeric(df['Store No'], errors='coerce').fillna(0).astype('int64').astype(str)
            s_article = pd.to_numeric(df['Article No'], errors='coerce').fillna(0).astype('int64').astype(str)
            df.insert(df.columns.get_loc('Article No')+1, 'Article UID', s_store + s_article)

        # 2. Merge Hierarchy (Region / Market Manager)
        if df_hirarchy is not None and 'Store No' in df.columns:
            df['Store No'] = df['Store No'].astype(str)
            df_hirarchy['Location'] = df_hirarchy['Location'].astype(str)
            df = df.merge(df_hirarchy[['Location', 'Market', 'Market Manager']], left_on='Store No', right_on='Location', how='left')
            df.rename(columns={'Market': 'Region'}, inplace=True)

        # 3. Merge Division V1
        if df_div is not None and 'Sub Division' in df.columns:
            df['Sub Division'] = df['Sub Division'].astype(str)
            df_div['Sub Division'] = df_div['Sub Division'].astype(str)
            df = df.merge(df_div[['Sub Division', 'Sub Division_V1']], on='Sub Division', how='left')

        # 4. Merge KVI Status
        if df_instock is not None and 'Article UID' in df.columns:
            df['Article UID'] = df['Article UID'].astype(str)
            df_instock['key'] = df_instock['key'].astype(str)
            df = df.merge(df_instock[['key', 'KVI_Flag', 'KVI_Allocation', 'KVI_Utilization']], left_on='Article UID', right_on='key', how='left')
            df.drop(columns=['key'], inplace=True, errors='ignore')

        # 5. Merge GST Changes
        if df_gst is not None and 'Article UID' in df.columns:
            df_gst['UID'] = df_gst['UID'].astype(str)
            df_gst['GST_Change'] = 'Yes'
            df = df.merge(df_gst[['UID', 'GST_Change']], left_on='Article UID', right_on='UID', how='left')
            df['GST_Change'] = df['GST_Change'].fillna('')
            df.drop(columns=['UID'], inplace=True, errors='ignore')

        # 6. Merge Historical YTD Sales
        if df_ytd is not None and 'Article UID' in df.columns:
            df['Article UID'] = df['Article UID'].astype(str).str.replace(r'\.0$', '', regex=True)
            df_ytd['Article UID'] = pd.to_numeric(df_ytd['Article UID'], errors='coerce').fillna(-1).astype('int64').astype(str)
            
            ytd_cols = [c for c in ['Article UID', '2021 YTD Sales', '2022 YTD Sales', '2023 YTD Sales', '2024 YTD Sales', '2025 YTD Sales'] if c in df_ytd.columns]
            df = df.merge(df_ytd[ytd_cols], on='Article UID', how='left')

        # 7. Calculate Average Sales & Day On Hand
        if day_of_year > 0:
            # Calc Average Daily Sales for all YTD columns
            sales_cols = ['YTD Sale Amt', '2021 YTD Sales', '2022 YTD Sales', '2023 YTD Sales', '2024 YTD Sales', '2025 YTD Sales']
            for col in sales_cols:
                if col in df.columns:
                    avg_col_name = col.replace('YTD Sales', 'Avg Sales').replace('Sale Amt', 'Avg Sales')
                    df[avg_col_name] = pd.to_numeric(df[col], errors='coerce') / day_of_year
            
            # Calc Day On Hand
            if 'YTD COST Amt' in df.columns and 'On Hand Cost' in df.columns:
                daily_cost_burn = pd.to_numeric(df['YTD COST Amt'], errors='coerce') / day_of_year
                on_hand_val = pd.to_numeric(df['On Hand Cost'], errors='coerce')
                df['Day On Hand'] = on_hand_val / daily_cost_burn
                df['Day On Hand'] = df['Day On Hand'].replace([np.inf, -np.inf], np.nan)
                
                # 8. Final Remarks Logic
                conditions = [(df['Day On Hand'] > 7), (df['Day On Hand'] <= 7)]
                choices = ['Price Support Required', 'Stock Required']
                df['Final Remarks'] = np.select(conditions, choices, default='')

        # 9. Clean up columns
        cols_drop = ['WEEK4_COST', 'WEEK4_QTY', 'WEEEK4_Sales', 'WEEK4_Sales']
        df.drop(columns=[c for c in cols_drop if c in df.columns], inplace=True)

        # 10. Filters
        if 'Article Status' in df.columns: 
            df = df[df['Article Status'].astype(str).str.strip().str.upper() != 'D']
        if 'Division' in df.columns: 
            df = df[~df['Division'].astype(str).str.lower().isin(['freebies', 'service article'])]
        if 'Store' in df.columns: 
            df = df[df['Store'].astype(str).str.strip().str.lower() != 'lucknow fc']

        log(f"    > Transformation Complete. Rows: {len(df)}")
        return df

    except Exception as e:
        log(f"    [ERROR] Transformation failed: {e}")
        return None

# ==============================================================================
# SECTION 3: ADVANCED INTELLIGENCE ENGINE (EXCEL + GRAPHS)
# ==============================================================================

def generate_excel_insights_report(df, date_str):
    """
    Generates a sophisticated Excel report with Charts, Store-Level Pareto Analysis,
    and Drill-Downs using XlsxWriter.
    """
    log("    > Spinning up Intelligence Engine (Excel Generation)...")
    
    # --- PRE-PROCESSING ---
    output = BytesIO()
    
    # Force Numeric for Calculations
    numeric_cols = ['YTD Sale Amt', 'FTD Sale Amt', 'On Hand Cost', 'YTD Avg Sales', 'Day On Hand', 'MTD Sale Amt']
    for col in numeric_cols:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # --- EXCEL WRITER INIT ---
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Styles
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#2F75B5', 'font_color': 'white', 'border': 1})
        fmt_subhead = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1})
        fmt_currency = workbook.add_format({'num_format': '₹ #,##0.00'})
        fmt_number = workbook.add_format({'num_format': '#,##0'})
        fmt_pct = workbook.add_format({'num_format': '0.0%'})
        
        # ----------------------------------------------------
        # SHEET 1: DASHBOARD (High Level)
        # ----------------------------------------------------
        log("      > Building Sheet 1: Executive Dashboard...")
        ws_dash = workbook.add_worksheet('Dashboard')
        
        # KPI Cards Calculation
        total_ftd = df['FTD Sale Amt'].sum() if 'FTD Sale Amt' in df.columns else 0
        total_ytd = df['YTD Sale Amt'].sum() if 'YTD Sale Amt' in df.columns else 0
        total_inv = df['On Hand Cost'].sum() if 'On Hand Cost' in df.columns else 0
        efficiency = (total_ytd / total_inv) if total_inv > 0 else 0
        
        # Write KPI Table
        data_kpi = [
            ['KPI Metric', 'Value'],
            ['Total FTD Sales (Today)', total_ftd],
            ['Total YTD Sales', total_ytd],
            ['Total Inventory Value', total_inv],
            ['Capital Efficiency Ratio', efficiency]
        ]
        
        ws_dash.write('B2', f"DAILY INTELLIGENCE REPORT: {date_str}", workbook.add_format({'bold': True, 'font_size': 14}))
        
        for i, row in enumerate(data_kpi):
            fmt = fmt_header if i == 0 else fmt_currency
            if i == 4: fmt = workbook.add_format({'num_format': '0.00'}) 
            ws_dash.write(4 + i, 1, row[0], fmt)
            ws_dash.write(4 + i, 2, row[1], fmt)

        # CHART: Top 10 Stores by FTD Sales
        if 'Store' in df.columns and 'FTD Sale Amt' in df.columns:
            top_stores = df.groupby('Store')['FTD Sale Amt'].sum().nlargest(10).reset_index()
            top_stores.to_excel(writer, sheet_name='Dashboard', startrow=12, startcol=1, index=False)
            
            chart_col = workbook.add_chart({'type': 'column'})
            chart_col.add_series({
                'name':       'FTD Sales',
                'categories': ['Dashboard', 13, 1, 22, 1],
                'values':     ['Dashboard', 13, 2, 22, 2],
                'fill':       {'color': '#4472C4'}
            })
            chart_col.set_title({'name': 'Top 10 Stores (Today\'s Sales)'})
            chart_col.set_y_axis({'name': 'Sales (₹)'})
            ws_dash.insert_chart('E5', chart_col)

        # ----------------------------------------------------
        # SHEET 2: STORE-LEVEL PARETO (Based on FTD)
        # ----------------------------------------------------
        log("      > Building Sheet 2: Store-Level Pareto...")
        ws_pareto = workbook.add_worksheet('Pareto_Analysis')
        
        if 'Store' in df.columns and 'FTD Sale Amt' in df.columns and 'Article Description' in df.columns:
            
            # --- PART A: SUMMARY TABLE (Store Level Metrics) ---
            ws_pareto.write('A1', 'STORE PARETO SUMMARY (Based on FTD Sales)', fmt_header)
            headers = ['Store', 'Total Articles Sold', 'Power SKUs (80% Sales)', 'Tail SKUs (20% Sales)', 'Total FTD Sales', 'Total MTD Sales', 'Total YTD Sales']
            for c, h in enumerate(headers): ws_pareto.write(2, c, h, fmt_subhead)
            
            stores = df['Store'].unique()
            row_idx = 3
            pareto_details_list = [] # To store detailed rows for Part B
            
            for store in stores:
                # Filter for Store & Items with Sales > 0 today
                store_df = df[(df['Store'] == store) & (df['FTD Sale Amt'] > 0)].copy()
                
                if store_df.empty: continue
                
                # Calculate Pareto
                store_df = store_df.sort_values(by='FTD Sale Amt', ascending=False)
                total_sales = store_df['FTD Sale Amt'].sum()
                store_df['Cum_Sales'] = store_df['FTD Sale Amt'].cumsum()
                store_df['Cum_Pct'] = store_df['Cum_Sales'] / total_sales
                
                # Tag Items
                store_df['Pareto_Class'] = np.where(store_df['Cum_Pct'] <= 0.80, 'A (Top 80%)', 'B (Tail 20%)')
                
                # Counts
                total_articles = len(store_df)
                power_skus = len(store_df[store_df['Pareto_Class'] == 'A (Top 80%)'])
                tail_skus = total_articles - power_skus
                
                # Sales Totals
                ftd_val = store_df['FTD Sale Amt'].sum()
                mtd_val = store_df['MTD Sale Amt'].sum()
                ytd_val = store_df['YTD Sale Amt'].sum()
                
                # Write Summary Row
                ws_pareto.write(row_idx, 0, store)
                ws_pareto.write(row_idx, 1, total_articles, fmt_number)
                ws_pareto.write(row_idx, 2, power_skus, fmt_number)
                ws_pareto.write(row_idx, 3, tail_skus, fmt_number)
                ws_pareto.write(row_idx, 4, ftd_val, fmt_currency)
                ws_pareto.write(row_idx, 5, mtd_val, fmt_currency)
                ws_pareto.write(row_idx, 6, ytd_val, fmt_currency)
                
                row_idx += 1
                
                # Save 'Power SKUs' for Part B (Detailed List)
                power_items = store_df[store_df['Pareto_Class'] == 'A (Top 80%)'][['Store', 'Article UID', 'Article Description', 'FTD Sale Amt', 'Pareto_Class']]
                pareto_details_list.append(power_items)

            # --- PART B: DETAILED RAW DATA (Power SKUs per Store) ---
            # Leave some space, then write the detailed list
            detail_start_row = row_idx + 3
            ws_pareto.write(detail_start_row, 0, 'DETAILED POWER SKUs (Articles contributing to 80% of Sales today)', fmt_header)
            
            if pareto_details_list:
                full_pareto_df = pd.concat(pareto_details_list)
                # Write to Excel starting below the summary
                full_pareto_df.to_excel(writer, sheet_name='Pareto_Analysis', startrow=detail_start_row+1, index=False)

        # ----------------------------------------------------
        # SHEET 3: REGIONAL DEEP DIVE (Top Item Analysis)
        # ----------------------------------------------------
        log("      > Building Sheet 3: Regional Deep Dive...")
        if 'Region' in df.columns and 'Store' in df.columns:
            ws_region = workbook.add_worksheet('Regional_Deep_Dive')
            row_cursor = 0
            
            unique_regions = df['Region'].dropna().unique()
            
            for region in unique_regions:
                ws_region.write(row_cursor, 0, f"REGION: {region}", fmt_header)
                row_cursor += 1
                
                reg_df = df[df['Region'] == region]
                top_arts = reg_df.groupby('Article Description')['FTD Sale Amt'].sum().nlargest(10).index.tolist()
                
                subset = reg_df[reg_df['Article Description'].isin(top_arts)]
                pivot = subset.pivot_table(index='Article Description', columns='Store', values='FTD Sale Amt', aggfunc='sum').fillna(0)
                
                ws_region.write(row_cursor, 0, "Article Description", fmt_header)
                for c, store_name in enumerate(pivot.columns):
                    ws_region.write(row_cursor, c+1, store_name, fmt_header)
                row_cursor += 1
                
                for art_name, row_data in pivot.iterrows():
                    ws_region.write(row_cursor, 0, art_name)
                    for c, val in enumerate(row_data):
                        ws_region.write(row_cursor, c+1, val, fmt_currency)
                    row_cursor += 1
                
                row_cursor += 2

        # ----------------------------------------------------
        # SHEET 4: ACTIONABLES (Urgent / Cash Traps / Margin)
        # ----------------------------------------------------
        log("      > Building Sheet 4: Actionables...")
        ws_action = workbook.add_worksheet('Actionables')
        
        ws_action.write('A1', 'URGENT REORDER (Top 50 Fast Movers)', fmt_header)
        if 'Final Remarks' in df.columns:
            urgent = df[(df['Final Remarks'] == 'Stock Required') & (df['YTD Avg Sales'] > 0)].sort_values('YTD Avg Sales', ascending=False).head(50)
            cols_urg = ['Article UID', 'Article Description', 'Store', 'Day On Hand', 'YTD Avg Sales']
            for c, col in enumerate(cols_urg): ws_action.write(1, c, col)
            for r, row in enumerate(urgent[cols_urg].values):
                for c, val in enumerate(row):
                    ws_action.write(r+2, c, val)

        ws_action.write('G1', 'CASH TRAPS (High Value Dead Stock)', fmt_header)
        if 'Day On Hand' in df.columns:
            traps = df[(df['Day On Hand'] > 180) & (df['On Hand Cost'] > 50000)].sort_values('On Hand Cost', ascending=False).head(50)
            cols_trap = ['Article UID', 'Article Description', 'Store', 'Day On Hand', 'On Hand Cost']
            for c, col in enumerate(cols_trap): ws_action.write(1, c+6, col)
            for r, row in enumerate(traps[cols_trap].values):
                for c, val in enumerate(row):
                    ws_action.write(r+2, c+6, val)

        ws_action.write('M1', 'MARGIN BLEED (Category Level)', fmt_header)
        cat_col = 'Sub Division_V1' if 'Sub Division_V1' in df.columns else 'Sub Division'
        if cat_col in df.columns and 'MTD IM %' in df.columns:
            margin = df.groupby(cat_col)[['MTD IM %', 'YTD IM %']].mean()
            margin['Drop'] = margin['YTD IM %'] - margin['MTD IM %']
            bleeders = margin[margin['Drop'] > 2].sort_values('Drop', ascending=False).reset_index()
            cols_marg = [cat_col, 'MTD IM %', 'YTD IM %', 'Drop']
            for c, col in enumerate(cols_marg): ws_action.write(1, c+12, col)
            for r, row in enumerate(bleeders[cols_marg].values):
                for c, val in enumerate(row):
                    ws_action.write(r+2, c+12, val)

        # ----------------------------------------------------
        # SHEET 5: CORRELATIONS (Vendors & Pricing)
        # ----------------------------------------------------
        log("      > Building Sheet 5: Correlations...")
        ws_corr = workbook.add_worksheet('Correlations')
        
        ws_corr.write('A1', 'TOP VENDORS CAUSING STOCKOUTS', fmt_header)
        if 'Vendor Name' in df.columns:
            vendor_risk = df[df['Final Remarks'] == 'Stock Required'].groupby('Vendor Name')['Article UID'].count().reset_index(name='Stockout Count')
            vendor_risk = vendor_risk.sort_values('Stockout Count', ascending=False).head(20)
            
            ws_corr.write(1, 0, 'Vendor Name')
            ws_corr.write(1, 1, 'Stockout Count')
            for r, row in enumerate(vendor_risk.values):
                ws_corr.write(r+2, 0, row[0])
                ws_corr.write(r+2, 1, row[1])

        ws_corr.write('E1', 'GST PRICE IMPACT (Elasticity Proxy)', fmt_header)
        if 'GST_Change' in df.columns and '2024 Avg Sales' in df.columns and 'MTD Qty' in df.columns:
            gst_items = df[df['GST_Change'] == 'Yes'].copy()
            if not gst_items.empty and 'Selling Price (With Tax)' in df.columns:
                # Approx Daily Volume Pre-Change (Sales / Price)
                gst_items['Est_Pre_Vol'] = gst_items['2024 Avg Sales'] / gst_items['Selling Price (With Tax)'].replace(0, 1)
                gst_items['Est_Post_Vol'] = gst_items['MTD Qty'] / 30
                gst_items['Vol Change %'] = ((gst_items['Est_Post_Vol'] - gst_items['Est_Pre_Vol']) / gst_items['Est_Pre_Vol'].replace(0, 1))
                
                view = gst_items[['Article Description', 'Selling Price (With Tax)', 'Vol Change %']].head(50)
                
                cols_gst = ['Article Description', 'Price', 'Vol Change %']
                for c, col in enumerate(cols_gst): ws_corr.write(1, c+4, col)
                for r, row in enumerate(view.values):
                    ws_corr.write(r+2, 4, row[0])
                    ws_corr.write(r+2, 5, row[1])
                    ws_corr.write(r+2, 6, row[2], fmt_pct)

    output.seek(0)
    return output

# ==============================================================================
# SECTION 4: MAIN ORCHESTRATOR
# ==============================================================================

def find_files_for_date(drive_service, date_str):
    """Finds source CSV/ZIP files for a given date."""
    log(f"  Querying source files for date: {date_str}")
    file_info = {}
    for prefix in FILE_PREFIXES:
        # Search for either CSV or ZIP
        q = f"'{SOURCE_FOLDER_ID}' in parents and (name='{prefix}_{date_str}.csv' or name='{prefix}_{date_str}.zip') and trashed=false"
        results = drive_service.files().list(q=q, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        if not items: return None
        file_info[prefix] = (items[0]['id'], items[0]['name'])
    return file_info

def check_and_copy_files(drive_service):
    """Main execution function."""
    if not drive_service: return

    # 1. Download Helpers
    log("\n--- Phase 1: Downloading Reference Data ---")
    df_hirarchy = download_csv_to_df(drive_service, 'hirarchy.csv', TARGET_FOLDER_ID)
    df_div = download_csv_to_df(drive_service, 'division_group.csv', TARGET_FOLDER_ID)
    df_gst = download_csv_to_df(drive_service, 'gst_change_list.csv', TARGET_FOLDER_ID)
    df_ytd = download_csv_to_df(drive_service, 'ytd_sales.csv', TARGET_FOLDER_ID)

    # 2. Date Fallback Logic (Today -> Yesterday)
    log("\n--- Phase 2: Locating Source Files ---")
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')
    calc_date = today - datetime.timedelta(days=1) # Average Sales based on yesterday
    
    file_info = find_files_for_date(drive_service, date_str)
    
    if not file_info:
        log(f"  [WARN] Files for {date_str} not found. Checking yesterday...")
        date_str = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        calc_date = today - datetime.timedelta(days=2)
        file_info = find_files_for_date(drive_service, date_str)

    if not file_info:
        log("❌ CRITICAL: No source files found for Today or Yesterday. Exiting.")
        return

    log(f"✅ Found files for {date_str}. Proceeding...")
    day_of_year = calc_date.timetuple().tm_yday
    log(f"  Using Day of Year: {day_of_year} for averages.")

    # 3. Load & Process Main Files
    log("\n--- Phase 3: Loading & Processing ---")
    df_article = load_file_to_df(drive_service, file_info['ArticleSalesReport'][0], file_info['ArticleSalesReport'][1])
    df_instock = load_file_to_df(drive_service, file_info['Overall_Instock'][0], file_info['Overall_Instock'][1])
    
    # Process Instock First (Need it for KVI in Article)
    df_instock = process_overall_instock(df_instock)
    
    # Process Main Article Report
    df_final = process_article_sales_report(df_article, df_hirarchy, df_div, df_instock, df_gst, df_ytd, day_of_year)
    
    # 4. Generate Outputs
    if df_final is not None:
        log("\n--- Phase 4: Output Generation ---")
        
        # A. Update Raw Data XLSM
        update_xlsm_data_sheet(
            drive_service, df_final, 
            "article_sales_report.xlsm", "Sheet1", TARGET_FOLDER_ID
        )
        
        # B. Upload Raw Data CSV (Instock)
        if df_instock is not None:
            upload_df_as_csv(drive_service, df_instock, f"Overall_Instock_{date_str}.csv", TARGET_FOLDER_ID)

        # C. Generate & Upload Advanced Excel Dashboard
        excel_buffer = generate_excel_insights_report(df_final, date_str)
        upload_excel_report(drive_service, excel_buffer, f"Business_Insights_Report_{date_str}.xlsx", TARGET_FOLDER_ID)

    # 5. Copy Originals
    log("\n--- Phase 5: Backup Original Files ---")
    for prefix in FILE_PREFIXES:
        f_id, f_name = file_info[prefix]
        # Check if exists before copying
        q = f"'{TARGET_FOLDER_ID}' in parents and name='{f_name}' and trashed=false"
        if not drive_service.files().list(q=q).execute().get('files'):
            drive_service.files().copy(fileId=f_id, body={'name': f_name, 'parents': [TARGET_FOLDER_ID]}).execute()
            log(f"  [BACKUP] Copied {f_name}")
        else:
            log(f"  [SKIP] {f_name} already exists in target.")

    # 6. Cleanup
    log("  Cleaning up memory...")
    del df_article, df_instock, df_final, df_hirarchy, df_div, df_gst, df_ytd
    gc.collect()
    
    log("\n=== SUCCESS: Pipeline Completed Successfully ===")

if __name__ == "__main__":
    srv = authenticate()
    if srv: check_and_copy_files(srv)
