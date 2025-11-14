# =============================================================================
# SECTION 1: IMPORT LIBRARIES
# =============================================================================
import os
import io
import time
import json
import traceback
import gc
import random
import re
import shutil
from pathlib import Path
import glob
import subprocess
import base64  # For decoding GitHub Secret

import pandas as pd
import requests
from gtts import gTTS  # Google Text-to-Speech (Fallback)

# --- Google API & Service Account Auth ---
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.exceptions import GoogleAuthError

print("✅ Libraries imported successfully!")

# =============================================================================
# SECTION 2: GOOGLE API & AUTHENTICATION
# =============================================================================

def setup_google_auth(secret_env_var_name):
    """
    Reads a base64-encoded Google Secret, decodes it,
    and saves it to a temporary file.
    """
    print("Setting up Google Authentication...")
    creds_json_string = os.environ.get(secret_env_var_name)
    
    if not creds_json_string:
        print(f"❌ ERROR: Environment variable '{secret_env_var_name}' not found.")
        print("   Please ensure you have added the GCP_SA_KEY secret to your GitHub repository.")
        return None

    try:
        # Decode the base64 string
        creds_json = base64.b64decode(creds_json_string).decode('utf-8')
        
        # Define a local path for the credentials file
        local_creds_path = "sa_creds.json"
        
        # Write the decoded JSON to the file
        with open(local_creds_path, 'w') as f:
            f.write(creds_json)
            
        print(f"✅ Service account key written to {local_creds_path}")
        return local_creds_path
        
    except Exception as e:
        print(f"❌ ERROR: Failed to decode or write service account key: {e}")
        return None

def authenticate_google(service_account_json_path):
    """Authenticates with Google using a service account JSON file."""
    print(f"Authenticating using: {service_account_json_path}")
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    try:
        creds = Credentials.from_service_account_file(service_account_json_path, scopes=SCOPES)
        gc_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        print("✅ Google Authentication successful.")
        return gc_client, drive_service
    except FileNotFoundError:
        print(f"❌ ERROR: Service account file not found at {service_account_json_path}")
        raise
    except GoogleAuthError as e:
        print(f"❌ ERROR: Google Authentication failed: {e}")
        raise
    except Exception as e:
        print(f"❌ ERROR: An unexpected error occurred during authentication: {e}")
        raise

def find_file_id(service, parent_folder_id, file_name):
    """Finds a file's ID in a specific Google Drive folder."""
    print(f"   - Searching for file '{file_name}' in folder '{parent_folder_id}'...")
    try:
        query = f"'{parent_folder_id}' in parents and name='{file_name}' and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if not files:
            print(f"   - ❌ WARNING: File not found: {file_name}")
            return None
        file_id = files[0]['id']
        print(f"   - ✅ Found file: {file_name} (ID: {file_id})")
        return file_id
    except Exception as e:
        print(f"   - ❌ ERROR searching for file '{file_name}': {e}")
        return None

def get_google_sheet_data(gc_client, sheet_file_id, worksheet_name="offer_articles"):
    """Fetches a Google Sheet worksheet into a pandas DataFrame."""
    print(f"Fetching Google Sheet data... (Sheet ID: {sheet_file_id}, Worksheet: {worksheet_name})")
    try:
        spreadsheet = gc_client.open_by_key(sheet_file_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"   - ⚠️ Warning: Worksheet '{worksheet_name}' not found. Trying 'Sheet1'.")
        try:
            worksheet = spreadsheet.sheet1
        except Exception as e:
            print(f"   - ❌ ERROR: Could not find '{worksheet_name}' or 'Sheet1'. {e}")
            raise
    except Exception as e:
        print(f"❌ ERROR opening Google Sheet: {e}")
        raise

    try:
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty:
            print("   - ❌ ERROR: The Google Sheet is empty or could not be read.")
            return None
            
        print(f"✅ Google Sheet data loaded successfully ({len(df)} rows).")
        
        # --- Column Validation ---
        # Map user's B, C, D... to expected headers
        required_headers = [
            'Article name',  # Col B
            'current mrp',   # Col C
            'selling price', # Col D
            'discount %',    # Col E
            'Storename',     # Col F
            'Store Code'     # Col G
        ]
        
        missing_headers = [col for col in required_headers if col not in df.columns]
        if missing_headers:
            print(f"❌ ERROR: The Google Sheet is missing required columns: {missing_headers}")
            print(f"   Please ensure your sheet has these exact headers: {required_headers}")
            print(f"   Found headers: {list(df.columns)}")
            return None
            
        print("   - All required columns found in Google Sheet.")
        return df
        
    except Exception as e:
        print(f"❌ ERROR fetching Google Sheet data: {e}")
        raise

def create_drive_folder(service, parent_id, folder_name):
    """Creates a folder on Google Drive, or returns ID if it exists."""
    # Check if folder already exists
    try:
        query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if files:
            folder_id = files[0]['id']
            print(f"   - ✅ Found existing folder: '{folder_name}' (ID: {folder_id})")
            return folder_id
    except Exception as e:
        print(f"   - ⚠️ Warning checking for folder: {e}. Will attempt to create.")

    # Create new folder
    print(f"   - Creating new folder: '{folder_name}'...")
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        file = service.files().create(body=file_metadata, fields='id').execute()
        folder_id = file.get('id')
        print(f"   - ✅ Created folder: '{folder_name}' (ID: {folder_id})")
        return folder_id
    except Exception as e:
        print(f"   - ❌ ERROR creating folder '{folder_name}': {e}")
        raise

def upload_file_to_drive(service, parent_folder_id, local_file_path):
    """Uploads a local file to a specific Google Drive folder."""
    file_name = os.path.basename(local_file_path)
    
    mime_types = {'.wav': 'audio/wav', '.mp3': 'audio/mpeg'}
    ext = os.path.splitext(file_name)[1].lower()
    mime_type = mime_types.get(ext, 'application/octet-stream')

    print(f"     - Uploading '{file_name}' (MIME: {mime_type})...")

    try:
        media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)
        file_metadata = {
            'name': file_name,
            'parents': [parent_folder_id]
        }
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"     - Upload {int(status.progress() * 100)}%")
        
        print(f"     - ✅ Upload complete: {file_name}")
    except Exception as e:
        print(f"     - ❌ ERROR uploading file '{file_name}': {e}")


# =============================================================================
# SECTION 3: MULTI-LINGUAL TTS TEXT GENERATION
# =============================================================================

def _clean_product_name(product_name, max_words=4):
    """Cleans and shortens product name."""
    cleaned_name = str(product_name).replace("'", "").replace('"', '').replace('.', '').strip()
    if not cleaned_name: return ""
    words = cleaned_name.split()
    if len(words) > max_words:
        cleaned_name = " ".join(words[:max_words])
    return cleaned_name

def generate_hindi_offer_text(product_name, mrp, selling_price, discount_percent):
    """Generates enthusiastic Hindi text."""
    cleaned_product_name = _clean_product_name(product_name)
    if not cleaned_product_name: cleaned_product_name = "यह प्रोडक्ट"

    intro_phrases = ["सुनिए सुनिए!", "आज की ख़ास डील!", "एक बेहतरीन ऑफर!", "ध्यान दीजिए!", "पेश है,"]
    outro_phrases = ["जल्दी कीजिये!", "मौका हाथ से जाने न दें!", "अभी खरीदें!", "यह ऑफर सीमित समय के लिए है!", "आज ही लाभ उठाएं!"]
    saving_phrases = ["बचत ही बचत!", "शानदार बचत!", "ज़बरदस्त डिस्काउंट!"]
    deal_phrases = ["शानदार डील!", "विशेष ऑफ़र!"]

    intro = random.choice(intro_phrases)
    outro = random.choice(outro_phrases)

    try:  # Check for percentage discount
        discount_val = float(discount_percent)
        if 0 < abs(discount_val) < 1: discount_val = int(round(discount_val * 100))
        else: discount_val = int(round(discount_val))
        if discount_val > 0:
            saving_intro = random.choice(saving_phrases)
            try:
                selling_price_num = int(float(selling_price))
                return f"{saving_intro} {cleaned_product_name} पर पाएं {discount_val} प्रतिशत की छूट! ऑफ़र प्राइस, सिर्फ़ {selling_price_num} रुपये! {outro}"
            except (ValueError, TypeError):
                return f"{saving_intro} {cleaned_product_name} पर पाएं {discount_val} प्रतिशत की छूट! {outro}"
    except (ValueError, TypeError): pass

    if isinstance(selling_price, str) and 'upto' in selling_price.lower():
        deal_intro = random.choice(deal_phrases)
        price_str_part = " ".join(selling_price.split()[-2:]) # "Upto Rs 500"
        return f"{deal_intro} {cleaned_product_name} पर! यह उपलब्ध है, {price_str_part}! {outro}"

    try:  # Default: MRP vs Selling Price
        mrp_val = int(float(mrp))
        selling_price_val = int(float(selling_price))
        return f"{intro} {cleaned_product_name}, जिसका MRP है {mrp_val} रुपये, अब मिलेगा सिर्फ़ {selling_price_val} रुपये के स्पेशल प्राइस में! {outro}"
    except (ValueError, TypeError):
        return f"पेश है {cleaned_product_name} पर, एक शानदार डील!"  # Simple fallback


def generate_telugu_offer_text(product_name, mrp, selling_price, discount_percent):
    """Generates enthusiastic Telugu text."""
    cleaned_product_name = _clean_product_name(product_name)
    if not cleaned_product_name: cleaned_product_name = "ఈ ప్రొడక్ట్"

    intro_phrases = ["వినండి వినండి!", "ఈ రోజు ప్రత్యేక డీల్!", "అద్భుతమైన ఆఫర్!", "గమనించండి!", "వచ్చేసింది,"]
    outro_phrases = ["త్వరపడండి!", "ఈ అవకాశం కోల్పోకండి!", "ఇప్పుడే కొనండి!", "ఈ ఆఫర్ పరిమిత సమయం మాత్రమే!", "ఈరోజే ప్రయోజనం పొందండి!"]
    saving_phrases = ["ఆదా అంటే ఇదే!", "భారీ ఆదా!", "అద్భుతమైన డిస్కౌంట్!"]
    deal_phrases = ["అద్భుతమైన డీల్!", "ప్రత్యేక ఆఫర్!"]

    intro = random.choice(intro_phrases)
    outro = random.choice(outro_phrases)

    try:  # Check for percentage discount
        discount_val = float(discount_percent)
        if 0 < abs(discount_val) < 1: discount_val = int(round(discount_val * 100))
        else: discount_val = int(round(discount_val))
        if discount_val > 0:
            saving_intro = random.choice(saving_phrases)
            try:
                selling_price_num = int(float(selling_price))
                return f"{saving_intro} {cleaned_product_name} పై, పొందండి {discount_val} శాతం తగ్గింపు! ఆఫర్ ధర, కేవలం {selling_price_num} రూపాయలు! {outro}"
            except (ValueError, TypeError):
                return f"{saving_intro} {cleaned_product_name} పై, పొందండి {discount_val} శాతం తగ్గింపు! {outro}"
    except (ValueError, TypeError): pass

    if isinstance(selling_price, str) and 'upto' in selling_price.lower():
        deal_intro = random.choice(deal_phrases)
        price_str_part = " ".join(selling_price.split()[-2:]) # "Upto Rs 500"
        return f"{deal_intro} {cleaned_product_name} పై! ఇది అందుబాటులో ఉంది, {price_str_part}! {outro}"

    try:  # Default: MRP vs Selling Price
        mrp_val = int(float(mrp))
        selling_price_val = int(float(selling_price))
        return f"{intro} {cleaned_product_name}, దీని MRP {mrp_val} రూపాయలు, ఇప్పుడు లభిస్తుంది కేవలం {selling_price_val} రూపాయల ప్రత్యేక ధరకే! {outro}"
    except (ValueError, TypeError):
        return f"వచ్చేసింది {cleaned_product_name} పై, ఒక అద్భుతమైన డీల్!"  # Simple fallback


def generate_offer_text(language_code, product_name, mrp, selling_price, discount_percent):
    """Master function to select the correct language generator."""
    if language_code == 'te':
        return generate_telugu_offer_text(product_name, mrp, selling_price, discount_percent)
    else:  # Default to Hindi
        return generate_hindi_offer_text(product_name, mrp, selling_price, discount_percent)

print("✅ TTS text generation functions defined!")

# =============================================================================
# SECTION 4: PIPER TTS AUDIO GENERATION
# =============================================================================

def _piper_hf_urls(model_id: str):
    """Gets the Hugging Face download URLs for a Piper model."""
    try:
        parts = model_id.split('-')
        lang_code, speaker, quality = parts[0], parts[1], parts[2]
        family = lang_code.split('_')[0]
        base = f"https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/{family}/{lang_code}/{speaker}/{quality}/{model_id}"
        return f"{base}.onnx", f"{base}.onnx.json"
    except Exception:
        return None, None

def ensure_piper_voice(model_id: str, voices_dir: str):
    """Downloads a Piper voice model and config if they don't exist."""
    os.makedirs(voices_dir, exist_ok=True)
    model_url, json_url = _piper_hf_urls(model_id)
    if not model_url:
        print(f"   - ❌ ERROR: Could not generate URLs for model '{model_id}'")
        return None, None

    model_path = os.path.join(voices_dir, f"{model_id}.onnx")
    json_path = os.path.join(voices_dir, f"{model_id}.onnx.json")

    def _download(url, path):
        print(f"     - Checking for: {os.path.basename(path)}")
        if os.path.exists(path):
            print("     - ✅ Already exists.")
            return True
        print(f"     - Attempting download from: {url}")
        try:
            with requests.Session() as session:
                r = session.get(url, stream=True, timeout=60)
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192 * 10):
                        if chunk: f.write(chunk)
                print("     - ✅ Download complete.")
                return True
        except Exception as e:
            print(f"     - ❌ Download failed: {e}")
            return False

    model_ok = _download(model_url, model_path)
    json_ok = _download(json_url, json_path)

    if model_ok and json_ok:
        print(f"   - ✅ Successfully verified voice: {model_id}")
        return model_path, json_path
    else:
        print(f"   - ❌ Failed to download/verify files for {model_id}.")
        return None, None

def generate_piper_tts_audio(model_path: str, text: str, output_wav_path: str, output_mp3_path: str):
    """
    Generates WAV and MP3 audio from text using a local Piper model
    via the command-line interface.
    """
    print("   - Generating audio via Piper TTS...")
    config_path = model_path + ".json"
    sample_rate = 22050  # Default
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            sample_rate = config.get('audio', {}).get('sample_rate', 22050)
        print(f"   - Using sample rate: {sample_rate}")
    except Exception as e:
        print(f"   - ⚠️ Warning: Could not read sample rate: {e}. Defaulting to {sample_rate}.")

    # Create a temporary directory for intermediate files
    TMP_AUDIO_DIR = "./tmp_audio"
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)
    tmp_wav = os.path.join(TMP_AUDIO_DIR, "temp_piper_out.wav")

    # Clean, safe text for shell command
    safe_text = text.replace("'", "'\\''")
    safe_model_path = f'"{model_path}"' if ' ' in model_path else model_path
    safe_tmp_wav = f'"{tmp_wav}"' if ' ' in tmp_wav else tmp_wav

    # Enthusiastic, randomized parameters
    length_scale = round(random.uniform(0.9, 1.0), 2)
    noise_scale = round(random.uniform(0.667, 0.8), 3)
    noise_w = round(random.uniform(0.8, 1.0), 3)
    
    print(f"   - Params: len_scale={length_scale}, noise_scale={noise_scale}, noise_w={noise_w}")

    command = (
        f"echo '{safe_text}' | "
        f"piper --model {safe_model_path} "
        f"--length_scale {length_scale} "
        f"--noise_scale {noise_scale} "
        f"--noise_w {noise_w} "
        f"--output_file {safe_tmp_wav}"
    )

    try:
        # 1. Generate Raw WAV using Piper
        process = subprocess.run(
            command, shell=True, check=True, capture_output=True, timeout=90
        )
        print("   - Piper subprocess complete.")
        
        if not os.path.exists(tmp_wav) or os.path.getsize(tmp_wav) < 1024:
            raise RuntimeError("Piper failed: Output WAV file is missing or empty.")

        # 2. Convert to final WAV (corrects sample rate if needed)
        print(f"   - Converting to final WAV: {os.path.basename(output_wav_path)}")
        subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", tmp_wav,
             "-ac", "1", "-ar", str(sample_rate), "-c:a", "pcm_s16le",
             output_wav_path],
            check=True, timeout=60
        )

        # 3. Convert to MP3
        print(f"   - Converting to MP3: {os.path.basename(output_mp3_path)}")
        subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", output_wav_path,
             "-map", "0:a:0", "-ac", "1", "-ar", str(sample_rate),
             "-codec:a", "libmp3lame", "-b:a", "128k",
             output_mp3_path],
            check=True, timeout=60
        )
        
        print("   - ✅ Audio generation successful.")

    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.decode(errors='ignore') if e.stderr else 'N/A'
        print(f"   - ❌ ERROR: Subprocess failed!")
        print(f"     - Command: {e.cmd}")
        print(f"     - Stderr: {stderr_output}")
        raise
    except Exception as e:
        print(f"   - ❌ ERROR during TTS/conversion: {e}")
        raise
    finally:
        # Clean up temporary file
        if os.path.exists(tmp_wav):
            os.remove(tmp_wav)

# =============================================================================
# SECTION 5: MAIN EXECUTION
# =============================================================================

def main():
    print("🏁 Starting Audio Generation Script 🏁")

    # --- Configuration ---
    GCP_SECRET_NAME = "GCP_SA_KEY"
    DRIVE_INPUT_FOLDER_ID = "1J2epmcfA8hT8YFk4Q7G9LM3qLZzw3W_H"
    DRIVE_OUTPUT_FOLDER_ID = "1EgSz_-mkxK-L0inIu0-_gJuES8267zfo"
    SHEET_FILE_NAME = "offer_articles"  # The Google Sheet file
    WORKSHEET_NAME = "offer_articles" # The specific tab/worksheet name
    
    LOCAL_VOICES_DIR = "./piper_voices"
    LOCAL_OUTPUT_DIR = "./audio_outputs"
    
    PIPER_VOICE_HI = "hi_IN-priyamvada-medium"
    PIPER_VOICE_TE = "te_IN-sushma-medium" # Telugu voice model
    
    # Store codes for Hindi voice
    HINDI_STORE_CODES = {
        4702, 4703, 4706, 4712, 4713, 4716, 4717, 4719, 4720, 
        4723, 4724, 4727, 4729, 4744, 4760, 4797, 4801, 4803
    }
    
    local_creds_path = None
    
    try:
        # --- 1. Setup & Authentication ---
        local_creds_path = setup_google_auth(GCP_SECRET_NAME)
        if not local_creds_path:
            raise RuntimeError("Failed to set up Google Authentication.")
            
        gc_client, drive_service = authenticate_google(local_creds_path)

        # --- 2. Setup Local Directories ---
        os.makedirs(LOCAL_VOICES_DIR, exist_ok=True)
        os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
        os.makedirs("./tmp_audio", exist_ok=True) # For ffmpeg intermediates
        
        # --- 3. Ensure TTS Models are Downloaded ---
        print("\n--- Ensuring TTS Voice Models ---")
        hindi_model_path, _ = ensure_piper_voice(PIPER_VOICE_HI, LOCAL_VOICES_DIR)
        telugu_model_path, _ = ensure_piper_voice(PIPER_VOICE_TE, LOCAL_VOICES_DIR)

        if not (hindi_model_path and telugu_model_path):
            raise RuntimeError("Failed to download one or more Piper TTS models. Aborting.")
            
        # --- 4. Get Sheet Data ---
        print("\n--- Fetching Google Sheet Data ---")
        sheet_file_id = find_file_id(drive_service, DRIVE_INPUT_FOLDER_ID, SHEET_FILE_NAME)
        if not sheet_file_id:
            raise RuntimeError(f"Could not find Google Sheet '{SHEET_FILE_NAME}' in input folder.")
            
        df = get_google_sheet_data(gc_client, sheet_file_id, worksheet_name=WORKSHEET_NAME)
        if df is None or df.empty:
            raise RuntimeError("Failed to load data or DataFrame is empty.")

        # --- 5. Process Each Row ---
        print("\n--- Processing Products ---")
        for index, row in df.iterrows():
            try:
                # --- 5a. Get Data & Determine Language ---
                article_name = str(row['Article name'])
                store_name = str(row['Storename'])
                store_code = int(row['Store Code'])
                
                print(f"\nProcessing Row {index+1}: '{article_name}' for Store: '{store_name}'")

                if store_code in HINDI_STORE_CODES:
                    lang_code = 'hi'
                    model_path = hindi_model_path
                    print(f"   - Store Code {store_code} found in Hindi list. Language: Hindi")
                else:
                    lang_code = 'te'
                    model_path = telugu_model_path
                    print(f"   - Store Code {store_code} not in Hindi list. Language: Telugu")

                # --- 5b. Generate Text ---
                text_to_speak = generate_offer_text(
                    lang_code,
                    article_name,
                    row['current mrp'],
                    row['selling price'],
                    row['discount %']
                )
                print(f"   - Generated Text: {text_to_speak[:75]}...")

                # --- 5c. Prepare Local Paths ---
                # Sanitize names for file systems
                safe_store_name = re.sub(r'[^\w\s-]', '', store_name).strip().replace(' ', '_')
                safe_article_name = re.sub(r'[^\w\s-]', '', article_name).strip().replace(' ', '_')[:100]
                if not safe_article_name: safe_article_name = f"product_{index}"

                local_store_folder = os.path.join(LOCAL_OUTPUT_DIR, safe_store_name)
                os.makedirs(local_store_folder, exist_ok=True)
                
                output_wav = os.path.join(local_store_folder, f"{safe_article_name}.wav")
                output_mp3 = os.path.join(local_store_folder, f"{safe_article_name}.mp3")

                # --- 5d. Generate Audio (with gTTS Fallback) ---
                try:
                    generate_piper_tts_audio(
                        model_path, text_to_speak, output_wav, output_mp3
                    )
                except Exception as e_piper:
                    print(f"   - ⚠️ Piper TTS failed: {e_piper}")
                    print("   - Attempting gTTS fallback...")
                    try:
                        tts_fallback = gTTS(text=text_to_speak, lang=lang_code, slow=False)
                        tts_fallback.save(output_mp3)
                        print("   - ✅ gTTS fallback (MP3) successful.")
                        # Create a dummy WAV file if gTTS succeeds? No, just upload the MP3.
                        if os.path.exists(output_wav): 
                            os.remove(output_wav) # Remove partial/failed Piper WAV
                    except Exception as e_gtts:
                        print(f"   - ❌ gTTS fallback also failed: {e_gtts}")
                        continue # Skip to next row

                # --- 5e. Upload to Google Drive ---
                print("   - Uploading audio files to Google Drive...")
                drive_store_folder_id = create_drive_folder(
                    drive_service, DRIVE_OUTPUT_FOLDER_ID, safe_store_name
                )
                
                if not drive_store_folder_id:
                    print(f"   - ❌ Skipping upload, could not create/find Drive folder for '{safe_store_name}'")
                    continue
                
                # Upload whatever files were successfully created
                if os.path.exists(output_mp3):
                    upload_file_to_drive(drive_service, drive_store_folder_id, output_mp3)
                if os.path.exists(output_wav):
                    upload_file_to_drive(drive_service, drive_store_folder_id, output_wav)

            except Exception as e_row:
                print(f"   - ❌ FAILED processing row {index+1}: {e_row}")
                traceback.print_exc()

    except Exception as e_main:
        print(f"--- ❌ CRITICAL ERROR IN MAIN EXECUTION ---")
        print(e_main)
        traceback.print_exc()
    
    finally:
        # --- 6. Cleanup ---
        print("\n--- Final Cleanup ---")
        def safe_rmtree(path):
            try:
                if os.path.exists(path):
                    shutil.rmtree(path)
                    print(f"🧹 Cleaned up: {path}")
            except Exception as e:
                print(f"   - ⚠️ Warn: Cleanup failed for {path}: {e}")
        
        def safe_remove(path):
            try:
                if os.path.exists(path):
                    os.remove(path)
                    print(f"🧹 Cleaned up: {path}")
            except Exception as e:
                print(f"   - ⚠️ Warn: Cleanup failed for {path}: {e}")

        safe_rmtree(LOCAL_VOICES_DIR)
        safe_rmtree(LOCAL_OUTPUT_DIR)
        safe_rmtree("./tmp_audio")
        if local_creds_path:
            safe_remove(local_creds_path)
            
        gc.collect()
        print("🏁 Script execution finished. 🏁")

if __name__ == "__main__":
    main()
