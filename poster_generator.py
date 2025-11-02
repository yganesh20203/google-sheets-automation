# 1. SETUP: Add all libraries to your requirements.txt file.

# 2. Import all necessary libraries
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps
import requests
import io
import os
import pandas as pd
import math
from datetime import datetime
import numpy as np
import json # Added for loading credentials from a string

# Google Drive Imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Excel Editing Imports
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

# Suppress security warnings for unverified HTTPS requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# ==============================================================================
# 3. COMMON HELPER FUNCTIONS (for Pillow)
# ==============================================================================

# Function to remove white background
def remove_white_background(image, tolerance=20):
    image = image.convert("RGBA")
    datas = image.getdata()
    new_data = []
    for item in datas:
        if item[0] > (255 - tolerance) and item[1] > (255 - tolerance) and item[2] > (255 - tolerance):
            new_data.append((255, 255, 255, 0))
        else:
            new_data.append(item)
    image.putdata(new_data)
    return image

# Function to wrap text
def wrap_text(draw, text, font, max_width):
    lines = []
    if not text:
        return [""]
    bbox = draw.textbbox((0, 0), text, font=font)
    if (bbox[2] - bbox[0]) <= max_width:
        return [text]
    words = text.split(' ')
    current_line = []
    for word in words:
        test_line = ' '.join(current_line + [word])
        bbox = draw.textbbox((0, 0), test_line, font=font)
        if (bbox[2] - bbox[0]) <= max_width:
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
            word_bbox = draw.textbbox((0, 0), word, font=font)
            if (word_bbox[2] - word_bbox[0]) > max_width:
                lines.append(word)
                current_line = []
    if current_line:
        lines.append(' '.join(current_line))
    return lines

# Helper function to create a drop shadow
def create_shadow(image, shadow_offset=(15, 15), shadow_color="#000000", iterations=10):
    shadow = Image.new('RGBA', image.size, (0, 0, 0, 0))
    alpha = image.split()[3]
    shadow_alpha = alpha.filter(ImageFilter.GaussianBlur(iterations))
    shadow_layer = Image.new('RGBA', image.size, shadow_color)
    shadow_layer.putalpha(shadow_alpha)
    shadow.paste(shadow_layer, shadow_offset)
    return Image.alpha_composite(shadow, image)

# Helper function for rounded rectangles with shadows
def draw_rounded_rectangle_with_shadow(draw, xy, radius, fill, shadow_color="#00000040", shadow_offset=(10, 10), blur_radius=15):
    x1, y1, x2, y2 = xy
    shadow_img = Image.new('RGBA', (int(x2-x1+blur_radius*2), int(y2-y1+blur_radius*2)), (0,0,0,0))
    shadow_draw = ImageDraw.Draw(shadow_img)
    shadow_draw.rounded_rectangle((blur_radius, blur_radius, x2-x1+blur_radius, y2-y1+blur_radius), radius=radius, fill=shadow_color)
    shadow_img = shadow_img.filter(ImageFilter.GaussianBlur(blur_radius))
    draw.bitmap((int(x1+shadow_offset[0]-blur_radius), int(y1+shadow_offset[1]-blur_radius)), shadow_img, fill=None)
    draw.rounded_rectangle(xy, radius=radius, fill=fill)
    
# Helper function to explicitly clean the selling price column
def clean_price(price_val):
    try:
        return float(price_val)
    except (ValueError, TypeError):
        return price_val

# Helper function to draw a halftone pattern
def draw_halftone_pattern(draw, width, height, color, step=30, dot_size=3):
    for x in range(0, width, step):
        for y in range(0, height, step):
            draw.ellipse((x, y, x + dot_size, y + dot_size), fill=color)

# ==============================================================================
# 4. GOOGLE DRIVE HELPER FUNCTIONS
# ==============================================================================

SCOPES = ['https://www.googleapis.com/auth/drive']

def authenticate_service_account():
    """Authenticates with Google Drive API using a service account JSON string from an env variable."""
    try:
        creds_json_string = os.environ.get('GCP_SA_KEY')
        if not creds_json_string:
            print("❌ CRITICAL ERROR: 'GCP_SA_KEY' environment variable not set.")
            print("Please ensure this secret is set in your GitHub repository settings.")
            return None
        
        creds_info = json.loads(creds_json_string)
        
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("✅ Google Drive authentication successful.")
        return service
    except json.JSONDecodeError:
        print("❌ CRITICAL ERROR: 'GCP_SA_KEY' is not a valid JSON string.")
        return None
    except Exception as e:
        print(f"❌ Error authenticating with Google Drive: {e}")
        return None

def clear_drive_folder(service, folder_id):
    """Deletes all files and folders within a specific Google Drive folder."""
    print(f"🗑️ Clearing Google Drive folder: {folder_id}...")
    try:
        page_token = None
        while True:
            response = service.files().list(q=f"'{folder_id}' in parents",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
            files = response.get('files', [])
            if not files:
                print("-> Folder is already empty.")
                break
                
            for file in files:
                print(f"    -> Deleting: {file.get('name')} (ID: {file.get('id')})")
                service.files().delete(fileId=file.get('id')).execute()
                
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        print("✅ Google Drive folder cleared.")
    except Exception as e:
        print(f"❌ Error clearing Google Drive folder: {e}")

def find_or_create_folder(service, folder_name, parent_folder_id):
    """Finds a folder by name. If it doesn't exist, creates it."""
    try:
        q = f"'{parent_folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        response = service.files().list(q=q, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        
        if files:
            return files[0].get('id')
        else:
            print(f"    -> Creating GDrive folder: '{folder_name}'")
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')
    except Exception as e:
        print(f"❌ Error finding or creating GDrive folder '{folder_name}': {e}")
        return None

def upload_file_to_drive(service, local_file_path, drive_folder_id, drive_file_name):
    """Uploads a *new* local file to a specific Google Drive folder."""
    try:
        file_metadata = {
            'name': drive_file_name,
            'parents': [drive_folder_id]
        }
        media = MediaFileUpload(local_file_path, mimetype='image/png')
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
    except Exception as e:
        print(f"❌ Error uploading file '{drive_file_name}' to Google Drive: {e}")

# --- NEW HELPER FUNCTIONS ---

def get_file_id_from_folder(service, folder_id, file_name):
    """Finds a file's ID by its name within a specific folder."""
    try:
        q = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
        response = service.files().list(q=q, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        
        if files:
            return files[0].get('id')
        else:
            print(f"    -> File not found in Drive: '{file_name}'")
            return None
    except Exception as e:
        print(f"❌ Error finding file '{file_name}' in Drive: {e}")
        return None

def download_file_from_drive(service, file_id, local_path):
    """Downloads a file from Drive given its file_id."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"    -> Downloading {local_path}: {int(status.progress() * 100)}%.")
        print(f"✅ Download complete: {local_path}")
    except Exception as e:
        print(f"❌ Error downloading file (ID: {file_id}) to '{local_path}': {e}")
        raise # Re-raise the exception to stop the script if a file is missing

def update_file_in_drive(service, local_file_path, file_id, mime_type):
    """Updates an existing file in Google Drive with a new local version."""
    try:
        print(f"    -> Updating file in Drive: {os.path.basename(local_file_path)}")
        media = MediaFileUpload(local_file_path, mimetype=mime_type)
        service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print(f"✅ File update complete: {os.path.basename(local_file_path)}")
    except Exception as e:
        print(f"❌ Error updating file '{os.path.basename(local_file_path)}' in Drive: {e}")

# ==============================================================================
# 5. POSTER FUNCTION 1 (DEFAULT - Orange Theme)
# ==============================================================================

def create_poster_default(image_path, product_name, price, selling_price, discount_percent, logo_path, company_name, location, output_path, log_file_path):
    # --- Poster Configuration ---
    DPI = 300
    WIDTH = int(27 / 2.54 * DPI)
    HEIGHT = int(21 / 2.54 * DPI)
    PADDING = 80
    HEADER_HEIGHT = 250
    
    # --- Color Palette ---
    BACKGROUND_COLOR = "#FFA500"
    SWOOSH_COLOR = "#FFFFFF"
    TEXT_BOX_COLOR = "#FFC300"
    TEXT_COLOR = "#000000"
    BORDER_COLOR = "#FFD700"
    WHITE_COLOR = "#FFFFFF"
    FOOTER_BG_COLOR = "#FFFFFF"
    DESIGN_ACCENT_COLOR = (255, 192, 0, 70)
    BORDER_WIDTH = 30
    
    # --- Create Canvas ---
    poster = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(poster, "RGBA")
    
    # --- Add Modern Halftone Background ---
    draw_halftone_pattern(draw, WIDTH, HEIGHT, DESIGN_ACCENT_COLOR, step=40, dot_size=4)

    # --- Load Fonts ---
    try:
        # Assumes fonts are in the same directory as the logo_path
        font_folder = os.path.dirname(logo_path) 
        oswald_bold_path = os.path.join(font_folder, "Oswald-Bold.ttf")
        lato_black_path = os.path.join(font_folder, "Lato-Black.ttf")
        font_product_name, font_mrp, font_price, font_header_bold, font_discount, font_offer_label, font_big_savings, font_footer, font_upto_offer = [ImageFont.truetype(path, size) for path, size in [(oswald_bold_path, 140), (lato_black_path, 120), (oswald_bold_path, 270), (oswald_bold_path, 85), (oswald_bold_path, 100), (lato_black_path, 80), (oswald_bold_path, 120), (lato_black_path, 40), (oswald_bold_path, 200)]]
        font_b1g1_badge = ImageFont.truetype(oswald_bold_path, 160)
    except IOError as e:
        print(f"Warning: Could not load fonts from {font_folder}. Error: {e}. Using default fonts.")
        font_product_name, font_mrp, font_price, font_header_bold, font_discount, font_offer_label, font_big_savings, font_footer, font_b1g1_badge, font_upto_offer = [ImageFont.load_default()]*10

    # --- Header Bar ---
    draw.rectangle([-20, -20, WIDTH+20, HEADER_HEIGHT], fill=WHITE_COLOR)
    try:
        logo = Image.open(logo_path).convert("RGBA")
        logo.thumbnail((400, 200))
        poster.paste(logo, (PADDING, (HEADER_HEIGHT - logo.height) // 2), logo)
        text_y_offset = 30
        draw.text((WIDTH - PADDING, HEADER_HEIGHT // 2 + text_y_offset), f"{company_name}\n{location}", fill=TEXT_COLOR, font=font_header_bold, align="right", anchor="rm")
    except Exception as e:
        print(f"Warning: Could not load logo: {e}")

    # --- White Swoosh Divider ---
    swoosh_start_x = WIDTH // 2 - 200
    draw.ellipse([swoosh_start_x - WIDTH, HEADER_HEIGHT, swoosh_start_x + WIDTH, HEIGHT * 2], fill=SWOOSH_COLOR)

    # --- Image Placement with Correct Scaling ---
    try:
        if isinstance(image_path, str) and image_path.startswith('http'):
            response = requests.get(image_path, verify=False)
            response.raise_for_status()
            product_image = Image.open(io.BytesIO(response.content))
        else:
            product_image = Image.open(image_path)
        
        product_image = remove_white_background(product_image.convert("RGBA"))
        
        # --- ROBUST IMAGE SIZING LOGIC ---
        max_img_w_area = int(swoosh_start_x * 0.9)
        max_img_h_area = HEIGHT - HEADER_HEIGHT - PADDING * 2
        
        circle_diameter = min(max_img_w_area, max_img_h_area) - 100
        product_container_size = int(circle_diameter * 1.1)

        if product_image.width > product_container_size or product_image.height > product_container_size:
            print(f"-> Shrinking large image...")
            product_image.thumbnail((product_container_size, product_container_size), Image.Resampling.LANCZOS)
        else:
            aspect_ratio = product_image.width / product_image.height
            if aspect_ratio > 1: # Wide image
                new_width = product_container_size
                new_height = int(new_width / aspect_ratio)
            else: # Tall or square image
                new_height = product_container_size
                new_width = int(new_height * aspect_ratio)
            print(f"-> Enlarging small image from {product_image.size} to ({new_width}, {new_height})")
            product_image = product_image.resize((new_width, new_height), Image.Resampling.LANCZOS)

        composite_canvas = Image.new('RGBA', (product_container_size, product_container_size), (0,0,0,0))
        composite_draw = ImageDraw.Draw(composite_canvas)
        
        circle_x = (product_container_size - circle_diameter) // 2
        circle_y = (product_container_size - circle_diameter) // 2
        composite_draw.ellipse((circle_x, circle_y, circle_x + circle_diameter, circle_y + circle_diameter), fill=WHITE_COLOR)

        paste_x = (product_container_size - product_image.width) // 2
        paste_y = (product_container_size - product_image.height) // 2
        composite_canvas.paste(product_image, (paste_x, paste_y), product_image)
        
        final_product_image = composite_canvas
        final_product_image_with_shadow = create_shadow(final_product_image, shadow_offset=(20, 20), iterations=25, shadow_color="#00000080")
        
        img_x = PADDING + (max_img_w_area - final_product_image_with_shadow.width) // 2
        img_y = HEADER_HEIGHT + PADDING + (max_img_h_area - final_product_image_with_shadow.height) // 2
        
        border_thickness = 20
        draw.ellipse((img_x - border_thickness, img_y - border_thickness, 
                        img_x + final_product_image.width + border_thickness, 
                        img_y + final_product_image.height + border_thickness), 
                        fill=BACKGROUND_COLOR)
        
        poster.paste(final_product_image_with_shadow, (img_x, img_y), final_product_image_with_shadow)
        image_area_right_boundary = img_x + final_product_image_with_shadow.width

    except Exception as e:
        print(f"Error loading product image: {e}. Using a placeholder.")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - FAILED - Product: '{product_name}', Image Path: '{image_path}', Error: {e}\n"
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(log_message)
        
        placeholder_rect = [PADDING, HEADER_HEIGHT + PADDING, swoosh_start_x - PADDING, HEIGHT - PADDING]
        draw.rectangle(placeholder_rect, fill="#CCCCCC")
        draw.text(((placeholder_rect[0] + placeholder_rect[2]) // 2, (placeholder_rect[1] + placeholder_rect[3]) // 2), "Image\nNot Found", font=font_offer_label, fill=TEXT_COLOR, anchor="mm", align="center")
        image_area_right_boundary = placeholder_rect[2]

    is_upto_offer = isinstance(selling_price, str) and 'upto' in str(selling_price).lower()
    is_b1g1_offer = isinstance(discount_percent, str) and 'b1g1' in str(discount_percent).lower()
    is_flash_sale_price = isinstance(selling_price, str) and 'flash sale' in str(selling_price).lower()
    is_numeric_price = isinstance(selling_price, (int, float))

    if not is_upto_offer:
        badge_radius, badge_center = 250, (400, HEADER_HEIGHT + 350)
        if is_b1g1_offer:
            badge_text, badge_font = ("B1G1", font_b1g1_badge)
        else:
            try:
                badge_text = f"{int(float(discount_percent))}%\nOFF"
            except (ValueError, TypeError):
                badge_text = str(discount_percent)
            badge_font = font_discount

        num_points, shadow_offset = 16, (10, 10)
        star_points, shadow_points = [], []
        for i in range(num_points * 2):
            r = badge_radius if i % 2 == 0 else badge_radius * 0.8
            angle = i * math.pi / num_points
            x, y = (badge_center[0] + r * math.sin(angle), badge_center[1] + r * math.cos(angle))
            star_points.append((x, y))
            shadow_points.append((x + shadow_offset[0], y + shadow_offset[1]))
        draw.polygon(shadow_points, fill="#00000050")
        draw.polygon(star_points, fill=TEXT_BOX_COLOR)
        draw.text(badge_center, badge_text, fill=TEXT_COLOR, font=badge_font, anchor="mm", align="center")

    MIN_GAP = 60
    text_panel_start_x = max(image_area_right_boundary + MIN_GAP, swoosh_start_x + 200)
    text_area_width = WIDTH - text_panel_start_x - PADDING
    text_panel_center_x = text_panel_start_x + text_area_width // 2
    
    product_name_y_start = HEADER_HEIGHT + PADDING * 1.5
    wrapped_name = wrap_text(draw, str(product_name), font_product_name, text_area_width)
    total_text_height, line_spacing = 0, 20
    for i, line in enumerate(wrapped_name):
        line_bbox = draw.textbbox((0, 0), line, font=font_product_name)
        total_text_height += (line_bbox[3] - line_bbox[1]) + (line_spacing if i < len(wrapped_name) - 1 else 0)

    box_padding_y, box_padding_x = 60, 60
    name_box_y2 = product_name_y_start + total_text_height + 2 * box_padding_y
    draw_rounded_rectangle_with_shadow(draw, (text_panel_start_x, product_name_y_start, WIDTH - PADDING, name_box_y2), radius=30, fill=TEXT_BOX_COLOR)
    
    current_y = product_name_y_start + box_padding_y
    for line in wrapped_name:
        line_bbox = draw.textbbox((0, 0), line, font=font_product_name)
        draw.text((text_panel_center_x, current_y), line, fill=TEXT_COLOR, font=font_product_name, anchor="mt")
        current_y += (line_bbox[3] - line_bbox[1]) + line_spacing

    bottom_y = HEIGHT - PADDING * 2 - 50
    mrp_text = f"MRP: Rs {price:,.2f}"
    draw.text((text_panel_center_x, bottom_y), mrp_text, fill=TEXT_COLOR, font=font_mrp, anchor="ms")
    mrp_bbox = draw.textbbox((text_panel_center_x, bottom_y), mrp_text, font=font_mrp, anchor="ms")
    
    if not is_b1g1_offer and not is_upto_offer and is_numeric_price:
        line_y = (mrp_bbox[1] + mrp_bbox[3]) // 2
        draw.line([(mrp_bbox[0], line_y), (mrp_bbox[2], line_y)], fill=TEXT_COLOR, width=6)

    price_box_y2 = mrp_bbox[1] - PADDING
    
    if is_b1g1_offer:
        offer_price_text, offer_price_font, offer_label_text = "Buy 1 Get 1", font_big_savings, "Special Offer"
    elif not is_numeric_price:
        offer_price_text, offer_price_font, offer_label_text = str(selling_price).upper(), font_price, "Special Offer"
    else:
        formatted_price = f"{selling_price:,g}"
        offer_price_text, offer_price_font, offer_label_text = f"Rs {formatted_price}/-", font_price, "Offer Price"
    
    vertical_spacing = 50
    price_bbox_calc = draw.textbbox((0,0), offer_price_text, font=offer_price_font)
    price_height = price_bbox_calc[3] - price_bbox_calc[1]
    label_bbox_calc = draw.textbbox((0,0), "Big Savings !!", font=font_big_savings)
    label_height = label_bbox_calc[3] - label_bbox_calc[1]
    price_box_y1 = price_box_y2 - (price_height + label_height + vertical_spacing + 2 * box_padding_y)
    
    draw_rounded_rectangle_with_shadow(draw, (text_panel_start_x, price_box_y1, WIDTH - PADDING, price_box_y2), radius=30, fill=TEXT_BOX_COLOR)
    
    price_y = price_box_y2 - box_padding_y
    draw.text((text_panel_center_x, price_y), offer_price_text, fill=TEXT_COLOR, font=offer_price_font, anchor="ms")
    price_bbox_on_canvas = draw.textbbox((text_panel_center_x, price_y), offer_price_text, font=offer_price_font, anchor="ms")
    label_y = price_bbox_on_canvas[1] - vertical_spacing
    draw.text((text_panel_start_x + box_padding_x, label_y), "Big Savings !!", fill=TEXT_COLOR, font=font_big_savings, anchor="ls")
    draw.text((WIDTH - PADDING - box_padding_x, label_y), offer_label_text, fill=TEXT_COLOR, font=font_offer_label, anchor="rs")
    
    FOOTER_BAR_HEIGHT = 80
    FOOTER_Y_OFFSET = 20
    
    footer_start_y = HEIGHT - FOOTER_BAR_HEIGHT - FOOTER_Y_OFFSET
    footer_end_y = HEIGHT - FOOTER_Y_OFFSET
    footer_text_y = footer_start_y + (FOOTER_BAR_HEIGHT // 2)
    
    draw.rectangle([0, footer_start_y, WIDTH, footer_end_y], fill=FOOTER_BG_COLOR)
    footer_text = "Offers applicable on selected range. T&C apply."
    
    draw.line([(0, footer_start_y), (WIDTH, footer_start_y)], fill="#DDDDDD", width=3)
    draw.text((PADDING, footer_text_y), footer_text, fill=TEXT_COLOR, font=font_footer, anchor="lm")
    
    draw.rectangle([0, 0, WIDTH-1, HEIGHT-1], outline=BORDER_COLOR, width=BORDER_WIDTH)

    poster.save(output_path)


# ==============================================================================
# 6. POSTER FUNCTION 2 (SPECIAL STORES - Muted/Red Theme)
# ==============================================================================

def create_poster_special_stores(image_path, product_name, price, selling_price, discount_percent, logo_path, company_name, location, output_path, log_file_path):
    # --- Poster Configuration ---
    DPI = 300
    WIDTH = int(27 / 2.54 * DPI)
    HEIGHT = int(21 / 2.54 * DPI)
    PADDING = 80
    HEADER_HEIGHT = 250
    
    # --- Color Palette ---
    BACKGROUND_COLOR = "#FFF8E1" 
    SWOOSH_COLOR = "#E0F7FA"      
    PRICE_BOX_COLOR = "#fa9b0c"   
    NAME_BOX_COLOR = "#E0F7FA"    
    TEXT_COLOR = "#000000"        
    BORDER_COLOR = "#1C4E80"      
    WHITE_COLOR = "#FFFFFF"       
    FOOTER_BG_COLOR = "#FFFFFF"
    DESIGN_ACCENT_COLOR = (224, 247, 250, 150)
    BORDER_WIDTH = 30
    
    # --- Create Canvas ---
    poster = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(poster, "RGBA")
    
    # --- Add Modern Halftone Background ---
    draw_halftone_pattern(draw, WIDTH, HEIGHT, DESIGN_ACCENT_COLOR, step=40, dot_size=4)

    # --- Load Fonts ---
    try:
        font_folder = os.path.dirname(logo_path) 
        oswald_bold_path = os.path.join(font_folder, "Oswald-Bold.ttf")
        lato_black_path = os.path.join(font_folder, "Lato-Black.ttf")
        try:
            font_product_name, font_mrp, font_price, font_header_bold, font_discount, font_offer_label, font_big_savings, font_footer, font_upto_offer = [
                ImageFont.truetype(path, size) for path, size in [
                    (oswald_bold_path, 140), (lato_black_path, 120), (oswald_bold_path, 240), 
                    (oswald_bold_path, 85), (oswald_bold_path, 100), (lato_black_path, 80), 
                    (oswald_bold_path, 120), (lato_black_path, 40), (oswald_bold_path, 200)
                ]
            ]
            font_b1g1_badge = ImageFont.truetype(oswald_bold_path, 160)
        except IOError:
            print("Warning: Custom fonts not found. Using default fonts.")
            font_product_name, font_mrp, font_price, font_header_bold, font_discount, font_offer_label, font_big_savings, font_footer, font_b1g1_badge, font_upto_offer = [ImageFont.load_default()]*10

    except Exception as e:
        print(f"Error during font setup: {e}. Using default fonts.")
        font_product_name, font_mrp, font_price, font_header_bold, font_discount, font_offer_label, font_big_savings, font_footer, font_b1g1_badge, font_upto_offer = [ImageFont.load_default()]*10

    # --- Header Bar ---
    draw.rectangle([-20, -20, WIDTH+20, HEADER_HEIGHT], fill=WHITE_COLOR)
    try:
        logo = Image.open(logo_path).convert("RGBA")
        logo.thumbnail((400, 200))
        poster.paste(logo, (PADDING, (HEADER_HEIGHT - logo.height) // 2), logo)
        text_y_offset = 30
        draw.text((WIDTH - PADDING, HEADER_HEIGHT // 2 + text_y_offset), f"{company_name}\n{location}", fill=TEXT_COLOR, font=font_header_bold, align="right", anchor="rm")
    except Exception as e:
        print(f"Warning: Could not load logo: {e}")

    # --- Swoosh Divider ---
    swoosh_start_x = WIDTH // 2 - 200
    draw.ellipse([swoosh_start_x - WIDTH, HEADER_HEIGHT, swoosh_start_x + WIDTH, HEIGHT * 2], fill=SWOOSH_COLOR)

    # --- Image Placement with Correct Scaling ---
    try:
        if isinstance(image_path, str) and image_path.startswith('http'):
            response = requests.get(image_path, verify=False)
            response.raise_for_status()
            product_image = Image.open(io.BytesIO(response.content))
        else:
            product_image = Image.open(image_path)
        
        product_image = remove_white_background(product_image.convert("RGBA"))
        
        max_img_w_area = int(swoosh_start_x * 0.9)
        max_img_h_area = HEIGHT - HEADER_HEIGHT - PADDING * 2
        
        circle_diameter = min(max_img_w_area, max_img_h_area) - 100
        product_container_size = int(circle_diameter * 1.1)

        if product_image.width > product_container_size or product_image.height > product_container_size:
            print(f"-> Shrinking large image...")
            product_image.thumbnail((product_container_size, product_container_size), Image.Resampling.LANCZOS)
        else:
            aspect_ratio = product_image.width / product_image.height
            if aspect_ratio > 1:
                new_width = product_container_size
                new_height = int(new_width / aspect_ratio)
            else:
                new_height = product_container_size
                new_width = int(new_height * aspect_ratio)
            print(f"-> Enlarging small image from {product_image.size} to ({new_width}, {new_height})")
            product_image = product_image.resize((new_width, new_height), Image.Resampling.LANCZOS)

        composite_canvas = Image.new('RGBA', (product_container_size, product_container_size), (0,0,0,0))
        composite_draw = ImageDraw.Draw(composite_canvas)
        
        circle_x = (product_container_size - circle_diameter) // 2
        circle_y = (product_container_size - circle_diameter) // 2
        composite_draw.ellipse((circle_x, circle_y, circle_x + circle_diameter, circle_y + circle_diameter), fill=WHITE_COLOR)

        paste_x = (product_container_size - product_image.width) // 2
        paste_y = (product_container_size - product_image.height) // 2
        composite_canvas.paste(product_image, (paste_x, paste_y), product_image)
        
        final_product_image = composite_canvas
        final_product_image_with_shadow = create_shadow(final_product_image, shadow_offset=(20, 20), iterations=25, shadow_color="#00000080")
        
        img_x = PADDING + (max_img_w_area - final_product_image_with_shadow.width) // 2
        img_y = HEADER_HEIGHT + PADDING + (max_img_h_area - final_product_image_with_shadow.height) // 2
        
        border_thickness = 20
        draw.ellipse((img_x - border_thickness, img_y - border_thickness, 
                        img_x + final_product_image.width + border_thickness, 
                        img_y + final_product_image.height + border_thickness), 
                        fill=BACKGROUND_COLOR)
        
        poster.paste(final_product_image_with_shadow, (img_x, img_y), final_product_image_with_shadow)
        image_area_right_boundary = img_x + final_product_image_with_shadow.width

    except Exception as e:
        print(f"Error loading product image: {e}. Using a placeholder.")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - FAILED - Product: '{product_name}', Image Path: '{image_path}', Error: {e}\n"
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(log_message)
            
        placeholder_rect = [PADDING, HEADER_HEIGHT + PADDING, swoosh_start_x - PADDING, HEIGHT - PADDING]
        draw.rectangle(placeholder_rect, fill="#CCCCCC")
        draw.text(((placeholder_rect[0] + placeholder_rect[2]) // 2, (placeholder_rect[1] + placeholder_rect[3]) // 2), "Image\nNot Found", font=font_offer_label, fill=TEXT_COLOR, anchor="mm", align="center")
        image_area_right_boundary = placeholder_rect[2]

    is_upto_offer = isinstance(selling_price, str) and 'upto' in str(selling_price).lower()
    is_b1g1_offer = isinstance(discount_percent, str) and 'b1g1' in str(discount_percent).lower()
    is_numeric_price = isinstance(selling_price, (int, float))

    if not is_upto_offer:
        badge_radius, badge_center = 250, (400, HEADER_HEIGHT + 350)
        if is_b1g1_offer:
            badge_text, badge_font = ("B1G1", font_b1g1_badge)
        else:
            try:
                if isinstance(discount_percent, (int, float)):
                    badge_text = f"{int(float(discount_percent))}%\nOFF"
                else:
                    badge_text = str(discount_percent)
            except (ValueError, TypeError):
                badge_text = str(discount_percent)
            badge_font = font_discount

        num_points, shadow_offset = 16, (10, 10)
        star_points, shadow_points = [], []
        for i in range(num_points * 2):
            r = badge_radius if i % 2 == 0 else badge_radius * 0.8
            angle = i * math.pi / num_points
            x, y = (badge_center[0] + r * math.sin(angle), badge_center[1] + r * math.cos(angle))
            star_points.append((x, y))
            shadow_points.append((x + shadow_offset[0], y + shadow_offset[1]))
        draw.polygon(shadow_points, fill="#00000050")
        draw.polygon(star_points, fill=PRICE_BOX_COLOR)
        draw.text(badge_center, badge_text, fill=WHITE_COLOR, font=badge_font, anchor="mm", align="center")

    MIN_GAP = 60
    text_panel_start_x = max(image_area_right_boundary + MIN_GAP, swoosh_start_x + 200)
    text_area_width = WIDTH - text_panel_start_x - PADDING
    text_panel_center_x = text_panel_start_x + text_area_width // 2
    
    product_name_y_start = HEADER_HEIGHT + PADDING * 1.5
    wrapped_name = wrap_text(draw, str(product_name), font_product_name, text_area_width)
    total_text_height, line_spacing = 0, 20
    for i, line in enumerate(wrapped_name):
        line_bbox = draw.textbbox((0, 0), line, font=font_product_name)
        total_text_height += (line_bbox[3] - line_bbox[1]) + (line_spacing if i < len(wrapped_name) - 1 else 0)

    box_padding_y, box_padding_x = 60, 60
    name_box_y2 = product_name_y_start + total_text_height + 2 * box_padding_y
    draw_rounded_rectangle_with_shadow(draw, (text_panel_start_x, product_name_y_start, WIDTH - PADDING, name_box_y2), radius=30, fill=NAME_BOX_COLOR)
    
    current_y = product_name_y_start + box_padding_y
    for line in wrapped_name:
        line_bbox = draw.textbbox((0, 0), line, font=font_product_name)
        draw.text((text_panel_center_x, current_y), line, fill=TEXT_COLOR, font=font_product_name, anchor="mt")
        current_y += (line_bbox[3] - line_bbox[1]) + line_spacing

    bottom_y = HEIGHT - PADDING * 2 - 50
    mrp_text = f"MRP: Rs {price:,.2f}"
    draw.text((text_panel_center_x, bottom_y), mrp_text, fill=TEXT_COLOR, font=font_mrp, anchor="ms")
    mrp_bbox = draw.textbbox((text_panel_center_x, bottom_y), mrp_text, font=font_mrp, anchor="ms")
    
    if not is_b1g1_offer and not is_upto_offer and is_numeric_price:
        line_y = (mrp_bbox[1] + mrp_bbox[3]) // 2
        draw.line([(mrp_bbox[0], line_y), (mrp_bbox[2], line_y)], fill=TEXT_COLOR, width=6)

    price_box_y2 = mrp_bbox[1] - PADDING
    
    if is_b1g1_offer:
        offer_price_text, offer_price_font, offer_label_text = "Buy 1 Get 1", font_big_savings, "Special Offer"
    elif not is_numeric_price:
        price_str = str(selling_price).upper()
        offer_price_text = price_str
        offer_price_font = font_price
        offer_label_text = "Special Offer"
    else:
        formatted_price = f"{selling_price:,g}"
        offer_price_text, offer_price_font, offer_label_text = f"Rs {formatted_price}/-", font_price, "Offer Price"
    
    vertical_spacing = 50
    price_bbox_calc = draw.textbbox((0,0), offer_price_text, font=offer_price_font)
    price_height = price_bbox_calc[3] - price_bbox_calc[1]
    label_bbox_calc = draw.textbbox((0,0), "Big Savings !!", font=font_big_savings)
    label_height = label_bbox_calc[3] - label_bbox_calc[1]
    price_box_y1 = price_box_y2 - (price_height + label_height + vertical_spacing + 2 * box_padding_y)
    
    draw_rounded_rectangle_with_shadow(draw, (text_panel_start_x, price_box_y1, WIDTH - PADDING, price_box_y2), radius=30, fill=PRICE_BOX_COLOR)
    
    price_y = price_box_y2 - box_padding_y
    draw.text((text_panel_center_x, price_y), offer_price_text, fill=WHITE_COLOR, font=font_price, anchor="ms")
    price_bbox_on_canvas = draw.textbbox((text_panel_center_x, price_y), offer_price_text, font=font_price, anchor="ms")
    label_y = price_bbox_on_canvas[1] - vertical_spacing
    draw.text((text_panel_start_x + box_padding_x, label_y), "Big Savings !!", fill=WHITE_COLOR, font=font_big_savings, anchor="ls")
    draw.text((WIDTH - PADDING - box_padding_x, label_y), offer_label_text, fill=WHITE_COLOR, font=font_offer_label, anchor="rs")
    
    FOOTER_BAR_HEIGHT = 80
    FOOTER_Y_OFFSET = 20
    
    footer_start_y = HEIGHT - FOOTER_BAR_HEIGHT - FOOTER_Y_OFFSET
    footer_end_y = HEIGHT - FOOTER_Y_OFFSET
    footer_text_y = footer_start_y + (FOOTER_BAR_HEIGHT // 2)
    
    draw.rectangle([0, footer_start_y, WIDTH, footer_end_y], fill=FOOTER_BG_COLOR)
    footer_text = "Offers applicable on selected range. T&C apply."
    
    draw.line([(0, footer_start_y), (WIDTH, footer_start_y)], fill="#DDDDDD", width=3)
    draw.text((PADDING, footer_text_y), footer_text, fill=TEXT_COLOR, font=font_footer, anchor="lm")
    
    draw.rectangle([0, 0, WIDTH-1, HEIGHT-1], outline=BORDER_COLOR, width=BORDER_WIDTH)

    poster.save(output_path)


# ==============================================================================
# 7. MAIN SCRIPT EXECUTION
# ==============================================================================

def main():
    try:
        # --- PATH CONFIGURATION ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Output folder will store logs, temp files, and downloaded inputs
        output_folder_path = os.path.join(script_dir, 'output')
        
        os.makedirs(output_folder_path, exist_ok=True)
        print(f"Using local output/temp folder: {output_folder_path}")
        
        # --- GOOGLE DRIVE CONFIGURATION ---
        # *** THIS IS YOUR NEW DATA FOLDER ***
        DATA_FOLDER_ID = '1J2epmcfA8hT8YFk4Q7G9LM3qLZzw3W_H'
        
        # PARENT_DRIVE_FOLDER_ID is for *poster uploads*
        PARENT_DRIVE_FOLDER_ID = os.environ.get('PARENT_DRIVE_FOLDER_ID')
        if not PARENT_DRIVE_FOLDER_ID:
            print("❌ CRITICAL ERROR: 'PARENT_DRIVE_FOLDER_ID' environment variable not set.")
            print("Please set this in your GitHub repository secrets or variables.")
            exit()
            
        # --- File Names in Google Drive ---
        # These *must* match the names in your Drive folder
        FILE_CONFIG = {
            'poster_raw_data.xlsx': {'id': None, 'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
            'offer_articles.csv': {'id': None, 'mime': 'text/csv'},
            'check_offer.xlsx': {'id': None, 'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
            'product_images_1.xlsx': {'id': None, 'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
            'logo_1.png': {'id': None, 'mime': 'image/png'},
            'Oswald-Bold.ttf': {'id': None, 'mime': 'font/ttf'},
            'Lato-Black.ttf': {'id': None, 'mime': 'font/ttf'},
        }

        # --- Local File Paths (will be in the 'output' folder) ---
        poster_raw_data_path = os.path.join(output_folder_path, 'poster_raw_data.xlsx')
        offer_articles_csv_path = os.path.join(output_folder_path, 'offer_articles.csv')
        check_offer_excel_path = os.path.join(output_folder_path, 'check_offer.xlsx')
        product_images_path = os.path.join(output_folder_path, 'product_images_1.xlsx')
        logo_path = os.path.join(output_folder_path, 'logo_1.png')
        # Fonts are also downloaded to output_folder_path
        
        # Log File Paths
        log_file_path = os.path.join(output_folder_path, 'failed_images.log')
        audit_log_path = os.path.join(output_folder_path, 'price_comparison_audit_log.xlsx')
        
        # --- STORE CONFIGURATION ---
        special_store_list = [
            'Flipkart Wholesale Amritsar', 
            'Flipkart Wholesale Jammu', 
            'Flipkart Wholesale Vizag'
        ]

        # ======================================================
        # --- 0. AUTHENTICATE AND DOWNLOAD INPUT FILES ---
        # ======================================================
        print("\n--- Starting Google Drive Authentication & Download ---")
        
        drive_service = authenticate_service_account()
        if not drive_service:
            print("❌ Halting script due to Google Drive authentication failure.")
            exit()

        print(f"Downloading all input files from Drive Folder ID: {DATA_FOLDER_ID}...")
        for file_name in FILE_CONFIG.keys():
            print(f"Searching for file: {file_name}")
            file_id = get_file_id_from_folder(drive_service, DATA_FOLDER_ID, file_name)
            if file_id:
                FILE_CONFIG[file_name]['id'] = file_id
                local_path = os.path.join(output_folder_path, file_name)
                download_file_from_drive(drive_service, file_id, local_path)
            else:
                print(f"❌ CRITICAL ERROR: File '{file_name}' not found in Google Drive folder.")
                print("Please ensure all 7 required files are in the folder and named correctly.")
                exit()
        
        print("✅ All input files downloaded successfully.")

        # ======================================================
        # --- 1. PRE-PROCESSING STEP ---
        # ======================================================
        print("\n--- Starting Pre-processing Step ---")
        
        print(f"Loading '{poster_raw_data_path}'...")
        raw_data_df = pd.read_excel(poster_raw_data_path, engine='openpyxl')
        
        print("Cleaning keys for raw data file...")
        store_key_raw = pd.to_numeric(raw_data_df['STORE'], errors='coerce').fillna(0).astype(int).astype(str)
        article_key_raw = pd.to_numeric(raw_data_df['ARTICLE_NUMBER'], errors='coerce').fillna(0).astype(int).astype(str)
        raw_data_df['key'] = store_key_raw + article_key_raw
        
        raw_data_df = raw_data_df[['key', 'SELLING_PRICE', 'mrp', 'OnHand_QTY']]
        raw_data_df = raw_data_df.rename(columns={'SELLING_PRICE': 'Raw_SELLING_PRICE', 'mrp': 'Raw_mrp', 'OnHand_QTY': 'Raw_OnHand_QTY'})

        print(f"Loading '{offer_articles_csv_path}'...")
        offer_articles_df = pd.read_csv(offer_articles_csv_path)
        original_csv_columns = offer_articles_df.columns.tolist()

        print("Cleaning keys for offer articles file...")
        store_key_offer = pd.to_numeric(offer_articles_df['Store Code'], errors='coerce').fillna(0).astype(int).astype(str)
        article_key_offer = pd.to_numeric(offer_articles_df['Article No.'], errors='coerce').fillna(0).astype(int).astype(str)
        offer_articles_df['key'] = store_key_offer + article_key_offer
        
        print("Merging raw data with offer articles...")
        merged_offers_df = pd.merge(offer_articles_df, raw_data_df, on='key', how='left')

        matches_found = merged_offers_df['Raw_mrp'].notna().sum()
        print(f"-> SUCCESS: Found {matches_found} matching rows between the two files.")
        if matches_found == 0:
            print("-> WARNING: No matches found. Check 'key' columns in 'price_comparison_audit_log.xlsx' for issues.")

        print("Comparing prices to find discrepancies...")
        
        original_mrp_numeric = pd.to_numeric(merged_offers_df['current mrp'], errors='coerce')
        raw_mrp_numeric = pd.to_numeric(merged_offers_df['Raw_mrp'], errors='coerce')
        original_sp_numeric = pd.to_numeric(merged_offers_df['selling price'], errors='coerce')
        raw_sp_numeric = pd.to_numeric(merged_offers_df['Raw_SELLING_PRICE'], errors='coerce')

        mrp_match = (original_mrp_numeric == raw_mrp_numeric)
        sp_match = (original_sp_numeric == raw_sp_numeric)
        
        merged_offers_df['check_flag'] = mrp_match & sp_match
        merged_offers_df['mrp_match'] = mrp_match
        merged_offers_df['sp_match'] = sp_match
        
        print(f"Saving comparison details to '{audit_log_path}'...")
        try:
            audit_columns = [
                'Store Code', 'Article No.', 'Article name', 'key',
                'current mrp', 'Raw_mrp', 'mrp_match',
                'selling price', 'Raw_SELLING_PRICE', 'sp_match',
                'check_flag'
            ]
            for col in audit_columns:
                if col not in merged_offers_df.columns:
                    merged_offers_df[col] = pd.NA
                    
            audit_df = merged_offers_df[audit_columns]
            audit_df.to_excel(audit_log_path, index=False, engine='openpyxl')
            print("✅ Comparison audit log saved.")
        except Exception as e:
            print(f"❌ Warning: Could not save audit log. Error: {e}")

        mismatched_rows_df = merged_offers_df[merged_offers_df['check_flag'] == False].copy()
        print(f"Found {len(mismatched_rows_df)} rows with price mismatches or missing raw data.")

        print(f"Loading headers from '{check_offer_excel_path}'...")
        with pd.ExcelFile(check_offer_excel_path, engine='openpyxl') as xls:
            original_df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], nrows=0)
            original_check_offer_headers = original_df.columns.tolist()
            
        print("Calculating new discount percentages for mismatched rows...")
        new_mrp = pd.to_numeric(mismatched_rows_df['Raw_mrp'], errors='coerce')
        new_sp = pd.to_numeric(mismatched_rows_df['Raw_SELLING_PRICE'], errors='coerce')

        discount_decimal = np.where(
            (new_mrp.notna()) & (new_sp.notna()) & (new_mrp > 0),
            (new_mrp - new_sp) / new_mrp, 0
        )
        discount_percentage_whole = np.round(discount_decimal * 100).astype(int)
        
        original_discount_is_numeric = pd.to_numeric(mismatched_rows_df['discount %'], errors='coerce').notna()

        mismatched_rows_df['discount %'] = np.where(
            original_discount_is_numeric,
            discount_percentage_whole,
            mismatched_rows_df['discount %']
        )
        
        mismatched_rows_df['current mrp'] = mismatched_rows_df['Raw_mrp']
        mismatched_rows_df['selling price'] = mismatched_rows_df['Raw_SELLING_PRICE']

        final_df_to_write = mismatched_rows_df.reindex(columns=original_check_offer_headers)
        
        # --- Rewrite 'check_offer.xlsx' LOCALLY ---
        print(f"Updating '{check_offer_excel_path}' locally with {len(final_df_to_write)} mismatched rows...")
        book = openpyxl.load_workbook(check_offer_excel_path)
        sheet = book.active
        sheet.delete_rows(2, sheet.max_row)
        for r in dataframe_to_rows(final_df_to_write, index=False, header=False):
            sheet.append(r)
        book.save(check_offer_excel_path)
        print(f"✅ Successfully updated '{check_offer_excel_path}' locally.")
        
        # --- *** NEW: UPLOAD 'check_offer.xlsx' back to Drive *** ---
        update_file_in_drive(drive_service, 
                             check_offer_excel_path, 
                             FILE_CONFIG['check_offer.xlsx']['id'], 
                             FILE_CONFIG['check_offer.xlsx']['mime'])

        # --- *** NEW: UPDATE AND OVERWRITE 'offer_articles.csv' *** ---
        print(f"Updating source file '{offer_articles_csv_path}' with new prices...")
        
        full_new_mrp = pd.to_numeric(merged_offers_df['Raw_mrp'], errors='coerce')
        full_new_sp = pd.to_numeric(merged_offers_df['Raw_SELLING_PRICE'], errors='coerce')
        
        full_discount_decimal = np.where(
            (full_new_mrp.notna()) & (full_new_sp.notna()) & (full_new_mrp > 0),
            (full_new_mrp - full_new_sp) / full_new_mrp, 0
        )
        full_discount_percentage_whole = np.round(full_discount_decimal * 100).astype(int)
        
        merged_offers_df['new_discount_pct'] = full_discount_percentage_whole
        full_original_discount_is_numeric = pd.to_numeric(merged_offers_df['discount %'], errors='coerce').notna()
        
        merged_offers_df['current mrp'] = np.where(
            merged_offers_df['check_flag'] == False,
            merged_offers_df['Raw_mrp'],
            merged_offers_df['current mrp']
        )
        merged_offers_df['selling price'] = np.where(
            merged_offers_df['check_flag'] == False,
            merged_offers_df['Raw_SELLING_PRICE'],
            merged_offers_df['selling price']
        )
        merged_offers_df['discount %'] = np.where(
            (merged_offers_df['check_flag'] == False) & (full_original_discount_is_numeric),
            merged_offers_df['new_discount_pct'],
            merged_offers_df['discount %']
        )
        
        final_csv_df = merged_offers_df[original_csv_columns]
        
        # Save back to the CSV LOCALLY
        final_csv_df.to_csv(offer_articles_csv_path, index=False)
        print(f"✅ Successfully overwrote '{offer_articles_csv_path}' locally.")
        
        # --- *** NEW: UPLOAD 'offer_articles.csv' back to Drive *** ---
        update_file_in_drive(drive_service, 
                             offer_articles_csv_path, 
                             FILE_CONFIG['offer_articles.csv']['id'], 
                             FILE_CONFIG['offer_articles.csv']['mime'])
        
        print("--- Pre-processing Step Complete ---")

        # ======================================================
        # --- 2. POSTER GENERATION ---
        # ======================================================
        print("\n--- Starting Poster Generation Step ---")

        # --- Clear POSTER (Output) Google Drive Folder ---
        clear_drive_folder(drive_service, PARENT_DRIVE_FOLDER_ID)

        # --- Load the UPDATED 'check_offer.xlsx' file (from local copy) ---
        print(f"\nReading UPDATED '{check_offer_excel_path}' for poster generation...")
        offer_articles_df_for_posters = pd.read_excel(check_offer_excel_path, header=0, engine='openpyxl')
        
        if offer_articles_df_for_posters.empty:
            print("✅ 'check_offer.xlsx' is empty. No price mismatches found. No posters to generate.")
            print("--- Poster Generation Complete ---")
            exit() 

        offer_articles_df_for_posters['selling price'] = offer_articles_df_for_posters['selling price'].apply(clean_price)

        print("\nReading product images Excel file (from local copy)...")
        product_images_df = pd.read_excel(product_images_path, header=0, usecols=['Article No.', 'Image Link'], engine='openpyxl')
        
        print("\nMerging the two dataframes on 'Article No.'...")
        merged_df_for_posters = pd.merge(offer_articles_df_for_posters, product_images_df, on='Article No.', how='left')
        print("-> Merge complete. Starting poster generation...")

        company = "Best Price"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # --- Main Generation Loop ---
        for index, row in merged_df_for_posters.iterrows():
            try:
                store_location = row['Storename']
                
                safe_store_name = "".join([c for c in str(store_location) if c.isalnum() or c in (' ', '-')]).rstrip()
                # Local output folder for this store
                store_folder_path = os.path.join(output_folder_path, safe_store_name)
                os.makedirs(store_folder_path, exist_ok=True)
                
                safe_product_name = "".join([c for c in str(row['Article name']) if c.isalnum() or c == ' ']).rstrip()
                
                if pd.isna(row['Image Link']) or str(row['Image Link']).strip() == "":
                    print(f"\nSKIPPING poster for: '{row['Article name']}' (Article No: {row['Article No.']}) - No Image Link found.")
                    log_message = f"{timestamp} - FAILED - Product: '{row['Article name']}', Article No: {row['Article No.']}, Error: Image Link not found in 'product_images_1.xlsx'.\n"
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_message)
                    continue
                
                print(f"\nGenerating poster for: '{row['Article name']}' for store '{store_location}'")
                
                if store_location in special_store_list:
                    print("-> Using SPECIAL (Muted) template.")
                    filename = f"{safe_product_name}_Muted.png"
                    output_filepath = os.path.join(store_folder_path, filename)
                    
                    create_poster_special_stores(
                        image_path=row['Image Link'], 
                        product_name=row['Article name'], 
                        price=row['current mrp'],
                        selling_price=row['selling price'],
                        discount_percent=row['discount %'],
                        logo_path=logo_path, # This is the local path in 'output/'
                        company_name=company,
                        location=store_location,
                        output_path=output_filepath,
                        log_file_path=log_file_path
                    )
                else:
                    print("-> Using DEFAULT (Orange) template.")
                    filename = f"{safe_product_name}_Default.png"
                    output_filepath = os.path.join(store_folder_path, filename)
                    
                    create_poster_default(
                        image_path=row['Image Link'], 
                        product_name=row['Article name'], 
                        price=row['current mrp'],
                        selling_price=row['selling price'],
                        discount_percent=row['discount %'],
                        logo_path=logo_path, # This is the local path in 'output/'
                        company_name=company,
                        location=store_location,
                        output_path=output_filepath,
                        log_file_path=log_file_path
                    )
                
                print(f"-> Saved locally: {output_filepath}")

                # --- Upload to Google Drive ---
                print(f"-> Uploading '{filename}' to Google Drive...")
                drive_store_folder_id = find_or_create_folder(drive_service, safe_store_name, PARENT_DRIVE_FOLDER_ID)
                
                if drive_store_folder_id:
                    upload_file_to_drive(drive_service, output_filepath, drive_store_folder_id, filename)
                    print("-> Upload complete.")
                else:
                    print(f"-> ❌ SKIPPING UPLOAD: Could not create/find GDrive folder for '{safe_store_name}'.")

            except Exception as e:
                print(f"❌ An unexpected error occurred for row {index} ({row.get('Article name')}): {e}")
                log_message = f"{timestamp} - FAILED - Product: '{row.get('Article name')}', Article No: {row.get('Article No.')}, Error: {e}\n"
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(log_message)
                continue
                
        print("\n✅ All posters have been processed.")
        
        if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
            print(f"\n⚠️ NOTE: Some images failed to generate. Check '{log_file_path}' for details.")
        
        print("--- Poster Generation Complete ---")

    except FileNotFoundError as e:
        print(f"❌ CRITICAL ERROR: File not found: {e}.")
        print(f"This may have happened during the initial download from Google Drive.")
    except KeyError as e:
        print(f"❌ CRITICAL ERROR: Column not found: {e}.")
        print("Please ensure your Excel/CSV files in Google Drive have all required columns.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

# ==============================================================================
# 8. SCRIPT ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    main()
