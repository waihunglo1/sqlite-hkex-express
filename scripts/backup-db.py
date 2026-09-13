import os.path
import logging
import os
import configparser
import time
from pathlib import Path
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

# Define the scopes required to modify Drive files
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def get_drive_service():
    """Authenticates the user and returns the Drive API service instance."""
    creds = None

    # token.json stores the user's access and refresh tokens
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                ".client_secrets.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def upload_file_to_drive(
    local_file_path, drive_folder_id=None, new_filename=None
):
    """Uploads a local file to Google Drive, logging progress at most once per minute.

    :param local_file_path: Path to the local file (e.g., 'data/report.pdf')
    :param drive_folder_id: Optional Drive Folder ID to upload into
    :param new_filename: Optional custom file name on Google Drive
    """
    service = get_drive_service()

    file_name = new_filename or os.path.basename(local_file_path)

    # File metadata
    file_metadata = {"name": file_name}

    if drive_folder_id:
        file_metadata["parents"] = [drive_folder_id]

    # Smaller chunk size (e.g., 2MB) ensures frequent status checks
    media = MediaFileUpload(
        local_file_path, chunksize=2 * 1024 * 1024, resumable=True
    )

    logging.info(f"Starting upload for '{file_name}' to Google Drive...")

    request = service.files().create(
        body=file_metadata, media_body=media, fields="id, name, webViewLink"
    )

    response = None
    last_log_time = time.time()
    log_interval = 60  # Interval in seconds (1 minute)

    while response is None:
        status, response = request.next_chunk()
        current_time = time.time()

        # Log progress if 1 minute has elapsed or if status is available
        if status and (current_time - last_log_time >= log_interval):
            progress = int(status.progress() * 100)
            logging.info(f"Upload Progress: {progress}%")
            last_log_time = current_time

    logging.info("Upload completed successfully! (100%)")
    logging.info(f"File ID: {response.get('id')}")
    logging.info(f"View Link: {response.get('webViewLink')}")
    return response

def hkBackupFile():
    config = configparser.ConfigParser()
    config.read('config/analyst-data-hk.ini', encoding='utf-8')
    sqliteConfig = config['SQLITE']
    fileName = sqliteConfig['FILE']
    return fileName

def usBackupFile():
    config = configparser.ConfigParser()
    config.read('config/analyst-data-us.ini', encoding='utf-8')
    sqliteConfig = config['SQLITE']
    fileName = sqliteConfig['FILE']
    return fileName

if __name__ == "__main__":
    # Load variables from .env file into environment
    env_path = Path(".") / ".env.local"
    load_dotenv(dotenv_path=env_path)

    filesToBackup = [hkBackupFile(), usBackupFile()]

    # Optional: Google Drive folder ID (from the URL when viewing the folder on drive.google.com)
    # e.g., '1a2b3c4d5e6f7g8h9i0j'
    FOLDER_ID = os.getenv("GDRIVE_BACKUP_FOLDER_ID")

    for file in filesToBackup:
        logging.info(f"Local file : {file}")
        logging.info(f"Folder Id : {FOLDER_ID}")
        upload_file_to_drive(local_file_path=file, drive_folder_id=FOLDER_ID)