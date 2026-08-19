import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Define the scope for uploading files
SCOPES = ['https://googleapis.com']

def authenticate_gdrive():
    """Handles authentication and returns the Drive service object."""
    creds = None
    # token.json stores the user's access tokens automatically after first login
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('.client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return build('drive', 'v3', credentials=creds)

def upload_file(local_path, folder_id=None):
    """Copies a local file into Google Drive."""
    service = authenticate_gdrive()
    file_name = os.path.basename(local_path)
    
    # Define file metadata
    file_metadata = {'name': file_name}
    if folder_id:
        file_metadata['parents'] = [folder_id]
        
    # Prepare the file content
    media = MediaFileUpload(local_path, resumable=True)
    
    # Execute the upload request
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    print(f"File uploaded successfully. Drive File ID: {file.get('id')}")

if __name__ == '__main__':
    # REPLACE THESE WITH YOUR ACTUAL VALUES
    LOCAL_FILE_PATH = 'my_document.pdf' 
    TARGET_FOLDER_ID = 'your_google_drive_folder_id_here' # Leave as None to upload to root directory
    
    upload_file(LOCAL_FILE_PATH, TARGET_FOLDER_ID)
