import os
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Use the same SCOPES as the main script
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SERVICE_ACCOUNT_FILE = 'gen-lang-client-0669898182-80f330433de5.json'
SHEET_ID = '1srvBC83XVx1LS4d8gIiwkWM41sS0Yu3puOHmzwlixrY'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_google_sheets_edit")

def get_google_sheets_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

def main():
    try:
        service = get_google_sheets_service()
        sheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_title = sheet['sheets'][0]['properties']['title']
        # Prepare test data
        test_columns = ['TestCol1', 'TestCol2']
        test_rows = [
            ['A', 1],
            ['B', 2]
        ]
        body = {
            'values': [test_columns] + test_rows
        }
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{sheet_title}!A1",
            valueInputOption="RAW",
            body=body
        ).execute()
        print("SUCCESS: Google Sheet was updated with test data using service account.")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main() 