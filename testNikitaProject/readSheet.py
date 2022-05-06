import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np

def read_sheet(file_name,worksheet_name):
    file_names = {
        'S1' : 'Training Schedule-New',
    }
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.worksheet(worksheet_name)

    full_data = sheet.get_all_values()
    data = full_data[3:]
    headers = full_data[2]
    sheet_df = pd.DataFrame(data, columns=headers)

    sheet_json = sheet_df.to_dict('records')

    return sheet_json