import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np
from .wpr_stat import wpr_stats
import pandera as pa
#from sheets import read_consolidated_report
#from mongodb import get_batch

def student():
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open('Gamification')
    
    sheet = file.worksheet('Student')

    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    gamification_df = pd.DataFrame(data, columns=headers)

    print("gamification_df", gamification_df)
    return gamification_df


def gamification_registration(file_name):
    file_names = {
        'G1' : 'Gamification',
    }
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.worksheet('Registration')

    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    registration_df = pd.DataFrame(data, columns=headers)

    registration_json = registration_df.round(2).to_dict('records')

    print("registration_df", registration_json)
    return registration_json

def gamification_login(mobile_number):
    file_names = {
        'G1' : 'Gamification',
    }
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open('Gamification')
    
    sheet = file.worksheet('Login')

    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    login_df = pd.DataFrame(data, columns=headers)

    login_json = login_df.round(2).to_dict('records')

    print("login_json", login_json)
    return login_json