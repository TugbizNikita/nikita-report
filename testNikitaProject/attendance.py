import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np

def read_batch_attendance(file_name):
    file_names = {
        'L2' : 'Attendance_JR_6_24Jan_Batch',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.worksheet('NewFormat_Attendance')

    full_data = sheet.get_all_values()
    data = full_data[3:]
    headers = full_data[2]
    attendance_df = pd.DataFrame(data, columns=headers)
    
    print(attendance_df)
        
   # print(attendance_date_df)
    #attendance_json = attendance_df.to_json(orient = 'records')
    ranges = []
    #data_frame = pd.DataFrame(columns=['Week_No', 'Above Avg', 'Avg', 'Below_Avg', 'DO'])
    
    records = []

    col_name = ['Attendance %']
    for i in col_name:       
        
        w1 = dict(attendance_df[i].value_counts())
        ranges = {'Superset ID':"", '0-20': 0, '21-40': 0, '41-60': 0, '61-80':0, '81-100':0}
        ranges['Superset ID'] = str(''.join(filter(lambda i: i.isdigit(), i)))
        
        for key, value in w1.items():
            key = key.replace('%', '')
            key = key.strip()

            if ((int(key) >= 0) and (int(key) < 21)):
                ranges['0-20'] += int(value)
            elif ((int(key) >= 21) and (int(key) < 41)):
                ranges['21-40'] += int(value)
            elif ((int(key) >= 41) and (int(key) < 61)):
                ranges['41-60'] += int(value)
            elif ((int(key) >= 61) and (int(key) < 81)):
                ranges['61-80'] += int(value)
            elif ((int(key) >= 81) and (int(key) < 101)):
                ranges['81-100'] += int(value)

        records.append(ranges)
       
    #df = pd.json_normalize(records)
    #df = records.to_json()
    #print(df)

    print(records)
    return records
    
read_batch_attendance('L2')
