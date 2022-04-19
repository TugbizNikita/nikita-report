import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index
from .sheets2 import read_lsr

def validateWSR(file_name):
    file_names = {
        'b4' : 'Batch_LSR_BI–V5 DB ETL Testing - 21-Dec-2021',
        'b5' : 'Batch_LSR_V&V-Automation Testing (Selenium+Java) - 04-Jan-2022',
        'b6' : 'Batch_LSR_V&V-Automation Testing (UFT+C#+VB Script) - 04-Jan-2022',
        'b7' : 'Batch_LSR_Systems C with Linux Jan 25th Batch2',
        'L1' : 'Batch_LSR_JA-1',
        'L2' : 'Batch_LSR_JR-6',
        'L3' : 'Batch_LSR_SFDC-1',
        'L4' : 'Batch_LSR_SFDC-1',
        'L5' : 'Batch_LSR_NCA-4',
        'L6' : 'Batch_LSR_JR 15',
        'L7' : 'Batch_LSR_JCAWS 6',
        'L8' : 'Batch_LSR_JCAWS 8',
        'L9' : 'Batch_LSR_JCAWS 9',
        'L10': 'Batch_LSR_JCAWS 10',
        'L11': 'Batch_LSR_JCGCP 11',
        'b8' : 'Batch_LSR Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'Batch_LSR Systems_C CPP Linux Programming Batch 2',
        'C1' : 'Batch_LSR CIS Feb 2022',
        'L12': 'Batch_LSR_JR 12',
        'L13': 'Batch_LSR_JAb-6',
        'L14': 'Batch_LSR_SFDC 2',
        'L15': 'Batch_LSR NC4',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.get_worksheet(0)

    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    lsr_data_df = pd.DataFrame(data, columns=headers)
    lsr_data_df['Week_No'] = lsr_data_df['Week_No'].astype('Int64')
    col_headers = headers
    config_headers = ['Week_No', 'Batch Mentor','Learning status']

    diff_col = set(config_headers) - set(col_headers)
    diff_col_list = list(diff_col)
    if(len(diff_col_list) > 0):
        return JsonResponse({'Column name missing': diff_col_list})

    schema = pa.DataFrameSchema(
       columns={
        "Week_No": Column(int, checks=Check.le(15)),
        "Batch Mentor": Column(str, Check.str_matches(r'^[a-z]|[A-Z]')),
        "Learning status": Column(str, Check.str_matches(r'^[a-z]|[A-Z]|[0-9]')),
        })
    try:
        validated_df = schema(lsr_data_df)
        print(validated_df)
        return "Good to go"
    except Exception as e:
        print(str(e))
        return str(e)

# def validateWPR(file_name):
#     file_names = {
#     'b1':'WPR_JEE FS Devops Cloud(GCP)  - 30-Nov-2021-24-Jan-22',
#     'b2' :'WPR_NET Core - 30-Nov-2021-24-Jan-22',
#     'b3' :'WPR-JEE with DevOps & Cloud(GCP) Dec 2nd Batch2-Updated on 24-Jan-22',
#     'b4':'WPR-BI V5-DB ETL Testing Dec 21st Batch-Updated on 24-Jan-22',
#     'b5':'WPR_V&V_SELJ_BP_04-01-22_47_24-Jan-22',
#     'b6':'WPR_V&V_UFT_BP_06-01-22_58_24-Jan-22',
#     'b7' :'Systems C with Linux Jan 25th Batch2',
#     'L1' : 'JA-1-Updated on 25-Jan-22',
#     'L2' : 'JEE Full Stack 2.0 with React Batch 2 JR-6',
#     'L3' : 'WPR - JR7',
#     'L4' : 'Digital CRM SFDC Batch 1',
#     'L5' : 'NET Core with Azure',
#     'L6' : 'WPR_JR-15', 
#     'L8' : 'WPR_JCAWS-8',
#     'L9' : 'WPR_JCAWS 6 & 9',
#     'L10': 'WPR_JCAWS 10',
#     'L11': 'WPR_JCGCP  11',
#     'b8' : 'WPR  Systems_C CPP Linux Programming Feb 22nd Batch1',
#     'b9' : 'WRP Systems_C CPP Linux Programming Feb 22nd Batch2',
#     'C1' : 'WPR CIS Feb 2022',
#     'L12': 'WPR_JR 12',
#     'L13': 'WPR_JAb-6',
#     'L14': 'WPR_SFDC2',
#     'L15': 'WPR_NC4',
#     'C1' : 'WPR CIS Mar 2022',
#     'C2' : 'WPS CIS Report',
# }

#     scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
#             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
#     creds = ServiceAccountCredentials.from_json_keyfile_name(
#         "creds.json", scope)
#     client = gspread.authorize(creds)

#     file = client.open(file_names[file_name])
#     sheet = file.get_worksheet(0)

#     full_data = sheet.get_all_values()
#     data = full_data[2:]
#     headers = full_data[1]
#     df = pd.DataFrame(data, columns=headers)
#     df['W2-Technical'] = df['W2-Technical'].astype('float64')
#     df['W3-Technical'] = df['W3-Technical'].astype('float64')
#     df['W4-Technical'] = df['W4-Technical'].astype('float64')
#     df['W5-Technical'] = df['W5-Technical'].astype('float64')
#     df['W6-Technical'] = df['W6-Technical'].astype('float64')
#     df['W7-Technical'] = df['W7-Technical'].astype('float64')
#     df['W8-Technical'] = df['W8-Technical'].astype('float64')
#     df['W9-Technical'] = df['W9-Technical'].astype('float64')
#     df['W10-Technical'] = df['W10-Technical'].astype('float64')
#     df['W11-Technical'] = df['W11-Technical'].astype('float64')
#     df['W12-Technical'] = df['W12-Technical'].astype('float64')
#     df['W13-Technical'] = df['W13-Technical'].astype('float64')
#     df['W14-Technical'] = df['W14-Technical'].astype('float64')
#     df['W15-Technical'] = df['W15-Technical'].astype('float64')
#     col_headers = headers
#     config_headers = ['W1-Technical', 'W2-Technical','W3-Technical','W4-Technical', 'W5-Technical','W6-Technical','W7-Technical', 'W8-Technical','W9-Technical',
#                         'W10-Technical', 'W11-Technical','W12-Technical','W13-Technical', 'W14-Technical','W15-Technical','W1-Soft Skill', 'W2-Soft Skill','W3-Soft Skill','W4-Soft Skill', 'W5-Soft Skill','W6-Soft Skill','W7-Soft Skill', 'W8-Soft Skill','W9-Soft Skill',
#                         'W10-Soft Skill', 'W11-Soft Skill','W12-Soft Skill','W13-Soft Skill', 'W14-Soft Skill','W15-Soft Skill','W1-Learning Status Remark', 'W2-Learning Status Remark','W3-Learning Status Remark','W4-Learning Status Remark', 'W5-Learning Status Remark','W6-Learning Status Remark','W7-Learning Status Remark', 'W8-Learning Status Remark','W9-Learning Status Remark','W10-Learning Status Remark', 
#                         'W11-Learning Status Remark','W12-Learning Status Remark','W13-Learning Status Remark', 'W14-Learning Status Remark','W15-Learning Status Remark']

#     diff_col = set(config_headers) - set(col_headers)
#     diff_col_list = list(diff_col)
#     if(len(diff_col_list) > 0):
#         return JsonResponse({'Column name missing': diff_col_list})
        
#     #df = df.replace('%',' ', regex=True)
    
    
#     for i in headers:
#         if i.endswith('-Technical'):
#                 schema = pa.DataFrameSchema(
#                 columns={
#                     i: Column(float, checks = Check.le(5)),
#                 })
#                 try:
#                     validated_df = schema(df)
#                 except Exception as e:
#                     print(str(e))
#                     return str(e)
            
#     for k in headers:
#         if k.endswith('_Remark'):
#                 schema = pa.DataFrameSchema(
#                 columns={
#                     "W2-Learning_Status_Remark": Column(str, Check.str_matches(r'^[a-z]|[A-Z]')),

#                 })
#                 try:
#                     validated_df = schema(df)
#                 except Exception as e:
#                     print(str(e))
#                     return str(e)
            

#         #df.drop(df.index[df == 'NA'], inplace = True)


