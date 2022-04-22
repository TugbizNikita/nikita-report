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

def validate_consolidated(file_name):
    file_names = {
        'b1':  'JEE FS Devops Cloud(GCP) 48days - 30-Nov-2021',
        'b2':  '.NET Core',
        'b3':  'JEE FS Devops Cloud(GCP) 48days - 02-Dec-2021',
        'b4':  'CG - BI - V5 DB ETL Testing',
        'b5':  'V&V - Automation Testing (Selenium+Java)',
        'b6':  'UFT+C#+VB Script',
        'b7':  'Systems C with Linux',
        'b8' : 'Consolidated Report Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'Consolidated Report Systems_C CPP Linux Programming Batch 2',
        'b10': 'Consolidated V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11': 'Consolidated V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12':'Consolidated Java + Dellboomi',
        'L1':  'LS-JA-1,2',
        'L2' : 'JEE Full Stack 2.0 with React CAMP Batch',
        'L3' : 'JEE Full Stack 2.0 with React CAMP Batch 25-Jan-22 JR-7',
        'L4' : 'Digital CRM SFDC CAMP Batch 27-Jan-22 SFDC-1',
        'L5' : 'NET Core with Azure CAMP Batch 24-Jan-22 NCA-3',
        'L6' : 'JEE Full Stack 2.0 with React Batch 15',
        'L7' : 'JCAWS 6',
        'L8' : 'JCAWS 8',
        'L9' : 'JCAWS-9',
        'L10': 'JCAWS-10',
        'L11': 'JCGCP-11',
        'L12': 'JR-12',
        'L13': 'JAb-6',
        'L14': 'SFDC-2',
        'L15': 'Consolidated Report NC4',
        'L16': 'Consolidated Report JANG-2',
        'L17': 'Consolidated Report JANG-3',
        'L15': 'Consolidated Report NC4',
        'L16': 'Consolidated Report JANG-2',
        'L17': 'Consolidated Report JANG-3',
        'L18': 'Consolidated Report JRN-14',
        'C1' : 'Consolidated Report CIS Feb 2022',
        'C2' : 'Consolidated Report CIS 2',
        'C3' : 'Consolidated Report CIS 3',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.worksheet('B_Info')

    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    B_info_df = pd.DataFrame(data, columns=headers)
    NV_Stats_1 = {}
    if('Vendor' in B_info_df):
        col_headers = B_info_df['Vendor']
        config_headers = ['LOT', 'Variant','Batch Name','CFMG Code', 'Type','Location','Start Date', 'End Date','Total','Drop out', 'Status']

        diff_col = set(config_headers) - set(col_headers)
        diff_col_list = list(diff_col)
        if(len(diff_col_list) > 0):
            return JsonResponse({'Column name missing': diff_col_list})
        else:
            T_Info = read_Tinfo(file, 'T_Info')
            if T_Info['status'] != 'Success':
                return T_Info
            else:
                NV_Stats = {'NV1': None, 'NV2': None, 'NV3': None, 'NV4': None,
                'NV5': None, 'NV6': None, 'NV7': None, 'NV8': None}
                nv_all_exam_data = pd.DataFrame()
                for i in NV_Stats:
                    NV_Stats_1[i] = read_nv_report(file.worksheet(i), i)
                print("NV_Stats_1", NV_Stats_1)
                for i in NV_Stats_1:
                      if NV_Stats_1['status'] != 'Success':
                        return NV_Stats_1
                      else:
                          return 'NV sheets are correct'

    else:
        return 'Vendor name is missing'
    

def read_Tinfo(file, sheet_name):
    sheet = file.worksheet(sheet_name)
    full_data = sheet.get_all_values()
    data = full_data[1:]
    headers = full_data[0]
    T_info_df = pd.DataFrame(data, columns=headers)

    col_headers = headers
    config_headers = ['Name','Type','Module','Feedback']

    diff_col = set(config_headers) - set(col_headers)
    diff_col_list = list(diff_col)
    if(len(diff_col_list) > 0):
        return {'status': 'Failure','Column name missing':diff_col_list}
    else:
        return {'status': 'Success'}
    
def read_nv_report(sheet,exam_name):

    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i]
                         [0].strip()] = exam_details_list[i][1].strip()

    if exam_details['Status'] != 'Done':
        return {'Result': 'Result Not Available', 'Exam Details': exam_details}
    data = full_data[10:]
    headers = full_data[9]
    NV_df = pd.DataFrame(data, columns=headers)

    col_headers = headers
    config_headers = ['Email Id','Percentage']

    diff_col = set(config_headers) - set(col_headers)
    diff_col_list = list(diff_col)
    if(len(diff_col_list) > 0):
        return {'status': 'Failure','Column name missing':diff_col_list}
    else:
        return {'status': 'Success'}