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

def read_schedule():
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open('Training Schedule-New')
    
    sheet = file.get_worksheet(9)
    try:
            full_data = sheet.get_all_values()
            data = full_data[3:]
            headers = full_data[2]
            df = pd.DataFrame(data, columns=headers)
            df = df.loc[df['Trainer_Name'] == 'Exam']
            df2 = df[['crCode','Batch_Name','Course_Name', 'Start_Date']].copy()

            #print(df2.Start_Date)
            #cr = df.crCode
            #js = df2.to_json(orient = 'index')
            #display(js)    
            #display(df.Batch_Code)
    except:
        pass  

def read_batch_candidates(file_name):
    file_names = {
        'b4' : 'Candidate_Sheet_BI - V5 DB ETL Testing',
        'b5' : 'Candidate_Sheet_V&V - Automation Testing (Selenium+Java)',
        'b6' : 'Candidate_Sheet_V&V - Automation Testing (UFT+C#+VB Script) - 04-Jan-2022',
        'b7' : 'Candidate_Sheet_Systems C with Linux Jan 25th Batch2',
        'b8' : 'Candidate Sheet Systems_C CPP Linux Programming Batch 1',
        'b9' : 'Candidate Sheet Systems_C CPP Linux Programming Batch 2',
        'b10': 'Candidate_Sheet-V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11': 'Candidate_Sheet-V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12': 'Candidate_Sheet-Java + Dellboomi',
        'L1' : 'Candidate_Sheet-JA1',
        'L2' : 'Candidate_Sheet-JR6',
        'L3' : 'Candidate_Sheet-JR 7',
        'L4' : 'Candidate_Sheet-SFDC-1',
        'L5' : 'Candidate_Sheet-NCA 4',
        'L6' : 'Candidate_Sheet-JR15',
        'L7' : 'Candidate_Sheet-JCAWS 6',
        'L8' : 'Candidate_Sheet-JCAWS 8',
        'L9' : 'Candidate_Sheet-JCAWS 9',
        'L10': 'Candidate_Sheet-JCAWS 10',
        'L11': 'Candidate_Sheet-JCGCP 11',
        'L12': 'Candidate Sheet_JR 12',
        'L13': 'Candidate Sheet_ JAb 6',
        'L14': 'Candidate Sheet_SFDC 2',
        'L15': 'Candidate Sheet NC4',
        'L16':'Candidate Sheet_JANG 2',
        'L17':'Candidate Sheet_JANG 3',
        'L18':'Candidate Sheet_JRN 14_Novevista_Batchlist-25-Mar-22',
        'C1' : 'Candidate Sheet CIS Feb 2022',
        'C2' : 'Candidate Sheet CIS 2',
        'C3' :'Candidate sheet CIS3',
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
    original_df = pd.DataFrame(data, columns=headers)

    '''Initial_Size = original_df.index
    Initial_Size = len(Initial_Size)
    print('Intial Size', Initial_Size)'''

    '''df = original_df.loc[original_df['Drop_Out'] == 'Yes']
    candidate_dropout_df = df[['Candidate name','Week_No']].copy()
    candidate_dropout_df2 = candidate_dropout_df.groupby("Week_No").size().reset_index(name = 'Dropout/Abscondee Count')
    number_of_drop_out = candidate_dropout_df2['Dropout/Abscondee Count'].sum()

    df1 = original_df.loc[original_df['Transfer_Out'] == 'Yes']
    candidate_transfer_out_df = df1[['Candidate name','Week_No']].copy()
    candidate_transfer_out_df2 = candidate_transfer_out_df.groupby("Week_No").size().reset_index(name = 'Transfer-Out Count')
   
    #number_of_transfer_out = candidate_transfer_out_df2['Transfer-Out Count'].sum()

    df2 = original_df.loc[original_df['Transfer_In'] == 'Yes']
    candidate_transfer_in_df = df2[['Candidate name','Week_No']].copy()
    candidate_transfer_in_df2 = candidate_transfer_in_df.groupby("Week_No").size().reset_index(name = 'Transfer-In Count')

    candidate_dropout_new =candidate_dropout_df2.merge(candidate_transfer_out_df2[['Week_No','Transfer-Out Count']], on = 'Week_No', how = 'left')

    frames = [candidate_dropout_df2, candidate_transfer_out_df2, candidate_transfer_in_df2]
    result = pd.concat(frames)

    return result'''

    df = original_df.loc[original_df['Drop_Out'] == 'Yes']
    candidate_dropout_df = df[['Candidate name','Week_No_DO']].copy()
    candidate_dropout_df2 = candidate_dropout_df.groupby("Week_No_DO").size().reset_index(name = 'Dropout/Abscondee Count')
    number_of_drop_out = candidate_dropout_df2['Dropout/Abscondee Count'].sum()
    candidate_dropout_df2.rename(columns={'Week_No_DO': 'Week_No'}, inplace=True)
    #print(candidate_dropout_df2)

    df1 = original_df.loc[original_df['Transfer_Out'] == 'Yes']
    candidate_transfer_out_df = df1[['Candidate name','Week_No_TO']].copy()
    candidate_transfer_out_df2 = candidate_transfer_out_df.groupby("Week_No_TO").size().reset_index(name = 'Transfer-Out Count')
    number_of_transfer_out = candidate_transfer_out_df2['Transfer-Out Count'].sum()
    candidate_transfer_out_df2.rename(columns={'Week_No_TO': 'Week_No'}, inplace=True)
    #print(candidate_transfer_out_df2)

    #number_of_transfer_out = candidate_transfer_out_df2['Transfer-Out Count'].sum()

    df2 = original_df.loc[original_df['Transfer_In'] == 'Yes']
    candidate_transfer_in_df = df2[['Candidate name','Week_No_TI']].copy()
    candidate_transfer_in_df2 = candidate_transfer_in_df.groupby("Week_No_TI").size().reset_index(name = 'Transfer-In Count')
    number_of_transfer_in = candidate_transfer_in_df2['Transfer-In Count'].sum()
    candidate_transfer_in_df2.rename(columns={'Week_No_TI': 'Week_No'}, inplace=True)
    #print(candidate_transfer_in_df2)

    #frames = [candidate_dropout_df2,  candidate_transfer_in_df2,candidate_transfer_out_df2]
    #result = pd.concat(frames)
    #result = result.fillna(0)
    #print(result)

    #new_df = candidate_dropout_df2.merge(candidate_transfer_out_df2.assign(both = candidate_dropout_df2['Week_No']))
    new_df = candidate_dropout_df2.merge(candidate_transfer_out_df2, on = 'Week_No', how = 'outer').fillna(0)
    print(new_df)

    new_df1 = new_df.merge(candidate_transfer_in_df2, on = 'Week_No', how = 'outer').fillna(0)
    print(new_df1)

    return new_df1

def read_batch_consolidated(file_name):
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

    b_info = file.worksheet('B_Info')
    b_info_list = b_info.get_all_values()[0:11]
    b_info = {}
    for i in range(len(b_info_list)):
        if b_info_list[i][0] != '':
            b_info[b_info_list[i][0]] = b_info_list[i][1]
    
    b_info_records = []
    b_info_records.append(b_info)
    batch_data_df = pd.json_normalize(b_info_records)
    #print(batch_data_df)
    return batch_data_df

#read_batch_consolidated('b1')

def read_batch_lsr(file_name, download):
    file_names = {
        'b4' : 'Batch_LSR_BI–V5 DB ETL Testing - 21-Dec-2021',
        'b5' : 'Batch_LSR_V&V-Automation Testing (Selenium+Java) - 04-Jan-2022',
        'b6' : 'Batch_LSR_V&V-Automation Testing (UFT+C#+VB Script) - 04-Jan-2022',
        'b7' : 'Batch_LSR_Systems C with Linux Jan 25th Batch2',
        'b8' : 'Batch_LSR Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'Batch_LSR Systems_C CPP Linux Programming Batch 2',
        'b10' : 'Batch_LSR V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11' : 'Batch_LSR V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12' : 'Batch_LSR Java + Dellboomi',
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
        'L12': 'Batch_LSR_JR 12',
        'L13': 'Batch_LSR_JAb-6',
        'L14': 'Batch_LSR_SFDC 2',
        'L15': 'Batch_LSR NC4',
        'L16': 'Batch_LSR_JANG-2',
        'L17': 'Batch_LSR_JANG-3',
        'L18' : 'Batch_LSR_JRN-14',
        'C1' : 'Batch_LSR CIS Feb 2022',
        'C2' : 'Batch_LSR CIS 2',
        'C3' : 'WPS Report CIS 3',
        
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
    
    lsr_all_data_json = lsr_data_df.round(2).to_dict('records')

    candidate_info_df1 = read_batch_candidates(file_name)
    '''print(candidate_info_df1)
    Initial_Size = candidate_info_df1['Initial Size']
    print(Initial_Size)
    lsr_data_df['Initial_Size'] = candidate_info_df1['Initial Size']'''
    lsr_data_df = lsr_data_df.merge(candidate_info_df1.assign(both = candidate_info_df1['Week_No']), how='left').fillna({'Dropout/Abscondee Count':0})
    lsr_data_df = lsr_data_df.merge(candidate_info_df1.assign(both = candidate_info_df1['Week_No']), how='left').fillna({'Transfer-In Count':0})
    lsr_data_df = lsr_data_df.merge(candidate_info_df1.assign(both = candidate_info_df1['Week_No']), how='left').fillna({'Transfer-Out Count':0})
    del lsr_data_df["both"]
    
    batch_info_df = read_batch_consolidated(file_name)
    
    lsr_data_df['tmp'] = 1
    batch_info_df['tmp'] = 1
    lsr_data_df = pd.merge(lsr_data_df, batch_info_df, on=['tmp'])
    lsr_data_df = lsr_data_df.drop('tmp', axis=1)

    lsr_data_df.apply(pd.to_numeric, errors='coerce')
    
    lsr_data_df.loc[0, 'Current Batch Size'] = int(lsr_data_df.loc[0,  'Intial Size']) - int(lsr_data_df.loc[0,  'Dropout/Abscondee Count']) - int(lsr_data_df.loc[0,  'Transfer-Out Count'])
    
    #used previous row to for current batch size calculation
    for i in range(1, len(lsr_data_df)):
        lsr_data_df.loc[i, 'Current Batch Size'] = lsr_data_df.loc[i-1,  'Current Batch Size'] + lsr_data_df.loc[i, 'Transfer-In Count'] - lsr_data_df.loc[i, 'Transfer-Out Count'] - lsr_data_df.loc[i, 'Dropout/Abscondee Count']
    
    '''tempIBS = lsr_data_df['Intial Size'].apply(pd.to_numeric, errors='coerce').get(0)
    lsr_data_df = lsr_data_df.reset_index()  # make sure indexes pair with number of rows
    for index, row in lsr_data_df.iterrows():
        tempIBS = tempIBS + row['Transfer-In Count'].apply(pd.to_numeric, errors='coerce')  - row['Transfer-Out Count'].apply(pd.to_numeric, errors='coerce') - row['Dropout/Abscondee Count'].apply(pd.to_numeric, errors='coerce') 
        row['Current Batch Size'] = tempIBS'''

    wpr_stats_df = wpr_stats(file_name)

    lsr_data_df = pd.merge(lsr_data_df, wpr_stats_df, on='Week_No', how='left')

    lsr_data_df = lsr_data_df.rename({'Week_No': 'Sr.No'}, axis=1)
    lsr_data_df = lsr_data_df.rename({'CFMG Code': 'CFMG Batch Code'}, axis=1)
    lsr_data_df = lsr_data_df.rename({'Type': 'Batch Type'}, axis=1)
    lsr_data_df = lsr_data_df.rename({'Intial Size': 'Initial Batch Size'}, axis=1) 
    lsr_data_df = lsr_data_df.rename({'Above_Average': 'Above Average Pax Count'}, axis=1)
    lsr_data_df = lsr_data_df.rename({'Average': 'Average Pax Count'}, axis=1)
    lsr_data_df = lsr_data_df.rename({'Below_Average': 'Below Average Pax Count'}, axis=1)

    column_names = ["Vendor", "Sr.No", "LOT","Variant","Batch Name","CFMG Batch Code","Batch Type","Location","Start Date","End Date","Initial Batch Size","Dropout/Abscondee Count","Transfer-Out Count","Transfer-In Count","Current Batch Size","Batch Mentor","Learning status","Above Average Pax Count", "Average Pax Count", "Below Average Pax Count", "DO", "NA"]
    lsr_data_df = lsr_data_df.reindex(columns=column_names) 
    lsr_data_json = lsr_data_df.to_json(orient = 'records')
    lsr_data_df.to_csv('L18WSR.csv', index = False)
    CFMG_Code = batch_info_df['CFMG Code']
    CFMG_Code = CFMG_Code.get(0)
    if download == True:
        result = {'lsr_data_df': lsr_data_df, 'sheet_name':CFMG_Code}
    else:
        result = {lsr_data_json}
    
    return result

read_batch_lsr('L18',True)

#read_batch_candidates('C1')


def read_lsr(file_name):
    file_names = {
       'b4' : 'Batch_LSR_BI–V5 DB ETL Testing - 21-Dec-2021',
        'b5' : 'Batch_LSR_V&V-Automation Testing (Selenium+Java) - 04-Jan-2022',
        'b6' : 'Batch_LSR_V&V-Automation Testing (UFT+C#+VB Script) - 04-Jan-2022',
        'b7' : 'Batch_LSR_Systems C with Linux Jan 25th Batch2',
        'b8' : 'Batch_LSR Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'Batch_LSR Systems_C CPP Linux Programming Batch 2',
        'b10' : 'Batch_LSR V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11' : 'Batch_LSR V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12' : 'Batch_LSR Java + Dellboomi',
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
        'L12': 'Batch_LSR_JR 12',
        'L13': 'Batch_LSR_JAb-6',
        'L14': 'Batch_LSR_SFDC 2',
        'L15': 'Batch_LSR NC4',
        'L16': 'Batch_LSR_JANG-2',
        'L17': 'Batch_LSR_JANG-3',
        'L18' : 'Batch_LSR_JRN-14',
        'C1' : 'Batch_LSR CIS Feb 2022',
        'C2' : 'Batch_LSR CIS 2',
        'C3' : 'WPS Report CIS 3',
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

    #lsr_data_df['Week_No'] = lsr_data_df['Week_No'].astype('Int64')
    #lsr_data_df['Batch Mentor'] = lsr_data_df['Batch Mentor'].astype('Int64')
    #lsr_data_df[lsr_data_df['Batch Mentor'].astype(str).str.isdigit()]
    return lsr_data_df

#read_lsr('b4')

def read_wpr(file_name):
    file_names = {
        'b1':'WPR_JEE FS Devops Cloud(GCP)  - 30-Nov-2021-24-Jan-22',
        'b2' :'WPR_NET Core - 30-Nov-2021-24-Jan-22',
        'b3' :'WPR-JEE with DevOps & Cloud(GCP) Dec 2nd Batch2-Updated on 24-Jan-22',
        'b4':'WPR-BI V5-DB ETL Testing Dec 21st Batch-Updated on 24-Jan-22',
        'b5':'WPR_V&V_SELJ_BP_04-01-22_47_24-Jan-22',
        'b6':'WPR_V&V_UFT_BP_06-01-22_58_24-Jan-22',
        'b7' :'Systems C with Linux Jan 25th Batch2',
        'b8' : 'WPR  Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'WRP Systems_C CPP Linux Programming Feb 22nd Batch2',
        'b10':'WPR  V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11':'WPR  V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12':'WPR  Java + Dellboomi',
        'L1' : 'JA-1-Updated on 25-Jan-22',
        'L2' : 'JEE Full Stack 2.0 with React Batch 2 JR-6',
        'L3' : 'WPR - JR7',
        'L4' : 'Digital CRM SFDC Batch 1',
        'L5' : 'NET Core with Azure',
        'L6' : 'WPR_JR-15', 
        'L8' : 'WPR_JCAWS-8',
        'L9' : 'WPR_JCAWS 6 & 9',
        'L10': 'WPR_JCAWS 10',
        'L11': 'WPR_JCGCP  11',
        'L12': 'WPR_JR 12',
        'L13': 'WPR_JAb-6',
        'L14': 'WPR_SFDC2',
        'L15': 'WPR_NC4',
        'L16': 'WPR_JANG-2',
        'L17': 'WPR_JANG-3',
        'L18': 'WPR_JRN-14',
        'C1' : 'WPR CIS Feb 2022',
        'C2' : 'WPR CIS 2',
        'C3' : 'WPR CIS 3',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    sheet = file.get_worksheet(0)

    full_data = sheet.get_all_values()
    data = full_data[2:]
    headers = full_data[1]
    df = pd.DataFrame(data, columns=headers)
    
    return df

def read_candidates(file_name):
    file_names = {
        'b4' : 'Candidate_Sheet_BI - V5 DB ETL Testing',
        'b5' : 'Candidate_Sheet_V&V - Automation Testing (Selenium+Java)',
        'b6' : 'Candidate_Sheet_V&V - Automation Testing (UFT+C#+VB Script) - 04-Jan-2022',
        'b7' : 'Candidate_Sheet_Systems C with Linux Jan 25th Batch2',
        'b8' : 'Candidate Sheet Systems_C CPP Linux Programming Batch 1',
        'b9' : 'Candidate Sheet Systems_C CPP Linux Programming Batch 2',
        'b10': 'Candidate_Sheet-V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11': 'Candidate_Sheet-V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12': 'Candidate_Sheet-Java + Dellboomi',
        'L1' : 'Candidate_Sheet-JA1',
        'L2' : 'Candidate_Sheet-JR6',
        'L3' : 'Candidate_Sheet-JR 7',
        'L4' : 'Candidate_Sheet-SFDC-1',
        'L5' : 'Candidate_Sheet-NCA 4',
        'L6' : 'Candidate_Sheet-JR15',
        'L7' : 'Candidate_Sheet-JCAWS 6',
        'L8' : 'Candidate_Sheet-JCAWS 8',
        'L9' : 'Candidate_Sheet-JCAWS 9',
        'L10': 'Candidate_Sheet-JCAWS 10',
        'L11': 'Candidate_Sheet-JCGCP 11',
        'L12': 'Candidate Sheet_JR 12',
        'L13': 'Candidate Sheet_ JAb 6',
        'L14': 'Candidate Sheet_SFDC 2',
        'L15': 'Candidate Sheet NC4',
        'L16':'Candidate Sheet_JANG 2',
        'L17':'Candidate Sheet_JANG 3',
        'L18':'Candidate Sheet_JRN 14_Novevista_Batchlist-25-Mar-22',
        'C1' : 'Candidate Sheet CIS Feb 2022',
        'C2' : 'Candidate Sheet CIS 2',
        'C3' :'Candidate sheet CIS3',
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
    candidates_df = pd.DataFrame(data, columns=headers)
    #print(headers)
    return candidates_df

def wpr_stats_common(file_name):
    file_names = {
        'b1':'WPR_JEE FS Devops Cloud(GCP)  - 30-Nov-2021-24-Jan-22',
        'b2' :'WPR_NET Core - 30-Nov-2021-24-Jan-22',
        'b3' :'WPR-JEE with DevOps & Cloud(GCP) Dec 2nd Batch2-Updated on 24-Jan-22',
        'b4':'WPR-BI V5-DB ETL Testing Dec 21st Batch-Updated on 24-Jan-22',
        'b5':'WPR_V&V_SELJ_BP_04-01-22_47_24-Jan-22',
        'b6':'WPR_V&V_UFT_BP_06-01-22_58_24-Jan-22',
        'b7' :'Systems C with Linux Jan 25th Batch2',
        'b8' : 'WPR  Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'WRP Systems_C CPP Linux Programming Feb 22nd Batch2',
        'b10':'WPR  V& V Automation (Selenium+Java)Apr 5 Batch 3',
        'b11':'WPR  V& V Automation (Selenium+Java)Apr 5 Batch 4',
        'b12':'WPR  Java + Dellboomi',
        'L1' : 'JA-1-Updated on 25-Jan-22',
        'L2' : 'JEE Full Stack 2.0 with React Batch 2 JR-6',
        'L3' : 'WPR - JR7',
        'L4' : 'Digital CRM SFDC Batch 1',
        'L5' : 'NET Core with Azure',
        'L6' : 'WPR_JR-15', 
        'L8' : 'WPR_JCAWS-8',
        'L9' : 'WPR_JCAWS 6 & 9',
        'L10': 'WPR_JCAWS 10',
        'L11': 'WPR_JCGCP  11',
        'L12': 'WPR_JR 12',
        'L13': 'WPR_JAb-6',
        'L14': 'WPR_SFDC2',
        'L15': 'WPR_NC4',
        'L16': 'WPR_JANG-2',
        'L17': 'WPR_JANG-3',
        'L18': 'WPR_JRN-14',
        'C1' : 'WPR CIS Feb 2022',
        'C2' : 'WPR CIS 2',
        'C3' : 'WPR CIS 3',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    sheet = file.get_worksheet(0)

    full_data = sheet.get_all_values()
    data = full_data[2:]
    headers = full_data[1]
    df = pd.DataFrame(data, columns=headers)

    # print(dict(df['W2-Technical'].apply(pd.to_numeric).value_counts()))
    ranges = []
    #data_frame = pd.DataFrame(columns=['Week_No', 'Above Avg', 'Avg', 'Below_Avg', 'DO'])
    
    records = []
    for i in headers:
        
        if i.endswith('-Technical'):
            
            w1 = dict(df[i].value_counts())
            #print(w1)
            ranges = {'Week_No':"", 'Above_Average': 0, 'Average': 0, 'Below_Average': 0, 'NA': 0}
            ranges['Week_No'] = str(''.join(filter(lambda i: i.isdigit(), i)))
            # avg_technical_week_wise[i] = round(df[df[i].apply(lambda x: x.isnumeric())][i].apply(pd.to_numeric).mean(), 2)
            for key, value in w1.items():
                key = key.strip()

                if ((key == 'NA') or (key == '') or (key == 'na') or (key == 'Na') or (key == 'N/A') or (key == 'Absent') or (key == 'DO') or (key == 'Do') or (key == 'D/O') or (key == 'Transfer Out') or (key == 'Tranfer out') or (key == 'Transfer out') or (key == 'Tranfer Out')):
                    ranges['NA'] += int(value)
                elif ((float(key) >= 0) and (float(key) < 3)):
                    ranges['Below_Average'] += int(value)
                elif ((float(key) == 3)):
                    ranges['Average'] += int(value)

                elif ((float(key) > 3) and (float(key) <= 5)):
                    ranges['Above_Average'] += int(value)

            records.append(ranges)

    df = pd.json_normalize(records)
    
    
    #return result


    #candifdate sheet data for drop out
    drop_info_data = read_batch_candidates(file_name)
    df = df.merge(drop_info_data.assign(both = drop_info_data['Week_No']), how='left').fillna({'Dropout/Abscondee Count':0})
    df = df.merge(drop_info_data.assign(both = drop_info_data['Week_No']), how='left').fillna({'Transfer-In Count':0})
    df = df.merge(drop_info_data.assign(both = drop_info_data['Week_No']), how='left').fillna({'Transfer-Out Count':0})
    del df["both"]
    print(df)

    df_json = df.to_json(orient = 'records')
    result = json.loads(df_json)
    print(result)
    return result

#read_candidates('L4')
#read_batch_lsr('L8',True)
