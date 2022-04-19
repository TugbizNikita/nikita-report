import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np

def avg_finder(x):
    LB_Weightage = 20/100
    MF = 100
    LB_Weightage_NV = int(MF * LB_Weightage)
    mark_list = []
    for i in range(8):
        # print(f'NV{i+1}')
        # print(x)
        if f'NV{i+1}' in x:
            if x[f'NV{i+1}'] != 0:
                mark_list.append(x[f'NV{i+1}'])
        else:
            break
    if len(mark_list) == 0:
        return 0
    else:
        nv_avg_marks =  round(sum(mark_list)/len(mark_list), 2)
        return (nv_avg_marks * LB_Weightage_NV )

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
    if('Superset ID' in headers):
        candidates_df = candidates_df[['Superset ID']].copy()
        candidates_df = candidates_df.rename({'Superset ID': 'Email ID'}, axis=1)
    else:
        #candidates_df = candidates_df[['Email ID']].copy()
        candidates_df = candidates_df[['Email ID','Candidate Registered Email ID']].copy()
        candidates_df = candidates_df.rename({'Email ID': 'Email ID'}, axis=1)
        candidates_df = candidates_df.rename({'LMS_Email_ID': 'Candidate Registered Email ID'}, axis=1)

    #candidates_df = candidates_df[['Email ID']].copy()
    
    #print(df2)
    return candidates_df

#read_candidates('L1')


def read_consolidated_report_new(file_name):
    df_list_of_nv = []
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

    candidate_info_df1 = read_candidates(file_name)
    
    m1_imp_report = read_m1_improvement_report(file=file)
    improvement_df1 = pd.DataFrame()
    improvement_df = pd.DataFrame()
    is_improvement_empty = m1_imp_report['is_Empty']
    improvement_stats = 'No Data'
    if is_improvement_empty == False:
        if m1_imp_report['stats']['Exam_Details'] != None:
            improvement_stats = m1_imp_report['stats']['Exam_Details']
        improvement_df = m1_imp_report['df']
        improvement_stats = m1_imp_report['stats']
        
    improvement_df1 = improvement_df.rename({'Email ID': 'Email ID'}, axis=1)

    shadow_report = read_shadow_project(file=file)
    is_shadow_empty = shadow_report['is_Empty']
    shadow_stats = 'No Data'
    if is_shadow_empty == False:
        if shadow_report['stats']['Exam_Details'] != None:
            shadow_stats = shadow_report['stats']['Exam_Details']
        shadow_df = shadow_report['df']
        
        shadow_stats = shadow_report['stats']
        shadow_df = shadow_df.rename({'Email Id': 'Email ID'}, axis=1)
    
    M1_sheet = file.worksheet('CG_M1')

    full_data = M1_sheet.get_all_values()
    exam_details_list = full_data[0:9]
    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)
    df['Capgemini Email ID'] = df['Capgemini Email ID'].str.upper() # this is M1 df

    M1_df = pd.DataFrame()
    M1_df['Email ID'] = df['Capgemini Email ID']
    M1_df['EmpID'] = df['EmpID']
    M1_df['M1'] = df['TOTAL']

    Student_all_data_df = pd.merge(
        candidate_info_df1, M1_df, on='Email ID', how='left')
    
    Student_all_data_df = pd.merge(
        Student_all_data_df, improvement_df1, on='Email ID', how='left')
        

    NV_Stats = {'NV1': None, 'NV2': None, 'NV3': None, 'NV4': None,
                'NV5': None, 'NV6': None, 'NV7': None, 'NV8': None}
    nv_all_exam_data = pd.DataFrame()
    for i in NV_Stats:
        NV_Stats[i] = read_nv_report(file.worksheet(i), i, df_list_of_nv)

    nv_all_exam_data = pd.concat(df_list_of_nv)
    nv_all_exam_data = nv_all_exam_data.sort_values('Email ID')

    nv_all_exam_data = nv_all_exam_data.drop(
        columns=['Points received', 'Points scored', 'Is Passed', 'First Name', 'Last Name'], errors='ignore')
    nv_all_exam_data = nv_all_exam_data.rename({'Email ID': 'Email ID'})
    nv_all_exam_data = nv_all_exam_data.pivot_table(
        'Percentage', ['Email ID'], 'Exam Name')
    nv_all_exam_data = nv_all_exam_data.fillna(0)
    nv_all_exam_data['Avg_Of_NV'] = nv_all_exam_data.apply(
        lambda row: avg_finder(row), axis=1)
    
    Student_all_data_df = pd.merge(
        Student_all_data_df, nv_all_exam_data, on='Email ID', how='left')

    '''Student_all_data_df['Difference_M1_and_NV'] = Student_all_data_df['M1'].apply(pd.to_numeric, errors='coerce') - \
        Student_all_data_df['Avg_Of_NV']'''
    
    if is_improvement_empty == False:
        Student_all_data_df = pd.merge(Student_all_data_df, improvement_df1, on='Email ID', how='left')
    if is_shadow_empty == False:
        Student_all_data_df = pd.merge(Student_all_data_df, shadow_df, on='Email ID', how='left')

    sprint_1_data = 'No Data'
    sprint_2_data = 'No Data'

    sprint_1 = read_sprint(file, 'Sprint_1')
    
    if sprint_1['status'] == 'success':
        sprint_1_data = sprint_1['Stats']
        sprint_1_df = sprint_1['df'].rename({'Marks': 'Sprint_1'}, axis=1)
        Student_all_data_df = pd.merge(
            Student_all_data_df, sprint_1_df, on='Email ID', how='left')

    sprint_2 = read_sprint(file, 'Sprint_2')
    if sprint_2['status'] == 'success':
        sprint_2_data = sprint_2['Stats']
        sprint_2_df = sprint_2['df'].rename({'Marks': 'Sprint_2'}, axis=1)
        Student_all_data_df = pd.merge(
            Student_all_data_df, sprint_2_df, on='Email ID', how='left')

    pre_assessment_data = 'No Data'
    post_assessment_data = 'No Data'

    pre_assessment = read_Assessment(file, 'Pre_Assessment')
    if pre_assessment['status'] == 'success':
        pre_assessment_data = pre_assessment['Stats']
        pre_assessment_df = pre_assessment['df'].rename({'Marks': 'Pre_Assessment'}, axis=1)
        Student_all_data_df = pd.merge(
            Student_all_data_df, pre_assessment_df, on='Email ID', how='left')

    post_assessment = read_Assessment(file, 'Post_Assessment')
    if post_assessment['status'] == 'success':
        post_assessment_data = post_assessment['Stats']
        post_assessment_df = post_assessment['df'].rename({'Marks': 'Post_Assessment'}, axis=1)
        Student_all_data_df = pd.merge(
            Student_all_data_df, post_assessment_df, on='Email ID', how='left')


    b_info = file.worksheet('B_Info')
    t_info = file.worksheet('T_Info')

    t_info = t_info.get_all_records()

    b_info_list = b_info.get_all_values()[0:12]
    b_info = {}
    for i in range(len(b_info_list)):
        if b_info_list[i][0] != '':
            b_info[b_info_list[i][0]] = b_info_list[i][1]


    nv_all_exam_data_json = nv_all_exam_data.round(2).to_dict('records')

    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0]] = exam_details_list[i][1]

    Pass = 0  # greater than 60
    fail = 0  # less than 60 and greater than 0 or 0
    drop_out = 0  # status = Drop Out
    absent = 0  # status =Absent
    not_able_to_submit = 0  # status = Not able to submit
    avg_pass_mark = 0
    avg_fail_mark = 0

    drop_out = len(df.loc[df['Status'] == 'Drop Out'].index)
    df = df[df.Status != 'Drop Out']

    absent = len(df.loc[df['Status'] == 'Absent'].index)
    df = df[df.Status != 'Absent']

    not_able_to_submit = len(
        df.loc[df['Status'] == 'Not able to submit'].index)
    df = df[df.Status != 'Not able to submit']

    fail_df = df.loc[df['Improvement_counter'] == '1']
    fail_df["TOTAL"] = fail_df["TOTAL"].apply(pd.to_numeric)
    avg_fail_mark = fail_df['TOTAL'].mean()
    fail = len(fail_df.index)
    df = df[df.Improvement_counter != 1]

    Pass_df = df.loc[df['Improvement_counter'] == '0']
    avg_pass_mark = Pass_df['TOTAL'].apply(pd.to_numeric).mean()
    Pass_df['TOTAL'] = Pass_df['TOTAL'].apply(pd.to_numeric)
    Pass = len(Pass_df.index)

    pass_percentage = {
        "60-70": 0,
        "70-80": 0,
        "80-90": 0,
        "90-100": 0,
    }

    fail_percentage = {
        "0-25": 0,
        "25-50": 0,
        "50-55": 0,
        "55-59.99": 0,
    }
    if Pass_df.empty == False:

        pass_percentage = {
            "60-70": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 60) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 70)].index) / (Pass_df.shape[0]) * 100, 2),
            "70-80": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 70) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 80)].index) / (Pass_df.shape[0]) * 100, 2),
            "80-90": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 80) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 90)].index) / (Pass_df.shape[0]) * 100, 2),
            "90-100": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 90) & (Pass_df['TOTAL'].apply(pd.to_numeric) <= 100)].index) / (Pass_df.shape[0]) * 100, 2),
        }
    
    if fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 0) & (fail_df['TOTAL'].apply(pd.to_numeric) < 25)].index) / (fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 25) & (fail_df['TOTAL'].apply(pd.to_numeric) < 50)].index) / (fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 50) & (fail_df['TOTAL'].apply(pd.to_numeric) < 55)].index) / (fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 55) & (fail_df['TOTAL'].apply(pd.to_numeric) < 60)].index) / (fail_df.shape[0]) * 100, 2),
        }


    

    Student_all_data_json = Student_all_data_df.to_json(orient = 'records')
    Student_all_data_df.to_csv('new.csv', index = False)
    
    
    trainer_feedback = read_wpr(file_name)
    Student_all_data_df = pd.merge(
            Student_all_data_df, trainer_feedback, on='Email ID', how='left')
    print("trainer_feedback",trainer_feedback)
    Student_all_data_df = Student_all_data_df.fillna(0)
    print(Student_all_data_df)

    Student_all_data_json = Student_all_data_df.round(2).to_dict('records')
    #nv_all_exam_data_json = nv_all_exam_data.round(2).to_dict('records')
    Student_all_data_df.to_csv('new.csv', index = False)

    return Student_all_data_json
     
def read_m1_improvement_report(file):
    try:
        sheet = file.worksheet('CG_M1_Improvement')
    except:
        return {'is_Empty': True, 'df': None, 'stats': None}
    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0].strip()] = exam_details_list[i][1].strip()
    if exam_details['Status'] != 'Done':
        return {'is_Empty': True, 'stats':{'Exam Details': exam_details}}
    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)

    imprv_fail_df = df.loc[df['Improvement_counter'] == '2']
    imprv_fail_df["Improvement_Total"] = imprv_fail_df["Improvement_Total"].apply(pd.to_numeric)
    avg_fail_mark = imprv_fail_df['Improvement_Total'].mean()
    fail = len(imprv_fail_df.index)
    df = df[df.Improvement_counter != 1]

    imprv_pass_df = df.loc[df['Improvement_counter'] == '1']
    avg_pass_mark = imprv_pass_df['Improvement_Total'].apply(pd.to_numeric).mean()
    imprv_pass_df['Improvement_Total'] = imprv_pass_df['Improvement_Total'].apply(pd.to_numeric)
    Pass = len(imprv_pass_df.index)

    pass_percentage = {"60-70":0, "70-80":0, "80-90":0, "90-100":0}
    fail_percentage = {"0-25":0, "25-50":0, "50-55":0, "55-59.99":0}

    if imprv_pass_df.empty == False:

        pass_percentage['60-70'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 60) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 70)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['70-80'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 70) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 80)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['80-90'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 80) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 90)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['90-100'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 90) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) <= 100)].index) / (imprv_pass_df.shape[0]) * 100, 2)

    if imprv_fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 0) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 25)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 25) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 50)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 50) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 55)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 55) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 60)].index) / (imprv_fail_df.shape[0]) * 100, 2)
            
        }
        
    df = df[['Capgemini Email ID', 'Improvement_Total']].copy()
    
    df = df.rename(
        columns={'Capgemini Email ID': 'Email ID'})
    df['Email ID'] = df["Email ID"].str.upper()
    return {
        'stats':{
        "Exam_Details": exam_details,
        "Average_Pass_Marks": round(avg_pass_mark, 2),
        "Average_Fail_Marks": round(avg_fail_mark, 2),
        "Pass": Pass,
        "Fail": fail,
        "Pass_Percentage": pass_percentage,
        "Fail_Percentage": fail_percentage
        },
        "df": df,
        'is_Empty': False
    }

def read_nv_report(sheet, exam_name, df_list_of_nv):
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
    df = pd.DataFrame(data, columns=headers)
    df['Exam Name'] = exam_name
    #df['Email ID'] = df['Email Id']
    df['Email ID'] = df['Email Id'].str.upper()
   
    
    df_list_of_nv.append(df)

    Pass = 0  # greater than 60
    fail = 0  # less than 60 and greater than 0 or 0
    avg_pass_mark = 0
    avg_fail_mark = 0

    fail_df = df.loc[df['Is Passed'] == 'Fail']
    fail_df["Percentage"] = fail_df["Percentage"].apply(pd.to_numeric)
    avg_fail_mark = fail_df['Percentage'].mean()
    fail = len(fail_df.index)
    df = df[df['Is Passed'] != 1]

    Pass_df = df.loc[df['Is Passed'] == 'PASS']
    avg_pass_mark = Pass_df['Percentage'].apply(pd.to_numeric).mean()
    Pass_df['Percentage'] = Pass_df['Percentage'].apply(pd.to_numeric)
    Pass = len(Pass_df.index)
    pass_percentage = {"60-70":0, "70-80":0, "80-90":0, "90-100":0}
    fail_percentage = {"0-25":0, "25-50":0, "50-55":0, "55-59.99":0}

    if Pass_df.empty == False:

        pass_percentage = {
            "60-70": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 60) & (Pass_df['Percentage'].apply(pd.to_numeric) < 70)].index) / (Pass_df.shape[0]) * 100, 2),
            "70-80": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 70) & (Pass_df['Percentage'].apply(pd.to_numeric) < 80)].index) / (Pass_df.shape[0]) * 100, 2),
            "80-90": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 80) & (Pass_df['Percentage'].apply(pd.to_numeric) < 90)].index) / (Pass_df.shape[0]) * 100, 2),
            "90-100": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 90) & (Pass_df['Percentage'].apply(pd.to_numeric) <= 100)].index) / (Pass_df.shape[0]) * 100, 2),
        }
    
    if fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 0) & (fail_df['Percentage'].apply(pd.to_numeric) < 25)].index) / (fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 25) & (fail_df['Percentage'].apply(pd.to_numeric) < 50)].index) / (fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 50) & (fail_df['Percentage'].apply(pd.to_numeric) < 55)].index) / (fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 55) & (fail_df['Percentage'].apply(pd.to_numeric) < 60)].index) / (fail_df.shape[0]) * 100, 2),
        }

    return {
        'Result': 'Exam Done',
        'Total_Count': fail + Pass,
        'Pass': Pass,
        'Fail': fail,
        'Average_Pass_Mark': round(avg_pass_mark, 2),
        'Average_Fail_Mark': round(avg_fail_mark, 2),
        'Pass_Percentage': pass_percentage,
        'Fail_Percentage': fail_percentage,
        'Exam_Details': exam_details,
    }

def read_sprint(file, sheet_name):
    sheet = file.worksheet(sheet_name)
   
    try:

        full_data = sheet.get_all_values()
        exam_details_list = full_data[0:9]
        data = full_data[10:]
        headers = full_data[9]
        df = pd.DataFrame(data, columns=headers)

        exam_details = {}
        for i in range(len(exam_details_list)):
            if exam_details_list[i][0] != '':
                exam_details[exam_details_list[i][0]] = exam_details_list[i][1]

       
        average_marks = df['Marks'].apply(pd.to_numeric).mean()
        df['Email ID'] = df['Email ID'].str.upper()
        #display(df)

        return {
            'status': 'success',
            "Stats":{
                "Exam_Details": exam_details,
                "Average_Marks": round(average_marks, 2)
            },
            "df": df[['Marks', 'Email ID']].copy(),
        }

    except Exception as e:
        # print(e)
        return {
            'status': 'fail',
            "Stats":{
            "Exam_Details": 'Not Available',
            "Average_Marks": 'Not Availble'},
            'Error': f'''{e}'''
        }

def read_shadow_project(file):
    try:
        sheet = file.worksheet('Shadow_Project')
    except:
        return {'is_Empty': True, 'df': None, 'stats': None}
    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0].strip()] = exam_details_list[i][1].strip()
    if exam_details['Status'] != 'Done':
        return {'is_Empty': True, 'stats':{'Exam Details': exam_details}, 'df': None}

    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)
    # display(df)
    df = df[['Marks', 'Email Id']].copy()
    df = df.rename({'Marks': 'Shadow_Project'}, axis=1)

    return {
        'is_Empty': False,
        'stats':{
            'Exam_Details': exam_details, 
            'Absent': len(df.loc[df['Shadow_Project'] == 'AB'].index),
            'Drop_Out': len(df.loc[df['Shadow_Project'] == 'DO'].index),
            'Total': df.shape[0]
        },
        'df': df
    }

def read_Assessment(file, sheet_name):
    sheet = file.worksheet(sheet_name)
   
    try:

        full_data = sheet.get_all_values()
        assessment_details_list = full_data[0:9]
        data = full_data[10:]
        headers = full_data[9]
        df = pd.DataFrame(data, columns=headers)

        assessment_details = {}
        for i in range(len(assessment_details_list)):
            if assessment_details_list[i][0] != '':
                assessment_details[assessment_details_list[i][0]] = assessment_details_list[i][1]

       
        average_marks = df['Marks'].apply(pd.to_numeric).mean()
        df['Email ID'] = df['Email ID'].str.upper()
        #display(df)

        return {
            'status': 'success',
            "Stats":{
                "Assessment_Details": assessment_details,
                "Average_Marks": round(average_marks, 2)
            },
            "df": df[['Marks', 'Email ID']].copy(),
        }

    except Exception as e:
        # print(e)
        return {
            'status': 'fail',
            "Stats":{
            "Assessment_Details": 'Not Available',
            "Average_Marks": 'Not Availble'},
            'Error': f'''{e}'''
        }

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
    student_trainer_data = pd.Series()
    avg_technical_week_wise = {}
    week_no = 0
    if 'Email ID' in headers:
        student_trainer_data = df[['Email ID']].copy()
        for i in headers:
            if i.endswith('-Technical'):
                student_trainer_data_new = df[['Email ID', i]].copy()
                #student_trainer_data = pd.concat([student_trainer_data, student_trainer_data_new], axis=1)
                student_trainer_data = pd.merge(
                    student_trainer_data, student_trainer_data_new, on='Email ID', how='left')
                week_no+=1
        
        cols = student_trainer_data.columns.drop('Email ID')
        student_trainer_data[cols] = student_trainer_data[cols].apply(pd.to_numeric, errors='coerce')
        student_trainer_data['Total'] = student_trainer_data.sum(axis=1)
        student_trainer_data['percentage'] = (student_trainer_data['Total'] / (week_no*5) )*100
    else:
        student_trainer_data = df[['Employee ID']].copy()
        for i in headers:
            if i.endswith('-Technical'):
                student_trainer_data_new = df[['Employee ID', i]].copy()
                student_trainer_data = pd.merge(
                    student_trainer_data, student_trainer_data_new, on='Employee ID', how='left')
                student_trainer_data = student_trainer_data.replace(r'^\s*$', np.NaN, regex=True)
                week_no+=1
            #avg_technical_week_wise[i] = student_trainer_data[i].apply(lambda x: x.isnumeric()).apply(pd.to_numeric).mean()

        cols = student_trainer_data.columns.drop('Employee ID')
        student_trainer_data[cols] = student_trainer_data[cols].apply(pd.to_numeric, errors='coerce')
        student_trainer_data['Total'] = student_trainer_data.sum(axis=1)
        student_trainer_data['percentage'] = (student_trainer_data['Total'] / (week_no*5) )*100  

        student_trainer_data = student_trainer_data.rename({'Employee ID': 'Email ID'}, axis=1)        
            
    #print("student_trainer_data",student_trainer_data)
    
    #print(avg_technical_week_wise)
    #return {'weeklyAvgStats': avg_technical_week_wise, 'allData': df.to_dict('records')}

    return student_trainer_data

#read_consolidated_report('L1')
#read_wpr('L1')

