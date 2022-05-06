import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np
import math
from pymongo import MongoClient
from .studentAllData import read_consolidated_report_new
import re

client = MongoClient("mongodb://aditya:aditya@44.199.59.134:27017/admin")
mydb = client["TestPrepnovelvista"]
mycol = mydb["GamificationConfiguration"]
result = mycol.find({"name" : "Set1"})
list_cur = list(result)
print(list_cur)
for x in list_cur:
    print(x['weightage'])
    for y in x['weightage']:
        if(y['key'] == 'M1'):
            LB_Weightage_M1 = y['value']
            print(LB_Weightage_M1,'LB_Weightage_M1')
        
        if(y['key'] == 'NV'):
            LB_Weightage_NV = y['value']
            print(LB_Weightage_NV,'LB_Weightage_NV')
        
        if(y['key'] == 'Technical'):
            LB_Weightage_Technical = y['value']
            print(LB_Weightage_Technical,'LB_Weightage_Technical')

        if(y['key'] == 'Sprint1'):
            LB_Weightage_Sprint1 = y['value']
            print(LB_Weightage_Sprint1,'LB_Weightage_Sprint1')
        
        if(y['key'] == 'Sprint2'):
            LB_Weightage_Sprint2 = y['value']
            print(LB_Weightage_Sprint2,'LB_Weightage_Sprint2')

        if(y['key'] == 'pre_soft_skill'):
            LB_Weightage_pre_soft_skill = y['value']
            print(LB_Weightage_pre_soft_skill,'LB_Weightage_pre_soft_skill')

        if(y['key'] == 'post_soft_skill'):
            LB_Weightage_post_soft_skill = y['value']
            print(LB_Weightage_post_soft_skill,'LB_Weightage_post_soft_skill')

        if(y['key'] == 'Multiplication_factor'):
            Multiplication_factor = y['value']
            print(Multiplication_factor,'Multiplication_factor')



def avg_finder(x):
    CG_Weightage = LB_Weightage_NV/100
    LB_Weightage_NV1 = CG_Weightage*(100/60)
    LB_Weightage_NV2 = int(Multiplication_factor * LB_Weightage_NV1)
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
        return (nv_avg_marks * LB_Weightage_NV2 )

def m1_point(x):
    CG_Weightage = LB_Weightage_M1/100
    LB_Weightage = CG_Weightage*(100/60)
    LB_Weightage_M2 = int(Multiplication_factor * LB_Weightage)
    print("TData",x)
    mark_list = []

    if not(math.isnan(x['M1'])) and int(x['M1']) >= 60 :
        mark_list.append(x['M1'])
    else:
        mark_list.append(x['Improvement_Total_x'])

    #print('mark_list', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage_M2 )

def Pre_Assessment_point(x):
    CG_Weightage = LB_Weightage_pre_soft_skill/100
    LB_Weightage_pre_ss = CG_Weightage*(100/60)
    LB_Weightage_Pre_Assessment = int(Multiplication_factor * LB_Weightage_pre_ss)
    mark_list = []
    mark_list.append(x['Pre_Assessment'])

    #print('mark_list Pre_Assessment', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage_Pre_Assessment)

def Post_Assessment_point(x):
    CG_Weightage = LB_Weightage_post_soft_skill/100
    LB_Weightage_post_ss = CG_Weightage*(100/60)
    LB_Weightage_Post_Assessment = int(Multiplication_factor * LB_Weightage_post_ss)
    mark_list = []
    mark_list.append(x['Post_Assessment'])

    #print('mark_list Post_Assessment', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage_Post_Assessment)

def Sprint1_point(x):
    CG_Weightage = LB_Weightage_Sprint1/100
    LB_Weightage = CG_Weightage*(100/60)
    LB_Weightage_Sprint = int(Multiplication_factor * LB_Weightage)
    mark_list = []
    mark_list.append(x['Sprint_1'])

    #print('mark_list Sprint_1', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage_Sprint)

def Sprint2_point(x):
    CG_Weightage = LB_Weightage_Sprint2/100
    LB_Weightage = CG_Weightage*(100/60)
    LB_Weightage_Sprint = int(Multiplication_factor * LB_Weightage)
    mark_list = []
    mark_list.append(x['Sprint_2'])

    #print('mark_list Sprint_2', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage_Sprint)

def Technical_point(x):
    LB_Weightage = LB_Weightage_Technical
    #MF = 100
    #LB_Weightage_Sprint2 = int(MF * LB_Weightage)
    mark_list = []
    mark_list.append(x['percentage'])
    #print('mark_list percentage', mark_list)
    if len(mark_list) == 0:
        return 0
    else:
        return (mark_list[0] * LB_Weightage)

def leader_point(file_name):
    all_data = read_consolidated_report_new(file_name)
    all_data_df = pd.DataFrame(all_data)
    
    all_data_df['M1'] = all_data_df['M1'].apply(pd.to_numeric, errors='coerce')
    all_data_df['Improvement_Total_x'] = all_data_df['Improvement_Total_x'].apply(pd.to_numeric, errors='coerce')
    all_data_df['Pre_Assessment'] = all_data_df['Pre_Assessment'].apply(pd.to_numeric, errors='coerce')
    all_data_df['Post_Assessment'] = all_data_df['Post_Assessment'].apply(pd.to_numeric, errors='coerce')
    all_data_df['Sprint_1'] = all_data_df['Sprint_1'].apply(pd.to_numeric, errors='coerce')
    all_data_df['Sprint_2'] = all_data_df['Sprint_2'].apply(pd.to_numeric, errors='coerce')
    for i in all_data_df.columns:
            if i.endswith('-Technical'):
                all_data_df[i] = all_data_df[i].apply(pd.to_numeric, errors='coerce')

    all_data_df['M1_points'] = all_data_df.apply(
        lambda row: m1_point(row), axis=1)
    
    all_data_df['NV_points'] = all_data_df.apply(
        lambda row: avg_finder(row), axis=1)

    all_data_df['Pre_Assessment_point'] = all_data_df.apply(
        lambda row: Pre_Assessment_point(row), axis=1)

    all_data_df['Post_Assessment_point'] = all_data_df.apply(
        lambda row: Post_Assessment_point(row), axis=1)
    
    all_data_df['Sprint1_point'] = all_data_df.apply(
        lambda row: Sprint1_point(row), axis=1)
   
    all_data_df['Sprint2_point'] = all_data_df.apply(
        lambda row: Sprint2_point(row), axis=1)
    
    all_data_df['Technical_point'] = all_data_df.apply(
         lambda row: Technical_point(row), axis=1)
    
    all_data_df['leader_point'] = all_data_df['M1_points'] + all_data_df['NV_points'] + all_data_df['Pre_Assessment_point'] + all_data_df['Post_Assessment_point'] + all_data_df['Sprint1_point'] + all_data_df['Sprint2_point'] + all_data_df['Technical_point']
    all_data_df['leader_point'] = all_data_df['leader_point'].apply(pd.to_numeric, errors='coerce')
    leadership_points = all_data_df.to_json(orient="records")
    result = json.loads(leadership_points)
    
    all_data_df.to_csv('marks.csv', index = False)

    client = MongoClient("mongodb://aditya:aditya@44.199.59.134:27017/admin")
    mydb = client["TestPrepnovelvista"]
    mycol = mydb["ActivityDescripter"]
    all_data = result
    for x in all_data:
        x['leader_point'] = round(x['leader_point'])
        loginId = x['Candidate Registered Email ID']
        # loginId1 = re.compile('.*x["Candidate Registered Email ID"].*', re.IGNORECASE)
        print("loginId1",loginId)
        updateData = { "leadershipPoints" : x['leader_point']}
        print("loginId1",updateData)
        mycol.update_one({'loginId': loginId}, {"$set" : updateData})
        #mycol.update_one({'loginId': x['Candidate Registered Email ID']}, {"$set" : updateData})
    print("leadership point upadted for",file_name)
    
    return leadership_points

#leader_point('b4')