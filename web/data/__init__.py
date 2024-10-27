import os
import pandas as pd
import streamlit as st
import os.path

from googleapiclient.discovery import build



# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = st.secrets['GOOGLE_SHEETS']['SHEET_ID']
API_KEY= st.secrets['GOOGLE_SHEETS']['API_KEY']

def authenticate_sheets(api_key):
    return build('sheets', 'v4', developerKey=api_key).spreadsheets()

def get_data_file():

    if(st.secrets['ENV'] == 'dev'):
        f_ptr = open('data/form-response.csv', 'r')
    else:
        sheets = authenticate_sheets(API_KEY)
        
        data = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=st.secrets['GOOGLE_SHEETS']['SHEET_NAME']).execute()
        
        values = data.get('values', [])
        f_ptr = pd.DataFrame(values[1:], columns=values[0])
        
        f_ptr.to_csv('data/form-response-prod.csv', index=False)
        f_ptr = open('data/form-response-prod.csv', 'r')
        del values
        del data
        # f_ptr = None
        
    if(f_ptr):
        return f_ptr
    return None


def rename_columns(df: pd.DataFrame):
    # function to convert all column names to snake case
    # replace new line with space
    
    df = df.dropna(axis=1, how='all')
    # drop columns whose data type is string
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace('\n', ' ')
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    return df
    pass