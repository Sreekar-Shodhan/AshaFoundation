from bs4 import BeautifulSoup
from collections import defaultdict
import re
import polars as pl
import projectDB
import sqlite3
def read_and_analyze(i):
    with open(f"saveData/ashasup_{i}.html", "r") as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    return soup


def extract_funding(text, pid: int):
    parts = re.findall(r'(.*?(?:USD|INR|EUR|GBP|CHF|CAD)\s+\d+)', text)
    print(text)
    result = []
    for record in parts:
        pattern = r'(\w+)\s+(\d+)([A-Za-z\./\-\s]+)(USD|INR|EUR|GBP|CHF|CAD)\s+(\d+)'
        matches = re.match(pattern, record)

        if matches:
            month = matches.group(1)
            year = matches.group(2)
            area = matches.group(3)
            currency = matches.group(4)
            amount = matches.group(5)
            
            print(month, year, area, currency, amount,'######')
            result.append((pid,month, year, area, currency, amount))
    return result


def extract_status(text,pid):
    pattern = r'Status:(.*?)Project Steward:(.*?)Project Partner...:(.*?)Other Contacts:(.*?)Project Address:(.*?)Tel:(.*?)Stewarding Chapter:(.*?)'
    match = re.search(pattern, text)
    if match:
        Status = match.group(1)
        Project_Steward = match.group(2)
        Project_Partner = match.group(3)
        Other_Contacts = match.group(4)
        Project_Address = match.group(5)
        Tel = match.group(6)
        Stewarding_Chapter = match.group(7)
        print(Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter)
    
    result =  [pid,Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter]
    return result 

def extract_data(i):
    soupO = read_and_analyze(i)
    pid = i
    projects = soupO.find_all('div', {"class": "x-accordion-inner"})
    project_glance  = projects[0].text 
    project_status  = projects[1].text
    project_funding = projects[2].text
    project_desc    = projects[3].text
    
    project_status_value=extract_status(project_status,pid)
    project_funding_value = extract_funding(project_funding,pid)
    print(project_funding_value)
    print(project_status_value)
    column_names = ['pid','month', 'year', 'area', 'currency', 'amount']
    
    df = pl.DataFrame(project_funding_value, schema=column_names,orient='row')
    print(df)
    column_names2 = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter']
    df2 = pl.DataFrame([project_status_value], schema=column_names2,orient='row')
    print(df2)
   # dataframe_dict(project_funding_value)
   # projectDB.insert_data(return_value,project_funding_value)
    return project_status_value,project_funding_value

def dataframe_dict(tuple_to_convert):
    print(type(tuple_to_convert))
    # Create DataFrame
    column_names = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter']
    df = pl.DataFrame(tuple_to_convert, schema=column_names)
    #df = pl.DataFrame(dict_to_convert)
    print(df)
    return df

def insert_to_database(project_status,project_funding):
    data_tuple = (
        project_status['Status'],
        project_status['Project Steward'],
        project_status['Project Partner'],
        project_status['Other Contacts'],
        project_status['Project Address'],
        project_status['Tel'],
        project_status['Stewarding Chapter']
    )
    
    funding_tuple = tuple(
                (pid, tuple((year, currency, amount) for year, details in funding_data.items() for currency, amount in details.items()))
                for pid, funding_data in project_funding.items()
            )

# Example: Output of funding_tuples
    print(funding_tuple)
    projectDB.insert_data(data_tuple,funding_tuple)    
    return data_tuple,funding_tuple


def get_data_from_db():
    db = sqlite3.connect('project_database.db')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM project_status")
    project_status = cursor.fetchall()
    # cursor.execute("SELECT * FROM project_funding")
    # project_funding = cursor.fetchall()
      # Convert the data to a Polars DataFrame
    column_names = [description[0] for description in cursor.description]
   # df = pl.DataFrame(project_status, schema=column_names)

    # Display the DataFrame
    #print(df)
    
    return project_status,project_funding
def get_funding_from_db():
    db = sqlite3.connect('project_database.db')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM project_funding")
    project_funding = cursor.fetchall()
    # cursor.execute("SELECT * FROM project_funding")
    # project_funding = cursor.fetchall()
      # Convert the data to a Polars DataFrame
    column_names = [description[0] for description in cursor.description]
   # df = pl.DataFrame(project_funding, schema=column_names)

    # Display the DataFrame
   # print(df,'***')
for pid in range(1, 1354):
#pid = 11
    project_status_value, project_funding_value = extract_data(pid)
#insert_to_database(return_value, project_funding)
#get_funding_from_db()


