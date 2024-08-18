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
    pattern = r"(\w{3} \d{4})(\w+)(USD|INR)\s+(\d+)"
    print(text)
    matches = re.findall(pattern, text)
    print(matches)
    cumulative_amounts = {}

    if matches:
        for match in matches:
            year_month, city, currency, amount = match
            year = int(year_month[3:])
            if year not in cumulative_amounts:
                cumulative_amounts[year] = {}
            if currency not in cumulative_amounts[year]:
                cumulative_amounts[year][currency] = 0
            cumulative_amounts[year][currency] += int(amount)

    result = {pid: {}}
    for year, currencies in cumulative_amounts.items():
        for currency, amount in currencies.items():
            if year not in result[pid]:
                result[pid][year] = {}
            result[pid][year][currency] = amount

    return result

def extract_status(text):
    pattern = r'Status:(.*?)Project Steward:(.*?)Project Partner...:(.*?)Other Contacts:(.*?)Project Address:(.*?)Tel:(.*?)Stewarding Chapter:(.*?)'
    match = re.search(pattern, text)
    if match:
        return {
            'Status': match.group(1).strip(),
            'Project Steward': match.group(2).strip(),
            'Project Partner': match.group(3).strip(),
            'Other Contacts': match.group(4).strip(),
            'Project Address': match.group(5).strip(),
            'Tel': match.group(6).strip(),
            'Stewarding Chapter': match.group(7).strip()
        }
    return dict()

def extract_data(i):
    soupO = read_and_analyze(i)
    pid = i
    projects = soupO.find_all('div', {"class": "x-accordion-inner"})
    project_glance  = projects[0].text 
    project_status  = projects[1].text
    project_funding = projects[2].text
    project_desc    = projects[3].text
    
    return_value=extract_status(project_status)
    project_funding_value = extract_funding(project_funding,pid)
    print(project_funding_value)
   # projectDB.insert_data(return_value,project_funding_value)
    return return_value,project_funding_value

#def dataframe_dict(dict_to_convert):
    
   # df = pl.DataFrame.from_dict(dict_to_convert, orient='index')
   # return df

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
    # funding_tuple=tuple((year, amount) for year, amount in project_funding.items())
    # print(funding_tuple)
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
for pid in range(1, 1353):
#pid = 11
    return_value, project_funding = extract_data(pid)
    insert_to_database(return_value, project_funding)
    get_funding_from_db()


