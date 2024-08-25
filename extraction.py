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
            
            #print(month, year, area, currency, amount,'######')
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
      #  print(Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter)
    
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
  #  print(project_funding_value)
   # print(project_status_value)
    column_names = ['pid','month', 'year', 'area', 'currency', 'amount']
    
    df = pl.DataFrame(project_funding_value, schema=column_names,orient='row')
   # print(df)
    
    column_names2 = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter']
    df2 = pl.DataFrame([project_status_value], schema=column_names2,orient='row')
   # print(df2)
    
    return project_status_value,project_funding_value


# Initialize empty lists to store all data
all_funding_data = []
all_status_data = []

# Main loop
for pid in range(1, 1354):
    project_status_value, project_funding_value = extract_data(pid)
    all_status_data.append(project_status_value)
    all_funding_data.extend(project_funding_value)

funding_column_names = ['pid', 'month', 'year', 'area', 'currency', 'amount']
status_column_names = ['pid', 'Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter']

funding_df = pl.DataFrame(all_funding_data, schema=funding_column_names)
status_df = pl.DataFrame(all_status_data, schema=status_column_names)

def calculate_cumulative_funding():
    # Ensure 'year' and 'amount' are of correct types
    funding_df_processed = funding_df.with_columns([
        pl.col('year').cast(pl.Int32),
        pl.col('amount').cast(pl.Float64)
    ])


    # Group by year and calculate yearly total
        # Group by 'pid' and 'year', then sum the amounts for each combination
    pid_year_totals = funding_df_processed.group_by(['pid', 'year']).agg([
        pl.col('amount').sum().alias('yearly_total')
    ]).sort(['pid', 'year'])

    # Calculate the cumulative sum of amounts within each 'pid'
    funding_cumulative_df = pid_year_totals.with_columns([
        pl.col('yearly_total').cum_sum().over('pid').alias('cumulative_amount')
    ])
    
    print("Cumulative Funding DataFrame:")
    print(funding_cumulative_df)
    print("****")

    funding_cumulative_df.write_csv('DataCSV/cumulative_funding.csv')

# Save to CSV
funding_df.write_csv('DataCSV/consolidated_funding.csv')
status_df.write_csv('DataCSV/consolidated_status.csv')

# Calculate and save cumulative funding
calculate_cumulative_funding()
# def insert_to_database(project_status,project_funding):
#     data_tuple = (
#         project_status['Status'],
#         project_status['Project Steward'],
#         project_status['Project Partner'],
#         project_status['Other Contacts'],
#         project_status['Project Address'],
#         project_status['Tel'],
#         project_status['Stewarding Chapter']
#     )
    
#     funding_tuple = tuple(
#                 (pid, tuple((year, currency, amount) for year, details in funding_data.items() for currency, amount in details.items()))
#                 for pid, funding_data in project_funding.items()
#             )

# # Example: Output of funding_tuples
#     print(funding_tuple)
#     projectDB.insert_data(data_tuple,funding_tuple)    
#     return data_tuple,funding_tuple


# for pid in range(1, 1354):
# #pid = 11
#     project_status_value, project_funding_value = extract_data(pid)
# #insert_to_database(return_value, project_funding)



