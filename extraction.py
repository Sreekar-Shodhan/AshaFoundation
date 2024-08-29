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
            chapter = matches.group(3)
            currency = matches.group(4)
            amount = matches.group(5)
            
            #print(month, year, chapter, currency, amount,'######')
            result.append((pid,month, year, chapter, currency, amount))
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
        extracted_state = extract_state(Project_Address)
      #  print(Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter)
    
    result =  [pid,Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter,extracted_state]
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
    column_names = ['pid','month', 'year', 'chapter', 'currency', 'amount']
    
    df = pl.DataFrame(project_funding_value, schema=column_names,orient='row')
    print(df)
    
    column_names2 = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter','Extracted State']
    df2 = pl.DataFrame([project_status_value], schema=column_names2,orient='row')
    print(df2)
    return project_status_value,project_funding_value

def convert_to_DF():
# Initialize empty lists to store all data
    all_funding_data = []
    all_status_data = []

    # Main loop
    for pid in range(1, 1354):
        project_status_value, project_funding_value = extract_data(pid)
        all_status_data.append(project_status_value)
        all_funding_data.extend(project_funding_value)

    funding_column_names = ['pid', 'month', 'year', 'chapter', 'currency', 'amount']
    status_column_names = ['pid', 'Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter','Extracted State']

    funding_df = pl.DataFrame(all_funding_data, schema=funding_column_names)
    status_df = pl.DataFrame(all_status_data, schema=status_column_names).drop_nulls()

    # Save to CSV
    funding_df.write_csv('DataCSV/consolidated_funding.csv')
    status_df.write_csv('DataCSV/consolidated_status.csv')

    
def cumulative_funding_yearCurr():
    funding_df = pl.read_csv('DataCSV/consolidated_funding.csv')
    funding_df_processed = funding_df.with_columns([
        pl.col('year').cast(pl.Int32),
        pl.col('amount').cast(pl.Float64),
        pl.col('currency').cast(pl.Utf8)
    ])
    
    yearCurr = funding_df_processed.group_by(['year', 'currency']).agg([
        pl.col('amount').sum().alias('total_amount')
    ]).sort(['year', 'currency']) 
    
    yearCurr.write_csv('DataCSV/yearCurr.csv')

def calculate_funding_pidYear():
    # Ensure 'year' and 'amount' are of correct types
    funding_df = pl.read_csv('DataCSV/consolidated_funding.csv')
    funding_df_processed = funding_df.with_columns([
        pl.col('year').cast(pl.Int32),
        pl.col('amount').cast(pl.Float64),
        
    ])
    # Group by year and calculate yearly total
        # Group by 'pid' and 'year', then sum the amounts for each combination
    pid_year_totals = funding_df_processed.group_by(['pid', 'year','chapter','currency']).agg([
        pl.col('amount').sum().alias('yearly_total')
    ]).sort(['pid', 'year'])

    # Calculate the cumulative sum of amounts within each 'pid'
    # funding_cumulative_df = pid_year_totals.with_columns([
    #     pl.col('yearly_total').cum_sum().over('pid').alias('cumulative_amount')
    # ])
    
    print("Cumulative Funding DataFrame:")
    print(pid_year_totals)
    print("****")

    pid_year_totals.write_csv('DataCSV/cumulative_funding.csv')

def extract_state(text):
    namechangedict = {state.upper(): state.upper() for state in [
        "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", 
        "Chhattisgarh", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", 
        "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", 
        "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", 
        "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", 
        "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
        "Andaman and Nicobar Islands", "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu", 
        "Lakshadweep", "Delhi", "Puducherry", 
        "Ladakh", "Jammu and Kashmir", "Uttaranchal" , "Pondicherry", "Orissa" ,
    ]}
    namechangedict["PONDICHERRY"] = "PUDUCHERRY"
    namechangedict["UTTARANCHAL"] = "UTTARAKHAND"
    namechangedict["ORISSA"] = "ODISHA"

    #text = "hello bihar, this is a test"

    pattern = "(?i)" + "|".join(namechangedict.keys())
    match = re.search(pattern, text)
    
    if match:
        extracted_string = match.group()
        extracted_string = namechangedict[extracted_string.upper()]
    else:
        extracted_string = None
    print(f"Extracted string: {extracted_string}")

    return extracted_string

def final_df():
    status_df = pl.read_csv('DataCSV/consolidated_status.csv')
    funding_df = pl.read_csv('DataCSV/cumulative_funding.csv')
    
    final_df = status_df.join(funding_df, on='pid', how='left').select([
    'pid', 'year', 'chapter',  'currency', 'yearly_total', 'Extracted State'
    ]).rename({'Extracted State': 'state'})
    print("Final DataFrame:")
    print(final_df)
    print("****")
    
    final_df.write_csv('DataCSV/final_df.csv')

def state_year():
    final_df = pl.read_csv('DataCSV/final_df.csv')
    # Convert 'year' to integer type if it's not already
    final_df = final_df.with_columns(pl.col('year').cast(pl.Int64))

    final_df = final_df.filter(pl.col('currency') == 'USD')
    # Perform the grouping and aggregation
    new_df = final_df.group_by(['state', 'year']).agg([
        pl.col('yearly_total').sum().alias('state_total_amount')
    ])
    new_df.write_csv('DataCSV/state_year.csv')


def total_year_df():
    new_df = pl.read_csv('DataCSV/state_year.csv')
    
    total_year_df = new_df.groupby('year').agg([
        pl.col('state_total_amount').sum().alias('total_amount')
    ]).sort('year')
    total_year_df.write_csv('DataCSV/total_year.csv')

def percentage_state_df():
    state_year_df = pl.read_csv('DataCSV/state_year.csv')
    total_year_df = pl.read_csv('DataCSV/total_year.csv')
    
    # Join state_year_df with total_year_df
    joined_df = state_year_df.join(total_year_df, on='year', how='left')
    
    # Calculate the percentage of total amount for each state and year
    percentage_df = joined_df.with_columns((pl.col('state_total_amount')/pl.col('total_amount')* 100).alias('percentage'))
    percentage_df = percentage_df.sort(['year','state'])
    percentage_df.write_csv('DataCSV/percentage_year_state.csv')
    
    percentage_df = percentage_df.sort(['state','year'])
    percentage_df.write_csv('DataCSV/percentage_state_year.csv')
    
def state_chapter_df():
    #drop pid, currency only usd, and drop currency, 
    final_df = pl.read_csv('DataCSV/final_df.csv')
    final_df = final_df.filter(pl.col('currency') == 'USD')
    final_df = final_df.select(['year','state', 'chapter', 'yearly_total'])
    final_df = final_df.sort(['state','year'])
    final_df.write_csv('DataCSV/state_chapter.csv')
    
def percentage_state_chapter():
    #out x% y% came from silicon valley 
    state_chapter_df = pl.read_csv('DataCSV/state_chapter.csv')
    
    
    
# def only_states():
#     status_df = pl.read_csv('DataCSV/consolidated_status.csv')
    
#     # Extract states from the 'Project Address' column
#     extracted_states = extract_state(status_df['Project Address'].to_list())
    
#     # Add the extracted states as a new column
#     status_df = status_df.with_columns([
#         pl.Series(name="Extracted State", values=extracted_states)
#     ])
    
#     # Save the updated DataFrame back to CSV
#     status_df.write_csv('DataCSV/consolidated_status_with_states.csv')


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



