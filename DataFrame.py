import polars as pl

class FDF:
    columns = ['pid','month', 'year', 'area', 'currency', 'amount']
    schema = {"pid":pl.Int64, "month":pl.Int64, "year":pl.Int64, "area":pl.Utf8, "currency":pl.Utf8, "amount":pl.Int64}

    def __init__(self):
        pass    
    
def funding_DF(project_funding_value:list[tuple]):
    pass 

    column_names = ['pid','month', 'year', 'area', 'currency', 'amount']
    
    df = pl.DataFrame(project_funding_value, schema=column_names,orient='row')
    print(df)
    df.write_csv(f'DataCSV/funding{pid}.csv')
    column_names2 = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter']
    df2 = pl.DataFrame([project_status_value], schema=column_names2,orient='row')
    print(df2)
    df2.write_csv(f'DataCSV/status{pid}.csv')