
import sqlite3
import polars as pl
def insert_data(project_status,project_funding):
    print(project_status)
    print(project_funding,'********')
    db = sqlite3.connect('project_database.db')

    cursor = db.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS project_status
                    (id INTEGER PRIMARY KEY,
                    status TEXT,
                    project_steward TEXT,
                    project_partner TEXT,
                    other_contacts TEXT,
                    project_address TEXT,
                    tel TEXT,
                    stewarding_chapter TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS project_funding
                    (id INTEGER PRIMARY KEY,
                    year INTEGER,
                    amount TEXT)''')
    
    # Prepare the INSERT statement
    insert_query = '''
    INSERT INTO project_status (status, project_steward, project_partner, other_contacts, project_address, tel, stewarding_chapter)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    '''

    for row in project_funding:
        cursor.execute("INSERT INTO project_funding (year, amount) VALUES (?, ?)", 
               (row[0], row[1]))

    cursor.execute(insert_query, project_status)
    
    query = 'SELECT * FROM project_status'
    #query2 = 'SELECT * FROM project_funding'
    cursor.execute(query)
    #cursor.execute(query2)
    # Fetch all rows and column names
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    # cursor.execute(query2)
    # rows2 = cursor.fetchall()
    # column_names2 = [description[0] for description in cursor.description]
    # df2 = pl.DataFrame(rows2, schema=column_names2)
    
    
    # Convert the data to a Polars DataFrame
    df = pl.DataFrame(rows, schema=column_names)

    # Display the DataFrame
    print(df)
    # print(df2)
    db.commit()

    db.close()
