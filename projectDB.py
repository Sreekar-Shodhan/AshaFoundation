
import sqlite3
import polars as pl

def insert_data(project_status, project_funding):
   # print(project_status)
   # print(project_funding, '********')

    db = sqlite3.connect('project_database.db')
    cursor = db.cursor()

    # Create the project_status table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS project_status
                    (id INTEGER PRIMARY KEY,
                    status TEXT,
                    project_steward TEXT,
                    project_partner TEXT,
                    other_contacts TEXT,
                    project_address TEXT,
                    tel TEXT,
                    stewarding_chapter TEXT)''')

    # Create the project_funding table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS project_funding
                    (id INTEGER PRIMARY KEY,
                    pid INTEGER,
                    year INTEGER,
                    currency TEXT,
                    amount INTEGER)''')

    # Insert data into the project_status table
    insert_status_query = '''
        INSERT INTO project_status (status, project_steward, project_partner, other_contacts, project_address, tel, stewarding_chapter)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(insert_status_query, project_status)

    # Insert data into the project_funding table
    insert_funding_query = '''
        INSERT INTO project_funding (pid, year, currency, amount)
        VALUES (?, ?, ?, ?)
    '''

    # Iterate through the outer tuple for project_funding
    for pid, funding_data in project_funding:
        # Iterate through the inner tuples
        for year, currency, amount in funding_data:
            cursor.execute(insert_funding_query, (pid, year, currency, amount))

    # Query the project_status table
    query = 'SELECT * FROM project_status'
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    # Convert the data to a Polars DataFrame
   # df = pl.DataFrame(rows, schema=column_names)

    # Display the DataFrame
   # print(df)

    # Commit the changes and close the database connection
    db.commit()
    db.close()
