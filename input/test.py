import sqlite3
import csv

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by the db_file"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"SQLite version: {sqlite3.version}")
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)

def insert_records(conn, table_name, records):
    """Insert records into the specified table"""
    sql = f"INSERT INTO {table_name} VALUES ({', '.join(['?']*len(records[0]))})"
    try:
        c = conn.cursor()
        c.executemany(sql, records)
        conn.commit()
    except sqlite3.Error as e:
        print(e)

def main():
    database = r"./sqlite/db/data_quality.db"
    csv_file = r"./input/Final Tables.csv"
    table_name = "final_tables"

    # Create a connection to the SQLite database
    conn = create_connection(database)

    if conn is not None:
        # Create table
        create_table_sql = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                ID integer PRIMARY KEY,
                                Schema_TableName text, 
                                Process_ID integer, 
                                Status text, 
                                Last_Update_Date_Column text, 
                                Primary_identifier_Column varchar,
                                Total_Downstreams integer, 
                                Final_Table_Description text , 
                                Modified datetime, 
                                Modified By text
                            ); """
        create_table(conn, create_table_sql)

        # Read data from CSV file
        with open(csv_file, newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # Skip header row if exists
            data = [tuple(row) for row in csv_reader]

        # Insert records into the database
        insert_records(conn, table_name, data)

        conn.close()
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()
