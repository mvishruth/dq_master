import sqlite3
import csv
import sql.create_tables as ct

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by the db_file"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"SQLite version: {sqlite3.version}")
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql,table_name):
    """ create a table from connection and sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try: 
        c = conn.cursor()
        create_table_sql = create_table_sql.format(table_name = table_name)
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)

def insert_records(conn, table_name, records):
    """Insert records into the specified table"""
    sql = f"INSERT INTO {table_name} VALUES ({', '.join(['?']*len(records[0]))})"
    print(sql)
    try:
        c = conn.cursor()
        c.executemany(sql, records)
        conn.commit()
    except sqlite3.Error as e:
        print(e)

# Read Data from CSV
def insert_csv(conn, table_name, csv_file):        
    with open(csv_file, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)  # Skip header row if exists
        data = [tuple(row) for row in csv_reader]

        # Insert records into the database
        insert_records(conn, table_name, data)
        conn.close()

# def upsert_records(conn, table_name, csv_file):
#     """Upsert records from CSV file into SQLite table (SCD Type 2)"""
#     cursor = conn.cursor()

#     with open(csv_file, 'r', newline='') as csvfile:
#         csv_reader = csv.reader(csvfile)
#         next(csv_reader)  # Skip header row if exists
#         for row in csv_reader:
#             # Check if record exists in the table
#             cursor.execute("SELECT * FROM {table_name} WHERE table_id = ?", (row[0],))
#             existing_record = cursor.fetchone()

#             # Get current timestamp
#             current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#             if existing_record:
#                 # Record already exists, update end timestamp
#                 cursor.execute("UPDATE {table_name} SET scd_to = ? WHERE id = ? AND scd_to IS NULL", (NULL, row[0]))
                
#                 # Insert new version of the record
#                 cursor.execute("INSERT INTO {table_name} VALUES ({', '.join(['?']*len(records[0]))}))
#             else:
#                 # Insert new record
#                 cursor.execute("INSERT INTO {table_name} (id, name, value, scd_from) VALUES (?, ?, ?, ?)", (row[0], row[1], row[2], current_timestamp))

#     conn.commit()

def main():
    database = r"./sqlite/db/data_quality.db"

    # Create a connection to the SQLite database
    conn = create_connection(database)
    
    if conn is not None:

        # create final table
        # Create a connection to the SQLite database
        conn = create_connection(database)
        table_name = 'tmp_final_tables'
        create_table(conn, ct.sql_create_final_tables, table_name)
        print("final_tables created")
        insert_csv(conn,  table_name, "./input/Final Tables.csv")

        # create process info
        # Create a connection to the SQLite database
        conn = create_connection(database)
        table_name = 'tmp_process_info'
        create_table(conn, ct.sql_create_process_info,table_name)
        print("process info created")
        insert_csv(conn, table_name , "./input/Process info.csv")

    else:
        print ("Error connecting")
    
if __name__ == '__main__':
    main()