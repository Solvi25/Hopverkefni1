import pyodbc
import pandas as pd

# Connect to the database
db_username = 'vgbiUser'
db_password = 'SulliDulli1'
db_server = 'hrvgbi.database.windows.net'
db_name = 'demo'

#ODBC driver
#print(pyodbc.drivers())
odbc_driver = 'ODBC Driver 18 for SQL Server'

#SQL CONN STR
conn_str_template = 'Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn_str = conn_str_template.format(driver=odbc_driver, username=db_username, password=db_password, dbserver=db_server, db=db_name)

#CONNECT TO DB
print('Connect to DB')
cnxn = pyodbc.connect(conn_str, autocommit=False) #autocommit=False for faster upload of data using executemany
cursor = cnxn.cursor()

csv_file_path = "LAU04000_20250203-224302.csv" 

# Read CSV into a Pandas DataFrame
df = pd.read_csv(csv_file_path, delimiter=";", quotechar='"')

# Rename columns to SQL-friendly names
df.columns = ["YearMonth", "Visitala", "MonthlyChange", "YearlyChange"]

# Convert "." to NULL for missing values
df.replace(".", None, inplace=True)

# Define SQL table name
table_name = "hagstofa"

# Drop table if it already exists
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")

# Create table in SQL Server
sql_create_table = f"""
CREATE TABLE {table_name} (
    YearMonth VARCHAR(10),
    Visitala FLOAT,
    MonthlyChange FLOAT,
    YearlyChange FLOAT NULL
);
"""
cursor.execute(sql_create_table)

# Insert Data into SQL Table
print(f"Inserting data into {table_name}...")
insert_query = f"INSERT INTO {table_name} (YearMonth, Visitala, MonthlyChange, YearlyChange) VALUES (?, ?, ?, ?)"

for index, row in df.iterrows():
    cursor.execute(insert_query, row["YearMonth"], row["Visitala"], row["MonthlyChange"], row["YearlyChange"])

cursor.commit()

#DISCONNECT
print('Close connections')
cursor.close()
cnxn.close()
print('Done')
