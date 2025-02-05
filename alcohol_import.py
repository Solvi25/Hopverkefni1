import pyodbc
import pandas as pd

# Database credentials
db_username = 'vgbiUser'
db_password = 'SulliDulli1'
db_server = 'hrvgbi.database.windows.net'
db_name = 'demo'
odbc_driver = 'ODBC Driver 18 for SQL Server'

# SQL Connection String
conn_str_template = 'Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn_str = conn_str_template.format(driver=odbc_driver, username=db_username, password=db_password, dbserver=db_server, db=db_name)

# CONNECT TO DB
print('Connecting to DB...')
cnxn = pyodbc.connect(conn_str, autocommit=False)  # autocommit=False for faster uploads
cursor = cnxn.cursor()

# Path to your CSV file
csv_file_path = "HEI07202_20250204-175510.csv"  # Update if needed

# Read CSV into a Pandas DataFrame
df = pd.read_csv(csv_file_path, delimiter=";", quotechar='"')

# Convert "." to NULL for missing values
df.replace(".", None, inplace=True)

# Define SQL table name
table_name = "alcohol"

# Drop table if it already exists
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")

# Dynamically create table based on CSV columns
column_types = {
    "FLOAT": ["int64", "float64"],
    "VARCHAR(255)": ["object"]
}

# Determine SQL column types
sql_columns = []
for col in df.columns:
    dtype = df[col].dtype.name
    sql_type = next((sql for sql, dtypes in column_types.items() if dtype in dtypes), "VARCHAR(255)")
    sql_columns.append(f"[{col}] {sql_type}")

# Create table query
sql_create_table = f"""
CREATE TABLE {table_name} (
    {', '.join(sql_columns)}
);
"""
cursor.execute(sql_create_table)
print(f"Table '{table_name}' created successfully.")

# Insert Data into SQL Table
print(f"Inserting data into {table_name}...")
insert_query = f"INSERT INTO {table_name} ({', '.join([f'[{col}]' for col in df.columns])}) VALUES ({', '.join(['?' for _ in df.columns])})"

# Insert data row by row
for _, row in df.iterrows():
    cursor.execute(insert_query, tuple(row))

cnxn.commit()  # Commit transaction

# DISCONNECT
print('Closing connections...')
cursor.close()
cnxn.close()
print('Data successfully inserted into "alcohol" table!')
