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

csv_file_path = "HEI07202_20250204-175510.csv"

# Read CSV into a Pandas DataFrame
df = pd.read_csv(csv_file_path, delimiter=";", quotechar='"')

df.replace({".": 0.0, "": None}, inplace=True)

dtype_mapping = {
    "Alkóhóllítrar": "string",
    "Ár": "int64",
    "Alls": "float64",
    "Bjór": "float64",
    "Létt vín": "float64",
    "Sterk vín": "float64",
}

# Convert numeric columns properly
for col, dtype in dtype_mapping.items():
    if col in df.columns:
        if dtype != "string":  # Convert only numeric columns
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)


table_name = "alcohol"

# Drop table if it already exists
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")

sql_columns = [
    "[Alkóhóllítrar] VARCHAR(255)",
    "[Ár] INT",
    "[Alls] FLOAT",
    "[Bjór] FLOAT",
    "[Létt vín] FLOAT",
    "[Sterk vín] FLOAT"
]

# Create table query
sql_create_table = f"CREATE TABLE {table_name} ({', '.join(sql_columns)});"
cursor.execute(sql_create_table)

# Insert Data into SQL Table
print(f"Inserting data into {table_name}...")
insert_query = f"INSERT INTO {table_name} ({', '.join([f'[{col}]' for col in dtype_mapping.keys()])}) VALUES ({', '.join(['?' for _ in dtype_mapping.keys()])})"

# Insert data row by row
for _, row in df.iterrows():
    cursor.execute(insert_query, tuple(row[col] for col in dtype_mapping.keys()))

cnxn.commit()  # Commit transaction

# DISCONNECT
print('Closing connections...')
cursor.close()
cnxn.close()
