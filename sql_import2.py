import pyodbc
import pandas as pd
import requests
from io import StringIO

# Database connection parameters
db_username = 'vgbiUser'
db_password = 'SulliDulli1'
db_server = 'hrvgbi.database.windows.net'
db_name = 'demo'
odbc_driver = 'ODBC Driver 18 for SQL Server'

# SQL connection string
conn_str_template = 'Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn_str = conn_str_template.format(driver=odbc_driver, username=db_username, password=db_password, dbserver=db_server, db=db_name)

# URL of the text file
url = "https://www.vedur.is/Medaltalstoflur-txt/Stod_001_Reykjavik.ManMedal.txt"

# Fetch the data from the URL
response = requests.get(url)
if response.status_code == 200:
    data = response.text
else:
    print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
    exit()

# Load data into a Pandas DataFrame
df = pd.read_csv(StringIO(data), delimiter=r"\s+", skiprows=10)

# Rename columns based on the dataset definitions
df.columns = [
    "station",        # "stod" - Weather station number
    "year",           # "ar" - Year
    "month",          # "man" - Month (1-12, 13 = yearly average)
    "avg_temp",       # "t" - Average temperature (°C)
    "avg_max_temp",   # "tx" - Average maximum temperature (°C)
    "max_temp",       # "txx" - Highest recorded temperature (°C)
    "max_temp_date",  # "txx_dag1" - Date of highest temperature
    "avg_min_temp",   # "tn" - Average minimum temperature (°C)
    "min_temp",       # "tnn" - Lowest recorded temperature (°C)
    "min_temp_date",  # "tnn_dag1" - Date of lowest temperature
    "humidity",       # "rh" - Average relative humidity (%)
    "total_precip",   # "r" - Total precipitation (mm)
    "max_precip",     # "rx" - Maximum daily precipitation (mm)
    "max_precip_date",# "rx_dag1" - Date of max precipitation
    "pressure",       # "p" - Average atmospheric pressure (hPa)
    "cloud_cover",    # "nh" - Average cloud cover (0-8)
    "sunshine_hours", # "sun" - Total sunshine hours
    "wind_speed"      # "f" - Average wind speed (m/s)
]

# Convert missing values "." to NULL
df.replace(".", None, inplace=True)

# Ensure all columns are of the correct data type
df["station"] = pd.to_numeric(df["station"], errors='coerce').fillna(0).astype(int)
df["year"] = pd.to_numeric(df["year"], errors='coerce').fillna(0).astype(int)
df["month"] = pd.to_numeric(df["month"], errors='coerce').fillna(0).astype(int)
df["avg_temp"] = pd.to_numeric(df["avg_temp"], errors='coerce').fillna(0).astype(float)
df["avg_max_temp"] = pd.to_numeric(df["avg_max_temp"], errors='coerce').fillna(0).astype(float)
df["max_temp"] = pd.to_numeric(df["max_temp"], errors='coerce').fillna(0).astype(float)
df["max_temp_date"] = pd.to_numeric(df["max_temp_date"], errors='coerce').fillna(0).astype(int)
df["avg_min_temp"] = pd.to_numeric(df["avg_min_temp"], errors='coerce').fillna(0).astype(float)
df["min_temp"] = pd.to_numeric(df["min_temp"], errors='coerce').fillna(0).astype(float)
df["min_temp_date"] = pd.to_numeric(df["min_temp_date"], errors='coerce').fillna(0).astype(int)
df["humidity"] = pd.to_numeric(df["humidity"], errors='coerce').fillna(0).astype(float)
df["total_precip"] = pd.to_numeric(df["total_precip"], errors='coerce').fillna(0).astype(float)
df["max_precip"] = pd.to_numeric(df["max_precip"], errors='coerce').fillna(0).astype(float)
df["max_precip_date"] = pd.to_numeric(df["max_precip_date"], errors='coerce').fillna(0).astype(int)
df["pressure"] = pd.to_numeric(df["pressure"], errors='coerce').fillna(0).astype(float)
df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors='coerce').fillna(0).astype(float)
df["sunshine_hours"] = pd.to_numeric(df["sunshine_hours"], errors='coerce').fillna(0).astype(float)
df["wind_speed"] = pd.to_numeric(df["wind_speed"], errors='coerce').fillna(0).astype(float)

# Define SQL table name
table_name = "weather_data"

# Connect to the database
print('Connecting to the database...')
cnxn = pyodbc.connect(conn_str, autocommit=False)
cursor = cnxn.cursor()

# Drop table if it already exists
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")

# Create table in SQL Server
sql_create_table = f"""
CREATE TABLE {table_name} (
    station INT,
    year INT,
    month INT,
    avg_temp FLOAT,
    avg_max_temp FLOAT,
    max_temp FLOAT,
    max_temp_date INT,
    avg_min_temp FLOAT,
    min_temp FLOAT,
    min_temp_date INT,
    humidity FLOAT,
    total_precip FLOAT,
    max_precip FLOAT,
    max_precip_date INT,
    pressure FLOAT,
    cloud_cover FLOAT,
    sunshine_hours FLOAT,
    wind_speed FLOAT
);
"""
cursor.execute(sql_create_table)

# Insert data into SQL table
print(f"Inserting data into {table_name}...")
insert_query = f"""
    INSERT INTO {table_name} (
        station, year, month, avg_temp, avg_max_temp, max_temp, max_temp_date,
        avg_min_temp, min_temp, min_temp_date, humidity, total_precip, max_precip,
        max_precip_date, pressure, cloud_cover, sunshine_hours, wind_speed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for index, row in df.iterrows():
    values = (
        row["station"], row["year"], row["month"], row["avg_temp"], row["avg_max_temp"], row["max_temp"], row["max_temp_date"],
        row["avg_min_temp"], row["min_temp"], row["min_temp_date"], row["humidity"], row["total_precip"], row["max_precip"],
        row["max_precip_date"], row["pressure"], row["cloud_cover"], row["sunshine_hours"], row["wind_speed"]
    )
    print(values)  # Print the values to inspect them
    cursor.execute(insert_query, values)

# Commit the transaction
cnxn.commit()

#DISCONNECT
print('Close connections')
cursor.close()
cnxn.close()
print('Done')
