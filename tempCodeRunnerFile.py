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
conn_str_template = (
    'Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};'
    'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
)
conn_str = conn_str_template.format(
    driver=odbc_driver,
    username=db_username,
    password=db_password,
    dbserver=db_server,
    db=db_name
)

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
    "station",        # REMOVE THIS COLUMN LATER
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

# Remove the 'station' column
df = df.drop(columns=["station"])

# Filter data to include only records from January 1989 onwards
df = df[(df['year'] > 1988) | ((df['year'] == 1988) & (df['month'] >= 1))]

# Convert missing values "." to None
df.replace(".", None, inplace=True)

# Ensure all columns are of the correct data type
numeric_columns = [
    "year", "month", "avg_temp", "avg_max_temp", "max_temp",
    "max_temp_date", "avg_min_temp", "min_temp", "min_temp_date", "humidity",
    "total_precip", "max_precip", "max_precip_date", "pressure", "cloud_cover",
    "sunshine_hours", "wind_speed"
]
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Define SQL table name
table_name = "weather_data"

# Connect to the database
print('Connecting to the database...')
cnxn = pyodbc.connect(conn_str, autocommit=False)
cursor = cnxn.cursor()

# Drop table if it already exists
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")

# Create table in SQL Server (without station column)
sql_create_table = f"""
CREATE TABLE {table_name} (
    year INT,
    month INT,
    avg_temp FLOAT NULL,
    avg_max_temp FLOAT NULL,
    max_temp FLOAT NULL,
    max_temp_date INT NULL,
    avg_min_temp FLOAT NULL,
    min_temp FLOAT NULL,
    min_temp_date INT NULL,
    humidity FLOAT NULL,
    total_precip FLOAT NULL,
    max_precip FLOAT NULL,
    max_precip_date INT NULL,
    pressure FLOAT NULL,
    cloud_cover FLOAT NULL,
    sunshine_hours FLOAT NULL,
    wind_speed FLOAT NULL
);
"""
cursor.execute(sql_create_table)

# Insert data into SQL table
print(f"Inserting data into {table_name}...")
insert_query = f"""
    INSERT INTO {table_name} (
        year, month, avg_temp, avg_max_temp, max_temp, max_temp_date,
        avg_min_temp, min_temp, min_temp_date, humidity, total_precip, max_precip,
        max_precip_date, pressure, cloud_cover, sunshine_hours, wind_speed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for index, row in df.iterrows():
    values = (
        row["year"],
        row["month"],
        row["avg_temp"] if pd.notna(row["avg_temp"]) else None,
        row["avg_max_temp"] if pd.notna(row["avg_max_temp"]) else None,
        row["max_temp"] if pd.notna(row["max_temp"]) else None,
        row["max_temp_date"] if pd.notna(row["max_temp_date"]) else None,
        row["avg_min_temp"] if pd.notna(row["avg_min_temp"]) else None,
        row["min_temp"] if pd.notna(row["min_temp"]) else None,
        row["min_temp_date"] if pd.notna(row["min_temp_date"]) else None,
        row["humidity"] if pd.notna(row["humidity"]) else None,
        row["total_precip"] if pd.notna(row["total_precip"]) else None,
        row["max_precip"] if pd.notna(row["max_precip"]) else None,
        row["max_precip_date"] if pd.notna(row["max_precip_date"]) else None,
        row["pressure"] if pd.notna(row["pressure"]) else None,
        row["cloud_cover"] if pd.notna(row["cloud_cover"]) else None,
        row["sunshine_hours"] if pd.notna(row["sunshine_hours"]) else None,
        row["wind_speed"] if pd.notna(row["wind_speed"]) else None
    )
    
    try:
        cursor.execute(insert_query, values)
    except Exception as e:
        print(f"Error inserting row {index}: {e}")
        print(f"Problematic values: {values}")

# Commit the transaction
cnxn.commit()

# Disconnect
print('Closing connections...')
cursor.close()
cnxn.close()
print('Done')
