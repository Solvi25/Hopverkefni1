import pyodbc

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

#EMPTY INPUT TABLE
print('Prepare input table')
cursor.execute('insert into test (id) values(10)')
cursor.commit()

#DISCONNECT
print('Close connections')
cursor.close()
cnxn.close()
print('Done')
