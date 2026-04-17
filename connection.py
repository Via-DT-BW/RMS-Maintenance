import pyodbc

def connect():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.30.64.44;DATABASE=RMS_Maintenance;"
        "UID=kms_platform;PWD=kms_platform123!"
    )
    return conn