# Databricks notebook source
# %sh
# curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# sudo apt-get update
# sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import pyodbc
import pandas as pd

# COMMAND ----------

#Prod FINANCE
jdbcHostname = "goaspacspsprdkgs001.database.windows.net"  
jdbcDatabase = "goaspacspdprdkgs003"  
jdbcPort = "1433"  
username = "kgsprdsqldbuser02"  
password = "W$lc0me@123"  
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)  
connectionProperties = {  
  "user" : username,  
  "password" : password,  
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
}  

# COMMAND ----------

# Prod
server = 'goaspacspsprdkgs001.database.windows.net'
database = 'goaspacspdprdkgs003' 
username = 'kgsprdsqldbuser02' 
password = 'W$lc0me@123'
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(
f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)
cursor = conn.cursor()

# COMMAND ----------

