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

jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")
jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")
jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")

username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")
password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")

# Using service principal

# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="applicationId")
# password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="appregkgs1datasecret")
  
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {  
  "user" : username,  
  "password" : password,  
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
}

# COMMAND ----------

import pyodbc
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(
f'DRIVER={driver};SERVER={jdbcHostname};DATABASE={jdbcDatabase};UID={username};PWD={password}'
)
cursor = conn.cursor()

# COMMAND ----------

