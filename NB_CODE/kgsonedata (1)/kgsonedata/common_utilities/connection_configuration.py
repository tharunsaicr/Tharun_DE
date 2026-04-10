# Databricks notebook source
landing_path_url = "/mnt/landinglayermount/"
config_path_url = "/mnt/configmount/"
config_hist_path_url = "/mnt/configmount/history/"
raw_hist_savepath_url="/mnt/rawlayermount/history/"
raw_curr_savepath_url="/mnt/rawlayermount/current/"
raw_stg_savepath_url="/mnt/rawlayermount/staging/"

trusted_hist_savepath_url="/mnt/trustedlayermount/history/"
trusted_curr_savepath_url="/mnt/trustedlayermount/current/"
trusted_stg_savepath_url="/mnt/trustedlayermount/staging/"

output_filepath_url = "/mnt/outputfiles/"

bad_filepath_url = "/mnt/badfiles/"


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