# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

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

LOCATION,DATE,EMPLOYEE_NUMBER,NAME,DEPARTMENT as TEAM,BU2,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key

# COMMAND ----------

# jdbc_url = "jdbc:sqlserver://your_server;databaseName=your_database"
# jdbc_properties = {"user": "your_username", "password": "your_password"}
 
sql_query = "SELECT * FROM trusted_hist_admin_xport_no_shows"
 
# Read data from SQL Server into a PySpark DataFrame
sql_server_df = spark.read.jdbc(url=jdbcUrl, table=f"({sql_query}) AS subquery", properties=connectionProperties)
sql_server_df = sql_server_df.withColumnRenamed('TEAM','DEPARTMENT').drop('Month_Key')
display(sql_server_df)

# COMMAND ----------

# processName='admin'
# tableName='xport_no_shows'

# sql_server_df.write \
# .mode("append") \
# .format("delta") \
# .option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
# .option("compression","snappy") \
# .saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.trusted_hist_admin_xport_no_shows

# COMMAND ----------



# COMMAND ----------

# jdbc_url = "jdbc:sqlserver://your_server;databaseName=your_database"
# jdbc_properties = {"user": "your_username", "password": "your_password"}
 
sql_query = "SELECT * FROM trusted_hist_admin_xport_planned_vs_actuals"
 
# Read data from SQL Server into a PySpark DataFrame
sql_server_df = spark.read.jdbc(url=jdbcUrl, table=f"({sql_query}) AS subquery", properties=connectionProperties)
sql_server_df = sql_server_df.withColumnRenamed('BU','DEPARTMENT')\
    .withColumnRenamed('Location','LOCATION')\
    .withColumnRenamed('Date','DATE')\
    .withColumnRenamed('VARIANCE','NO_SHOWS')\
    .withColumnRenamed('Planned','PLANNED')\
    .withColumnRenamed('Actual','ACTUAL')\
    .drop('Month_Key')

sql_server_df =sql_server_df.withColumn('PLANNED',sql_server_df.PLANNED.cast('String'))
sql_server_df =sql_server_df.withColumn('NO_SHOWS',sql_server_df.NO_SHOWS.cast('String'))
sql_server_df =sql_server_df.withColumn('ACTUAL',sql_server_df.ACTUAL.cast('String'))
display(sql_server_df)

# COMMAND ----------

processName='admin'
tableName='xport_planned_vs_actuals'

sql_server_df.write \
.mode("append") \
.format("delta") \
.option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- truncate table
# MAGIC select count(1) from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals

# COMMAND ----------

