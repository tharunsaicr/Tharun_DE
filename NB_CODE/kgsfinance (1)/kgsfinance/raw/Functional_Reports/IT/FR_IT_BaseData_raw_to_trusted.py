# Databricks notebook source
# This NB handles Base data
#Read data from RAW curr and write into Trusted current (overwrite) & Trusted history(append) tables
# calling a Notebook which loads delta sql tables to SQL database

# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")


print("tableName   :"+tableName)
print("processName :"+processName)
print("ReportName  :"+ReportName)

# COMMAND ----------

import string
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Reading Raw current data
#Load raw contractor actual cost
raw_curr_contractor_url=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName
print(raw_curr_contractor_url)
currentDf= spark.read.format("delta").load(raw_curr_contractor_url)
#currentDf.createOrReplaceTempView("currentDf1")

# COMMAND ----------

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Performing transformations & logics
currentDf=currentDf.withColumn("Profit_Cost_Centre_Code",col("Profit_Cost_Centre_Code").cast('int'))\
.withColumn("Debit_Amt",col("Debit_Amt").cast('double'))\
.withColumn("Credit_Amt",col("Credit_Amt").cast('double'))\
.withColumn("Net_Amount",col("Net_Amount").cast('double'))\
.withColumn("Project_ID",col("Project_ID").cast('int'))\
.withColumn("Account_Code",col("Account_Code").cast('int'))\
.withColumn("Period",F.to_date("Period","MMM yyyy"))


# COMMAND ----------

display(currentDf)

# COMMAND ----------

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Hist & Current table names & path
saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+processName + "_"+ tableName
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)
curr_table_name= saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+processName+"_"+tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName})
else:
    print("Creating Curr & Hist tables on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})


# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})