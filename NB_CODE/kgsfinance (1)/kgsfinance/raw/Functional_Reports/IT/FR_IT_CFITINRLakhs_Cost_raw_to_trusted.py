# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables
# This CFIT NB is common for Cost Actual, forecast & Plan and HC Actual ,forecast  & Plan
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

# DBTITLE 1,Call Unpivot module
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Reading Raw current data
raw_curr_contractor_url=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName
print(raw_curr_contractor_url)
currentDf= spark.read.format("delta").load(raw_curr_contractor_url)

print(raw_curr_contractor_url)
print("old Row count ",currentDf.count())
print("old Column count ",str(len(currentDf.columns)))

# COMMAND ----------

# DBTITLE 1,Unpivoting the months
import string
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
currentDf=unpivotdf(currentDf)
print("new Row count ",currentDf.count())
print("new Column count ",str(len(currentDf.columns)))

#display(currentDf)

# COMMAND ----------

# DBTITLE 1,Performing transformations & logics
month_list=["OCT","NOV","DEC"]

currentDf=currentDf.withColumnRenamed("Amount","Value").withColumn('MMM',upper(substring(col("Month"),0,3)))
currentDf=currentDf.withColumn("Calendar_Year",when(currentDf['MMM'].isin(month_list),currentDf['Financial_Year']-1).otherwise((currentDf['Financial_Year']))).withColumn("Value",col("Value").cast('double')).withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("MMM"),'MMM'),'MM')))
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