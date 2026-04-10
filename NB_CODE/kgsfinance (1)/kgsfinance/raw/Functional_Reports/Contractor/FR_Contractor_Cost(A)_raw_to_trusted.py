# Databricks notebook source
# This NB handles Cost actuals
#Read files from RAW curr the write into Trusted current (overwrite) & Trusted history(append) tables
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

import re,string
import pytz
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
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
currentDf.createOrReplaceTempView("currentDf1")

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Performing transformations & logics
currentDf=currentDf.withColumn("Profit_Cost_Centre_Code",col("Profit_Cost_Centre_Code").cast('int')).withColumn("Function_Code",col("Function_Code").cast('int'))\
.withColumn("Debit_Amt",col("Debit_Amt").cast('double'))\
.withColumn("Credit_Amt",col("Credit_Amt").cast('double'))\
.withColumn("Net_Amount",col("Net_Amount").cast('double'))\
.withColumn("Project_ID",col("Project_ID").cast('int')).withColumn("Account_Code",col("Account_Code").cast('int'))\
.withColumn("Created_By",col("Created_By").cast('int'))\
.withColumn('Creation_Date',when(col('Creation_Date').like('%/%/%'),to_date(from_unixtime(unix_timestamp('Creation_Date', 'd/MM/yyyy')))).otherwise(to_date(from_unixtime(unix_timestamp('Creation_Date', 'dd-MM-yyyy')))))\
.withColumn('GL_Date',when(col('GL_Date').like('%/%/%'),to_date(from_unixtime(unix_timestamp('GL_Date', 'd/MM/yyyy')))).otherwise(to_date(from_unixtime(unix_timestamp('GL_Date', 'dd-MM-yyyy')))))\
.withColumn('DD_UTR_Number',col("DD_UTR_Number").cast('int'))\
.withColumn('Receipt_Number',col('Receipt_Number').cast('int'))\
.withColumn("Customer_PAN_No",col("Customer_PAN_No").cast('int'))\
.withColumn('Date',when(col('Date').like('%/%/%'),to_date(from_unixtime(unix_timestamp('Date', 'd/MM/yyyy')))).otherwise(to_date(from_unixtime(unix_timestamp('Date', 'dd-MM-yyyy')))))\
.withColumn("amount_in_dollar_000",col("amount_in_dollar_000").cast('double'))\
.withColumn("amount_in_dollar_million",col("amount_in_dollar_million").cast('double'))\
.withColumn("Period",F.to_date("Period","MMM yyyy"))


# COMMAND ----------

currentDf.select("Date").distinct().show()

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

# DBTITLE 1,Loading Delta trusted tables to SQL database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})