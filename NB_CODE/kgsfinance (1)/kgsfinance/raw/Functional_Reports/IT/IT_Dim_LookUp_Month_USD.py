# Databricks notebook source
# This NB handles Dim lookup USD data
#Read data from source file and write into Trusted current (overwrite) & Trusted history(append) tables
# calling a Notebook which loads delta sql tables to SQL database

# COMMAND ----------

dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
ProcessName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "File_Year", defaultValue = "")
File_Year = dbutils.widgets.get("File_Year")

dbutils.widgets.text(name = "File_Month", defaultValue = "")
File_Month = dbutils.widgets.get("File_Month")

print(filePath)
print(tableName)
print(ProcessName)
print(ReportName)
print(File_Year)
print(File_Month)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

from delta.tables import *
import pyspark
from datetime import datetime
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","True").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)   


# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")


#Triming the column names
for name in currentDf.columns:
    currentDf = currentDf.withColumnRenamed(name,colNameTrim(name,TrimValue=" "))

#triming all the values
currentDf=leadtrailremove(currentDf)


# COMMAND ----------

# DBTITLE 1,Adding current date, file year & month columns

currentDf=currentDf\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime))) \
.withColumn("File_Year", lit(File_Year)) \
.withColumn("File_Month", lit(File_Month)) 
display(currentDf)

# COMMAND ----------

# DBTITLE 1,Loading Trusted Current Table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ ReportName+"_"+ProcessName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Hist & Current table names & path
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +ProcessName + "_"+ tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName)
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +ProcessName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':ProcessName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':ProcessName,'ReportName':ReportName})