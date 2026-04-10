# Databricks notebook source
# Dimensions used

# CF Function
# CF Headcount Remarks
# Year Classification

# COMMAND ----------

# DBTITLE 1,Importing Required Functions
from pyspark.sql.functions import substring,lower,col, lit,when,concat,trim
from datetime import datetime
import re
from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
currentdatetime= datetime.now()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(filePath)
print(tableName)
print(processName)
print(reportName)


# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Format Headers Data

print("File Path : ",finance_landing_path_url+filePath)

#read the csv file
currentDf =spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)


#replace special characters values with "_"
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
    

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

print(finance_raw_curr_savepath_url)

# COMMAND ----------

# DBTITLE 1,Load Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

print(finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName)

# COMMAND ----------

#raw table name in azure sql db
saveTableName = "kgsfinancedb.raw_curr_"+reportName + "_" +processName + "_"+ tableName
print(saveTableName)
print("path : ",finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)