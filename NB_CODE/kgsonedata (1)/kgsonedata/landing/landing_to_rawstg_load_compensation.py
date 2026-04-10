# Databricks notebook source
# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(filePath)
print(tableName)
print(processName)


# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,Import libraries
from datetime import datetime
from pyspark.sql.functions import col, lit,lower,udf
import io
import pandas as pd
import msoffcrypto
import urllib
import requests
import pyspark.sql.functions as f
from bs4 import BeautifulSoup
from pyspark.sql.types import ArrayType,StringType,BooleanType
from pyspark.sql import functions as F
import re
currentdatetime= datetime.now()

# COMMAND ----------

fileNameStr = filePath.split('/')[-1].split('.')[0]

last_underscore_index = fileNameStr.rfind("_")

if last_underscore_index != -1:
    fileName = fileNameStr[:last_underscore_index]
    print(fileName)
else:
    print("Underscore not found in the string.")

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
fileYear = fileDate[2:4]
password_fileYear = str(fileDate[2:4])
print(password_fileYear)
fileMonth = fileDate[4:6]
# print(fileYear)
# print(fileMonth)
if(fileMonth in ('10','11','12')):
    FY = str(int(fileDate[0:4])+1)
    Rating_FY = "FY"+fileDate[2:4]
    # print(FY)
    # print(Rating_FY)
else:
    FY = fileDate[0:4]
    Rating_FY = "FY"+str(int(fileDate[2:4])-1)
    # print(FY)
    # print(Rating_FY)

# COMMAND ----------

def replace(column, value):
    return when(column == value, lit(None)).otherwise(column)

# COMMAND ----------

filePath="dbfs:"+landing_path_url+filePath

# print(landing_path_url)
print(filePath)

# COMMAND ----------

# DBTITLE 1,Reading the password txt file
# html_file_path = 'dbfs:/mnt/landinglayermount/pending/compensation/comp_passwd_'+fileDate+'.txt'
# print(html_file_path)
# html_content_df=spark.read.text(html_file_path)
if fileName == 'FinanceMetrics':
    extracted_password = 'kgsctc'+password_fileYear
if fileName == 'YEC':
    extracted_password = 'yec@'+password_fileYear
if fileName == 'KGS_Ratings':
    extracted_password = 'yec@'+password_fileYear
print(extracted_password)

# COMMAND ----------

sheetName= "'" +tableName+"'"
print(sheetName)

# COMMAND ----------

try:
    # Your code to read the Excel workbook
    currentDf = spark.read.format("com.crealytics.spark.excel")\
                .option("inferschema","false")\
                .option("header","true")\
                .option("dataAddress",sheetName+"!A1")\
                .option("tempFileThreshold", 10000000)\
                .option("workbookPassword",extracted_password)\
                .option("maxByteArraySize",150000000)\
                .option("inferDateTimeFormat","false")\
                .load(filePath)
except (Exception, RuntimeError) as e:
    if "Unknown sheet "+tableName in str(e):
        print("If")
        # If the error is due to an unknown sheet, exit the notebook
        spark.sql("truncate table kgsonedatadb.raw_curr_"+ processName + "_" +tableName)
        dbutils.notebook.exit("Exiting notebook as sheet '"+tableName+"' not present in the xlsx workbook.")
    else:
        print("else")
        # If it's a different AnalysisException, let it propagate
        raise e

# COMMAND ----------

for col in currentDf.columns:
    if processName == 'compensation':
        if("Aggregated HC" in col):
            print(col)
            currentDf=currentDf.withColumnRenamed(col,'Aggregated HC')
        if("Aggregated Monthly Gross Amount" in col):
            print(col)
            currentDf=currentDf.withColumnRenamed(col,'Aggregated Monthly Gross Amount')
        if("Aggregated Annual CTC" in col):
            print(col)
            currentDf=currentDf.withColumnRenamed(col,'Aggregated Annual CTC')

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

currentDf=colcaststring(currentDf,currentDf.columns)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

if tableName == 'KGS_Ratings':
    currentDf = currentDf.withColumn("FY",lit(Rating_FY))
else:
    currentDf = currentDf.withColumn("FY",lit(FY))

currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))

if 'File_Date' not in currentDf.columns:
    print("File_Date does not exists")
    currentDf=currentDf.withColumn("File_Date", lit(fileDate))
else:
    print("File_Date exists")

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

if tableName == 'KGS_Analytics':
    tableName ='finance_metrics'
if tableName == 'Joiners':
    tableName = 'finance_metrics_joiners'
if tableName == 'Leavers':
    tableName = 'finance_metrics_leavers'
if tableName == 'KGS_Ratings':
    tableName = 'employee_ratings'
print(tableName)

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Load Raw Staging Data

currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from kgsonedatadb.raw_stg_talent_acquisition_CWK
# MAGIC
# MAGIC -- select * from kgsonedatadb.raw_hist_talent_acquisition_cwk

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

saveTableName = "kgsonedatadb.raw_stg_"+processName + "_"+ tableName
#print(tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from kgsonedatadb.raw_stg_talent_acquisition_FTE

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})

# COMMAND ----------

