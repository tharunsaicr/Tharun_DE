# Databricks notebook source
# This NB handles Cost plan & Forecast data
#Read files from landing and write into RAW curr(overwrite) & RAW history(append) tables
# Check no. of records in source vs delta tables

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print("filePath   :"+ filePath)
print("tableName  :"+tableName)
print("processName:"+ processName)
print("ReportName :"+ ReportName)

# landinglayer/pending/csv_input/functional_report/contractor/2022/09/CFY Plan.csv
# landinglayer/pending/functional_reports/Contractor/2022/09/CFY Plan.csv

# COMMAND ----------

# DBTITLE 1,Fecting the Year of the file
File_Year= filePath.split('/')[-3]
File_Month = filePath.split('/')[-2]
print(File_Year)
print(File_Month)
if File_Month in ('10','11','12'):
    print('true')
    finYear=int(File_Year)+1
    print("Finyear +1:",finYear)
else:
    finYear=int(File_Year)
    print("finyear:",finYear)
fileYear= 'FY'+str(finYear)[2:4]
print("file_year:",fileYear)

# COMMAND ----------

# DBTITLE 1,Import Statements
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from datetime import datetime
import re,string
import pytz

import pyspark.sql.functions as F
currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(currentDf.columns)
print("no of columns:",columnLen)
row = currentDf.count()
print("Row ",row)
for col in currentDf.columns:
     currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)


# COMMAND ----------

#Renaming the repeating columns
currentDf=colNameRepeating(currentDf,columnLen)
#triming all the values
currentDf=leadtrailremove(currentDf)
#converting all columns to string datatype
currentDf=colcaststring(currentDf, currentDf.columns)
#display(currentDf)


# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names 
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))    
    

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
 
print("Row ",currentDf.count())
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

print("Row ",currentDf.count())

# COMMAND ----------

# DBTITLE 1,Deriving  financial_year and Dated_On columns 
currentdatetime= datetime.now()        
currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime))).withColumn("Financial_Year",lit(finYear).cast('int'))
display(currentDf)

# COMMAND ----------

#Final list of columns
final_col_list=currentDf.columns
print(final_col_list)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Load Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Current table name & path created
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_" +tableName
print("saveTableName :", saveTableName)
print("path :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Comparing the source count with current table count
if(spark._jsparkSession.catalog().tableExists(saveTableName)):
    tableDf = spark.sql("select * from "+saveTableName)

    tableDf_row = tableDf.count()
    print("Row ",tableDf_row)

    tableDf_col = len(tableDf.columns)
    print("Column ",tableDf_col)

    if((row == tableDf_row) & (column == tableDf_col)):
        print("Row and Column Count is Matching!!")
    else:
        print("Row Count is NOT Matching!!")
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Loading Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+processName + "_" + tableName)