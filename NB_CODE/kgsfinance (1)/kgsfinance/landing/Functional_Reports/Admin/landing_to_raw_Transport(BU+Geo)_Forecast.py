# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables

# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables


# COMMAND ----------

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

#Getting SheetName and year
FileYear= filePath.split('/')[-3]
fileMonth = filePath.split('/')[-2]
if fileMonth in('10','11','12'):
    FinYear=str(int(FileYear)+1)
else:
        FinYear=FileYear
type(FinYear)
print(FinYear)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from pyspark.sql import functions as F
import re,string
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

print("File Path : ",finance_landing_path_url+filePath)
readDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",") \
.option("escape","\"").option("multiLine","true").option("escapeQuotes","true").load(finance_landing_path_url+filePath)
#display(readDf)

columnLen = len(readDf.columns)
print("no of columns:",columnLen)
row = readDf.count()
print("Row ",row)


for col in readDf.columns:
    readDf=readDf.withColumnRenamed(col,replacechar(col))
    
#triming all the values
readDf=leadtrailremove(readDf)
display(readDf)

# COMMAND ----------

#Renaming the repeating columns
currentDf=colNameRepeating(readDf,columnLen)

#converting all columns to string datatype
currentDf=colcaststring(currentDf, currentDf.columns)
display(currentDf)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
 
print("Row ",currentDf.count())
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

print("Row ",currentDf.count())

# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names
for name in currentDf.columns:
        currentDf = currentDf.withColumnRenamed(name,colNameTrim(name,"_"))    

# COMMAND ----------

# DBTITLE 1,Deriving  financial_year and Dated_On columns 
currentdatetime= datetime.now()        
loadDf=currentDf.withColumn("Dated_On",  to_timestamp(lit(currentdatetime))).withColumn("Financial_Year",lit(FinYear))
display(loadDf)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = loadDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(loadDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Load into raw current table
loadDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.raw_curr_"+reportName + "_" +processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

print("Table Created : ", saveTableName)
print("path",finance_raw_curr_savepath_url)

# COMMAND ----------

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

# DBTITLE 1,Load into Raw History Table
loadDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true") \
.option("path",finance_raw_hist_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_hist_"+ reportName + "_"+ processName + "_" + tableName)