# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables
# This NB handles "From Niraj" data
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

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call unpivot utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot_BU_pack

# COMMAND ----------

# DBTITLE 1,Import Statements
import string
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)   


# COMMAND ----------

#Removing the Junk columns (blank in source)
junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

#triming all the values
currentDf=leadtrailremove(currentDf)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")


# COMMAND ----------

# DBTITLE 1,Dropping the unwanted columns
col_list=['_c1']
print(col_list)
currentDf=currentDf.drop(*col_list)
#display(currentDf)


# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names
#Assign the unwanted trailing character/space to TrimVlaue
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue=" "))   
    
#display(currentDf)   

# COMMAND ----------

# DBTITLE 1,Unpivoting the columns
from pyspark.sql.functions import expr

#list of cols NOT to be unpivoted
other_cols = ["Category"]
currentDf = unpivotdf_bu(currentDf,other_cols)
display(currentDf)


# COMMAND ----------

# DBTITLE 1,Deriving  financial_year & Dated_On columns and renaming the columns
#adding the Dated_on and Financial_Year columns


currentDf=currentDf\
.withColumn("Financial_Year",concat(lit("20"),substring(currentDf['Attribute'],-4,2)).cast('int'))\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
display(currentDf)


# COMMAND ----------

#Final list of columns
final_col_list=currentDf.columns
print(final_col_list)

# COMMAND ----------

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
print("Table Created : ", saveTableName)
print("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

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
 currentDf.write\
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+processName + "_" + tableName)

# COMMAND ----------

