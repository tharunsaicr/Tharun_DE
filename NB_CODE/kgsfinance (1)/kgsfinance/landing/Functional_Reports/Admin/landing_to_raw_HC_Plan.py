# Databricks notebook source
# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables
#Read files from landing and write into RAW curr(overwrite) & history(append) tables



# COMMAND ----------

# DBTITLE 1,Importing Required Functions
from datetime import datetime
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from pyspark.sql import functions as F
import re,string
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

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

# #Getting SheetName and year
FileYear= filePath.split('/')[-3]
fileMonth = filePath.split('/')[-2]
if fileMonth in('10','11','12'):
    FinYear=str(int(FileYear)+1)
else:
        FinYear=FileYear
type(FinYear)
print(FinYear)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentdatetime= datetime.now()
print("File Path : ",finance_landing_path_url+filePath)

#read the csv file
readDf =spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

#column length
columnLen = len(readDf.columns)
print("no of columns:",columnLen)

#replace special characters values with "_"
for col in readDf.columns:
    readDf=readDf.withColumnRenamed(col,replacechar(col))
    

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

readDf=readDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in readDf.columns])
readDf = readDf.dropna("all")

# COMMAND ----------

# DBTITLE 1,Dropping Empty Columns and Renaming repeated Columns
import string

#Renaming the repeating columns
current_df=colNameRepeating(readDf,columnLen)

#triming all the space values at start and of column values
current_df=leadtrailremove(current_df)

#Dropping empty columns
drop_col_df=current_df.select(current_df.colRegex("`_c_.+`"))
# display(drop_df)
col_list=drop_col_df.columns
# print(col_list)

load_df=current_df.drop(*col_list,"_c")

#remove "_" at start and end of column namwe
for name in load_df.columns:
        load_df = load_df.withColumnRenamed(name,colNameTrim(name,"_"))
display(load_df)


# COMMAND ----------

# DBTITLE 1,Deriving Month,Year and Financial Year Column
#derive  Financial year and dated-on column from period

final_df=load_df.withColumn('Financial_Year',lit(FinYear)).withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
display(final_df)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = final_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(final_df.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Load to Raw Current Delta Table
final_df.write \
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

# DBTITLE 1,Load to Raw History Table
 final_df.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+reportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ reportName + "_"+ processName + "_" + tableName)

# COMMAND ----------

