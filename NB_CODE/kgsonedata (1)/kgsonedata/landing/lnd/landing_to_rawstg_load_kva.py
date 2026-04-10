# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables

# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables


# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

# dbutils.widgets.text(name = "FileName", defaultValue ="")
# FileName = dbutils.widgets.get("FileName")

print(filePath)
print(tableName)
print(processName)
#print(FileName)

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate) #yymmdd_hhmmss

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

#Extract filename & date - switch case for each file

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit,to_timestamp
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# DBTITLE 1,Format Headers Data
# landing_path="/mnt/landinglayermount/"
print("File Path : ",landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("skipRows", 2).option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)


for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))



#currentDf=currentDf.withColumn("File_Name", lit(FileName))
display(currentDf)

# COMMAND ----------

# Typecast every column to String  - added on 3/10/2023
currentDf=colcaststring(currentDf,currentDf.columns)
display(currentDf)

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# DBTITLE 1,Load Staging Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

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
print(tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists(saveTableName)):
#     tableDf = spark.sql("select * from "+saveTableName)

#     tableDf_row = tableDf.count()
#     print("Row ",tableDf_row)

#     tableDf_col = len(tableDf.columns)
#     print("Column ",tableDf_col)

#     if((row == tableDf_row) & (column == tableDf_col)):
#         print("Row and Column Count is Matching!!")
#     else:
#         print("Row Count is NOT Matching!!")
#         fail
    
# else:
#     print("Table does not exists")
#     fail

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/raw/rawstg_to_rawcurr_clean_data",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

