# Databricks notebook source
from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp
from datetime import datetime
import pytz
import string

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName").lower()

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(filePath)
print(tableName)
print(processName)

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/common_components

# COMMAND ----------

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

print("File Path : ",edw_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(edw_landing_path_url+filePath)

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Trim and Convert Column name to Upper Case
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,toUpper((trimchar(col))))

# COMMAND ----------

if tableName=='dim_hr_phone_type':
    currentDf=currentDf.withColumnRenamed('PHONE_TYPE','PHONE_TYPE_1')\
                       .withColumnRenamed('PHONE TYPE','PHONE_TYPE_2')

if tableName=='fact_apar':
    currentDf=currentDf.withColumnRenamed('INVOICE_ID','INVOICE_ID_1')\
                       .withColumnRenamed('INVOICE ID','INVOICE_ID_2')

# COMMAND ----------

# DBTITLE 1,Ignore ASCII in Column Name
# for col in currentDf.columns:
#     currentDf=currentDf.withColumnRenamed(col,ascii_ignore(col))

# COMMAND ----------

# DBTITLE 1,Format Headers Data
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

# DBTITLE 1,Rename Repeating Column Name
# if tableName=='fact_apar':
#     columnLen = len(currentDf.columns)
#     print("no of columns:",columnLen)
#     currentDf=colNameRepeating(currentDf,columnLen)

# COMMAND ----------

#Typecast every column to String
currentDf=colcaststring(currentDf,currentDf.columns)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

#pass the unwanted character in the TrimValue
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.lower().startswith("_c1") or column.lower().startswith("_c2") or column.lower().startswith("_c3") or column.lower().startswith("_c4") or column.lower().startswith("_c5") or column.lower().startswith("_c6") or column.lower().startswith("_c7") or column.lower().startswith("_c8") or column.lower().startswith("_c9") or column.lower().startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

databaseName = 'kgsonedatadbedw'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# DBTITLE 1,Load Raw Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",edw_raw_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadbedw.raw_curr_" +tableName)

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
# histTableName = "kgsonedatadb.raw_hist_"+ processName + "_" + tableName

# if spark._jsparkSession.catalog().tableExists(histTableName):
#     print("Table "+ histTableName +" exist")
#     currentDf.write \
#     .mode("append") \
#     .format("delta") \
#     .option("mergeschema","true")  \
#     .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
#     .option("compression","snappy") \
#     .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)


# else:
#     print("Table "+ histTableName +"  does not exist")
#     currentDf.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .option("overwriteSchema","true")  \
#     .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
#     .option("compression","snappy") \
#     .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw'

dbutils.notebook.run("/Workspace/kgsonedataedw/raw/Data_Cleanup",8000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})

# COMMAND ----------

