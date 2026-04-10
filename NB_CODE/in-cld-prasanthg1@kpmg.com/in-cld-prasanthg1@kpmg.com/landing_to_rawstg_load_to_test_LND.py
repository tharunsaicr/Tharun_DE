# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

print("File Path : ",landing_path_url+filePath)

if ((processName.lower() == 'it') & (tableName.lower() == 'daily_exit_report')):
    currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("skipRows", 1).option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)
    
else:
    currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)
   


# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.lower().startswith("_c1") or column.lower().startswith("_c2") or column.lower().startswith("_c3") or column.lower().startswith("_c4") or column.lower().startswith("_c5") or column.lower().startswith("_c6") or column.lower().startswith("_c7") or column.lower().startswith("_c8") or column.lower().startswith("_c9") or column.lower().startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Trim and Convert Column name to Upper Case
wave1List = ['bgv','employee_engagement','global_mobility','headcount','headcount_monthly','jml','lnd','talent_acquisition']

if processName not in wave1List:
    for col in currentDf.columns:
        currentDf=currentDf.withColumnRenamed(col,toUpper((trimchar(col))))

# COMMAND ----------

# DBTITLE 1,Format Headers Data
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

# Double was getting converted with Scientific Notation while converting to String
typeCastCheckDf = spark.sql("select * from kgsonedatadb.config_data_type_cast")

# double Column List
doublecheckDf = typeCastCheckDf.select("Column_Name").where((lower(typeCastCheckDf.Process_Name) == processName.lower()) & (lower(typeCastCheckDf.Delta_Table_Name) == tableName.lower()) & (upper(typeCastCheckDf.Type_Cast_To) == ('DOUBLE')))

doubleList = doublecheckDf.rdd.flatMap(lambda x: x).collect()

print(doubleList)

for colName,data_type in currentDf.dtypes:
    if (colName in doubleList) & (data_type == 'double'):
        currentDf = currentDf.withColumn(colName,when(currentDf[colName].cast(DoubleType()).isNotNull(), format_number(currentDf[colName], 2)).otherwise(currentDf[colName]))
    


# COMMAND ----------

# Typecast every column to String
currentDf=colcaststring(currentDf,currentDf.columns)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# DBTITLE 1,Load Raw Staging Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.test_raw_stg_"+ processName + "_" +tableName)

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

dbutils.notebook.run("/Users/in-cld-prasanthg1@kpmg.com/Data_Cleanup_to_test_LND",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})