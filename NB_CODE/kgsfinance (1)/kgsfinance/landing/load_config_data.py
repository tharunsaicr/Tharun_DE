# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

#Commented and hardcoded on 1/20/2023
# dbutils.widgets.text(name = "ProcessName", defaultValue = "")
# processName = dbutils.widgets.get("ProcessName")
processName = 'config'

print(filePath)
print(tableName)
print(processName)

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
from pyspark.sql.functions import col, lit

currentdatetime= datetime.now()

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)


for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Load Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",config_path_url+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb."+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsonedatadb."+processName + "_"+ tableName
print("Table Created : ", saveTableName)

# COMMAND ----------

# %sql
# UPDATE kgsonedatadb.config_SerialDateConversionColumns
# SET DeltaTableName = "contingent_raw_dump"
# WHERE ProcessName = "headcount" and SerialDate_ColumnName = "Date_of_Joining";

# COMMAND ----------

