# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print(tableName)
print(processName)
print(ReportName)

# COMMAND ----------

#Extract filename & date - switch case for each file

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit,to_timestamp
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentDf = spark.sql("select * from kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)
currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Load History Data
currentDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true")  \
.option("path",finance_trusted_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_hist_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file