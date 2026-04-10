# Databricks notebook source
# MAGIC %sql
# MAGIC -- select max(BATCH_ID) from kgsonedatadbedw.trusted_curr_fact_ap_invoice_register
# MAGIC describe table kgsonedatadbedw.trusted_curr_fact_ap_invoice_register

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/edw/fact/

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadbedw.raw_curr_fact_ap_invoice_register

# COMMAND ----------

dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp
from datetime import datetime
import pytz
import string

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

finance_landing_path_url+filePath

# COMMAND ----------

currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").load(finance_landing_path_url+filePath)
#.option("escapeQuotes", "true").partitionBy("column")
currentDf.count()

# COMMAND ----------

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))


#triming all the values
currentDf=leadtrailremove(currentDf)
display(currentDf)
print(currentDf.count())

# COMMAND ----------

print(currentDf.count())

# COMMAND ----------

