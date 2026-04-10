# Databricks notebook source
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

print(filePath)
print(tableName)

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import pytz
from pyspark.sql import functions
from pyspark.sql.functions import dense_rank,expr,concat,to_timestamp,length, regexp_replace
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

print("File Path : ",landing_path_url+filePath)

# COMMAND ----------

print("File Path : ",landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape",'"').option("quote",'"').load(landing_path_url+filePath)


for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

currentDf=currentDf.withColumn('Completion_Date',to_date(currentDf['Completion_Date'], 'dd-MMM-yy'))

# COMMAND ----------

# display(currentDf)
currentDf.count()

# COMMAND ----------

currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb."+tableName)

# COMMAND ----------

#.option("path",config_path_url+"test_lnd_"+tableName) \

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from kgsonedatadb.lnd_sep_ytd

# COMMAND ----------

