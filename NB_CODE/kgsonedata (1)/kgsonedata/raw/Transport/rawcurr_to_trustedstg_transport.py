# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

display(currentDf)

# COMMAND ----------

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
    currentDf=currentDf.withColumnRenamed(col,toUpper((trimchar(col))))


# COMMAND ----------

# DBTITLE 1,Triming the "-" from the column names
if (tableName.lower() == 'on_time') | (tableName.lower() == 'no_show'):
    columnLen = len(currentDf.columns)
    print("no of columns:",columnLen)
    for i in range(columnLen):
        currentDf=currentDf.withColumnRenamed(currentDf.columns[i], currentDf.columns[i].strip(string.digits).strip('_'))
    

# COMMAND ----------

# DBTITLE 1,converting string  into time
from pyspark.sql.functions import date_format
if (tableName.lower() == 'on_time') | (tableName.lower() == 'no_show'):
    currentDf = currentDf.withColumn('SHIFTTIME', date_format('SHIFTTIME', 'HH:mm:ss'))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
currentDf = currentDf.withColumn("Dated_On",lit(currentdatetime))

# COMMAND ----------

# DBTITLE 1, Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

