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

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,to_timestamp,concat,regexp_extract,substring
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
currentDf = currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

if tableName in ('kgs_india_mandatory_training','kgs_mandatory_training_ki_led','kgs_uk_mandatory_training','kgs_us_mandatory_training'):
    for col_name in currentDf.columns:
        if col_name=="FINAL_BU":
            currentDf=currentDf.withColumnRenamed(col_name,"BU")

# COMMAND ----------

if tableName in ('euda'):
    new_columns=[col(c).alias(c.strip("_")) for c in currentDf.columns]
    currentDf=currentDf.select(*new_columns)

# COMMAND ----------

if tableName == 'intune_compliance':
    currentDf=currentDf.filter(currentDf.COMPLIANCE == 'Compliant')

# COMMAND ----------

MonthList=['kin_data']
if tableName.lower() in MonthList:
    # if(tableName.lower() == ''):
    #     currentDf = currentDf.withColumn("Month",concat(lit("20"),regexp_extract('Month','[0-9]+', 0) , lit(" "), substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3)))
    # else:
     currentDf = currentDf.withColumn("Month",concat(regexp_extract('Month','[0-9]+', 0) , lit(" "), substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3))) 

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

