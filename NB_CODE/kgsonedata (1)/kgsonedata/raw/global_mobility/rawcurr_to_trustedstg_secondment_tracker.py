# Databricks notebook source
tableName='secondment_tracker'
processName='global_mobility'

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import *

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

#This line is added to remove decimal characters that are automatically added to Employee_No column although the input is correct (a whole number as string)

currentDf = currentDf.withColumn('Employee_No', regexp_replace('Employee_No', '\\.0', ''))

# display(currentDf)

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