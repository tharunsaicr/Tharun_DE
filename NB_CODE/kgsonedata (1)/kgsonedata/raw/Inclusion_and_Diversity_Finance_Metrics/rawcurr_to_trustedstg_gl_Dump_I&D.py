# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "i_and_d"
tableName = "gl_dump"

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

import pyspark.sql.functions as F
import re
from pyspark.sql.functions import udf

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# @udf
# def ascii_ignore(x):
#     return x.encode('ascii', 'ignore').decode('ascii') if x else None

# df = df.ascii_udf('words')

#df = currentDf.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in currentDf.columns])

#display(df)

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