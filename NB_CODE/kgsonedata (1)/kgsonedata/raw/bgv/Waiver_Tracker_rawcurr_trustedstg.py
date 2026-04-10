# Databricks notebook source
# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

tableName = "waiver_tracker"
processName = "bgv"

# COMMAND ----------

finaldf = spark.sql("select * from kgsonedatadb.raw_curr_bgv_waiver_tracker")

# COMMAND ----------

# DBTITLE 1,drop duplicates
finaldf = finaldf.dropDuplicates() 

# COMMAND ----------

# Date was appearing as Timestamp
datecols = ['DOJ','Report_Date']

for columnName in finaldf.columns:
    if (columnName in datecols):
        print(columnName)
        finaldf = finaldf.withColumn(columnName, when((col(columnName).isNotNull()) & (col(columnName) != '-'), to_date(columnName,'yyyy-MM-dd'))\
            .otherwise(col(columnName)))

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
finaldf = finaldf.withColumn("Dated_On",lit(currentdatetime))

finaldf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})