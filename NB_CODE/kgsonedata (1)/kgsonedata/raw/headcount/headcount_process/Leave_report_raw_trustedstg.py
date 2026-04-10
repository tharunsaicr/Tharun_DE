# Databricks notebook source
dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
fileDate = dbutils.widgets.get("CurrentCutOffDate")

# COMMAND ----------

tableName = "leave_report"
processName = "headcount"
fileYear = fileDate[:4]
print(tableName)
print(processName)
print(fileDate)
print(fileYear)

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse


# COMMAND ----------

finalDf = spark.sql("select * from hive_metastore.kgsonedatadb.raw_curr_headcount_leave_report")

# COMMAND ----------

# query = "delete from kgsonedatadb.trusted_hist_headcount_leave_report where left(File_Date,4) = '"+fileYear+"'"
# print(query)

countDf = spark.sql("delete from kgsonedatadb.trusted_hist_headcount_leave_report where left(File_Date,4) = '"+fileYear+"'")

# display(countDf)

# COMMAND ----------

df_cc_bu = spark.sql("select * from (select rank() over(partition by cost_centre order by file_date desc,dated_on desc) as cc_bu_rank, * from kgsonedatadb.config_hist_cost_center_business_unit where file_date = (select max(file_date) from kgsonedatadb.config_hist_cost_center_business_unit where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+"))) cc_bu_hist where cc_bu_rank = 1")

df_cc_bu = df_cc_bu.drop("cc_bu_rank")

finalDf = finalDf.join(df_cc_bu,finalDf.Department == df_cc_bu.Cost_centre,"left").select(finalDf["*"],df_cc_bu["BU"])

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finalDf = finalDf.withColumn("Dated_On",lit(currentdatetime))

finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# kgsonedatadb.trusted_stg_leave_report

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})