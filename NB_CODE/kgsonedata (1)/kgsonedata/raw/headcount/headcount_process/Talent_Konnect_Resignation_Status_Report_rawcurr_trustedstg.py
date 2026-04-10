# Databricks notebook source
# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

tableName = "talent_konnect_resignation_status_report"
processName = "headcount"

# COMMAND ----------

# finaldf = spark.sql("select * from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report")

finaldf = spark.sql("select * from (select row_number() over(partition by employeenumber order by resignationdate desc) as tk_rownum,* from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report) where tk_rownum = 1")

finaldf = finaldf.drop("tk_rownum")

# COMMAND ----------

# countDf = spark.sql("delete from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where left(File_Date,4) = (select year(max(file_date)) from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report)")

# COMMAND ----------

# DBTITLE 1,drop duplicates
finaldf = finaldf.dropDuplicates() 

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
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