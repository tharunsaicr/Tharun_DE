# Databricks notebook source
processName = "employee_engagement"
tableName = "year_end"

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

rawCurrentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)
print(rawCurrentDf.count())
rawCurrentDf.createOrReplaceTempView("rawCurrentTable")

unique_identifier = "Emp_ID"

# COMMAND ----------

year_end_update_query = """ 
MERGE INTO kgsonedatadb.trusted_stg_"""+processName + """_"""+ tableName + """ as tct 
USING rawCurrentTable rct ON tct.{identifier} = rct.{identifier} 
WHEN MATCHED THEN UPDATE SET * 
WHEN NOT MATCHED THEN INSERT * 
""".format(identifier = unique_identifier)

# COMMAND ----------

spark.sql(year_end_update_query)

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# DBTITLE 1, Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","True") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})