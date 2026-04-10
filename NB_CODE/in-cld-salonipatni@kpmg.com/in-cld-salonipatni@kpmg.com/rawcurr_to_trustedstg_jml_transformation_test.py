# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")


print(tableName)
print(processName)

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

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

display(currentDf)

# COMMAND ----------

# Commented on 3/12 because this is deleting all rows even if there is a field has null
# Drop null columns
# col_to_drop = get_null_column_names(currentDf)
# print(col_to_drop)
# currentDf = currentDf.drop(*col_to_drop) 

# currentDf = currentDf.na.drop()

# display(currentDf)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")
display(currentDf)

# COMMAND ----------

if tableName.upper() == "UK_JOINER":
        
    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,GPID from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("GPID","ED_GPID")

    joinDf = currentDf.join(edDF,upper(currentDf.GPID) == upper(edDF.ED_GPID), "left")

    currentDf = joinDf

# COMMAND ----------

# DBTITLE 1,Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)


# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':'jml'})

# COMMAND ----------

