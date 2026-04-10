# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(tableName)
print(processName)

# COMMAND ----------

#Extract filename & date - switch case for each file

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit, upper, when

currentdatetime= datetime.now()

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentDf = spark.sql("select * from kgsonedatadb.trusted_stg_"+processName + "_" + tableName)

currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))

display(currentDf)

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list")

columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()
print("BU Column List ", columnList)

# COMMAND ----------

# DBTITLE 1,Standardize BU across all Process
for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
        .otherwise(joinDf[columnName]))
        
        joinDf = joinDf.drop(*buList.columns)


# COMMAND ----------

currentDf = joinDf

# COMMAND ----------

# DBTITLE 1,Load Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True")\
.option("path",trusted_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_"+ processName + "_" + tableName)

# COMMAND ----------

# DBTITLE 1,Load History Data
currentDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true")  \
.option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file

# COMMAND ----------

