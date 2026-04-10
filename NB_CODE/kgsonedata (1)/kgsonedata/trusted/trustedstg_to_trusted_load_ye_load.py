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
import pytz
from pyspark.sql.functions import col, lit, upper, when,to_timestamp

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentDf = spark.sql("select * from kgsonedatadb.trusted_stg_"+processName + "_" + tableName)

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

if(tableName == "talent_konnect_resignation_status_report_fortnight"):
    currentDf = currentDf.withColumn('APPROVED_LWD',currentDf['APPROVED_LWD'].cast(DateType()))

# display(currentDf)

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# DBTITLE 1,Standardize BU across all Process
for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
        .otherwise(joinDf[columnName]))
        
        joinDf = joinDf.drop(*buList.columns)
        
        currentDf = joinDf


# COMMAND ----------

display(currentDf)

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
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true")  \
.option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file
# display(currentDf)

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
dbutils.notebook.run("/kgsonedata/trusted/Delta_to_SQL_with_Select",6000, {'DeltaTableName':tableName, 'ProcessName':processName})