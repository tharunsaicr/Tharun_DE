# Databricks notebook source
###FACTS###


# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "RawDeltaTableName", defaultValue = "")
RawDeltaTableName = dbutils.widgets.get("RawDeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "TrustedTableName", defaultValue = "")
tableName = dbutils.widgets.get("TrustedTableName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print("RawDeltaTableName   :"+ RawDeltaTableName)
print("tableName  :"+tableName)
print("processName:"+ processName)
print("ReportName :"+ ReportName)

# COMMAND ----------

# DBTITLE 1,Call Connection Module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Cal Common Components Module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Read data from raw layer
#Load data for raw layer
raw_curr_dim=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+RawDeltaTableName
print(raw_curr_dim)
currentDf= spark.read.format("delta").load(raw_curr_dim)
display(currentDf)

# COMMAND ----------

#change col type
currentDf = currentDf.withColumn("DATA", currentDf.DATA.cast("double"))
display(currentDf)



# COMMAND ----------

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

#trusted layer table name
saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Load data to trusted current table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

curr_table_name= saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+processName+"_"+tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load data to trusted history table
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName})
else:
    print("Creating Curr & Hist tables on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})


# COMMAND ----------

# DBTITLE 1,Delta To SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta to SQL Load_BU",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})