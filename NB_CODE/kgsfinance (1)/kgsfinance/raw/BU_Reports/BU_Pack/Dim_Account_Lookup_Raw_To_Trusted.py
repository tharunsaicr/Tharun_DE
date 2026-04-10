# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "RawDeltaTableName", defaultValue = "")
RawDeltaTableName = dbutils.widgets.get("RawDeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "TrustedTableName", defaultValue = "")
tableName = dbutils.widgets.get("TrustedTableName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "File_Year", defaultValue = "")
File_Year = dbutils.widgets.get("File_Year")

dbutils.widgets.text(name = "File_Month", defaultValue = "")
File_Month = dbutils.widgets.get("File_Month")

print("tableName   :"+tableName)
print("processName :"+processName)
print("ReportName  :"+ReportName)
print(File_Year)
print(File_Month)

# COMMAND ----------

# DBTITLE 1,Call connection configuration module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,import Statements
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

#Load data from raw layer
raw_curr_dim=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+RawDeltaTableName
print(raw_curr_dim)
currentDf= spark.read.format("delta").load(raw_curr_dim)
display(currentDf)

# COMMAND ----------

#change column types for Pie Chart  Priority, Big Bucket Priority, SL Priority, Parent Priority to number

currentDf = currentDf.withColumn("Pie_Chart__Priority", currentDf.Pie_Chart__Priority.cast("double"))
currentDf = currentDf.withColumn("Big_Bucket_Priority", currentDf.Big_Bucket_Priority.cast("double"))
currentDf = currentDf.withColumn("SL_Priority", currentDf.SL_Priority.cast("double"))
currentDf = currentDf.withColumn("Parent_Priority", currentDf.Parent_Priority.cast("double"))
display(currentDf)


# COMMAND ----------

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

# DBTITLE 1,Adding housekeeping fields - Dated_On, File_Year, File_Month
currentdatetime= datetime.now()
currentDf=currentDf\
.withColumn("Dated_On", lit(currentdatetime)) \
.withColumn("File_Year", lit(File_Year).cast("int")) \
.withColumn("File_Month", lit(File_Month).cast("int")) 
display(currentDf)

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
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Load Trusted Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +processName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted History Data
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Delta To SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta to SQL Load_BU",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})