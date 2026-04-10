# Databricks notebook source
# This NB handles Dim lookup CFIT Designation & CFIT Category
#Read data from source file and write into Trusted current (overwrite) & Trusted history(append) tables
# calling a Notebook which loads delta sql tables to SQL database

# COMMAND ----------

dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
ProcessName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "File_Year", defaultValue = "")
File_Year = dbutils.widgets.get("File_Year")

dbutils.widgets.text(name = "File_Month", defaultValue = "")
File_Month = dbutils.widgets.get("File_Month")


print(filePath)
print(tableName)
print(ProcessName)
print(ReportName)
print(File_Year)
print(File_Month)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

import pyspark
from datetime import datetime
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","True").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)   


# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")


#Triming the column names
for name in currentDf.columns:
    currentDf = currentDf.withColumnRenamed(name,colNameTrim(name,TrimValue=" "))

#triming all the values
currentDf=leadtrailremove(currentDf)


# COMMAND ----------

# DBTITLE 1,Adding current date, file year & month columns


currentDf=currentDf\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("File_Year", lit(File_Year))\
.withColumn("File_Month", lit(File_Month)) 

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Filtering records
DesignationDf=currentDf.filter(currentDf.Category == "Designations")
CategoryDf=currentDf.filter(currentDf.Category != "Designations")

# COMMAND ----------

display(DesignationDf)
display(CategoryDf)

# COMMAND ----------

# DBTITLE 1,Current Load CFIT Designation
DesignationDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_designation") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ ReportName+"_"+ProcessName + "_" +tableName+"_designation")

# COMMAND ----------

# DBTITLE 1,Hist & Current table names & path (designation)
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +ProcessName + "_"+tableName+"_designation"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_designation")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +ProcessName + "_"+tableName+"_designation"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data (designation)
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    print("inside")
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_designation",'processName':ProcessName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_designation",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database (designation)
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_designation",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Current Load CFIT Category
CategoryDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_category") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ ReportName+"_"+ProcessName + "_" +tableName+"_category")

# COMMAND ----------

# DBTITLE 1,Hist & Current table names & path (category)
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +ProcessName + "_"+ tableName+"_category"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_category")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +ProcessName + "_"+ tableName+"_category"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data (category)
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_category",'processName':ProcessName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_category",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database (category)
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_category",'ProcessName':ProcessName,'ReportName':ReportName})