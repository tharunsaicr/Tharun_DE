# Databricks notebook source
# This NB contains the code to extract 2 tables from the source mapping files and 2 tables from HC fact tables
# Names in PBI : Employee Type & Payroll/Contractor, in delta table: Employee_Type & Designation_Mapping
# Names in PBI : Geo & Vendor List , in delta table: geo & Vendor

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

import re,string
import pytz
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data from Source File
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","True").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))


# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names 
#pass the unwanted character in the TrimValue
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))   

currentDf.createOrReplaceTempView("currentDf")   
display(currentDf)     

# COMMAND ----------

#Vendor mapping data
vendordf=spark.sql("select Vendor_Name, Vendor_Mapping from currentDf")
display(vendordf)
#Geo mapping data
geodf=spark.sql("""select Geo9 as Geo_Location, Geo10 as Geo_Level2 from currentDf""")
display(geodf)

# COMMAND ----------

# Replace empty value with None and drop null rows
print("Row ",geodf.count())
vendordf=vendordf.dropna("all")
geodf=geodf.dropna("all")
print("Row ",geodf.count())


# COMMAND ----------

# DBTITLE 1,Adding Dated_on column
currentdatetime= datetime.now()
vendor=vendordf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("File_Month", lit(File_Month))\
.withColumn("File_Year", lit(File_Year)) 
geo=geodf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("File_Month", lit(File_Month))\
.withColumn("File_Year", lit(File_Year)) 


# COMMAND ----------

# DBTITLE 1,Load current data in trusted layer (vendor)
#contractor vendor current table (Over write mode)
vendor.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_vendor") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ProcessName + "_" +tableName+"_vendor")

# COMMAND ----------

# DBTITLE 1,Current & Hist table names and  path (vendor)
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +ProcessName + "_"+ tableName+"_vendor"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_vendor")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +ProcessName + "_"+ tableName+"_vendor"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist data (vendor)
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_vendor",'processName':ProcessName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_vendor",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading Delta trusted tables to SQL database (vendor)
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_vendor",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Load current data in trusted layer (geo)
#contractor geo current table (Over write mode)

geo.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_geo") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ProcessName + "_" +tableName+"_geo")

# COMMAND ----------

# DBTITLE 1,Current & Hist table names and  path (geo)
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +ProcessName + "_"+ tableName+"_geo"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+ProcessName+"/"+tableName+"_geo")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +ProcessName + "_"+ tableName+"_geo"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist data (geo)
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_geo",'processName':ProcessName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_geo",'ProcessName':ProcessName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading Delta trusted tables to SQL database (geo)
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_geo",'ProcessName':ProcessName,'ReportName':ReportName})