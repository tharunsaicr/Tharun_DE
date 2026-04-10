# Databricks notebook source
# This NB handles hc actuals, plan & forecast and dim tables (employee & designation)
#Read files from RAW curr the write into Trusted current (overwrite) & Trusted history(append) tables
# calling a Notebook which loads delta sql tables to SQL database

# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "File_Year", defaultValue = "")
File_Year = dbutils.widgets.get("File_Year")

dbutils.widgets.text(name = "File_Month", defaultValue = "")
File_Month = dbutils.widgets.get("File_Month")


print("File_Year   :" +File_Year)
print("File_Month  :"+File_Month)

print("tableName   :"+tableName)
print("processName :"+processName)
print("ReportName  :"+ReportName)

# COMMAND ----------

import string
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call unpivot utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Reading Raw current data
#Load raw contractor data
raw_curr_contractor_url=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName
currentDf= spark.read.format("delta").load(raw_curr_contractor_url)    

print(raw_curr_contractor_url)
print("old Row count ",currentDf.count())
print("old Column count ",str(len(currentDf.columns)))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Unpivoting the month columns
import string
import pyspark.sql.functions as F

from pyspark.sql.functions import * 
currentDf=unpivotdf(currentDf)
print("new Row count ",currentDf.count())
print("new Column count ",str(len(currentDf.columns)))
display(currentDf)

# COMMAND ----------

# DBTITLE 1,Performing transformations & logics
month_list=["OCT","NOV","DEC"]
if tableName == "hc_plan" or tableName == "hc_forecast":
    currentDf=currentDf.withColumnRenamed("Amount","Contractor_Count").withColumn('MMM',upper(substring(col("Month"),0,3)))
    currentDf=currentDf.withColumn("Contractor_Count",col("Contractor_Count").cast('double'))\
.withColumn("cc",regexp_extract('CC_det', r'^(\d+)', 1)).withColumn("cc_name",regexp_extract('CC_det', r'^\d+\s(.*)', 1))\
.withColumn("Calendar_Year",when(currentDf['MMM'].isin(month_list),(currentDf['Financial_Year'].cast('int'))-1).otherwise((currentDf['Financial_Year']).cast('int'))).withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("MMM"),'MMM'),'MM')))

if tableName == "hc_actual":
    currentDf=currentDf.withColumnRenamed("Amount","Contractor_Count").withColumn('MMM',upper(substring(col("Month"),0,3)))
    currentDf=currentDf.withColumn("Contractor_Count",col("Contractor_Count").cast('double'))\
.withColumn("cc",regexp_extract('Cost_Centre2', r'^(\d+)', 1)).withColumn("cc_name",regexp_extract('Cost_Centre2', r'^\d+\s(.*)', 1))\
.withColumn("Calendar_Year",when(currentDf['MMM'].isin(month_list),(currentDf['Financial_Year'].cast('int'))-1).otherwise((currentDf['Financial_Year']).cast('int'))).withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("MMM"),'MMM'),'MM')))

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Hist & Current table names & path
saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName
print("path : ",finance_trusted_curr_savepath_url+processName+"/"+tableName)
curr_table_name= saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+processName+"_"+tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Loading Current Table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName})
else:
    print("Creating Curr & Hist tables on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})


# COMMAND ----------

# DBTITLE 1,Calling dimention Notebook (Dim from fact tables)
if(spark._jsparkSession.catalog().tableExists("kgsfinancedb.trusted_hist_fr_contractor_hc_plan")):
    if(spark._jsparkSession.catalog().tableExists("kgsfinancedb.trusted_hist_fr_contractor_hc_forecast")):
        if(spark._jsparkSession.catalog().tableExists("kgsfinancedb.trusted_hist_fr_contractor_hc_actual")):
            dbutils.notebook.run("/kgsfinance/raw/Functional_Reports/Contractor/Dimension Contractor From Fact",6000,{'DeltaTableName':'dim','ProcessName':processName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Create the HC Fact Hist table on trusted layer")


# COMMAND ----------

# DBTITLE 1,Loading Delta trusted tables to SQL database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})