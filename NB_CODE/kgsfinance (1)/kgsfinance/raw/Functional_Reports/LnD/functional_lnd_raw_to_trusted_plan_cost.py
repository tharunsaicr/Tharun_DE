# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print(tableName)
print(processName)
print(ReportName)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call common unpivot module
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Import Required Functions
import pyspark
from datetime import datetime
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import string
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

finance_trusted_curr_savepath_url =finance_trusted_curr_savepath_url
#+processName+'/'#+tableName+'/'
print(finance_trusted_curr_savepath_url)

# COMMAND ----------

# Functional Reports- Load LnD Plan Cost data from delta table raw_curr_fr_lnd_plan_cost
raw_curr_lnd_plan_cost = spark.read.table("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName)
raw_curr_lnd_plan_cost.count()

# COMMAND ----------

display(raw_curr_lnd_plan_cost)

# COMMAND ----------

# DBTITLE 1,Unpivoting the month columns
df_transform_lnd_plan_cost=unpivotdf(raw_curr_lnd_plan_cost)
display(df_transform_lnd_plan_cost)

# COMMAND ----------

#Renaming columns, changing datatype
Month_List=['OCT','NOV','DEC']
df_transform_lnd_plan_cost=df_transform_lnd_plan_cost.withColumn("CC_1",col("CC_1").cast("int"))\
                                             .withColumn('MMM',upper(substring(col("Month"),1,3)))\
                                             .withColumn("Calendar_Year",when(col("MMM").isin(Month_List),(col("Financial_Year").cast("int"))-1).otherwise((col("Financial_Year").cast("int"))))\
                                             .withColumn("YTD",col("YTD").cast("double"))\
                                             .withColumn("Amount",col("Amount").cast("double"))\
                                             .withColumnRenamed('Amount','Value')\
                                             .withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy")))\
                                             .withColumn("Period",concat(col("Calendar_Year"),lit(" "),col("Month")))\
                                             .withColumn("Period",F.to_date("Period","yyyy MMMM"))\
                                             .withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("MMM"),'MMM'),'MM')))\
                                             .withColumn("Dated_On",to_timestamp(lit(currentdatetime)))

# COMMAND ----------

display(df_transform_lnd_plan_cost)

# COMMAND ----------

print(finance_trusted_curr_savepath_url+ReportName+"/"+processName+'/'+tableName)

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current
df_transform_lnd_plan_cost.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path", finance_trusted_curr_savepath_url+ReportName+"/"+processName+'/'+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+'/'+tableName)

# COMMAND ----------

curr_table_name="kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+ processName + "_" +tableName

print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load data to final trusted table
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName})
else:
    print("Creating Curr & Hist tables on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})