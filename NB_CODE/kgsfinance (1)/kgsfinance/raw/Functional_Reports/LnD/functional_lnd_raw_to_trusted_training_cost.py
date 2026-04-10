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
# MAGIC %run /kgsfinance/common_utilities/Unpivot_LnD

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

# Functional Reports- Load LnD Training Cost data from delta table raw_curr_fr_lnd_training_cost_per_head
raw_curr_lnd_training_cost = spark.read.table("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName)
raw_curr_lnd_training_cost.count()

# COMMAND ----------

display(raw_curr_lnd_training_cost)

# COMMAND ----------

# DBTITLE 1,Unpivoting the month columns
# df= raw_curr_lnd_training_cost.groupby('Entity').pivot('Entity')
df_transform_lnd_training_cost=unpivotdf(raw_curr_lnd_training_cost)
display(df_transform_lnd_training_cost)

# COMMAND ----------

df_transform_lnd_training_cost=df_transform_lnd_training_cost.withColumn("Amount",col("Amount").cast("double"))
df_transform_lnd_training_cost=df_transform_lnd_training_cost.drop('Average_HC','MTD__L_D_Cost__USDk','Per_Head_Training_Cost_USD')

# COMMAND ----------

df_transform_lnd_training_cost.createOrReplaceTempView("input_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from input_table

# COMMAND ----------

# Transpose the table using SQL query
transposedDF = spark.sql("""
SELECT 
sum(CASE WHEN Entity = 'KGS' THEN (Amount) END) AS KGS,
sum(CASE WHEN Entity = 'GDC' THEN (Amount) END) AS GDC,
sum(CASE WHEN Entity = 'Total' THEN (Amount) END) AS Total,
Month
FROM
input_table
GROUP BY
Month
""")

# COMMAND ----------

display(transposedDF)

# COMMAND ----------

# df=transposedDF.withColumn('Calendar_Year',F.year(f.to_timestamp("Month","MMM_yy")))
# df=df.withColumn('MMM',substring(col("Month"),1,3))

# COMMAND ----------

#Renaming columns, changing datatype
Month_List=['OCT','NOV','DEC']
df_transform_lnd_training_cost=transposedDF.withColumnRenamed('Amount','Value')\
                                           .withColumn("Period",F.to_date("Month","MMM_yy"))\
                                           .withColumn('MMM',upper(substring(col("Month"),0,3)))\
                                           .withColumn('Calendar_Year',F.year(f.to_timestamp("Month","MMM_yy")))\
                                           .withColumn("Financial_Year",when(substring(col("MMM"),1,3).isin(Month_List),(col("Calendar_Year").cast("int"))+1).otherwise((col("Calendar_Year").cast("int"))))\
                                           .withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("MMM"),'MMM'),'MM')))\
                                           .withColumn("Dated_On",to_timestamp(lit(currentdatetime)))

# COMMAND ----------

display(df_transform_lnd_training_cost)

# COMMAND ----------

print(finance_trusted_curr_savepath_url+ReportName+"/"+processName+'/'+tableName)

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current
df_transform_lnd_training_cost.write \
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