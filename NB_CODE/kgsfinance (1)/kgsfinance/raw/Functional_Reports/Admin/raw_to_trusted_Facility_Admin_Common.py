# Databricks notebook source
#commom nb for transforming admin function cost,fy classification,fx rate and transport split regular sheets

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "CheckColName", defaultValue = "")
CheckColName = dbutils.widgets.get("CheckColName")

# COMMAND ----------

# DBTITLE 1,Import Required Functions
from datetime import datetime
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from pyspark.sql import functions as F
import re,string
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Calling Unpivot Utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Read from raw current Delta Table
raw_curr_admin_cost_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_admin_cost_url)
read_df= spark.read.format("delta").load(raw_curr_admin_cost_url)
# display(read_df)

# COMMAND ----------

# DBTITLE 1,Unpivot Month Columns for Transport Split Regular File
if tableName=='transport_split_regular':
    unpivotedDf=unpivotdf(read_df)
    print("new Row count ",unpivotedDf.count())
    print("new Column count ",str(len(unpivotedDf.columns)))

# COMMAND ----------

# DBTITLE 1,FY Classification Data Type Casting
if tableName=='fy_classification':
    #casting the column datatypes
    load_df=read_df.withColumn("Priority",col("Priority").cast("int")) \
                             .withColumn("Dated_On", lit(currentdatetime)) 
 

# COMMAND ----------

# DBTITLE 1,Transport Split Regular Data Type Casting
if tableName=='transport_split_regular':
    month_list=['oct','nov','dec']
    load_df=unpivotedDf.withColumn("Amount",(f.regexp_replace(f.col("Amount"), "[%]", "").cast("float"))) \
                .withColumn("Dated_On", to_timestamp(lit(currentdatetime))) \
                .withColumnRenamed("Amount","Value") 
           


# COMMAND ----------

# DBTITLE 1,Fx Rate Data Type Casting
if tableName=='fx_rate':
    load_df=read_df.withColumn("CFY_A",f.col("CFY_A").cast("int")) \
                   .withColumn("Dated_On", lit(currentdatetime))

# COMMAND ----------

if tableName=='function_cost':
    load_df=read_df.withColumn("Value",col("Value").cast("int"))  
    

# COMMAND ----------

#writing the  table to trusyted current layer
load_df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = load_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(load_df.columns)
print("Column ",column)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ reportName + "_" +processName + "_"+ tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName)

# COMMAND ----------

curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ reportName + "_" +processName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Calling Trusted Current to Trusted History Notebook
if tableName=='function_cost':
    if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
        dbutils.notebook.run("/kgsfinance/trusted/trusted_dynamic_hist_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName,'check_col_name':CheckColName})
    else:
        print("Creating  Hist table on trusted layer")
        dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

if(spark._jsparkSession.catalog().tableExists(saveTableName)):
    tableDf = spark.sql("select * from "+saveTableName)

    tableDf_row = tableDf.count()
    print("Row ",tableDf_row)

    tableDf_col = len(tableDf.columns)
    print("Column ",tableDf_col)

    if((row == tableDf_row) & (column == tableDf_col)):
        print("Row and Column Count is Matching!!")
    else:
        print("Row Count is NOT Matching!!")
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})