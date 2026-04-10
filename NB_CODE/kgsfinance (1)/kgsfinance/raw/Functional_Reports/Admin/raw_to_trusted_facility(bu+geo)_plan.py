# Databricks notebook source
# DBTITLE 1,Reading Table and Process Name
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

# COMMAND ----------

# DBTITLE 1,Import Required Functions
import string
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Call Connection Module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Calling Unpivot Utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Reading Dataframe from raw current Table
#Load raw contractor actual cost
raw_curr_contractor_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
currentDf= spark.read.format("delta").load(raw_curr_contractor_url)    

print(raw_curr_contractor_url)
print("old Row count ",currentDf.count())
print("old Column count ",str(len(currentDf.columns)))

# COMMAND ----------

# DBTITLE 1,Unpivot Month Columns
unpivotedDf=unpivotdf(currentDf)
print("new Row count ",currentDf.count())
print("new Column count ",str(len(currentDf.columns)))

# COMMAND ----------

Month_List=['OCT','NOV','DEC']

# COMMAND ----------

unpivotedDf=unpivotedDf.select("*").withColumn("CC_Number",regexp_extract('CC', r'^(\d+)', 1)) \
                                   .withColumn("CC_Name",regexp_extract('CC', r'^\d+\s(.*)', 1))
# display(unpivotedDf)

# COMMAND ----------

# DBTITLE 1,Casting Column Datatypes and Deriving Year,Month Column
casted_df=unpivotedDf.select("*").withColumn("CC_Number",col("CC_Number").cast("int")) \
                               .withColumn("Month",upper(substring(col("Month"),1,3))) \
                               .withColumn("Calendar_Year",when(col("Month").isin(Month_List),(col("Financial_Year").cast("int"))-1).otherwise((col("Financial_Year").cast("int")))) \
                               .withColumn("Amount",f.col("Amount").cast("double")) \
                               .withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))\
                               .withColumn("Dated_On",F.to_timestamp("Dated_On","yyyy-MM-dd HH:mm:ss.SSSSSS"))
                               
casted_df=casted_df.withColumn("Financial_Year",col("Financial_Year").cast("int"))

# COMMAND ----------

display(casted_df)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = casted_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(casted_df.columns)
print("Column ",column)

# COMMAND ----------

casted_df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ reportName + "_" +processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

curr_table_name="kgsfinancedb.trusted_curr_"+ reportName + "_" +processName + "_"+ tableName
hist_table_name="kgsfinancedb.trusted_hist_"+ reportName + "_" +processName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName})
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