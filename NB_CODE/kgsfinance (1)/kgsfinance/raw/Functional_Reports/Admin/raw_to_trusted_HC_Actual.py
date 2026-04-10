# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

# COMMAND ----------

# DBTITLE 1,Import Required Functions
from pyspark.sql import functions as F

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
raw_curr_hc_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_hc_url)
read_df= spark.read.format("delta").load(raw_curr_hc_url)
display(read_df)

# COMMAND ----------

# DBTITLE 1,Unpivot Month Columns
unpivotedDf=unpivotdf(read_df)
print("new Row count ",unpivotedDf.count())
print("new Column count ",str(len(unpivotedDf.columns)))

# COMMAND ----------

month_list=['oct','nov','dec']

# COMMAND ----------

#casting columns to required datatype
cast_df=unpivotedDf.select("*").withColumn("Average",col("Average").cast("double")) \
                           .withColumn("Month",f.upper(f.substring(col("Month"),1,3))) \
                           .withColumn("Cost_Centre_Name",f.expr("substring(Cost_Centre2,instr(Cost_Centre2,' ')+1)")) \
                           .withColumn('Cost_Centre_Code',f.split(col("Cost_Centre2")," ")[0]) \
                           .withColumn("BU_Shrinkage",(f.regexp_replace(f.col("BU_Shrinkage"), "[%]", "").cast("double"))/100) \
                           .withColumn("HC_post_shrinkage",col("HC_post_shrinkage").cast("double")) \
                           .withColumn("Amount",col("Amount").cast("double")) \
                           .withColumn("Financial_Year",col("Financial_Year").cast("int")) 
cast_df.printSchema()
# display(cast_df)

# COMMAND ----------

cast_df=cast_df.withColumn('Calendar_Year',when(f.lower(cast_df['Month']).isin(month_list),cast_df['Financial_Year'].cast('int')-1) \
                           .otherwise(cast_df['Financial_Year'].cast('int'))) \
                           .withColumn('Month_Key',f.concat(col("Calendar_Year"),f.from_unixtime(f.unix_timestamp(col("Month"),'MMM'),'MM'))) \
                            .withColumnRenamed("Amount","Value") \
                           .withColumnRenamed("BU_Shrinkage","BU_Shrinkage_in_Percent")

# COMMAND ----------

display(cast_df)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = cast_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(cast_df.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Drop Duplicates
# del_DuplicateDf=cast_df.dropDuplicates()
# countof=del_DuplicateDf.count()
# print(countof)

# COMMAND ----------

# DBTITLE 1,Write data to Trusted Current Delta table
cast_df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName)

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