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
# MAGIC
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Calling Unpivot Utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Read from raw current Delta Table
raw_curr_recruitment_team_cost_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_recruitment_team_cost_url)
read_df= spark.read.format("delta").load(raw_curr_recruitment_team_cost_url)

# COMMAND ----------

# DBTITLE 1,Rename Column Names
read_df=read_df.withColumnRenamed("CC_1","CC_Code") \
               .withColumnRenamed("CC_2","CC_Name")


# COMMAND ----------

display(read_df)

# COMMAND ----------

for column in read_df.columns:
    if column.startswith("YTD"):       
        read_df=read_df.drop(f.col(column))

# COMMAND ----------

# DBTITLE 1,Unpivot Month Columns
unpivotedDf=unpivotdf(read_df)
print("new Row count ",unpivotedDf.count())
print("new Column count ",str(len(unpivotedDf.columns)))

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

month_list=['OCT','NOV','DEC']

# COMMAND ----------

#casting columns to required datatype
cast_df=unpivotedDf.select("*").withColumn("Amount",f.col("Amount").cast("double")) \
                               .withColumn("CC_Code",f.col("CC_Code").cast("int")) \
                               .withColumn("Total",f.col("Total").cast("double")) \
                               .withColumn("Month",f.upper(f.substring(col("Month"),1,3))) \
                               .withColumn("Financial_Year",col("Financial_Year").cast("int")) 
cast_df.printSchema()
display(cast_df)

# COMMAND ----------

# DBTITLE 1,Deriving Calendar Year,Month key columns
load_df=cast_df.withColumn('Calendar_Year',when(cast_df['Month'].isin(month_list),cast_df['Financial_Year'].cast('int')-1) \
                           .otherwise(cast_df['Financial_Year'].cast('int'))) \
                           .withColumn('Month_Key',f.concat(col("Calendar_Year"),f.from_unixtime(f.unix_timestamp(col("Month"),'MMM'),'MM'))) 


# COMMAND ----------

display(load_df)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = load_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(load_df.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Write data to Trusted Current Delta table
load_df.write \
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
hist_table_name="kgsfinancedb.trusted_hist_"+reportName + "_" +processName + "_"+ tableName
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

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

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