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

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,Calling Unpivot Utility
# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

# DBTITLE 1,Read from raw current Delta Table
raw_curr_recruitment_cost_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_recruitment_cost_url)
read_df= spark.read.format("delta").load(raw_curr_recruitment_cost_url)

# COMMAND ----------

# DBTITLE 1,Drop YTD  Columns
column_to_Drop=[col for col in read_df.columns if col.startswith("YTD")]
read_df=read_df.drop(*column_to_Drop)

# COMMAND ----------

# DBTITLE 1,Unpivot Month Columns
unpivotedDf=unpivotdf(read_df)
print("new Row count ",unpivotedDf.count())
print("new Column count ",str(len(unpivotedDf.columns)))

# COMMAND ----------

# DBTITLE 1,Replace #Missing values in Columns with 0
for colname in unpivotedDf.columns:
    if colname!="Dated_On":
        unpivotedDf = unpivotedDf.withColumn(colname, when(f.col(colname)=="#Missing","0").otherwise(f.col(colname)))

# COMMAND ----------

# DBTITLE 1,Rename Columns
unpivotedDf=unpivotedDf.withColumnRenamed("CC","Operating_Unit") \
           .withColumnRenamed("trim","Cost_Center") \
           .withColumnRenamed("CC_1","Cost_Center_ID") \
           .withColumnRenamed("CC_2","Cost_Center_Name") \
           .withColumnRenamed("Account","Compensation_Type") \
           .withColumnRenamed("Amount","Value")

# COMMAND ----------

# DBTITLE 1,Derive Year,Dated_On and FY Columns
month_list=['october','november','december']
load_df=unpivotedDf.withColumn('Calendar_Year',when(f.lower(unpivotedDf['Month']).isin(month_list),unpivotedDf['Financial_Year'].cast('int')-1).otherwise(unpivotedDf['Financial_Year'].cast('int'))) \
                   .withColumn("Month",f.upper(f.substring(f.col("Month"),1,3))) \
                   .withColumn("Month_Key",f.concat(f.col("Calendar_Year"),f.from_unixtime(f.unix_timestamp(f.col("Month"),'MMM'),'MM'))) \
                   .withColumn("FY",f.concat(lit("FY"),f.substring(f.col("Financial_Year"),3,2)))

# COMMAND ----------

load_df=load_df.withColumn("Period",f.concat(f.col('Calendar_Year').cast('string'),lit(" "),f.col('Month')))

# COMMAND ----------

# DBTITLE 1,Convert Column DataTypes
load_df=load_df.withColumn("Value",f.col("Value").cast("double")) \
               .withColumn("Period",F.to_date("Period","yyyy MMMM")) \
               .withColumn("Financial_Year",F.col("Financial_Year").cast("int"))

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