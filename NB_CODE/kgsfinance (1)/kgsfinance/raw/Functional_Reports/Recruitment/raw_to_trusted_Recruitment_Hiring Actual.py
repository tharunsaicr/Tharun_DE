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
raw_curr_recruitment_hiring_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_recruitment_hiring_url)
read_df= spark.read.format("delta").load(raw_curr_recruitment_hiring_url)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

display(read_df)

# COMMAND ----------

# DBTITLE 1,Casting Columns
read_df=read_df.withColumn("Employee_Number",f.col("Employee_Number").cast("int")) \
                        .withColumn('Date_First_Hired',to_date('Date_First_Hired', 'yyyy-MM-dd')) \
                        .withColumn('End_Date_for_FTS',to_date('End_Date_for_FTS', 'yyyy-MM-dd')) \
                        .withColumn("Count",f.col("Count").cast("float")) \
                       .withColumn('Month_Hired',to_date('Month_Hired', 'yyyy-MM-dd')) \
                       .withColumn("Financial_Year",f.col("Financial_Year").cast("int")) \
                       .withColumn("Calendar_Year",f.col("Calendar_Year").cast("int"))

# COMMAND ----------

# DBTITLE 1,Cast period Column
for column in read_df.columns:
    if column.startswith("Period"):    
        read_df=read_df.withColumn("Period",f.to_date('Period', 'MMM-yyyy'))

# COMMAND ----------

# DBTITLE 1,Drop YTD Column
column_to_Drop=[col for col in read_df.columns if col.startswith("YTD")]
read_df=read_df.drop(*column_to_Drop)

# COMMAND ----------

# DBTITLE 1,Trim Column Values
read_df=read_df.withColumn("Cost_centre",f.trim(f.col("Cost_centre"))) \
                .withColumn("Month",f.date_format(f.col("Month_Hired"),'MMM')) \
                .withColumn("Month",f.upper(f.substring(f.col("Month"),1,3))) 

# COMMAND ----------

# DBTITLE 1,Derive Year,Dated_On and FY Columns
currentdatetime= datetime.now()
load_df=read_df.withColumn("Month_Key",f.concat(f.col("Calendar_Year"),f.from_unixtime(f.unix_timestamp(f.col("Month"),'MMM'),'MM')))  

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

# COMMAND ----------

