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

# DBTITLE 1,Read from raw current Delta Table
raw_curr_recruitment_cost_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_recruitment_cost_url)
read_df= spark.read.format("delta").load(raw_curr_recruitment_cost_url)

# COMMAND ----------

display(read_df)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,Changing Column DataTypes
#casting columns to required datatype
current_df=read_df.select("*").withColumn("Debit_Amt",f.col("Debit_Amt").cast("double")) \
                           .withColumn("Credit_Amt",f.col("Credit_Amt").cast("double")) \
                           .withColumn("Net_Amount",f.col("Net_Amount").cast("double")) \
                           .withColumn('Period',f.to_date('Period', 'MMM yyyy')) \
                           .withColumn("Profit_Cost_Centre_Code",f.col("Profit_Cost_Centre_Code").cast("int")) \
                           .withColumn("Function_Code",f.col("Function_Code").cast("int")) \
                           .withColumn("Project_ID",f.col("Project_ID").cast("int")) \
                           .withColumn("Account_Code",f.col("Account_Code").cast("int")) \
                           .withColumn("Created_By",f.col("Created_By").cast("int")) \
                           .withColumn("Bonus___1",f.col("Bonus___1").cast("float")) \
                           .withColumn("Markup",f.col("Markup").cast("float")) \
                           .withColumn("Bonus",f.col("Bonus").cast("float")) \
                           .withColumn("Gratuity___LE",f.col("Gratuity___LE").cast("float")) \
                           .withColumn("Total_Incl_Bonus___Gratuity",f.col("Total_Incl_Bonus___Gratuity").cast("double")) \
                           .withColumn("Total_Incl_Markup",f.col("Total_Incl_Markup").cast("double")) \
                           .withColumn('Month',f.upper(f.date_format('Period','MMM'))) \
                           .withColumn("Calendar_Year",F.year(F.to_date("Period","yyyy"))) \
                           .withColumn("Month_Key",f.concat(f.col("Calendar_Year"),f.from_unixtime(f.unix_timestamp(f.col("Month"),'MMM'),'MM'))) \
                           .withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy"))) \
                           .withColumn("Dated_On",F.to_timestamp("Dated_On","yyyy-MM-dd HH:mm:ss.SSSSSS"))

# COMMAND ----------

# DBTITLE 1,Drop YTD Columns
column_to_Drop=[col for col in read_df.columns if col.startswith("YTD")]
current_df=current_df.drop(*column_to_Drop)

# COMMAND ----------

# DBTITLE 1,Casting  USD Columns
for column in current_df.columns:
    if column.startswith("USD"):
        current_df=current_df.withColumn(column,f.col(column).cast("float")) 

# COMMAND ----------

# DBTITLE 1,Rename Columns
load_df=current_df.withColumnRenamed("Bonus___1","Bonus_In_Percent") \
           .withColumnRenamed("Markup","Markup_In_Percent") 

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

# COMMAND ----------

