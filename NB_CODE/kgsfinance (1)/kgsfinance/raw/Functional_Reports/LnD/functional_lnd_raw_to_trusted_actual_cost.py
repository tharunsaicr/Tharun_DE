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

import pyspark
from datetime import datetime
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

finance_trusted_curr_savepath_url =finance_trusted_curr_savepath_url
#+processName+'/'#+tableName+'/'
print(finance_trusted_curr_savepath_url)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# Functional Reports- Load LnD data from delta table raw_curr_fr_lnd_actual_cost
raw_curr_lnd_actual_cost = spark.read.table("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName) 
raw_curr_lnd_actual_cost.count()

# COMMAND ----------

#raw_curr_lnd_actual_cost.printSchema()
display(raw_curr_lnd_actual_cost)

# COMMAND ----------

#Renaming columns, changing datatypes
df_transform_lnd_actual_cost =raw_curr_lnd_actual_cost.select('*').withColumn('Profit_Cost_Centre_Code',col('Profit_Cost_Centre_Code').cast(IntegerType()))\
                                                  .withColumn('Function_Code',col('Function_Code').cast(IntegerType()))\
                                                  .withColumn("Debit_Amt",col("Debit_Amt").cast(DoubleType()))\
                                                  .withColumn("Credit_Amt",col("Credit_Amt").cast(DoubleType()))\
                                                  .withColumn("Net_Amount",col("Net_Amount").cast(DoubleType()))\
                                                  .withColumn('Project_ID',col('Project_ID').cast(IntegerType()))\
                                                  .withColumn('Account_Code',col('Account_Code').cast(IntegerType()))\
                                                  .withColumn("Period",F.to_date("Period","MMM yyyy"))\
                                                  .withColumn('Created_By',col('Created_By').cast(IntegerType()))\
                                                  .withColumn('Creation_Time',to_timestamp(col('Creation_Time'),'HH:mm:ss'))\
                                                  .withColumn('Customer_PAN_No',col('Customer_PAN_No').cast(IntegerType()))\
                                                  .withColumn('amount_in_dollar_000',col('amount_in_dollar_000').cast(DoubleType()))\
                                                  .withColumn("Calendar_Year",F.year(F.to_date("Period","yyyy"))) \
                                                  .withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy"))) \
                                                  .withColumn("Dated_On",to_timestamp(lit(currentdatetime)))



if 'amount_in_dollar_million' in df_transform_lnd_actual_cost.columns:
    df_transform_lnd_actual_cost=df_transform_lnd_actual_cost.select('*').withColumn('amount_in_dollar_million',col('amount_in_dollar_million').cast(DoubleType()))\
                                                             .withColumnRenamed('Bonus___1','Bonus_In_Percent')\
                                                             .withColumnRenamed('Markup','Markup_In_Percent')\
                                                             .withColumn('Bonus_In_Percent',when(col('Bonus_In_Percent').contains('%'),(f.regexp_replace(f.col("Bonus_In_Percent"),"[%]","")/100).cast(FloatType())).otherwise(col('Bonus_In_Percent').cast(FloatType())))\
                                                             .withColumn('Markup_In_Percent',when(col('Markup_In_Percent').contains('%'),(f.regexp_replace(f.col("Markup_In_Percent"),"[%]","")/100).cast(FloatType())).otherwise(col('Markup_In_Percent').cast(FloatType())))\
                                                             .withColumn('Bonus',col('Bonus').cast(IntegerType()))\
                                                             .withColumn('Gratuity___LE',col('Gratuity___LE').cast(IntegerType()))\
                                                             .withColumn('Total_Incl_Bonus___Gratuity',col("Total_Incl_Bonus___Gratuity").cast(DoubleType()))\
                                                             .withColumn('Total_Incl_Markup',col("Total_Incl_Markup").cast(DoubleType()))\
                                                             .withColumn('Check',col('Check').cast(BooleanType()))\
                                                             .withColumn('MI_Grouping_Check',col('MI_Grouping_Check').cast(BooleanType()))\
                                                             .withColumn('Entity_Name_Check',col('Entity_Name_Check').cast(BooleanType()))\
                                                             .withColumn('Description_Length',col('Description_Length').cast(IntegerType()))\
                                                             .withColumn('Jounal_Name_Check',col('Jounal_Name_Check').cast(BooleanType()))

# COMMAND ----------

display(df_transform_lnd_actual_cost)

# COMMAND ----------

#NA Checks on Training_Name_2 and Category_2 Columns 
df_transform_lnd_actual_cost=df_transform_lnd_actual_cost.withColumn('Training_Name_2',when(col('Training_Name_2').isNull(),'NA').otherwise(col('Training_Name_2')))\
                                                 .withColumn('Training_Name_2',when(col('Training_Name_2')=='#NA','NA').otherwise(col('Training_Name_2')))\
                                                 .withColumn('Category_2',when(col('Category_2').isNull(),'NA').otherwise(col('Category_2')))\
                                                 .withColumn('Category_2',when(col('Category_2')=="",'NA').otherwise(col('Category_2')))
                                                  

# COMMAND ----------

#Drop •	Remove "Customer PAN No", "Creation Time"
df_transform_lnd_actual_cost=df_transform_lnd_actual_cost.drop('Customer_PAN_No','Creation_Time')

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current
df_transform_lnd_actual_cost.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path", finance_trusted_curr_savepath_url+ReportName+"/"+processName+'/'+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

#stg_table_name= saveTableName
curr_table_name="kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" + tableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+processName+"_"+tableName
#print(stg_table_name)
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

# COMMAND ----------

#Datatype cross check from PBI
# --{"Profit/Cost Centre Code", Int64.Type}, 
# --{"Function Code", Int64.Type
# --{"Debit Amt", Int64.Type
# --{"Credit Amt", type number
# --Net Amount", type number
# --{"Project ID", Int64.Type
# --{"Account Code", Int64.Type
# --{"Period", type date
# --{"Created By", Int64.Type
# --{"Creation Time", type time}
# --Customer PAN No", Int64.Type
# --{"Bonus %", type number
# --Markup %", type number
# --Bonus", Int64.Type
# --{"Gratuity & LE", Int64.Type
# --Total Incl Bonus + Gratuity", type number
# --Total Incl Markup", type number
# --Check", type logical}
# --MI Grouping Check", type logical
# --Entity Name Check", type logical
# --Description Length", Int64.Type
# -Jounal Name Check", type logical
# --f", type logical
# {"$'000", type number
# {"$'millions", type number

#.withColumnRenamed("L", "MI_Map")\ .withColumn('Bonus_In_Percent',when(col('Bonus_In_Percent').str.contains('%'),(f.regexp_replace(f.col("Bonus___1"),"[%]","")/100).cast("float")).otherwise(col('Bonus_In_Percent')))
## Commented ,because renamed L to location in SOurce and dropped F as it is bool col
# .withColumnRenamed("f", "Blank3")  # need to check if this column is required or not
# .withColumn('f',col('f').cast(BooleanType()))\