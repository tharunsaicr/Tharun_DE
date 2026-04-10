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
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

finance_trusted_curr_savepath_url =finance_trusted_curr_savepath_url
#+processName+'/'#+tableName+'/'
print(finance_trusted_curr_savepath_url)

# COMMAND ----------

# Functional Reports- Load Travel dump data from delta table raw_curr_fr_travel_dump
df_raw_curr_travel_dump = spark.read.table("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName)
df_raw_curr_travel_dump.count()

# COMMAND ----------

display(df_raw_curr_travel_dump)

# COMMAND ----------

#Renaming columns, changing datatypes
df_transform_travel_dump=df_raw_curr_travel_dump.withColumn('Invoice_Date',to_date('Invoice_Date', 'yyyy-MM-dd'))\
.withColumn('Basic',col("Basic").cast(IntegerType()))\
.withColumn('YQ_Tax',col("YQ_Tax").cast(IntegerType()))\
.withColumn('YR_Tax',col("YR_Tax").cast(IntegerType()))\
.withColumn('JN_Tax',col("JN_Tax").cast(IntegerType()))\
.withColumn('EBT',col("EBT").cast(IntegerType()))\
.withColumn('Tax',col("Tax").cast(IntegerType()))\
.withColumn('OC_Tax',col("OC_Tax").cast(IntegerType()))\
.withColumn('OB_Charges',col("OB_Charges").cast(IntegerType()))\
.withColumn('OTHR_TAX',col("OTHR_TAX").cast(IntegerType()))\
.withColumn('Service_Tax',col("Service_Tax").cast(IntegerType()))\
.withColumn('CGST_Amount',col("CGST_Amount").cast(IntegerType()))\
.withColumn('CGST_Amount_1',col("CGST_Amount_1").cast(IntegerType()))\
.withColumn('IGST_Amount',col("IGST_Amount").cast(IntegerType()))\
.withColumn('Extra_Charges',col("Extra_Charges").cast(IntegerType()))\
.withColumn('Mang_Fee',col("Mang_Fee").cast(IntegerType()))\
.withColumn('Visa',col("Visa").cast(DoubleType()))\
.withColumn('SGST_Amount',col("SGST_Amount").cast(DoubleType()))\
.withColumn('CGST_Amount_2',col("CGST_Amount_2").cast(DoubleType()))\
.withColumn('IGST_Amount_1',col("IGST_Amount_1").cast(DoubleType()))\
.withColumn('CXL_Amount',col("CXL_Amount").cast(IntegerType()))\
.withColumn('Discount',col("Discount").cast(DoubleType()))\
.withColumn('Net_Amount',col("Net_Amount").cast(IntegerType()))\
.withColumn('Trvl_Dt',f.to_date('Trvl_Dt', 'yyyy-MM-dd'))\
.withColumn('Trvl_Dt_2',f.to_date('Trvl_Dt_2', 'yyyy-MM-dd'))\
.withColumn('Return_Date',to_date('Return_Date', 'yyyy-MM-dd'))\
.withColumn('PSR_Date',to_date(f.col('PSR_Date'), 'yyyy-MM-dd'))\
.withColumn('Lead_Time',col('Lead_Time').cast(IntegerType()))\
.withColumn('Ticket_Count',col('Ticket_Count').cast(IntegerType()))\
.withColumn('Employee_code',col('Employee_code').cast(IntegerType()))\
.withColumn('EMPLOYEE_CODE_1',col('EMPLOYEE_CODE_1').cast(IntegerType()))\
.withColumn('FUNCTION',col('FUNCTION').cast(IntegerType()))\
.withColumn('SUB_FUNCTION',col('SUB_FUNCTION').cast(IntegerType()))\
.withColumn('Project_Code',col('Project_Code').cast(IntegerType()))\
.withColumn("Calendar_Year",F.year(F.to_date("Calendar_Year","yyyy"))) \
.withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy"))) \
.withColumn("Dated_On",to_timestamp(lit(currentdatetime)))

# COMMAND ----------

df_transform_travel_dump=df_transform_travel_dump.withColumn('Month_Name',date_format('PSR_date','MMMM'))\
.withColumn('Year',date_format('PSR_date','yyyy'))

df_transform_travel_dump=df_transform_travel_dump.drop('Month')

df_transform_travel_dump=df_transform_travel_dump.withColumnRenamed('Month_Name','Month')\
                                                 .withColumnRenamed('Year','monthYear')

df_transform_travel_dump= df_transform_travel_dump.filter(df_transform_travel_dump.Sector.isNotNull())


# COMMAND ----------

#Using Colregex to get the TYPE_OF_FARE column name and making it one common format 'TYPE_OF_FARE'
col1=df_transform_travel_dump.select(df_transform_travel_dump.colRegex("`(TYPE_OF_FARE)+?.+`")).columns
new_col=col1[0]
print(new_col)
df_transform_travel_dump=df_transform_travel_dump.withColumnRenamed(new_col,'TYPE_OF_FARE')

# COMMAND ----------

#Using Colregex to get the BU Travel\OPE column name and making it one common format 'BU_Travel_OPE'
col1=df_transform_travel_dump.select(df_transform_travel_dump.colRegex("`(BU_Travel_)+?.+`")).columns
new_col=col1[0]
print(new_col)
df_transform_travel_dump=df_transform_travel_dump.withColumnRenamed(new_col,'BU_Travel_OPE')

# COMMAND ----------

#Filter BU_Travel_OPE is equal to "BU Travel"
df_transform_travel_dump=df_transform_travel_dump.filter(col("BU_Travel_OPE")=="BU Travel") 
df_transform_travel_dump.count()

# COMMAND ----------

display(df_transform_travel_dump)

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current
df_transform_travel_dump.write \
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

#stg_table_name= saveTableName
curr_table_name="kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+ processName + "_" +tableName
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

#Test script for Dev to be commented
# df=df_raw_curr_travel_dump.withColumn('Trvl_Dt_2',when(col('Trvl_Dt_2').like('%/%/%'),to_date(from_unixtime(unix_timestamp('Trvl_Dt_2', 'd/MM/yyyy')))).otherwise(to_date(from_unixtime(unix_timestamp('Trvl_Dt_2', 'dd-MM-yyyy')))))

# # columnName='Trvl_Dt_2'
# # df1=df_raw_curr_travel_dump.withColumn(columnName,changeDateFormat(col(columnName)))
# # # #to_date(from_unixtime(unix_timestamp('Return_Date', 'd-MMM-yy')))) .withColumn(columnName,changeDateFormat(col(columnName)))
# # # #df1=df.withColumn('Invoice_Date',to_date("Invoice_Date"))
# display(df.select('Trvl_Dt_2'))



#note1  Trvl_Dt  Trvl_Dt_2 are string instead of date type
# .withColumn('Trvl_Dt',when(col('Trvl_Dt').like('%/%/%'),to_date(from_unixtime(unix_timestamp('Trvl_Dt', '%d/%m/%y')))).otherwise(to_date(from_unixtime(unix_timestamp('Trvl_Dt', 'dd-MM-yyyy')))))\
# .withColumn('Trvl_Dt_2',when(col('Trvl_Dt_2').like('%/%/%'),to_date(from_unixtime(unix_timestamp('Trvl_Dt_2', '%d/%m/%y')))).otherwise(to_date(from_unixtime(unix_timestamp('Trvl_Dt_2', 'dd-MM-yyyy')))))\

# COMMAND ----------

#Test script for Dev to be commented
# Mang_Fee#Renaming columns, changing datatypes#Renaming columns, changing datatypes
# Invoice Date - Date
# Basic - whole number
# YQ_Tax:whole number
# YR_Tax:whole number
# JN_Tax:whole number
# EBT:whole number
# Tax:whole number
# OC_Tax:whole number
# OB_Charges:whole number
# OTHR_TAX:whole number
# Service_Tax:whole number
# CGST_Amount:whole number
# CGST_Amount_1:whole number
# IGST_Amount:whole number
# Extra_Charges:whole number
# Mang_Fee:whole number
# Visa:Float
# SGST_Amount:Float
# CGST_Amount_2:Float
# IGST_Amount_1:Float
#CXL_Amount:whole number
#Discount:Float
#Net_Amount:whole number
# Trvl_Dt: Date
# Trvl_Dt_2: Date
# Return_Date: Date
#PSR_Date: Date
#Lead_Time:whole number
#Ticket_Count: whole number
#Employee_code: whole number
#EMPLOYEE_CODE_1 :whole number
#FUNCTION : whole number
#SUB FUNCTION : whole number
#Project Code : whole number
#** Period - Date


# COMMAND ----------

