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
from pyspark.sql.functions import date_format

finance_trusted_curr_savepath_url =finance_trusted_curr_savepath_url

print(finance_trusted_curr_savepath_url)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# Functional Reports- Load Travel Actual data from delta table raw_curr_fr_travel_actual
df_raw_curr_travel_actual = spark.read.table("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName)
df_raw_curr_travel_actual.count()

# COMMAND ----------

#display(df_raw_curr_travel_actual)

# COMMAND ----------

#Renaming columns, changing datatypes
df_transform_travel_actual =df_raw_curr_travel_actual.select('*').withColumn('Profit_Cost_Centre_Code',col('Profit_Cost_Centre_Code').cast(IntegerType()))\
                                                                 .withColumn('Function_Code',col('Function_Code').cast(IntegerType()))\
                                                                 .withColumn("Debit_Amt",col("Debit_Amt").cast(DoubleType()))\
                                                                 .withColumn("Credit_Amt",col("Credit_Amt").cast(DoubleType()))\
                                                                 .withColumn("Net_Amount",col("Net_Amount").cast(DoubleType()))\
                                                                 .withColumn('Project_ID',col('Project_ID').cast(IntegerType()))\
                                                                 .withColumn('Account_Code',col('Account_Code').cast(IntegerType()))\
                                                                 .withColumn('Period',when(F.to_date(col("Period"), "MMM yyyy").isNotNull(),F.to_date(col("period"), "MMM yyyy")).otherwise(to_date("Period")))\
                                                                 .withColumn('Created_By',col('Created_By').cast(IntegerType()))\
                                                                 .withColumn('Creation_Time',date_format('Creation_Time', 'HH:mm:ss'))\
                                                                 .withColumnRenamed('Bonus___1','Bonus_In_Percent')\
                                                                 .withColumn('Bonus_In_Percent',when(col('Bonus_In_Percent').contains('%'),(f.regexp_replace(f.col("Bonus_In_Percent"),"[%]","")/100).cast(FloatType())).otherwise(col('Bonus_In_Percent').cast(FloatType())))\
                                                                 .withColumnRenamed('Markup','Markup_In_Percent')\
                                                                 .withColumn('Markup_In_Percent',when(col('Markup_In_Percent').contains('%'),(f.regexp_replace(f.col("Markup_In_Percent"),"[%]","")/100).cast(FloatType())).otherwise(col('Markup_In_Percent').cast(FloatType())))\
                                                                 .withColumn('Bonus',col('Bonus').cast(IntegerType()))\
                                                                 .withColumn('Gratuity___LE',col('Gratuity___LE').cast(IntegerType()))\
                                                                 .withColumn('Total_Incl_Bonus___Gratuity',col("Total_Incl_Bonus___Gratuity").cast(DoubleType()))\
                                                                 .withColumn('Total_Incl_Markup',col("Total_Incl_Markup").cast(DoubleType()))\
                                                                 .withColumn('Check',col('Check').cast(BooleanType()))\
                                                                 .withColumn('MI_Grouping_Check',col('MI_Grouping_Check').cast(BooleanType()))\
                                                                 .withColumn('Entity_Name_Check',col('Entity_Name_Check').cast(BooleanType()))\
                                                                 .withColumn('Description_Length',col('Description_Length').cast(IntegerType()))\
                                                                 .withColumn('Jounal_Name_Check',col('Jounal_Name_Check').cast(BooleanType()))\
                                                                 .withColumn('amount_in_dollar_000',col('amount_in_dollar_000').cast(DoubleType()))\
                                                                 .withColumn('amount_in_dollar_million',col('amount_in_dollar_million').cast(DoubleType()))\
                                                                 .withColumn("Calendar_Year",F.year(F.to_date("Period","yyyy"))) \
                                                                 .withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy")))

# COMMAND ----------

df_transform_travel_actual.count()

# COMMAND ----------

#df_transform_travel_actual#NA Checks on SL_Mapping
df_transform_travel_actual=df_transform_travel_actual.withColumn('SL_Mapping',when(col('SL_Mapping').contains("#N/A"),"").otherwise(col('SL_Mapping')))
df_transform_travel_actual.count()

# COMMAND ----------

#Using Colregex to get the BU Travel\OPE column name and making it one common format 'BU_Travel_OPE'
col1=df_transform_travel_actual.select(df_transform_travel_actual.colRegex("`(BU_Travel_)+?.+`")).columns
new_col=col1[0]
print(new_col)
df_transform_travel_actual=df_transform_travel_actual.withColumnRenamed(new_col,'BU_Travel_OPE')

# COMMAND ----------

#Filter BU_Travel_OPE is equal to "BU Travel"
df_transform_travel_actual=df_transform_travel_actual.filter(col("BU_Travel_OPE")=="BU Travel")
df_transform_travel_actual.count()

# COMMAND ----------

display(df_transform_travel_actual)

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current
df_transform_travel_actual.write \
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

#Test scripts added at the end 
# df=df_transform_travel_actual.withColumn('Creation_Time', date_format('Creation_Time', 'HH:mm:ss'))
# #split(df_transform_travel_actual['Creation_Time'], 'T').getItem(1)
# df1=df_transform_travel_actual.withColumn('Creation_Time',split(df_transform_travel_actual['Creation_Time'], 'T').getItem(1))

# split_col = F.split(df_transform_travel_actual['Creation_Time'], 'T')


# df=df_transform_travel_actual.select(df_transform_travel_actual.colRegex("`BU_Tr`")).columns
# display(df)
#df.colRegex("`.+name$`") df.select(df.colRegex("`col[123]`")).show() "`(col)+?.+`")


# df_raw_curr_travel_actual=df_raw_curr_travel_actual.withColumn('Creation_Time', date_format('Creation_Time', 'HH:mm:ss'))
# display(df_raw_curr_travel_actual.select('Creation_Time'))
 #.withColumn('Creation_Time',to_timestamp(col('Creation_Time'),'HH:mm:ss'))\ .withColumn('Creation_Time', date_format('Creation_Time', 'HH:mm:ss'))\