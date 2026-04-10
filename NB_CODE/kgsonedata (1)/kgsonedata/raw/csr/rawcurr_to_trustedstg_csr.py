# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,to_timestamp
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
import string
from pyspark.sql.functions import *


# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

#Entity Name Transformations

if (tableName in ['fund_recevied_in_csr_account', 'fund_recevied_in_csr_account_kgdc', 'budget', 'budget_kgdc','kgs_raw_data_cy']):

    currentDf = currentDf.withColumn("Entity",

       when((col("Entity")=="KGSPL"), "KGS")

       .when((col("Entity")=="KGSMPL"), "MPL")

       .when((col("Entity")=="KRCPL"), "KRC")

       .when((col("Entity")=="KGDCPL"), "GDC")
       
       .when((col("Entity")=="KPMG Global Services Management Private Limited"), "MPL")
       .when((col("Entity")=="KPMG Global Delivery Center Private Limited"), "GDC")
       .when((col("Entity")=="KPMG Global Services Private Limited"), "KGS")
       .when((col("Entity")=="KPMG Resource Centre Private Limited"), "KRC")

       .otherwise(col("Entity")))

else:
    if (tableName != 'per_hour_rate'):
        currentDf = currentDf.withColumn("Entity_Name",

        when((col("Entity_Name")=="KPMG RESOURCE CENTRE PRIVATE LIMITED"), "KRC")

        .when((col("Entity_Name")=="KPMG RESOURCE CENTRE PRIVATE LIMITED."), "KRC")

        .when((col("Entity_Name")=="KPMG GLOBAL SERVICES PRIVATE LIMITED."), "KGS")

        .when((col("Entity_Name")=="KPMG GLOBAL SERVICES PRIVATE LIMITED"), "KGS")

        .when((col("Entity_Name")=="KPMG GLOBAL SERVICES MANAGEMENT PRIVATE LIMITED."), "MPL")

        .when((col("Entity_Name")=="KPMG GLOBAL SERVICES MANAGEMENT PRIVATE LIMITED"), "MPL")

        .when((col("Entity_Name")=="KPMG GLOBAL DELIVERY CENTER PRIVATE LIMITED"), "GDC")

        .when((col("Entity_Name")=="KPMG GLOBAL DELIVERY CENTER PRIVATE LIMITED."), "GDC")

        .otherwise(col("Entity_Name")))

   

# display(currentDf)

# COMMAND ----------

# DBTITLE 1,Get NGO List and Tables to be Updated
ngoList = spark.sql("Select NGO_Name as Actual_NGO_Name,Standardized_NGO_Name, Focus_Area as Actual_Focus_Area  from kgsonedatadb.config_ngo_mapping_list")

ngoTableList = spark.sql("select * from kgsonedatadb.config_ngo_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

columnList = ngoTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

FocusAreaList = ['ngo_fund_budget', 'ngo_fund_budget_kgdc', 'ngo_fund_util', 'ngo_fund_util_kgdc']

# COMMAND ----------

# DBTITLE 1,Standardize NGO across all Tables
for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(ngoList,upper(currentDf[columnName]) == upper(ngoList['Actual_NGO_Name']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Standardized_NGO_Name.isNotNull(),initcap(col('Standardized_NGO_Name')))\
        .otherwise(initcap(joinDf[columnName])))

        if (tableName in FocusAreaList):
            joinDf = joinDf.withColumn('Focus_area',when(joinDf.Actual_Focus_Area.isNotNull(),col('Actual_Focus_Area'))\
            .otherwise(joinDf.FOCUS_AREA))
        
        joinDf = joinDf.drop(*ngoList.columns)
        
        currentDf = joinDf


# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
currentDf = currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

MonthList = ['kgspl_gl','kgdcpl_gl','krcpl_gl','indirect_exp_krc','indirect_exp_kgs', 'indirect_expense_kgdc','kgsmpl_gl','indirect_exp_kgsmpl']

OtherTables = ['ngo_fund_budget', 'ngo_fund_budget_kgdc', 'ngo_fund_util', 'ngo_fund_util_kgdc']

FundTables = ['fund_recevied_in_csr_account', 'fund_recevied_in_csr_account_kgdc']

FY_months = ['JAN', 'FEB', 'MAR']

if tableName.lower() in MonthList:

    currentDf = currentDf.withColumn("Year_Month",concat(regexp_extract('Period_Name','[0-9]+', 0) , lit(" "), substring(regexp_replace('Period_Name', "[^a-zA-Z]", ""),1,3)))

    currentDf = currentDf.withColumn("Financial_Year", when(upper(substring(regexp_replace('Period_Name', "[^a-zA-Z]", ""),1,3)).isin(FY_months),regexp_extract('Period_Name','[0-9]+', 0).cast('int')).otherwise(regexp_extract('Period_Name','[0-9]+', 0).cast('int')+1))

 

if tableName.lower() in OtherTables:

    currentDf = currentDf.withColumn("Financial_Year", when(upper(date_format('Month_and_Year', 'MMM')).isin(FY_months),year('Month_and_Year').cast('int')).otherwise(year('Month_and_Year').cast('int')+1))

 
if tableName.lower() in FundTables:

    currentDf = currentDf.withColumn("Financial_Year", when(upper(date_format('Date', 'MMM')).isin(FY_months),year('Date').cast('int')).otherwise(year('Date').cast('int')+1))


if tableName.lower() == 'kgs_raw_data_cy':

    currentDf = currentDf.withColumn("Financial_Year", when(upper(substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3)).isin(FY_months),regexp_extract('Month','[0-9]+', 0).cast('int')).otherwise(regexp_extract('Month','[0-9]+', 0).cast('int')+1))

# COMMAND ----------

# DBTITLE 1, Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

