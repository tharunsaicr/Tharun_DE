# Databricks notebook source
# ED - hive_metastore.kgsonedatadb.raw_curr_employee_dump - Done
# TD - hive_metastore.kgsonedata.raw_curr_termination_dump
# Contingent - hive_metastore.kgsonedata.raw_curr_contingent
# Converge - hive_metastore.kgsonedata.raw_curr_converge_report - Done
# TalentKonnect Resignation Status - hive_metastore.kgsonedata.raw_curr_talentkonnect_resignation_status - Done
# Loan Staff from KI - hive_metastore.kgsonedatadb.raw_curr_loaned_staff_from_ki - Done
# Leave Report - hive_metastore.kgs1data.current_leave_report - Done

# COMMAND ----------

# Issues Identified

# TD - 'Start_Date_Of_Current_Employer' e.g. 38261
# Contingent - 'Creation_Date','Last_Update_Date' e.g. 13-OCT-2017 14:38:42 PM but(12/6/2016  11:00:34 AM is working)

# COMMAND ----------

# Add functionality to clean data
# 1) Detect Date column and convert them to YYYY-MM-DD
# 2) Deal with TimeStamp data e.g in excel we have date = 38565 if we convert to shortDate in excel it works but not in python
# 3) Remove any extra spaces in data
# 4) Remove rows with no data
# 5) Remove empty column with no name


# COMMAND ----------

# DQ Check

#Type casting
#Handling column headers - replace ' ' & replace special characters
# replace special characters/Spaces in key Columns(Candidate_Id,Employee_Id) 
#Handling empty columns/rows
#Handling nulls for date columns while loading into raw
# Date functions
# Remove any extra character/spaces e.g. '  1234'
# Null check in Key Columns (Employee_Number, Candidate_ID, Cost_Center,BU,Client_Geography etc.)
# Column count check b/w file and table before Df is loaded - Done(landing_to_raw_load)
# Row count check b/w file and table after Df is loaded - Done(landing_to_raw_load)



# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(tableName)
print(processName)

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_convert_column_to_date

# COMMAND ----------

# DBTITLE 1,Get Computational Date Column
dateDf = spark.sql("select * from kgsonedatadb.config_convert_column_to_date")
dateDf = dateDf.select("ColumnName").where((dateDf.ProcessName == processName) & (dateDf.DeltaTableName == tableName ))
display(dateDf)

columnList = dateDf.rdd.flatMap(lambda x: x).collect()
print("List ", columnList)

# COMMAND ----------

# DBTITLE 1,Get Computational Serial Date Columns
sdateDf = spark.sql("select * from kgsonedatadb.config_SerialDateConversionColumns")
sdateDf = sdateDf.select("SerialDate_ColumnName","KeyColumn").where((sdateDf.ProcessName == processName) & (sdateDf.DeltaTableName == tableName ))
display(sdateDf)

serialcolumnList = sdateDf.select("SerialDate_ColumnName").rdd.flatMap(lambda x: x).collect()
print("List ", serialcolumnList)

keycolumnList = sdateDf.select("KeyColumn").rdd.flatMap(lambda x: x).collect()
print("Key List ", keycolumnList)

if(keycolumnList):
    keyColumn = keycolumnList[0]
    print("Key Column : ",keyColumn)
else:
    print("No column with serial date")

# COMMAND ----------

currentDf = spark.sql("select * from kgsfinancedb.raw_stg_"+processName + "_"+ tableName)
display(currentDf)

# COMMAND ----------

# DBTITLE 1,Detect Serial Date column and convert them to YYYY-MM-DD
serialformattedColumn = []
serialDateLenght = 5 #Standard length for Serial Dates

for columnName in currentDf.columns:
     if (columnName in serialcolumnList):
            
            dateCol = currentDf.select(columnName).rdd.flatMap(lambda x: x).collect()
            dateValLength = len(dateCol[0])
            
            if(dateValLength == serialDateLenght):
                print("ColumnName ",columnName)
                df = spark.sql("select date_add('1899-12-30',cast("+ columnName +" as Integer)) as Converted_"+columnName+", "+keyColumn+" as Config_"+keyColumn+" from kgsfinancedb.raw_stg_"+processName + "_"+ tableName)

                display(df)

                joindf = currentDf.join(df,currentDf[keyColumn] == df["Config_"+keyColumn],"left").select(currentDf["*"],df["Converted_"+columnName])
                joindf = joindf.withColumn(columnName,col("Converted_"+columnName))
                joindf = joindf.drop("Converted_"+columnName)

                serialformattedColumn.append(columnName)
                currentDf = joindf
            
print("serialformattedColumn: ",serialformattedColumn)            
display(currentDf)     

# COMMAND ----------

# DBTITLE 1,Detect Date column and convert them to YYYY-MM-DD
dateKeyword = "Date"
lwdKeyword = "LWD"


nullColumns = get_null_column_names(currentDf)

formattedColumn = []

for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)
        
        currentDf = currentDf.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName).isNull()) | (col(columnName) == "") | (col(columnName) == "#N/A")),"1900-01-01").otherwise(col(columnName)))
                     
        currentDf = currentDf.withColumn(columnName,changeDateFormat(col(columnName)))
        formattedColumn.append(columnName)
        

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Display Updated Date Column
if formattedColumn:
    print("Formatted Columns", formattedColumn)
    currentDf.select([col for col in formattedColumn]).show()
else:
    print("None of the column was formatted")

# COMMAND ----------

# if (tableName == "contingent" or tableName == "contingent_worker" or tableName == "contingent_worker_resigned"):
#     currentDf = currentDf.withColumn("Candidate_Id",regexp_replace(col("Candidate_Id"), "[^a-zA-Z0-9]", ""))
    

# COMMAND ----------

emp_col = "Employee_Number"
if emp_col in currentDf.columns:
    currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

# COMMAND ----------

# display(currentDf)

# COMMAND ----------

# DBTITLE 1,Load into Raw Current Table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

