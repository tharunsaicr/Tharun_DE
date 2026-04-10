# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(filePath)
print(tableName)
print(processName)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

print(landing_path_url+filePath)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col,lit,when,upper,to_timestamp
from pyspark.sql.functions import *
import pytz

import calendar

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(tableName)
print(fileDate)
fileYear = int(fileDate[:4])
fileMonth = calendar.month_name[int(fileDate[4:6])]

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",landing_path_url+filePath)

currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# print(currentDf.count())

# COMMAND ----------

# Typecast every column to String  - added on 3/10/2023
currentDf=colcaststring(currentDf,currentDf.columns)
# print(currentDf.count())

# COMMAND ----------

columnList = currentDf.columns
print(columnList)

if(tableName == "employee_details"):
    if "Comments" in columnList:
        currentDf = currentDf.withColumn("Comments", when(currentDf.Comments.isNull(), lit("")).otherwise(currentDf["Comments"].cast(StringType())))
    if "Remarks" in columnList:
        currentDf = currentDf.withColumn("Remarks", when(currentDf.Remarks.isNull(), lit("")).otherwise(currentDf["Remarks"].cast(StringType())))

# COMMAND ----------

# columnList = currentDf.columns
# print(columnList)

# if(tableName == "employee_details"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))
#     currentDf = currentDf.withColumn("Date_First_Hired", currentDf["Date_First_Hired"].cast(DateType()))
#     if "Comments" in columnList:
#         currentDf = currentDf.withColumn("Comments", when(currentDf.Comments.isNull(), lit("")).otherwise(currentDf["Comments"].cast(StringType())))
#     if "Remarks" in columnList:
#         currentDf = currentDf.withColumn("Remarks", when(currentDf.Remarks.isNull(), lit("")).otherwise(currentDf["Remarks"].cast(StringType())))

# if(tableName == "resigned_and_left"):
#     currentDf = currentDf.withColumn("Date_First_Hired", currentDf["Date_First_Hired"].cast(DateType()))
#     currentDf = currentDf.withColumn("Termination_Date", currentDf["Termination_Date"].cast(DateType()))
            
# if(tableName == "contingent_worker_resigned"):
#     currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))


# if(tableName == "contingent_worker"):
#     currentDf = currentDf.withColumn("Date_of_Joining", currentDf["Date_of_Joining"].cast(StringType()))

# if(tableName == "maternity_cases"):
#     currentDf = currentDf.withColumn("Date_First_Hired", currentDf["Date_First_Hired"].cast(DateType()))
#     currentDf = currentDf.withColumn("Start_Date", currentDf["Start_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("End_Date", currentDf["End_Date"].cast(DateType()))
    
# if(tableName == "sabbatical"):
#     currentDf = currentDf.withColumn("Date_First_Hired", currentDf["Date_First_Hired"].cast(DateType()))
#     currentDf = currentDf.withColumn("Leave_Start_Date", currentDf["Leave_Start_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("Leave_End_Date", currentDf["Leave_End_Date"].cast(DateType()))

# if(tableName == "secondee_outward"):
#     currentDf = currentDf.withColumn("Date_First_Hired", currentDf["Date_First_Hired"].cast(DateType()))
    
# if(tableName == "loaned_staff_resigned"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))
#     currentDf = currentDf.withColumn("Start_Date", currentDf["Start_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("End_Date", currentDf["End_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))
    
# if(tableName == "loaned_staff_from_ki"):
#     currentDf = currentDf.withColumn("Start_Date", currentDf["Start_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("End_Date", currentDf["End_Date"].cast(DateType()))

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

# # Drop null columns
# col_to_drop = get_null_column_names(currentDf)
# print(col_to_drop)
# currentDf = currentDf.drop(*col_to_drop) 

# COMMAND ----------

#commented and uncommented File_Month and File_Year on 3/10/2023 as this caused issue in Union in Employee_Dump_rawstg_trusted_stg script

# if tableName != 'encore_output':
#     print(tableName)
check_file_date = "File_Date"
check_dated_on = "Dated_On"

if check_file_date not in currentDf.columns:
    currentDf=currentDf.withColumn("File_Date", lit(fileDate))

# currentDf=currentDf.withColumn("File_Month", lit(fileMonth))
# currentDf=currentDf.withColumn("File_Year", lit(fileYear))

if check_dated_on not in currentDf.columns:
    currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
# buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
# buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

# columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

if processName == 'lnd':
    currentDf = currentDf.withColumn("File_Date", date_format(to_date(regexp_replace(col("Period"),'\'','-'), "MMM-yy"), "yyyyMMdd"))
if tableName == 'encore_output':
    currentDf = currentDf.withColumn("File_Date", concat(col('Month_Key'),lit("28")))
    currentDf = currentDf.drop('Month_Key')
    # currentDf = currentDf.withColumnRenamed("Is_Client_Facing?","Is_Client_Facing")

# COMMAND ----------

# DBTITLE 1,Load into Raw staging Table - Monthly database
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table - Monthly database
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'RAW_ADHOC'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# weeklyProcessName = "headcount"

# COMMAND ----------

# DBTITLE 1,Load into Trusted Current and History Table - Weekly database
# dbutils.notebook.run("/kgsonedata/landing/adhoc_landing_to_rawstg_load",6000, {'DeltaTableName':tableName, 'FilePath':filePath, 'ProcessName':weeklyProcessName})

# COMMAND ----------

