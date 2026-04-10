# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables

# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables


# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

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

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

#Extract filename & date - switch case for each file

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
import pytz
from pyspark.sql.functions import col,lit,when,upper,to_timestamp

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",landing_path_url+filePath)

currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)

for col in currentDf.columns:
    if((tableName.lower() == "leave_report") & (col == "Leave type")):
        print(col)
        currentDf=currentDf.withColumnRenamed("Leave type","Leave Type")
        # currentDf=currentDf.withColumnRenamed("Leave Type1","Leave Type")

    currentDf=currentDf.withColumnRenamed(col,replacechar(col))


# Typecast every column to String - added on 3/10/2023
currentDf=colcaststring(currentDf,currentDf.columns)
# print(currentDf.count())
display(currentDf)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file

# COMMAND ----------

# DBTITLE 1,Typecast every column to String
# Typecast every column to String  - added on 3/28/2023
currentDf=colcaststring(currentDf,currentDf.columns)
display(currentDf)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

print(currentDf.count())

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# emp_col = "Employee_Number"
# if emp_col in currentDf.columns:
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

# COMMAND ----------

# if(tableName == "employee_details"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

# if(tableName == "loaned_staff_resigned"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))
#     currentDf = currentDf.withColumn("Start_Date", currentDf["Start_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("End_Date", currentDf["End_Date"].cast(DateType()))
#     currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))
    
# if(tableName == "contingent_worker_resigned"):
#     currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))

# # commented on 1/3/2023 for monthly load
# # if(tableName == "loaned_resigned"):
# #     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(IntegerType()))
    
# # if(tableName == "loaned_staff_from_ki"):
# #     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(IntegerType()))
    
# # if(tableName == "talent_konnect_resignation_status_report"):
# #     currentDf = currentDf.withColumn("No_of_days_Waved", currentDf["No_of_days_Waved"].cast(IntegerType()))
# #     currentDf = currentDf.withColumn("PM_EMP_CODE", currentDf["PM_EMP_CODE"].cast(IntegerType()))
# #     currentDf = currentDf.withColumn("HRBP_EMP_Code_", currentDf["HRBP_EMP_Code_"].cast(IntegerType()))
    


# COMMAND ----------

# Compare count with config table on column count. - Yet to add

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# DBTITLE 1,Standardize BU across all Process
for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
        .otherwise(joinDf[columnName]))
        
        joinDf = joinDf.drop(*buList.columns)

        currentDf = joinDf

# COMMAND ----------

currentDf = currentDf.dropDuplicates()

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# print(currentDf.count())

# COMMAND ----------

# DBTITLE 1,Load Staging Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

saveTableName = "kgsonedatadb.raw_stg_"+processName + "_"+ tableName
print(tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw_adhoc'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})