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
from pyspark.sql.functions import col, lit

currentdatetime= datetime.now()
fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",landing_path_url+filePath)

currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

    
currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

display(currentDf)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

emp_col = "Employee_Number"
if emp_col in currentDf.columns:
    currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

# COMMAND ----------

if(tableName == "employee_details"):
    currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

if(tableName == "loaned_staff_resigned"):
    currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))
    currentDf = currentDf.withColumn("Start_Date", currentDf["Start_Date"].cast(DateType()))
    currentDf = currentDf.withColumn("End_Date", currentDf["End_Date"].cast(DateType()))
    currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))
    
if(tableName == "contingent_worker_resigned"):
    currentDf = currentDf.withColumn("LWD", currentDf["LWD"].cast(DateType()))

# commented on 1/3/2023 for monthly load
# if(tableName == "loaned_resigned"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(IntegerType()))
    
# if(tableName == "loaned_staff_from_ki"):
#     currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(IntegerType()))
    
if(tableName == "talent_konnect_resignation_status_report"):
    currentDf = currentDf.withColumn("No_of_days_Waved", currentDf["No_of_days_Waved"].cast(IntegerType()))
    currentDf = currentDf.withColumn("PM_EMP_CODE", currentDf["PM_EMP_CODE"].cast(IntegerType()))
    currentDf = currentDf.withColumn("HRBP_EMP_Code_", currentDf["HRBP_EMP_Code_"].cast(IntegerType()))
    


# COMMAND ----------

# Compare count with config table on column count. - Yet to add

# COMMAND ----------

currentDf = currentDf.dropDuplicates()

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

saveTableName = "kgsonedatadb.raw_stg_"+processName + "_"+ tableName
print(tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists(saveTableName)):
#     tableDf = spark.sql("select * from "+saveTableName)

#     tableDf_row = tableDf.count()
#     print("Row ",tableDf_row)

#     tableDf_col = len(tableDf.columns)
#     print("Column ",tableDf_col)

#     if((row == tableDf_row) & (column == tableDf_col)):
#         print("Row and Column Count is Matching!!")
#     else:
#         print("Row Count is NOT Matching!!")
#         fail
    
# else:
#     print("Table doesnot exists")
#     fail

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/raw/adhoc_raw_to_trusted_clean_data",6000, {'DeltaTableName':tableName, 'ProcessName':processName})