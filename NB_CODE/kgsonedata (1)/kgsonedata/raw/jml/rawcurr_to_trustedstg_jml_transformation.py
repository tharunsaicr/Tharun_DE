# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

print(tableName)
print(processName)
print(fileDate)

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")
display(currentDf)

# COMMAND ----------

# DBTITLE 1,If UK Joiners/Mover/Leaver get Employee Data from Employee_Dump
if tableName.upper() == "UK_JOINER":
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,GPID from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,GPID from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("GPID","ED_GPID")

    joinDf = currentDf.join(edDF,upper(currentDf.GPID) == upper(edDF.ED_GPID), "left")

    currentDf = joinDf

if tableName.upper() == "UK_MOVER":
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,UK_Employee_Number from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,UK_Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("UK_Employee_Number","ED_UK_Employee_Number")

    joinDf = currentDf.join(edDF,upper(currentDf.UK_Staff_ID) == upper(edDF.ED_UK_Employee_Number), "left")

    currentDf = joinDf


if tableName.upper() == "UK_LEAVER" :
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,UK_Employee_Number from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,UK_Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("UK_Employee_Number","ED_UK_Employee_Number")

    joinDf = currentDf.join(edDF,upper(currentDf.UK_Staff_ID) == upper(edDF.ED_UK_Employee_Number), "left")

    currentDf = joinDf

# COMMAND ----------

# DBTITLE 1,If US Joiners/Mover/Leaver get Employee Data from Employee_Dump
if tableName.upper() == "US_JOINER":
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("US_Employee_Number","ED_US_Employee_Number")

    joinDf = currentDf.join(edDF,upper(currentDf.PSID) == upper(edDF.ED_US_Employee_Number), "left")

    currentDf = joinDf

if tableName.upper() == "US_MOVER":
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("US_Employee_Number","ED_US_Employee_Number")

    joinDf = currentDf.join(edDF,upper(currentDf.PS_ID) == upper(edDF.ED_US_Employee_Number), "left")

    currentDf = joinDf


if tableName.upper() == "US_LEAVER" :
        
    # edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from kgsonedatadb.trusted_headcount_employee_dump")

    edDF = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,Position,US_Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+fileDate+"'"+",'yyyyMMdd'))) ed_hist where rank = 1")

    edDF = edDF.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Full_Name","ED_Full_Name")\
    .withColumnRenamed("Function","ED_Function")\
    .withColumnRenamed("Employee_Subfunction","ED_Employee_Subfunction")\
    .withColumnRenamed("Employee_Subfunction_1","ED_Employee_Subfunction_1")\
    .withColumnRenamed("Cost_centre","ED_Cost_centre")\
    .withColumnRenamed("Client_Geography","ED_Client_Geography")\
    .withColumnRenamed("Position","ED_Position")\
    .withColumnRenamed("US_Employee_Number","ED_US_Employee_Number")

    joinDf = currentDf.join(edDF,upper(currentDf.PS_ID) == upper(edDF.ED_US_Employee_Number), "left")

    currentDf = joinDf

# COMMAND ----------

# DBTITLE 1,Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)


# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':'jml'})