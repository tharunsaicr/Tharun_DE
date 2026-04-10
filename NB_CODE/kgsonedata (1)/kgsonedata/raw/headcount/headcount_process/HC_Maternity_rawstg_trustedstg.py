# Databricks notebook source
import datetime

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

current_cutoff_date = datetime.datetime.strptime(currentCutOff, '%Y-%m-%d').date()
current_cutoff_date = str(current_cutoff_date)

tableName = "maternity_cases"
processName = "headcount"

print(currentCutOff)
print(tableName)
# dbutils.widgets.removeAll()

convertedFileDate = current_cutoff_date
convertedFileDate = str(convertedFileDate)

print(convertedFileDate)

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

# leaveDf = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_leave_report")

# commented on 2023/09/06
# leaveDf = spark.sql("select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

# COMMAND ----------

leaveDf = spark.sql("select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report where left(to_date(File_Date,'yyyyMMdd'),4) <= 'left("+str(convertedFileDate)+",4)'"+")) hist where rank = 1")

# COMMAND ----------

display(leaveDf)

# COMMAND ----------

maternityDf = leaveDf.filter(((leaveDf.Leave_Type == "KGS Maternity Leave") | (leaveDf.Leave_Type == "KGS Maternity Miscarriage Leave")) & (leaveDf.Leave_End_Date > currentCutOff) & ((leaveDf.Leave_Start_Date < currentCutOff) & (leaveDf.Leave_Start_Date != "1900-01-01")) & (leaveDf.Approval_Status == "Approved") & ((col("Cancel_Status").isNull()) | (col("Cancel_Status")== "")))

# COMMAND ----------

display(maternityDf)

# COMMAND ----------

# maternityDf = leaveDf.filter((leaveDf.Leave_Type == "KGS Maternity Leave") & (leaveDf.UpdatedLeave_End_Date > currentCutOff) & (leaveDf.UpdatedLeave_Start_Date < currentCutOff) & (leaveDf.Approval_Status == "Approved") & (col("Cancel_Status").isNull()))


# 9/19/2022 6:11 pm
# maternityDf = leaveDf.filter(((leaveDf.Leave_Type == "KGS Maternity Leave") | (leaveDf.Leave_Type == "KGS Maternity Miscarriage Leave")) & (leaveDf.UpdatedLeave_End_Date > currentCutOff) & (leaveDf.UpdatedLeave_Start_Date < currentCutOff) & (leaveDf.Approval_Status == "Approved") & ((col("Cancel_Status").isNull()) | (col("Cancel_Status")== "")))

# COMMAND ----------

# display(maternityDf)

# COMMAND ----------

# maternityDf = maternityDf.drop("UpdatedLeave_End_Date","UpdatedLeave_Start_Date")
# display(maternityDf)

# COMMAND ----------

#Dummy data for previous months maternity report Will need to replace with actual table
# previousDf = spark.sql("select * from kgs1data.MaternityReport limit 10")

# previousDf = previousDf.withColumnRenamed("Employee_Number","Employee_Number_Previous")

# COMMAND ----------

previousDf = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_maternity_cases where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_maternity_cases where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) previousDf_hist where rank = 1")
previousDf = previousDf.drop("rank")

# previousDf = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_maternity_cases where dated_on in (select max(dated_on) from kgsonedata.trusted_hist_headcount_maternity_cases where to_date(dated_on) < to_date(current_timestamp))")

# COMMAND ----------

previousDf = previousDf.select("Employee_Number")


# COMMAND ----------

previousDf.createOrReplaceTempView("previousMaternityReport")
maternityDf.createOrReplaceTempView("currentMaternityReport")

# COMMAND ----------

# display(maternityDf)

# COMMAND ----------

# DBTITLE 1,Only new cases to maternity tab (vlookup from prev HC maternity)
# Same as sabbatical but Copy only new cases to maternity tab (vlookup from prev HC maternity tab to see if they are already present, only new cases to be appended)
# finalDf = maternityDf.join(previousDf, maternityDf.Employee_Number == previousDf.Employee_Number_Previous)\
# .select(maternityDf["*"],previousDf["Employee_Number_Previous"])

finalDf = spark.sql("select * from currentMaternityReport where Employee_Number NOT IN (select * from previousMaternityReport)")

# display(finalDf)


# COMMAND ----------

finalDf = finalDf.withColumn("Status",when((col("Leave_End_Date") < currentCutOff) & (col("Leave_End_Date") != "1900-01-01"), lit("No information on Resume / Extension"))\
                             .when(col("Leave_End_Date") >= currentCutOff, lit("Maternity")).otherwise(None))

finalDf = finalDf.withColumnRenamed("Leave_Start_Date","Start_Date")\
.withColumnRenamed("Leave_End_Date","End_Date")

finalDf = finalDf.select("Employee_Number","Start_Date","End_Date","Status")

# display(finalDf)

# COMMAND ----------

# DBTITLE 1,Get required columns from Employee Details
# Yet to add BU
employeeDf = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,Company_Name,BU,File_Date from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_details where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+current_cutoff_date+"'"+"))) empdtls_hist where rank = 1")

employeeDf = employeeDf.withColumnRenamed("Employee_Number","ED_Employee_Number")

# display(employeeDf)

# COMMAND ----------

# joinDf = finalDf.join(employeeDf,employeeDf.Employee_Number == finalDf.Maternity_Employee_Number,"left")

# # Yet to add BU

# joinDf = joinDf.select("Employee_Number","Full_Name","Function","Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Business_Category",'Operating_Unit',"User_Type","Client_Geography","Location","Sub_Location","Position","Job_Name","People_Group_Name","Employee_Category","Date_First_Hired","Company_Name","Maternity_Employee_Number","Start_Date","End_Date","Status","BU")


# finalDf = joinDf.drop("Maternity_Employee_Number")

# COMMAND ----------

joinDf = finalDf.join(employeeDf,employeeDf.ED_Employee_Number == finalDf.Employee_Number,"left")

joinDf = joinDf.select("Employee_Number","Full_Name","Function","Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Business_Category",'Operating_Unit',"User_Type","Client_Geography","Location","Sub_Location","Position","Job_Name","People_Group_Name","Employee_Category","Date_First_Hired","Company_Name","ED_Employee_Number","Start_Date","End_Date","Status","BU","File_Date")

finalDf = joinDf.drop("ED_Employee_Number")

# display(finalDf)

# COMMAND ----------

finalDf = finalDf.withColumn("Date_First_Hired", finalDf["Date_First_Hired"].cast(DateType()))\
.withColumn("Start_Date", finalDf["Start_Date"].cast(DateType()))\
.withColumn("End_Date", finalDf["End_Date"].cast(DateType()))

# display(finalDf)

# COMMAND ----------

# DBTITLE 1,drop duplicates
finalDf = finalDf.dropDuplicates() 

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finalDf = finalDf.withColumn("Dated_On",lit(currentdatetime))

finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})