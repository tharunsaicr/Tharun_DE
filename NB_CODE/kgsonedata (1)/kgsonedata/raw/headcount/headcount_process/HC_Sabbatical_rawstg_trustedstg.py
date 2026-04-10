# Databricks notebook source
import datetime

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

current_cutoff_date = datetime.datetime.strptime(currentCutOff, '%Y-%m-%d').date()

tableName = "sabbatical"
processName = "headcount"

print(currentCutOff)
print(tableName)
# dbutils.widgets.removeAll()


convertedFileDate = current_cutoff_date
# print(convertedFileDate)

# print(fileDate)
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

from dateutil.parser import parse
from pyspark.sql.functions import col,when,lit,date_sub,to_date,count
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# Commented and changed on 5/18/2023
# leaveDf = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_leave_report")


# commented on 2023-09-06
# leaveDf = spark.sql("select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")


# COMMAND ----------

leaveDf = spark.sql("select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report where left(to_date(File_Date,'yyyyMMdd'),4) <= 'left("+str(convertedFileDate)+",4)'"+")) hist where rank = 1")

# COMMAND ----------

# display(leaveDf)

# COMMAND ----------

# DBTITLE 1,Apply filter and other conditions
sabbaticalDf = leaveDf.filter((leaveDf.Leave_Type == "KGS Sabbatical Leave") & (leaveDf.Leave_End_Date > currentCutOff) & ((leaveDf.Leave_Start_Date < currentCutOff) & (leaveDf.Leave_Start_Date != "1900-01-01")) & (leaveDf.Approval_Status == "Approved") & ((col("Cancel_Status").isNull()) | (col("Cancel_Status") == "")))

# COMMAND ----------

# DBTITLE 1,Change the  emp category in emp details to confirmed-sabbatical
finaldf = sabbaticalDf.withColumn("Employee_Category",lit("Confirmed-Sabbatical"))
# display(finaldf)                                       

# COMMAND ----------

finaldf = finaldf.select("Employee_Number","Leave_Start_Date","Leave_End_Date")

# display(finaldf)

# COMMAND ----------

# DBTITLE 1,Get required fields from Employee Details
# employeeDf = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address from kgsonedatadb.trusted_stg_headcount_employee_details")

employeeDf = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,File_Date from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_details where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

employeeDf =employeeDf.withColumnRenamed("Employee_Number","ED_Employee_Number")

# display(employeeDf)

# COMMAND ----------

joinDf = finaldf.join(employeeDf,finaldf.Employee_Number == employeeDf.ED_Employee_Number,"left")
# display(joinDf)

# COMMAND ----------

finaldf = joinDf.select("Employee_Number","Full_Name","Function","Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Business_Category",'Operating_Unit',"User_Type","Client_Geography","Location","Sub_Location","Position","Job_Name","People_Group_Name","Employee_Category","Date_First_Hired","End_Date","Gender","Company_Name","Supervisor_Name","Performance_Manager","Email_Address","Leave_Start_Date","Leave_End_Date","File_Date")


# COMMAND ----------

finaldf = finaldf.withColumn("Date_First_Hired", finaldf["Date_First_Hired"].cast(DateType()))\
.withColumn("Leave_Start_Date", finaldf["Leave_Start_Date"].cast(DateType()))\
.withColumn("Leave_End_Date", finaldf["Leave_End_Date"].cast(DateType()))

display(finaldf)

# COMMAND ----------

# DBTITLE 1,drop duplicates
finaldf = finaldf.dropDuplicates()

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finaldf = finaldf.withColumn("Dated_On",lit(currentdatetime))

finaldf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True")\
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})