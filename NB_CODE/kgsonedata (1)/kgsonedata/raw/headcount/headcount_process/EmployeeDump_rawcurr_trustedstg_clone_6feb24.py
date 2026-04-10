# Databricks notebook source
# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Use this space to import any packages and functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from dateutil.parser import parse

# COMMAND ----------

# DBTITLE 1,Parameters & Variables declaration, intake
dbutils.widgets.text(name = "CurrentMonth", defaultValue = "")
current_month_left = dbutils.widgets.get("CurrentMonth") + " Left"

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

dbutils.widgets.text(name = "LastCutOffDate", defaultValue = "")
last_cutoff_date = dbutils.widgets.get("LastCutOffDate")

processName = "headcount"

# current_cutoff_date_minus_one = "2022-08-16"#to_date(current_cutoff_date,"YYYY-MM-DD") #Derive using date_sub function on current_cutoff_date
table_name = "employee_dump"

print(current_month_left)
print(current_cutoff_date)
print(last_cutoff_date)
# print(current_cutoff_date_minus_one)

# COMMAND ----------

# DBTITLE 1,Below drop column variables are specific to this notebook
#Below drop column variables are specific to this notebook
drop_columns_before_processing = ("Status","Entity","TK_Status","Termination_Date")
drop_columns_after_processing = ("TK_Status","UpdatedDate_First_Hired","UpdatedTermination_Date","last_cutoff_Status","TK_REQUESTSTATUS","TK_APPROVED_LWD","last_cutoff_Employee_Number","Last_Processed_Status","Sabbatical_Employee_Number")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where EMPLOYEENUMBER = '29564'
# MAGIC -- update kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report set APPROVED_LWD = '2022-09-01' where EMPLOYEENUMBER = '29564'

# COMMAND ----------

df_currentED = spark.sql("select * from kgsonedatadb.raw_curr_headcount_employee_dump")

df_currentTD = spark.sql("select Employee_Number as TD_Employee_Number from kgsonedatadb.raw_curr_headcount_termination_dump")

print(df_currentED.count())

df_currentTKStatus = spark.sql("select distinct EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where APPROVED_LWD <> '1900-01-01'")
print(df_currentTKStatus.count())
df_currentTKStatus = df_currentTKStatus.dropDuplicates()
print(df_currentTKStatus.count())

# df_currentconvergereport = spark.sql("select EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from kgsonedatadb.raw_curr_headcount_converge_report")
# df_currentconvergereport = df_currentconvergereport.dropDuplicates()

df_lastcutoff_ED = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) ed_hist where rank = 1")

df_lastcutoff_ED = df_lastcutoff_ED.drop("rank")

df_lastcutoff_EmployeeDetails = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_details where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) empdtls_hist where rank = 1")
df_lastcutoff_EmployeeDetails = df_lastcutoff_EmployeeDetails.drop("rank")

df_lastcutoff_EmployeeDetails_EmpNum = spark.sql("select Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) empdtls_hist where rank = 1")
df_lastcutoff_EmployeeDetails_EmpNum = df_lastcutoff_EmployeeDetails_EmpNum.drop("rank")

df_lastcutoff_ResignedLeft_EmpNum = spark.sql("select Employee_Number from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_resigned_and_left where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_resigned_and_left where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) res_lft_hist where rank = 1")
df_lastcutoff_ResignedLeft_EmpNum = df_lastcutoff_ResignedLeft_EmpNum.drop("rank")

df_lastcutoff_TKStatus = spark.sql("select EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from (select row_number() over(partition by EMPLOYEENUMBER order by file_date desc, dated_on desc, resignationdate desc) as rank, * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where APPROVED_LWD <> '1900-01-01' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) tk_hist where rank = 1")

df_lastcutoff_TKStatus = df_lastcutoff_TKStatus.drop("rank")

df_lastcutoff_TKStatus = df_lastcutoff_TKStatus.dropDuplicates()

# df_lastcutoff_convergereport = spark.sql("select EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from (select rank() over(partition by EMPLOYEENUMBER,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_converge_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_converge_report where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) converge_hist where rank = 1")
# df_lastcutoff_convergereport = df_lastcutoff_convergereport.drop("rank")

# df_lastcutoff_convergereport = df_lastcutoff_convergereport.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Creating additional columns required for processing
#Adding new column updateddate_first_hired to handle datetype issue using udf to use 'between' function

df_currentED = df_currentED.withColumn("UpdatedDate_First_Hired", when(df_currentED.Date_First_Hired.isNotNull() ,df_currentED.Date_First_Hired).otherwise("9999-12-31"))

df_currentED = df_currentED.withColumn("UpdatedDate_First_Hired", changeDateFormat(col("UpdatedDate_First_Hired")))
#Adding new column updateddate_first_hired to handle datetype issue using udf to use 'between' function

df_currentED = df_currentED.withColumn("UpdatedTermination_Date", when(df_currentED.Termination_Date.isNotNull() ,df_currentED.Termination_Date).otherwise("9999-12-31"))

df_currentED = df_currentED.withColumn("UpdatedTermination_Date", changeDateFormat(col("UpdatedTermination_Date")))

# display(df_currentED.select("UpdatedTermination_Date"))

# COMMAND ----------

# DBTITLE 1,Derive Entity name based on Company_Name, Update Status of these employees to KI
df_currentED=df_currentED.drop(*drop_columns_before_processing)

# df_currentED=df_currentED.withColumn("Last_Processed_Status",df_currentED.Status)
df_currentED = df_currentED.join(df_lastcutoff_ED,df_currentED.Employee_Number == df_lastcutoff_ED.Employee_Number,"left").select(df_currentED["*"],df_lastcutoff_ED["Status"].alias("Last_Processed_Status"))

print(df_currentED.count())

df_currentED=df_currentED.withColumn("Entity", when((df_currentED.Company_Name == "KPMG Global Services Management Private Limited") | (df_currentED.Company_Name == "KPMG Global Services Private Limited") | (df_currentED.Company_Name == "KPMG Resource Centre Private Limited") | (df_currentED.Company_Name == "KPMG Global Delivery Center Private Limited"),"KGS").otherwise("KI"))

df_currentED=df_currentED.withColumn("Status",df_currentED.Last_Processed_Status)
                                     
# display(df_currentED)
# df_currentED.select('Entity','Status').distinct().show()

# COMMAND ----------

display(df_currentED.filter(col("Employee_Number") == "103983"))

# COMMAND ----------

# DBTITLE 1,Applying transformations on Status column for KI entities based on HR process
#df_ed=df_ed.withColumn(col("Status"), when(col("Entity") == "KI","KI"))
df_currentED=df_currentED.withColumn("Status", when((df_currentED.Entity == "KI") & (df_currentED.Last_Processed_Status == "HC"),"KGS - KI").when((df_currentED.Entity == "KI") & ((df_currentED.Last_Processed_Status == "KGS - KI")|(df_currentED.Last_Processed_Status.isNull())),"KI").otherwise(df_currentED.Status))

# COMMAND ----------

# DBTITLE 1,Applying transformations on Status column for KGS entities based on HR process
#Deriving HC
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Last_Processed_Status == "New Joiners") | (df_currentED.Last_Processed_Status == "KI - KGS") | ((df_currentED.Last_Processed_Status.isNull()) & (df_currentED.Date_First_Hired <= last_cutoff_date))) ,"HC").otherwise(df_currentED.Status))

#Deriving KI-KGS
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & (df_currentED.Last_Processed_Status == "KI"),"KI - KGS").otherwise(df_currentED.Status))

#Deriving Old Termination
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & (lower(df_currentED.Last_Processed_Status).like("%left%")),"Old Termination").otherwise(df_currentED.Status))


#Deriving New Joiners
# df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Last_Processed_Status.isNull()) & (df_currentED.Date_First_Hired.between(last_cutoff_date,current_cutoff_date))),"New Joiners").otherwise(df_currentED.Status))

#Changed on 9/22/2022
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Last_Processed_Status.isNull()) & ((df_currentED.Date_First_Hired > last_cutoff_date) & (df_currentED.Date_First_Hired <= current_cutoff_date))),"New Joiners").otherwise(df_currentED.Status))


# COMMAND ----------

# DBTITLE 1,Deriving TK_Status & ResignationDate from TalentKonnectResignationFile and ConvergeReport
#Joining ED with TalentKonnect Resignation Status File
#f_currentED = df_currentED.join(df_currentTKStatus,df_currentED.Employee_Number == df_currentTKStatus.EMPLOYEENUMBER,"left").select(df_currentED["*"], df_currentTKStatus["REQUESTSTATUS"], df_currentTKStatus["APPROVED_LWD"])

#Joining ED with TalentKonnect Resignation Status File & Converge Report File

df_currentED = df_currentED.join(df_currentTKStatus,df_currentED.Employee_Number == df_currentTKStatus.EMPLOYEENUMBER,"left").select(df_currentED["*"],df_currentTKStatus["REQUESTSTATUS"].alias("TK_REQUESTSTATUS"),df_currentTKStatus["APPROVED_LWD"].alias("TK_APPROVED_LWD"))

print(df_currentED.count())

df_currentED = df_currentED.withColumn("Termination_Status",df_currentED["TK_REQUESTSTATUS"]).withColumn("Termination_Date",df_currentED["TK_APPROVED_LWD"])

#Deriving current month left cases
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Last_Processed_Status == "HC") | (upper(df_currentED.Last_Processed_Status) == "NEW JOINERS") | (upper(df_currentED.Last_Processed_Status) == "KI - KGS")) & ((upper(df_currentED.Termination_Status) == "APPROVED BY PARTNER") | (upper(df_currentED.Termination_Status) == "CLEARANCE APPROVED PMS") | (upper(df_currentED.Termination_Status) == "APPROVED BY PM") | (upper(df_currentED.Termination_Status) == "APPROVED") | (upper(df_currentED.Termination_Status) == "LWD APPROVED - RESIGNATION ON BEHALF") | (upper(df_currentED.Termination_Status) == "TERMINATED") | (upper(df_currentED.Termination_Status) == "WITHDRAWAL REJECTED")) & ((df_currentED.Termination_Date >= last_cutoff_date) & (df_currentED.Termination_Date < current_cutoff_date)),current_month_left).otherwise(df_currentED.Status))


#Deriving HC cases
df_currentED = df_currentED.withColumn("Status", when(((df_currentED.Entity == "KGS") & (df_currentED.Last_Processed_Status == "HC")) & (df_currentED.Termination_Date >= current_cutoff_date) & ((upper(df_currentED.Termination_Status) == "WITHDRAW APPROVED") | (upper(df_currentED.Termination_Status) == "WITHDRAWN WITHOUT APPROVAL") | (upper(df_currentED.Termination_Status) == "REVERSE TERMINATION") | (upper(df_currentED.Termination_Status).like("%PENDING%")) | (upper(df_currentED.Termination_Status) == "REJECTED")),"HC").otherwise(df_currentED.Status))


#Deriving New Joiners & Left
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((upper(df_currentED.Termination_Status) == "APPROVED BY PARTNER") | (upper(df_currentED.Termination_Status) == "CLEARANCE APPROVED PMS") | (upper(df_currentED.Termination_Status) == "APPROVED BY PM") | (upper(df_currentED.Termination_Status) == "APPROVED") | (upper(df_currentED.Termination_Status) == "LWD APPROVED - RESIGNATION ON BEHALF") | (upper(df_currentED.Termination_Status) == "TERMINATED") | (upper(df_currentED.Termination_Status) == "WITHDRAWAL REJECTED")) & ((df_currentED.Termination_Date > last_cutoff_date) & (df_currentED.Termination_Date <= current_cutoff_date)) & ((df_currentED.UpdatedDate_First_Hired > last_cutoff_date) & (df_currentED.UpdatedDate_First_Hired <= current_cutoff_date)),"New Joiners + Left").otherwise(df_currentED.Status))

print(df_currentED.count())

#Deriving Old Termination cases
#Joining with lastcutoff Resigned and Left - #9/13/2022 - CR
df_currentED = df_currentED.join(df_lastcutoff_ResignedLeft_EmpNum,df_currentED.Employee_Number == df_lastcutoff_ResignedLeft_EmpNum.Employee_Number,"left").select(df_currentED["*"],df_lastcutoff_ResignedLeft_EmpNum["Employee_Number"].alias("lastcutoff_RsgndLft_EmpNum"))

print(df_currentED.count())

df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Termination_Date < last_cutoff_date) & (df_currentED.Termination_Date != "1900-01-01")) & ((upper(df_currentED.Termination_Status) == "APPROVED BY PARTNER") | (upper(df_currentED.Termination_Status) == "CLEARANCE APPROVED PMS") | (upper(df_currentED.Termination_Status) == "APPROVED BY PM") | (upper(df_currentED.Termination_Status) == "APPROVED") | (upper(df_currentED.Termination_Status) == "LWD APPROVED - RESIGNATION ON BEHALF") | (upper(df_currentED.Termination_Status) == "TERMINATED") | (upper(df_currentED.Termination_Status) == "WITHDRAWAL REJECTED")),"Old Termination").otherwise(df_currentED.Status))

print(df_currentED.count())

df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((upper(df_currentED.Termination_Status) == "APPROVED BY PARTNER") | (upper(df_currentED.Termination_Status) == "CLEARANCE APPROVED PMS") | (upper(df_currentED.Termination_Status) == "APPROVED BY PM") | (upper(df_currentED.Termination_Status) == "APPROVED") | (upper(df_currentED.Termination_Status) == "LWD APPROVED - RESIGNATION ON BEHALF") | (upper(df_currentED.Termination_Status) == "TERMINATED") | (upper(df_currentED.Termination_Status) == "WITHDRAWAL REJECTED") | (upper(df_currentED.Termination_Status).isNull())) & (df_currentED.lastcutoff_RsgndLft_EmpNum.isNotNull()),"Old Termination").otherwise(df_currentED.Status))

print(df_currentED.count())

#9/13/2022 - CR
#Deriving current month left cases - although termination date is older than last cutoff date
#Joining with lastcutoff TK status & lastcutoff convergereport
# df_currentED = df_currentED.join(df_lastcutoff_TKStatus,df_currentED.Employee_Number == df_lastcutoff_TKStatus.EMPLOYEENUMBER,"left").join(df_lastcutoff_convergereport,df_currentED.Employee_Number == df_lastcutoff_convergereport.EMPLOYEENUMBER,"left").select(df_currentED["*"],df_lastcutoff_TKStatus["REQUESTSTATUS"].alias("last_TK_REQUESTSTATUS"),df_lastcutoff_TKStatus["APPROVED_LWD"].alias("last_TK_APPROVED_LWD"),df_lastcutoff_convergereport["REQUESTSTATUS"].alias("last_Conv_REQUESTSTATUS"),df_lastcutoff_convergereport["APPROVED_LWD"].alias("last_Conv_APPROVED_LWD"))

df_currentED = df_currentED.join(df_lastcutoff_TKStatus,df_currentED.Employee_Number == df_lastcutoff_TKStatus.EMPLOYEENUMBER,"left").select(df_currentED["*"],df_lastcutoff_TKStatus["REQUESTSTATUS"].alias("last_TK_REQUESTSTATUS"),df_lastcutoff_TKStatus["APPROVED_LWD"].alias("last_TK_APPROVED_LWD"))


# df_currentED = df_currentED.join(df_lastcutoff_convergereport,df_currentED.Employee_Number == df_lastcutoff_convergereport.EMPLOYEENUMBER,"left").select(df_currentED["*"],df_lastcutoff_convergereport["REQUESTSTATUS"].alias("last_Conv_REQUESTSTATUS"),df_lastcutoff_convergereport["APPROVED_LWD"].alias("last_Conv_APPROVED_LWD"))


df_currentED = df_currentED.withColumn("last_Termination_Status",df_currentED["last_TK_REQUESTSTATUS"]).withColumn("last_Termination_Date",df_currentED["last_TK_APPROVED_LWD"])


#Joining with lastcutoff Employee Details
df_currentED = df_currentED.join(df_lastcutoff_EmployeeDetails_EmpNum,df_currentED.Employee_Number == df_lastcutoff_EmployeeDetails_EmpNum.Employee_Number,"left").select(df_currentED["*"],df_lastcutoff_EmployeeDetails_EmpNum["Employee_Number"].alias("lastcutoff_EmpDtls_EmpNum"))


df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & ((df_currentED.Last_Processed_Status == "HC") | (upper(df_currentED.Last_Processed_Status) == "NEW JOINERS") | (upper(df_currentED.Last_Processed_Status) == "KI - KGS")) & (((df_currentED.Termination_Date < last_cutoff_date) & (df_currentED.Termination_Date != "1900-01-01")) & (df_currentED.lastcutoff_EmpDtls_EmpNum.isNotNull())) & ((upper(df_currentED.Termination_Status) == "APPROVED BY PARTNER") | (upper(df_currentED.Termination_Status) == "CLEARANCE APPROVED PMS") | (upper(df_currentED.Termination_Status) == "APPROVED BY PM") | (upper(df_currentED.Termination_Status) == "APPROVED") | (upper(df_currentED.Termination_Status) == "LWD APPROVED - RESIGNATION ON BEHALF") | (upper(df_currentED.Termination_Status) == "TERMINATED") | (upper(df_currentED.Termination_Status) == "WITHDRAWAL REJECTED")),current_month_left).otherwise(df_currentED.Status))


#CR - 9/23
df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & (df_currentED.lastcutoff_RsgndLft_EmpNum.isNotNull()),"Old Termination").otherwise(df_currentED.Status))


df_currentED = df_currentED.withColumn("Status", when((df_currentED.Entity == "KGS") & (df_currentED.lastcutoff_RsgndLft_EmpNum.isNotNull()) & (upper(df_currentED.Termination_Status).like("%REVERSE%TERMINAT%")) ,"HC").otherwise(df_currentED.Status))


# COMMAND ----------

display(df_currentED.filter(col("Employee_Number") == "103983"))

# COMMAND ----------

# DBTITLE 1,Validate against last cut off Employee Details and mark manual left cases as Old Termination
df_currentED_Join = df_currentED.join(df_lastcutoff_EmployeeDetails,df_currentED.Employee_Number == df_lastcutoff_EmployeeDetails.Employee_Number,"right").select(df_lastcutoff_EmployeeDetails["*"],df_currentED.Employee_Number.alias("curr_Emp_Num"))

df_currentED_Join = df_currentED_Join.filter(df_currentED_Join.curr_Emp_Num.isNull())
df_currentED_Join = df_currentED_Join.withColumn("Status",lit("Old Termination"))

dropEmpDtlsColumns = ("curr_Emp_Num","Dated_On","BU","Requested_By","Current_Base_Location_of_the_Candidate","Office_Location_in_the_KGS_Offer_Letter","WFA_Option__Permanent_12_Months_","_c30","_c31","_c32")
df_currentED_Join = df_currentED_Join.drop(*dropEmpDtlsColumns)

for column in [column for column in df_currentED.columns if column not in df_currentED_Join.columns]:
    df_currentED_Join = df_currentED_Join.withColumn(column, lit(None))

df_currentED.printSchema()
df_currentED_Join.printSchema()    

print(df_currentED.count())

df_currentED=df_currentED.unionByName(df_currentED_Join)

print(df_currentED.count())

# COMMAND ----------

# DBTITLE 1,Update Employee Category to Confimed-Sabbatical
leaveDf = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_leave_report")

# Apply filter and other conditions
sabbaticalDf = leaveDf.filter((leaveDf.Leave_Type == "KGS Sabbatical Leave") & (leaveDf.Leave_End_Date > current_cutoff_date) & ((leaveDf.Leave_Start_Date < current_cutoff_date) & (leaveDf.Leave_Start_Date != "1900-01-01")) & (leaveDf.Approval_Status == "Approved") & ((col("Cancel_Status").isNull()) | (col("Cancel_Status") == "")))

# Change the  emp category in emp details to confirmed-sabbatical
sabbaticalDf = sabbaticalDf.withColumn("Employee_Category",lit("Confirmed-Sabbatical"))
sabbaticalDf = sabbaticalDf.withColumnRenamed("Employee_Number","Sabbatical_Employee_Number")

# Update Employee Category to Confirmed-Sabbatical in Employee Dump
df_currentED = df_currentED.join(sabbaticalDf, df_currentED.Employee_Number == sabbaticalDf.Sabbatical_Employee_Number, "left").select(df_currentED["*"],sabbaticalDf["Sabbatical_Employee_Number"])

df_currentED = df_currentED.withColumn("Employee_Category",when(col("Sabbatical_Employee_Number").isNotNull(), lit("Confirmed-Sabbatical"))\
                         .otherwise(col("Employee_Category")))

# display(df_currentED.filter(col("Sabbatical_Employee_Number").isNotNull()))

# COMMAND ----------

# df_geo_usertype_validation = spark.sql("select Employee_Number, case when config_Geo is null then concat(Remarks,\" | Invalid Client_Geogrpaphy\") when config_Geo is not null and ED_UserType <> config_UserType then concat(Remarks,\" | Geo-UserType mismatch\") else Remarks end as Remarks from (select A.User_Type as ED_UserType, B.User_Type as config_UserType, A.Employee_Number,A.Client_Geography as ED_Geo, B.Client_Geography as config_Geo,A.Remarks from kgsonedatadb.trusted_stg_headcount_employee_dump A left join kgsonedatadb.config_clientgeography_and_usertype B on A.Client_Geography = B.Client_Geography)")

# df_currentED = df_currentED.join(df_geo_usertype_validation, df_currentED.Employee_Number == df_geo_usertype_validation.Employee_Number, "left").select(df_currentED["*"],df_geo_usertype_validation["Remarks"])

# headcount | trusted_stg_headcount_employee_dump | config_clientgeography_and_usertype | A.Client_Geography = B.Client_Geography

# COMMAND ----------

# Changed Date format to YYYY-MM-DD for Termination_Date as it is used in Resign and Left Tab
df_currentED = df_currentED.withColumn("Termination_Date", when(df_currentED.Termination_Date.isNotNull() ,df_currentED.Termination_Date).otherwise("9999-01-01"))

df_currentED = df_currentED.withColumn("Termination_Date", changeDateFormat(col("Termination_Date")))
display(df_currentED.select("Termination_Date").filter(col("Termination_Date") != "9999-01-01"))

# COMMAND ----------

# display(df_currentED)
# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,Remove computational columns
df_currentED = df_currentED.drop(*drop_columns_after_processing)

# COMMAND ----------

# display(df_currentED.filter(col("Employee_Number") == "103983"))
print(df_currentED.count())

# COMMAND ----------

# DBTITLE 1,Write processed ED into trusted staging ED
#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
df_currentED = df_currentED.withColumn("Dated_On",lit(currentdatetime))
df_currentED = df_currentED.withColumn("Remarks",lit(""))

df_currentED.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+processName+"/employee_dump") \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +table_name)

# COMMAND ----------

# DBTITLE 1,Write processed ED to trusted final
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':table_name,'ProcessName':processName})