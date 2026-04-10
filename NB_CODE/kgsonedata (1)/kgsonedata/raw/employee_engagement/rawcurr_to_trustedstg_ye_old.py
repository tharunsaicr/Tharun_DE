# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "employee_engagement"
tableName = "year_end"

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,FileDate conversion
dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")



fileYear = FileDate[:4]
fileYear_FY = "FY"+FileDate[2:4]
prevFileYear = int(fileYear) - 1
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileDay+'-'+fileMonth+'-'+fileYear

if(int(fileMonth) in [10,11,12]):
    fileYear_FY = "FY"+str(int(FileDate[2:4])+1)
else:
    fileYear_FY = "FY"+FileDate[2:4]

print("Year ", fileYear)
print("FY ", fileYear_FY)
print("Prev Year ", prevFileYear)
print("Month ", fileMonth)
print("Day ", fileDay)
print("ConvertedDate ", convertedFileDate)


# COMMAND ----------

import datetime
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
print(convertedFileDate)

convertedFileDate = datetime.datetime.strptime(convertedFileDate, '%Y-%m-%d').date()


# COMMAND ----------

from datetime import date
from datetime import datetime

fileDateTimestamp = datetime.combine(convertedFileDate, datetime.min.time())

print(fileDateTimestamp)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Joining yec and employee_dump Delta Table

from pyspark.sql.types import *
import datetime
from datetime import *
import pyspark
from pyspark.sql.functions import lit, col, split
from pyspark.sql import functions as F

#Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
# df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS'")

df_emp_dump = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1")

# print(df_emp_dump.count())

df_emp_dump = df_emp_dump.withColumn("Approved_LWD",F.date_format(F.to_date(col('Approved_LWD')),'yyyy-MM-dd')).withColumn("CurrentDate",lit(convertedFileDate))

df_emp_dump = df_emp_dump.filter((col('Approved_LWD') >= col('CurrentDate')) | (col('Approved_LWD').isNull()))

# print(df_emp_dump.count())

#Join with config_job_code_distribution_category_mapping to get distribution_category
df_jobcode_distcategory = spark.sql("select * from kgsonedatadb.config_job_code_distribution_category_mapping where processname = 'Employee_Engagement' and tablename = 'Year_End'")

df_emp_dump = df_emp_dump.join(df_jobcode_distcategory, ((df_emp_dump.Position == df_jobcode_distcategory.Position) & (df_emp_dump.Job_Name == df_jobcode_distcategory.Job_Name)),"left").select(df_emp_dump["*"],df_jobcode_distcategory["Mapping"])

df_emp_dump=df_emp_dump.withColumnRenamed("Cost_centre","Cost_Center").withColumnRenamed("Client_Geography","Client_Geo").withColumnRenamed("Email_Address","Email1").withColumnRenamed("Performance_Manager","Performance_manager_name")
#.withColumn("Date_as_of",to_date(current_timestamp()));

# print(df_emp_dump.count())

#Compensation YEC data from delta table raw_curr_compensation_yec
df_comp_yec = spark.sql("select * from (select rank() over(partition by EMP_ID,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_compensation_yec where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_compensation_yec where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) yec_hist where rank = 1")

df_com_yec=df_comp_yec.withColumnRenamed("EMP_CATEGORY","Emp_Category").withColumnRenamed("Location","Location1").withColumnRenamed("FULL_NAME","Full_Name1").withColumnRenamed("Post_Graduation___Highest_Qualification","Post_Graduation_Highest_Qualification");

# print(df_com_yec.count())

#joining YEC and ED the data
df_join_emp = df_emp_dump.join(df_com_yec, df_emp_dump.Employee_Number==df_com_yec.EMP_ID,'left').select(df_com_yec['*'],df_emp_dump.Employee_Number,df_emp_dump.Position,df_emp_dump.Date_First_Hired,df_emp_dump.Email1,df_emp_dump.Performance_manager_name,df_emp_dump.Full_Name,df_emp_dump.Function,df_emp_dump.Employee_Subfunction,df_emp_dump.Cost_Center,df_emp_dump.Location,df_emp_dump.Job_Name,df_emp_dump.Gender,df_emp_dump.Email1,df_emp_dump.Employee_Category,df_emp_dump.Client_Geo,df_emp_dump.Operating_Unit,df_emp_dump.Mapping)

# display(df_join_emp)

df_join_emp = df_join_emp.drop('EMP_ID')
# print(df_join_emp.count())


# COMMAND ----------

# print(df_emp_dump.count())

# COMMAND ----------

from datetime import datetime

now = fileDateTimestamp
col_name_Date_as_of = "30_Sep_"+now.strftime("%Y")
Date_as_of = now.strftime("%Y")+"-09-30"
print(col_name_Date_as_of)
print(Date_as_of)


df_add_col = df_join_emp\
.withColumnRenamed("Employee_Number","Emp_ID")\
.withColumn("Pitched_at_current_positioning_", col("Pitched_at__Current_Positioning_"))\
.withColumn("DOJ", col("Date_First_Hired"))\
.withColumn("Graduation", col("GRAD"))\
.withColumn("Proposed_Column_Promotion_History_", lit("null"))\
.withColumn("Current_financial_year", lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_", lit("null"))\
.withColumnRenamed("Mapping","Distribution_category")\
.withColumn("Comments_on_Promotion_Exceptions", lit("null"))\
.withColumn("BP_comments", lit("null"))\
.withColumn("PD_comments", lit("null"))\
.withColumn("HR_head_comments", lit("null"))\
.withColumn("Promotion_category", lit("null"))\
.withColumn("Promotion_approved", lit("null"))\
.withColumn("Date_as_of",F.to_date(lit(Date_as_of)))\
.withColumn("Include_in_rating_distribution?",lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_",lit("null"))\
.withColumn("Promotion_recommendation_Yes_or_No_",lit("null"))\
.withColumn("New_pitching",lit("null"))\
.withColumn("Team_Band_as_per_CP_document",lit("null"))\
.withColumn("Rating_Jump_or_Dip",lit("null"))\
.withColumn("New_Designation",lit("null"))\
.withColumn("New_Cost_Center",col("Cost_Center"))\
.withColumn("New_job_code",col("Job_Name"))
# .withColumn("Effective_date",lit("null"))


df_add_col = df_add_col.drop('Full_Name1','FUNCTION_1','SUB_FUNCTION1','Location1','Years_at_current_designation__YY_MM_')

# print(df_add_col.count())
# display(df_add_col)


# COMMAND ----------

# DBTITLE 1,Deriving column names based on Dates

# Rework
Years_in = "Years_in_the_Firm_as_on_" + col_name_Date_as_of
Total_years = "Total_years_of_work_experience_as_on_" +col_name_Date_as_of
years_at = "Years_at_current_designation_as_on_" +col_name_Date_as_of

DfCurrent = df_add_col.withColumn(Years_in,  F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1)) \
.withColumn(Total_years, F.round(F.round(((split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)*365)+(split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2)*31))/365,scale=1)+F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1),scale=1))\
.withColumn(years_at, F.round(F.datediff(col('Date_as_of'),col('Date_since_at_current_designation'))/365,scale=1)).withColumn("Work_experience_prior_to_KPMG__Years_",F.round(((split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)*365)+(split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2)*31))/365,scale=1))


# COMMAND ----------

# DBTITLE 1,Deriving column name for current year rating
import datetime
today = datetime.date.today()

currYear = str(today.year)
prevYear = str(today.year-1)

#print(currYear, ' ',prevYear)
previousYear = prevYear[2:] 
currentYear = currYear[2:]

proposedRatingCol = 'Proposed_Rating__Oct_'+previousYear+'_-_Sep_'+currentYear

DfCurrent= DfCurrent.withColumn(proposedRatingCol,lit("null"))


# COMMAND ----------

# DBTITLE 1,Calculation functiontag from config_year_end_costcenter_functiontag mapping table
#functiontag
from pyspark.sql.functions import col

#4/10/2023
#Bring BU by joining CC_BU with ED
df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")
# print(df_config_cc_bu.count())

DfCurrent = DfCurrent.join(df_config_cc_bu,DfCurrent.Cost_Center == df_config_cc_bu.Cost_centre,"left").select(DfCurrent["*"],df_config_cc_bu["BU"])

# DfCurrent = DfCurrent.withColumnRenamed("BU","Function_Tag");
# print(DfCurrent.count())

# COMMAND ----------

# DBTITLE 1,Calculating Jobcode from config_year_end_jobcode mapping table
# #year_end_jobcode config table
# df_year_end_jobcode = spark.read.table("kgsonedatadb.config_year_end_jobcode").withColumnRenamed("Function","Function1").withColumnRenamed("Position","Position1").withColumnRenamed("Job_Name","New_job_code");

# DfCurrent= DfCurrent.join(df_year_end_jobcode, (DfCurrent.Function==df_year_end_jobcode.Function1)&(DfCurrent.Position==df_year_end_jobcode.Position1),'left').select(DfCurrent["*"],df_year_end_jobcode["New_job_code"])

# DfCurrent = DfCurrent.drop('Function1','Position1')

# # print(DfCurrent.count())

# COMMAND ----------


yearStartDate = prevYear+'-10-01' 
yearEndDate = currYear+'-09-30'


if (int(fileMonth) <= 9):
    yearStartDate = prevYear+'-10-01' 
    yearEndDate = currYear+'-09-30'

if (int(fileMonth) >= 10):
    yearStartDate = currYear+'-10-01' 
    yearEndDate = currYear+'-09-30'

# yearStartDate = datetime.datetime.strptime(yearStartDate, '%Y-%m-%d').date()
# yearEndDate = datetime.datetime.strptime(yearEndDate, '%Y-%m-%d').date()

print(yearStartDate)
print(yearEndDate)

# COMMAND ----------

# DBTITLE 1,calculation No of days (unpaid leave) taken in current financial year from headcount_leave_report
# from pyspark.sql.window import Window

# # Filtering Leaves between File Financial year(Sep_FY - Oct_FY), unpaid leaves where Approval_Status is Approved and Cancel_Status is not Yes
# # df_leave = spark.sql("select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date order by Name) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date>= '"+yearStartDate+"' and Leave_End_Date<= '"+yearEndDate+"' and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'")

# df_leave = spark.sql("select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date>= '"+yearStartDate+"' and Leave_End_Date<= '"+yearEndDate+"' and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'")
# # display(df_leave)
# print(df_leave.count())

# df_leave = df_leave.withColumn("Leave_Count",get_count_working_days_udf(df_leave.Leave_Start_Date,df_leave.Leave_End_Date))
# df_leave = df_leave.withColumn("Leave_Count",col('Leave_Count').cast(IntegerType()))

# windowSpec  = Window.partitionBy("Employee_Number").orderBy("Name")

# # print(windowSpec)
# # display(df_leave.select("Leave_Count").distinct())
# df_leave = df_leave.withColumn("Total_Unpaid_Leaves_Taken",sum("Leave_Count").over(windowSpec))

# df_leave = df_leave.select("Employee_Number","Name","Total_Unpaid_Leaves_Taken").distinct()

# COMMAND ----------

# DBTITLE 1,calculation No of days (unpaid leave) taken in current financial year from headcount_leave_report
from pyspark.sql.window import Window

# df_leave = spark.sql("select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date>= '"+yearStartDate+"' and Leave_End_Date<= '"+yearEndDate+"' and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'")

df_leave = spark.sql("select * from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_Type order by dated_on desc,Leave_End_Date desc,date_of_approved desc) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date between '"+yearStartDate+"' and '"+yearStartDate+"' or (Leave_End_Date>= '"+yearStartDate+"' and Leave_Start_Date < '"+yearEndDate+"')and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'")

print(yearStartDate)
print(yearEndDate)
print(df_leave.count())
# display(df_leave.select("Leave_End_Date").distinct())

df_leave = df_leave.withColumn("calc_Leave_Start_Date",when(((df_leave.Leave_Start_Date<yearStartDate) & (df_leave.Leave_End_Date>yearStartDate)),yearStartDate).otherwise(df_leave.Leave_Start_Date))
df_leave = df_leave.withColumn("calc_Leave_End_Date",when(df_leave.Leave_End_Date>yearEndDate,yearEndDate).otherwise(df_leave.Leave_End_Date))

# display(df_leave.select("Leave_Start_Date","Leave_End_Date","calc_Leave_Start_Date","calc_Leave_End_Date"))

df_leave = df_leave.withColumn("Leave_Count",get_count_working_days_udf(df_leave.calc_Leave_Start_Date,df_leave.calc_Leave_End_Date))
df_leave = df_leave.withColumn("Leave_Count",col('Leave_Count').cast(IntegerType()))

df_leave = df_leave.groupBy("Employee_Number").sum("Leave_Count").withColumnRenamed("sum(Leave_Count)","no_of_days_taken_in_current_year")
# display(df_leave)
# df_leave = df_leave.select("Employee_Number","Name","Total_Unpaid_Leaves_Taken").distinct()
df_leave = df_leave.select("Employee_Number","no_of_days_taken_in_current_year").distinct()

# display(df_leave)

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

DfCurrent=DfCurrent.join(df_leave,DfCurrent.Emp_ID==df_leave.Employee_Number,'left').select(DfCurrent['*'],df_leave.no_of_days_taken_in_current_year)

# DfCurrent = DfCurrent.withColumnRenamed("Total_Unapid_Leaves_Taken","no_of_days_taken_in_"+fileYear_FY)
# DfCurrent = DfCurrent.withColumnRenamed("Total_Unapid_Leaves_Taken","no_of_days_taken_in_current_year")
DfCurrent = DfCurrent.withColumn("no_of_days_taken_in_current_year",col('no_of_days_taken_in_current_year').cast(IntegerType()))


# print(DfCurrent.count())

# COMMAND ----------

# DBTITLE 1,Calculation Encore_winner from encore_output
#Encore_winner

df_encore = spark.sql("select distinct Emp_No,Quarter,Category from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = '"+fileYear_FY+"'")

df_encore = df_encore.withColumn('Encore_winner',concat_ws(' Q', df_encore.Category, df_encore.Quarter))

df_encore = df_encore.groupBy("Emp_No").agg(concat_ws(", ",collect_list("Encore_winner")).alias("Encore_winner"))

DfCurrent = DfCurrent.join(df_encore, DfCurrent.Emp_ID==df_encore.Emp_No, 'left').select(DfCurrent['*'],df_encore.Encore_winner)

DfCurrent = DfCurrent.drop('Quarter','Category')

# COMMAND ----------

display(DfCurrent.select("Position","Job_Name","Distribution_category").distinct())

# COMMAND ----------

# DBTITLE 1,populating Effective_date from current year
#Effective_date
sameyear_NextFY_List = ['10','11','12']
DfCurrent = DfCurrent.withColumn("curr_year",when(lit(fileMonth).isin(sameyear_NextFY_List),lit(fileYear)).otherwise(lit(prevFileYear))).withColumn('Effective_date1',lit("1/10"))
DfCurrent = DfCurrent.withColumn('Effective_date',concat_ws("/",DfCurrent.Effective_date1,DfCurrent.curr_year))
DfCurrent = DfCurrent.drop('curr_timestamp','curr_year','Effective_date1')

# print(DfCurrent.count())
# display(DfCurrent)

# COMMAND ----------

# DBTITLE 1, selecting Column as per the order in output
currentDf= DfCurrent.select("Emp_ID","Full_name","Function","Employee_Subfunction","Cost_center","BU")
list_of_col = currentDf.columns
                     
emp_data=df_emp_dump.select("Location","Position","Job_Name")
emp_data = emp_data.columns

dateasof=DfCurrent.select("Date_as_of")
dateasof = dateasof.columns

yec_data=df_com_yec.select("Pitched_at__Current_Positioning_","Date_since_at_current_designation")
yec_data = yec_data.columns

years_at_current_designation = [column for column in DfCurrent.columns if column.startswith("Years_at")]

doj = DfCurrent.select("DOJ")
doj = doj.columns

years_in_the_Firm_columns = [column for column in DfCurrent.columns if column.startswith("Years_in_the_Firm_as")]

work_exp= DfCurrent.select("Work_experience_prior_to_KPMG__Years_")
work_exp = work_exp.columns

Total_years_of_work_experience_as_on = [column for column in DfCurrent.columns if column.startswith("Total_years_of_work_experience_as_on_")]

ratings_columns = [column for column in DfCurrent.columns if column.startswith("RATING")]

list1=DfCurrent.select("Post_Graduation_Highest_Qualification","Graduation","Gender","Email1","Employee_Category","Include_in_rating_distribution?","no_of_days_taken_in_current_year","Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_","Distribution_category","Performance_manager_name")
list1 = list1.columns

Proposed_Rating = [column for column in DfCurrent.columns if column.startswith("Proposed_Rating__")]

list2=DfCurrent.select("Promotion_recommendation_Yes_or_No_","Encore_winner","Comments_on_Promotion_Exceptions","BP_comments","Rating_Jump_or_Dip","PD_comments","HR_head_comments","Promotion_category","Promotion_approved","New_Cost_Center","New_Designation","New_job_code","New_pitching")
list2 = list2.columns
                 
cost_centre = [column for column in DfCurrent.columns if column.startswith("Cost")]                
                 
list3=DfCurrent.select("Client_Geo","Operating_unit","Effective_date","Team_Band_as_per_CP_document")
list3 = list3.columns

combined=list_of_col+emp_data+dateasof+yec_data+years_at_current_designation+doj+years_in_the_Firm_columns+work_exp+Total_years_of_work_experience_as_on+ratings_columns+list1+Proposed_Rating+list2+list3

#print(list_of_col)

final = DfCurrent.select(combined)

# print(final.count())
display(final)

# COMMAND ----------

# DBTITLE 1,Adding File_Date 
final=final.withColumn("File_Date", lit(FileDate))


# COMMAND ----------

final=final.withColumnRenamed("Job_Name","Job_Code").withColumnRenamed("Position","Designation").withColumnRenamed("Position","Designation").withColumnRenamed("Employee_Category","Emp_Category").withColumnRenamed("Employee_Subfunction","Sub_function").withColumnRenamed("Email1","Email").dropDuplicates()
# display(final.count())

# COMMAND ----------

final.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

