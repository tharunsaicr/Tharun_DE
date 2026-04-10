# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "employee_engagement"
tableName = "year_end"

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

print("Year ", fileYear)
print("Year ", fileYear_FY)
print("Year ", prevFileYear)
print("Month ", fileMonth)
print("Day ", fileDay)
print("ConvertedDate ", convertedFileDate)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Joining yec and employee_dump Delta Table
import pyspark
import datetime
from pyspark.sql.functions import lit, col, split
from pyspark.sql import functions as F
 
#Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230118' and  Entity = 'KGS'")

df_emp_dump = df_emp_dump.withColumn("Approved_LWD",F.date_format(F.to_date(col('Approved_LWD')),'yyyy-MM-dd')).withColumn("CurrentDate",F.current_date())

df_emp_dump = df_emp_dump.filter((col('Approved_LWD') >= col('CurrentDate')) | (col('Approved_LWD').isNull()))

display(df_emp_dump.count())

df_emp_dump=df_emp_dump.withColumnRenamed("Cost_centre","Cost_Center").withColumnRenamed("Client_Geography","Client_Geo").withColumnRenamed("Email_Address","Email1").withColumnRenamed("Performance_Manager","Performance_manager_name")
#.withColumn("Date_as_of",to_date(current_timestamp()));

# print(df_emp_dump.count())

#Compensation YEC data from delta table raw_curr_compensation_yec
df_com_yec=spark.sql("select * from kgsonedatadb.trusted_hist_compensation_yec where File_Date = '20230101'")

df_com_yec=df_com_yec.withColumnRenamed("EMP_CATEGORY","Emp_Category").withColumnRenamed("Location","Location1").withColumnRenamed("FULL_NAME","Full_Name1").withColumnRenamed("Post_Graduation___Highest_Qualification","Post_Graduation_Highest_Qualification");

print(df_com_yec.count())

# display(df_com_yec)

#joining YEC and ED the data
df_join_emp = df_emp_dump.join(df_com_yec, df_emp_dump.Employee_Number==df_com_yec.EMP_ID,'left').select(df_com_yec['*'],df_emp_dump.Employee_Number,df_emp_dump.Position,df_emp_dump.Date_First_Hired,df_emp_dump.Email1,df_emp_dump.Performance_manager_name,df_emp_dump.Full_Name,df_emp_dump.Function,df_emp_dump.Employee_Subfunction,df_emp_dump.Cost_Center,df_emp_dump.Location,df_emp_dump.Job_Name,df_emp_dump.Gender,df_emp_dump.Email1,df_emp_dump.Employee_Category,df_emp_dump.Client_Geo,df_emp_dump.Operating_Unit)

df_join_emp = df_join_emp.drop('EMP_ID')
print(df_join_emp.count())


# COMMAND ----------

# df_com_yec_test = df_com_yec.select("Work_experience_prior_to_KPMG__Years_")

# df_com_yec_test = df_com_yec_test.withColumn("year",split(df_com_yec_test['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)).withColumn("month",split(df_com_yec_test['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2))

# df_com_yec_test = df_com_yec_test.withColumn("prio_work_exp",F.round(((split(df_com_yec_test['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)*365)+(split(df_com_yec_test['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2)*31))/365,scale=1))

# display(df_com_yec_test)

# COMMAND ----------

from datetime import datetime
now = datetime.now()
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
.withColumn("Distribution_category", lit("null"))\
.withColumn("Comments_on_Promotion_Exceptions", lit("null"))\
.withColumn("BP_comments", lit("null"))\
.withColumn("PD_comments", lit("null"))\
.withColumn("HR_head_comments", lit("null"))\
.withColumn("Promotion_category", lit("null"))\
.withColumn("Promotion_approved", lit("null"))\
.withColumn("Date_as_of",F.to_date(lit(Date_as_of)))\
.withColumn("Include_in_rating_distribution?",lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_",lit("null"))\
.withColumn("Distribution_category",lit("null"))\
.withColumn("Promotion_recommendation_Yes_or_No_",lit("null"))\
.withColumn("New_pitching",lit("null"))\
.withColumn("Team_Band_as_per_CP_document",lit("null"))\
.withColumn("Rating_Jump_or_Dip",lit("null"))\
.withColumn("New_Designation",lit("null"))
# .withColumn("Encore_winner",lit("null"))\
# .withColumn("Effective_date",lit("null"))


df_add_col = df_add_col.drop('Full_Name1','FUNCTION_1','SUB_FUNCTION1','Location1','Years_at_current_designation__YY_MM_')

print(df_add_col.count())
# display(df_add_col)


# COMMAND ----------

# DBTITLE 1,Deriving column names based on Dates
# from datetime import datetime

# current dateTime
# now = datetime.now()

# convert to string
# date_time_str = now.strftime("%Y")
# date1 = now.strftime("%b-%y")
# date1 = date1.replace('-','_')
# date_time_str = date_time_str.replace('-','')
# print('DateTime String:', date_time_str+"-09-30")

# Rework
Years_in = "Years_in_the_Firm_as_on_" + col_name_Date_as_of
Total_years = "Total_years_of_work_experience_as_on_" +col_name_Date_as_of
years_at = "Years_at_current_designation_as_on_" +col_name_Date_as_of

DfCurrent = df_add_col.withColumn(Years_in,  F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1)) \
.withColumn(Total_years, F.round(F.round(((split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)*365)+(split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2)*31))/365,scale=1)+F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1),scale=1))\
.withColumn(years_at, F.round(F.datediff(col('Date_as_of'),col('Date_since_at_current_designation'))/365,scale=1)).withColumn("Work_experience_prior_to_KPMG__Years_",F.round(((split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(0)*365)+(split(df_add_col['Work_experience_prior_to_KPMG__Years_'], ' ').getItem(2)*31))/365,scale=1))

# DfCurrent = df_add_col.withColumn(Years_in, F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1)) \
# .withColumn(Total_years, col("TOTAL_WORK_EXP_INCL_KPMG"))\
# .withColumn(years_at, F.round(F.datediff(col('Date_as_of'),col('Date_since_at_current_designation'))/365,scale=1))

print(DfCurrent.count())
# display(DfCurrent)

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

print(DfCurrent.count())
# display(DfCurrent)

# COMMAND ----------

# DBTITLE 1,Calculation functiontag from config_year_end_costcenter_functiontag mapping table
#functiontag
from pyspark.sql.functions import col
#year_end_costcenter_functiontag config table
# df_year_end_costcenter_functiontag = spark.read.table("kgsonedatadb.config_year_end_costcenter_functiontag");

# DfCurrent = DfCurrent.join(df_year_end_costcenter_functiontag, DfCurrent.Cost_Center == df_year_end_costcenter_functiontag.Cost_centre,"left").select(df_add_col["*"],df_year_end_costcenter_functiontag["Function_tag"])

#4/10/2023
#Bring BU by joining CC_BU with ED
df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")
print(df_config_cc_bu.count())

DfCurrent = DfCurrent.join(df_config_cc_bu,DfCurrent.Cost_Center == df_config_cc_bu.Cost_centre,"left").select(DfCurrent["*"],df_config_cc_bu["BU"])

DfCurrent = DfCurrent.withColumnRenamed("BU","Function_Tag");
print(DfCurrent.count())

print(DfCurrent.count())
# display(DfCurrent)
#df.select("Cost_center","Function_Tag").show()

# COMMAND ----------

# DBTITLE 1,Calculating Jobcode from config_year_end_jobcode mapping table
#year_end_jobcode config table
df_year_end_jobcode = spark.read.table("kgsonedatadb.config_year_end_jobcode").withColumnRenamed("Function","Function1").withColumnRenamed("Position","Position1").withColumnRenamed("Job_Name","New_job_code");

DfCurrent= DfCurrent.join(df_year_end_jobcode, (DfCurrent.Function==df_year_end_jobcode.Function1)&(DfCurrent.Position==df_year_end_jobcode.Position1),'left').select(DfCurrent["*"],df_year_end_jobcode["New_job_code"])

DfCurrent = DfCurrent.drop('Function1','Position1')
#df_add_col2  = df_add_col2.dropDuplicates()

print(DfCurrent.count())
# display(DfCurrent)

#df.select("Function","Designation","New_jobcode").show())


# COMMAND ----------

# DBTITLE 1,calculation No of days taken in current financial year from headcount_leave_report
import datetime
from pyspark.sql.functions import *
df_leave = spark.read.table("kgsonedatadb.trusted_headcount_leave_report")
list=[fileYear]
df_leave = df_leave.withColumn('Year',date_format(col('Leave_Start_Date'),'y'))
df_leave = df_leave.filter(df_leave.Year.isin(list)) 

DfCurrent=DfCurrent.join(df_leave,DfCurrent.Emp_ID==df_leave.Employee_Number,'left').select(DfCurrent['*'],df_leave.Leave_Start_Date,df_leave.Leave_End_Date)

DfCurrent = DfCurrent.withColumnRenamed("Leave_Start_Date","Leave_Start_Date1")

#create an array containing all days between start_date and end_date
DfCurrent = DfCurrent.withColumn('days', F.expr('sequence(Leave_Start_Date1, Leave_End_Date, interval 1 day)'))\
.withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day'))\
.withColumn('no_of_days_taken_in_current_year', F.expr('size(weekdays)'))

# print(DfCurrent.count())
# DfCurrent = DfCurrent.distinct()

print(DfCurrent.count())
DfCurrent = DfCurrent.drop('days','weekdays','Year','Leave_Start_Date1','Leave_End_Date')

# print(DfCurrent.count())
# display(DfCurrent)

#display(df.select('Year','Leave_Start_Date','Leave_End_Date','no_of_days_taken_in_FY23'))
#df.withColumn("curr_timestamp", current_timestamp())\

# COMMAND ----------

# DBTITLE 1,Calculation Encore_winner from encore_output
#Encore_winner
queryStr = "select * from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = '"+fileYear_FY+"'"
print(queryStr)

# df_encore = spark.sql("select * from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and ((year = "+fileYear+" and quarter in (1,2,3,4)) or (year = "+str(prevFileYear)+" and quarter = 1))")

df_encore = spark.sql("select * from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = '"+fileYear_FY+"'")

print(df_encore.count())
display(df_encore)

DfCurrent = DfCurrent.join(df_encore, DfCurrent.Emp_ID==df_encore.Emp_No, 'left').select(DfCurrent['*'],df_encore.Quarter,df_encore.Category)

# reward_status_list = ['APPROVED','APPROVAL NOT REQUIRED']

# df_encore = df_encore.where( ( F.upper(col("Reward_Status")).isin (list)))
# display(DfCurrent)

print(DfCurrent.count())

#Filter IS IN List values
# Quarter = ['1','2','3','4']
# DfCurrent = DfCurrent.filter(DfCurrent.Quarter.isin (Quarter)) 
DfCurrent = DfCurrent.withColumn('Encore_winner',concat_ws(' Q', df_encore.Category, df_encore.Quarter))
DfCurrent = DfCurrent.drop('Quarter','Category')

print(DfCurrent.count())
display(DfCurrent)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = 'FY23'

# COMMAND ----------

# DBTITLE 1,populating Effective_date from current year
#Effective_date
sameyear_NextFY_List = ['10','11','12']
DfCurrent = DfCurrent.withColumn("curr_year",when(lit(fileMonth).isin(sameyear_NextFY_List),lit(fileYear)).otherwise(lit(prevFileYear))).withColumn('Effective_date1',lit("1/10"))
DfCurrent = DfCurrent.withColumn('Effective_date',concat_ws("/",DfCurrent.Effective_date1,DfCurrent.curr_year))
DfCurrent = DfCurrent.drop('curr_timestamp','curr_year','Effective_date1')

print(DfCurrent.count())
# display(DfCurrent)

# COMMAND ----------

# DBTITLE 1, selecting Column as per the order in output
currentDf= DfCurrent.select("Emp_ID","Full_name","Function","Employee_Subfunction","Cost_center","Function_tag")
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

list2=DfCurrent.select("Promotion_recommendation_Yes_or_No_","Encore_winner","Comments_on_Promotion_Exceptions","BP_comments","Rating_Jump_or_Dip","PD_comments","HR_head_comments","Promotion_category","Promotion_approved","New_Designation","New_job_code","New_pitching")
list2 = list2.columns
                 
cost_centre = [column for column in DfCurrent.columns if column.startswith("Cost")]                
                 
list3=DfCurrent.select("Client_Geo","Operating_unit","Effective_date","Team_Band_as_per_CP_document")
list3 = list3.columns

combined=list_of_col+emp_data+dateasof+yec_data+years_at_current_designation+doj+years_in_the_Firm_columns+work_exp+Total_years_of_work_experience_as_on+ratings_columns+list1+Proposed_Rating+list2+list3

#print(list_of_col)

final = DfCurrent.select(combined)

print(final.count())
# display(final)

# COMMAND ----------

# DBTITLE 1,Adding File_Date 
final=final.withColumn("File_Date", lit(FileDate))


# COMMAND ----------

final=final.withColumnRenamed("Job_Name","Job_Code").withColumnRenamed("Position","Designation").withColumnRenamed("Position","Designation").withColumnRenamed("Employee_Category","Emp_Category").withColumnRenamed("Employee_Subfunction","Sub_function").withColumnRenamed("Email1","Email").dropDuplicates()
# display(final)

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

