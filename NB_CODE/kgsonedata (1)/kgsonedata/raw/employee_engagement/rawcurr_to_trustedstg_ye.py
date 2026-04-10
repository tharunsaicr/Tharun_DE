# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "employee_engagement"
tableName = "year_end"

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

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
    rating_FY = "FY"+FileDate[2:4]
    rating_FY_1 = "FY"+str(int(FileDate[2:4])-1)
    rating_FY_2 = "FY"+str(int(FileDate[2:4])-2)
else:
    fileYear_FY = "FY"+FileDate[2:4]
    rating_FY = "FY"+str(int(FileDate[2:4])-1)
    rating_FY_1 = "FY"+str(int(FileDate[2:4])-2)
    rating_FY_2 = "FY"+str(int(FileDate[2:4])-3)

print("Year ", fileYear)
print("FY ", fileYear_FY)
print("Prev Year ", prevFileYear)
print("Month ", fileMonth)
print("Day ", fileDay)
print("ConvertedDate ", convertedFileDate)
print("rating_FY ",rating_FY)
print("rating_FY_1 ",rating_FY_1)
print("rating_FY_2 ",rating_FY_2)

# COMMAND ----------

import datetime
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
print(convertedFileDate)

convertedFileDate = datetime.datetime.strptime(convertedFileDate, '%Y-%m-%d').date()
print(convertedFileDate)

# COMMAND ----------

from datetime import date
from datetime import datetime

fileDateTimestamp = datetime.combine(convertedFileDate, datetime.min.time())

print(fileDateTimestamp)

# COMMAND ----------

# DBTITLE 1,Joining yec and employee_dump Delta Table

from pyspark.sql.types import *
import datetime
from datetime import *
import pyspark
from pyspark.sql.functions import lit, col, split, first
from pyspark.sql import functions as F

#Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
# df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS'")

# trusted_hist_headcount_employee_dump_table_path = onedata_trusted_hist_savepath_url+"headcount/employee_dump"
# DeltaTable.forPath(spark, trusted_hist_headcount_employee_dump_table_path).toDF().createOrReplaceTempView("trusted_hist_headcount_employee_dump")

# Above code has been changed as per peer review comments to restrict data volume while reading itself
df_emp_dump = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1")

# print(df_emp_dump.count())

df_emp_dump = df_emp_dump.withColumn("Approved_LWD",F.date_format(F.to_date(col('Approved_LWD')),'yyyy-MM-dd')).withColumn("CurrentDate",lit(convertedFileDate))

df_emp_dump = df_emp_dump.filter((col('Approved_LWD') >= col('CurrentDate')) | (col('Approved_LWD').isNull()))

# print(df_emp_dump.count())

# config_job_code_distribution_category_mapping_table_path = onedata_config_path_url+"job_code_distribution_category_mapping"
# DeltaTable.forPath(spark, config_job_code_distribution_category_mapping_table_path).toDF().createOrReplaceTempView("config_job_code_distribution_category_mapping")

# Above code has been changed as per peer review comments to restrict data volume while reading itself

#Join with config_job_code_distribution_category_mapping to get distribution_category
df_jobcode_distcategory = spark.sql("select * from kgsonedatadb.config_job_code_distribution_category_mapping where processname = 'Employee_Engagement' and tablename = 'Year_End'")

df_emp_dump = df_emp_dump.join(df_jobcode_distcategory, ((df_emp_dump.Position == df_jobcode_distcategory.Position) & (df_emp_dump.Job_Name == df_jobcode_distcategory.Job_Name)),"left").select(df_emp_dump["*"],df_jobcode_distcategory["Mapping"])

df_emp_dump=df_emp_dump.withColumnRenamed("Cost_centre","Cost_Center").withColumnRenamed("Client_Geography","Client_Geo").withColumnRenamed("Email_Address","Email").withColumnRenamed("Performance_Manager","Performance_manager_name")
#.withColumn("Date_as_of",to_date(current_timestamp()));

# print(df_emp_dump.count())

#Compensation YEC data from delta table raw_curr_compensation_yec
df_comp_yec = spark.sql("select EMPLOYEE_NUMBER as EMP_ID, PITCHED_AT__CURRENT_POSITIONING_ as Pitched_at__Current_Positioning_, DATE_SINCE_AT_CURRENT_DESIGNATION as Date_since_at_current_designation, YEARS_AT_CURRENT_DESIGNATION as YEARS_AT_CURRENT_DESIGNATION, WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_ as Work_experience_prior_to_KPMG__Years_, TOTAL_YEARS_OF_WORK_EXPERIENCE__YY_MM_FORMAT_ as TOTAL_YEARS_OF_WORK_EXPERIENCE, YEAR_OF_PITCHING as Pitching_Year from (select rank() over(partition by EMPLOYEE_NUMBER order by file_date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_compensation_yec where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+") yec_hist where rank = 1")

# Below piece of code was removed from above line in order to resolve a bug
# where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_compensation_yec

df_com_yec=df_comp_yec.withColumnRenamed("EMP_CATEGORY","Emp_Category").withColumnRenamed("Location","Location1").withColumnRenamed("FULL_NAME","Full_Name1")

# display(df_com_yec.where(df_com_yec["Emp_ID"]=='98879'))

# print(df_com_yec.count())

#joining YEC and ED the data
df_join_emp = df_emp_dump.join(df_com_yec, df_emp_dump.Employee_Number==df_com_yec.EMP_ID,'left').select(df_com_yec['*'],df_emp_dump.Employee_Number,df_emp_dump.Position,df_emp_dump.Date_First_Hired,df_emp_dump.Email,df_emp_dump.Performance_manager_name,df_emp_dump.Full_Name,df_emp_dump.Function,df_emp_dump.Employee_Subfunction,df_emp_dump.Cost_Center,df_emp_dump.Location,df_emp_dump.Job_Name,df_emp_dump.Gender,df_emp_dump.Email,df_emp_dump.Employee_Category,df_emp_dump.Client_Geo,df_emp_dump.Operating_Unit,df_emp_dump.Mapping)


# display(df_join_emp)

df_join_emp = df_join_emp.drop('EMP_ID')
# print(df_join_emp.count())

df_qual_dump = spark.sql("select qual_dump.EMP_ID as Qual_EMP_ID, qual_dump.Post_Graduation___Highest_Qualification as Post_Graduation_Highest_Qualification, qual_dump.GRAD as GRAD from (select rank() over(partition by EMP_ID order by File_Date desc, Dated_On desc) as Rank_Grad,* from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where EMP_ID is not null) qual_dump where qual_dump.Rank_Grad = 1")

df_join_emp = df_join_emp.join(df_qual_dump, df_join_emp.Employee_Number==df_qual_dump.Qual_EMP_ID,'left').select(df_join_emp['*'],df_qual_dump.GRAD,df_qual_dump.Post_Graduation_Highest_Qualification)

df_ratings = spark.sql("select emp_rating.Employee_Number as Rating_Employee_Number, emp_rating.FY as Rating_FY, emp_rating.Rating as Emp_Rating from (select rank() over(partition by Employee_Number,FY order by File_Date desc, Dated_On desc) as Rank_Rating,* from kgsonedatadb.trusted_hist_compensation_employee_ratings where Employee_Number is not null) emp_rating where emp_rating.Rank_Rating = 1")

df_ratings_pivot = df_ratings.groupBy("Rating_Employee_Number").pivot("Rating_FY").agg(first("Emp_Rating"))

df_ratings_pivot = df_ratings_pivot.withColumnRenamed(rating_FY,"Last_FY_Rating").withColumnRenamed(rating_FY_1,"Previous_FY_Rating").withColumnRenamed(rating_FY_2,"Previous_to_Previous_FY_Rating")

df_join_emp = df_join_emp.join(df_ratings_pivot, df_join_emp.Employee_Number==df_ratings_pivot.Rating_Employee_Number,'left').select(df_join_emp['*'],df_ratings_pivot["Last_FY_Rating"],df_ratings_pivot["Previous_FY_Rating"],df_ratings_pivot["Previous_to_Previous_FY_Rating"])

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
.withColumn("Pitching_Year", col("Pitching_Year"))\
.withColumn("DOJ", col("Date_First_Hired"))\
.withColumn("Graduation", col("GRAD"))\
.withColumn("Proposed_Column_Promotion_History_", lit("null"))\
.withColumn("Current_financial_year", lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_", lit("null"))\
.withColumnRenamed("Mapping","Distribution_category")\
.withColumn("Promotion_or_Progession", lit("null"))\
.withColumn("BP_comments", lit("null"))\
.withColumn("Comments_on_Promotion_Exceptions", lit("null"))\
.withColumn("PD_comments", lit("null"))\
.withColumn("HR_head_comments", lit("null"))\
.withColumn("Promotion_category", lit("null"))\
.withColumn("Promotion_approved", lit("null"))\
.withColumn("Date_as_of",F.to_date(lit(Date_as_of)))\
.withColumn("Include_in_rating_distribution?",lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_",lit("null"))\
.withColumn("Promotion_recommendation_Yes_or_No_",lit("null"))\
.withColumn("New_Pitched_at__proposed_positioning",lit("null"))\
.withColumn("Team_Band_as_per_CP_document",lit("null"))\
.withColumn("Rating_Jump_or_Dip",lit("null"))\
.withColumn("New_Designation",lit("null"))\
.withColumn("New_job_code",lit("null"))\
.withColumn("Remarks",lit("null"))\
.withColumn("Promotion_or_Progession",lit("null"))\
.withColumn("New_Pitching_Year",lit("null"))



# .withColumn("Effective_date",lit("null"))
# .withColumn("New_Cost_Center",col("Cost_Center"))\

df_add_col = df_add_col.drop('Full_Name1','FUNCTION_1','SUB_FUNCTION1','Location1','Years_at_current_designation__YY_MM_')

# print(df_add_col.count())
# display(df_add_col)


# COMMAND ----------

# Split the tenure value into years and months
df_add_col = df_add_col.withColumn("derive_Work_experience_prior", split(col("Work_experience_prior_to_KPMG__Years_"), "\\."))

# Extract years and months
df_add_col = df_add_col.withColumn("years_workexp", col("derive_Work_experience_prior")[0].cast("double"))
df_add_col = df_add_col.withColumn("months_workexp", col("derive_Work_experience_prior")[1].cast("double"))

# Calculate the decimal tenure
df_add_col = df_add_col.withColumn("derive_Work_experience_prior", F.round(col("years_workexp") + col("months_workexp") / 12,scale=1))

df_add_col = df_add_col.drop("years_workexp","months_workexp")

df_add_col.count()

# display(df_add_col.select("Work_experience_prior_to_KPMG__Years_"))

# COMMAND ----------

# DBTITLE 1,Deriving column names based on Dates

# Rework
# Years_in = "Years_in_the_Firm_as_on_" + col_name_Date_as_of
# Total_years = "Total_years_of_work_experience_as_on_" +col_name_Date_as_of
# years_at = "Years_at_current_designation_as_on_" +col_name_Date_as_of

Years_in = "Years_in_the_Firm_as_on_30_Sep_Current_FY"
Total_years = "Total_years_of_work_experience_as_on_30_Sep_Current_FY"
years_at = "Years_at_current_designation_as_on_30_Sep_Current_FY"

DfCurrent = df_add_col.withColumn(Years_in,  F.round(F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1)) \
.withColumn(Total_years, F.round(df_add_col['derive_Work_experience_prior']+F.datediff(col('Date_as_of'),col('DOJ'))/365,scale=1))\
.withColumn(years_at, F.round(F.datediff(col('Date_as_of'),col('Date_since_at_current_designation'))/365,scale=1)).withColumn("Work_experience_prior_to_KPMG__Years_",df_add_col['derive_Work_experience_prior'].cast("string"))


# COMMAND ----------

# DBTITLE 1,Deriving column name for current year rating
import datetime
today = datetime.date.today()

currYear = str(today.year)
prevYear = str(today.year-1)

#print(currYear, ' ',prevYear)
previousYear = prevYear[2:] 
currentYear = currYear[2:]

# proposedRatingCol = 'Proposed_Rating__Oct_'+previousYear+'_-_Sep_'+currentYear
proposedRatingCol = 'Proposed_Rating_Current_FY'

DfCurrent= DfCurrent.withColumn(proposedRatingCol,lit("null"))


# COMMAND ----------

# DBTITLE 1,Calculation functiontag from config_year_end_costcenter_functiontag mapping table
#functiontag
from pyspark.sql.functions import col

#4/10/2023
#Bring BU by joining CC_BU with ED

# config_cc_bu_sl_table_path = onedata_config_path_url+"cc_bu_sl"
# DeltaTable.forPath(spark, config_cc_bu_sl_table_path).toDF().createOrReplaceTempView("temp_config_cc_bu_sl")

df_config_cc_bu = spark.sql("select distinct Cost_centre,Final_BU as BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1")

# df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")
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
from pyspark.sql.window import Window

# trusted_hist_headcount_leave_report_table_path = onedata_trusted_hist_savepath_url+"headcount/leave_report"
# DeltaTable.forPath(spark, trusted_hist_headcount_leave_report_table_path).toDF().createOrReplaceTempView("trusted_hist_headcount_leave_report")

df_leave = spark.sql("select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date>= '"+yearStartDate+"' and Leave_End_Date<= '"+yearEndDate+"' and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'")

df_leave = df_leave.withColumn("Leave_Count",get_count_working_days_udf(df_leave.Leave_Start_Date,df_leave.Leave_End_Date))
df_leave = df_leave.withColumn("Leave_Count",col('Leave_Count').cast(IntegerType()))

df_leave = df_leave.groupBy("Employee_Number").sum("Leave_Count").withColumnRenamed("sum(Leave_Count)","no_of_days_taken_in_current_year")
# display(df_leave)
# df_leave = df_leave.select("Employee_Number","Name","Total_Unpaid_Leaves_Taken").distinct()
df_leave = df_leave.select("Employee_Number","no_of_days_taken_in_current_year").distinct()

# display(df_leave.where(df_leave["Employee_Number"]=='97993'))

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

# trusted_hist_employee_engagement_encore_output_table_path = onedata_trusted_hist_savepath_url+"employee_engagement/encore_output"
# DeltaTable.forPath(spark, trusted_hist_employee_engagement_encore_output_table_path).toDF().createOrReplaceTempView("trusted_hist_employee_engagement_encore_output")

df_encore = spark.sql("select * from (select rank() over(partition by Emp_No order by File_Date desc, Dated_On desc) rank_Encore,* from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = '"+fileYear_FY+"'"+") A where rank_Encore = 1")

df_encore = df_encore.groupBy("Emp_No").agg(concat_ws(", ", collect_list(concat_ws(' Q', df_encore.Category, df_encore.Quarter))).alias("Encore_winner"))

DfCurrent = DfCurrent.join(df_encore, DfCurrent.Emp_ID==df_encore.Emp_No, 'left').select(DfCurrent['*'],df_encore.Encore_winner)

# DfCurrent = DfCurrent.withColumn('Encore_winner',concat_ws(' Q', df_encore.Category, df_encore.Quarter))

DfCurrent = DfCurrent.drop('Quarter','Category')

# print(DfCurrent.count())

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
DfCurrent = DfCurrent.withColumnRenamed("Full_name","Employee_Name").withColumnRenamed("Employee_Subfunction","Sub_Function").withColumnRenamed("Cost_center","Cost_centre")

currentDf= DfCurrent.select("Emp_ID","Employee_Name","Function","Sub_Function","Cost_centre","BU","Client_Geo","Operating_unit")

list_of_col = currentDf.columns
                     
emp_data=df_emp_dump.select("Location","Position","Job_Name")
emp_data = emp_data.columns

dateasof=DfCurrent.select("Date_as_of")
dateasof = dateasof.columns

yec_data=df_com_yec.select("Pitched_at__Current_Positioning_","Pitching_Year","Date_since_at_current_designation")
yec_data = yec_data.columns

years_at_current_designation = [column for column in DfCurrent.columns if column.startswith("Years_at")]

doj = DfCurrent.select("DOJ")
doj = doj.columns

years_in_the_Firm_columns = [column for column in DfCurrent.columns if column.startswith("Years_in_the_Firm_as")]

work_exp= DfCurrent.select("Work_experience_prior_to_KPMG__Years_")
work_exp = work_exp.columns

Total_years_of_work_experience_as_on = [column for column in DfCurrent.columns if column.startswith("Total_years_of_work_experience_as_on_")]

ratings_columns = DfCurrent.select("Last_FY_Rating","Previous_FY_Rating","Previous_to_Previous_FY_Rating")
ratings_columns = ratings_columns.columns
# [column for column in DfCurrent.columns if column.upper().startswith("RATING_CURRENT")]

list1=DfCurrent.select("Post_Graduation_Highest_Qualification","Graduation","Gender","Email","Employee_Category","Performance_manager_name","Encore_winner","Include_in_rating_distribution?","no_of_days_taken_in_current_year","Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_","Distribution_category","Team_Band_as_per_CP_document")
list1 = list1.columns

Proposed_Rating = [column for column in DfCurrent.columns if column.startswith("Proposed_Rating_")]

list2=DfCurrent.select("Rating_Jump_or_Dip","Promotion_recommendation_Yes_or_No_","Promotion_or_Progession","BP_comments","Comments_on_Promotion_Exceptions","PD_comments","HR_head_comments","Promotion_category","Promotion_approved","New_Designation","New_job_code","New_Pitched_at__proposed_positioning","New_Pitching_Year","Remarks")
list2 = list2.columns
                 
cost_centre = [column for column in DfCurrent.columns if column.startswith("Cost")]                
                 
# list3=DfCurrent.select("Effective_date")
# list3 = list3.columns

combined=list_of_col+emp_data+dateasof+yec_data+years_at_current_designation+doj+years_in_the_Firm_columns+work_exp+Total_years_of_work_experience_as_on+ratings_columns+list1+Proposed_Rating+list2
# +list3

#print(list_of_col)

finalDf = DfCurrent.select(combined)
# print(finalDf.count())
finalDf = finalDf.filter(~  lower(col("Position")).isin("partner","partner-lt","partner-gdc","partner coo"))
# print(finalDf.count())


# COMMAND ----------

# display(finalDf)

# COMMAND ----------

# DBTITLE 1,Adding File_Date 
finalDf=finalDf.withColumn("File_Date", lit(FileDate))


# COMMAND ----------

finalDf=finalDf.withColumnRenamed("Job_Name","Job_Code").withColumnRenamed("Position","Designation").withColumnRenamed("Position","Designation").withColumnRenamed("Employee_Category","Emp_Category").withColumnRenamed("Employee_Subfunction","Sub_function").dropDuplicates()

# display(finalDf.groupBy("Emp_ID").agg(count("Emp_ID")).where(count("Emp_ID")>1))

# COMMAND ----------

# DBTITLE 1,Writing data in Isolated ENV
finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# finalDf.write \
# .mode("overwrite") \
# .format("delta") \
# .option("overwriteSchema","true") \
# .option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
# .option("compression","snappy") \
# .saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

