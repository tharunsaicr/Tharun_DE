# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "employee_engagement"
tableName = "year_end"

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

import pyspark
import datetime
from pyspark.sql.functions import lit, col, split
from pyspark.sql import functions as F

def current_local_date():
    return F.from_utc_timestamp(F.current_timestamp(), 'Europe/Riga').cast('date')
 #Compensation YEC data from delta table raw_curr_compensation_yec
df_com_yec=spark.read.table("kgsonedatadb.trusted_compensation_yec").withColumnRenamed("EMP_CATEGORY","Emp_Category").withColumnRenamed("Location","Location1").withColumnRenamed("FULL_NAME","Full_Name1").withColumnRenamed("Post_Graduation___Highest_Qualification","Post_Graduation_Highest_Qualification");



#Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
df_emp_dump=spark.read.table("kgsonedatadb.trusted_headcount_employee_dump").withColumnRenamed("Cost_centre","Cost_Center").withColumnRenamed("Client_Geography","Client_Geo").withColumnRenamed("Email_Address","Email1").withColumnRenamed("Performance_Manager","Performance_manager_name")
#.withColumn("Date_as_of",to_date(current_timestamp()));

#joining YEC and ED the data
df_join_emp = df_emp_dump.join(df_com_yec, df_emp_dump.Employee_Number==df_com_yec.EMP_ID,'left').select(df_com_yec['*'],df_emp_dump.Employee_Number,df_emp_dump.Position,df_emp_dump.Date_First_Hired,df_emp_dump.Email1,df_emp_dump.Performance_manager_name,df_emp_dump.Full_Name,df_emp_dump.Function,df_emp_dump.Employee_Subfunction,df_emp_dump.Cost_Center,df_emp_dump.Location,df_emp_dump.Job_Name,df_emp_dump.Gender,df_emp_dump.Email1,df_emp_dump.Employee_Category,df_emp_dump.Client_Geo,df_emp_dump.Operating_Unit)

df_join_emp = df_join_emp.drop('EMP_ID')


# COMMAND ----------

df_add_col = df_join_emp\
.withColumnRenamed("Employee_Number","Emp_ID")\
.withColumn("Years_at_current_designation_as_on_30Sep22", col("Position"))\
.withColumn("Pitched_at_current_positioning_", col("Pitched_at__Current_Positioning_"))\
.withColumn("Graduation", col("GRAD"))\
.withColumn("Years_in_the_Firm_as_on_30Sep22", (F.months_between(current_local_date(), F.col('Date_First_Hired')) / 12).cast('int'))\
.withColumn("Total_years_of_work_experience_as_on_30Sep22", col("TOTAL_WORK_EXP_INCL_KPMG"))\
.withColumn("Proposed_Column_Promotion_History_", lit("null"))\
.withColumn("Current_financial_year", lit("null"))\
.withColumn("Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_", lit("null"))\
.withColumn("Distribution_category", lit("null"))\
.withColumn("Proposed_Rating_Oct-Sep_", lit("null"))\
.withColumn("Comments_on_Promotion_Exceptions", lit("null"))\
.withColumn("BP_comments", lit("null"))\
.withColumn("PD_comments", lit("null"))\
.withColumn("HR_head_comments", lit("null"))\
.withColumn("Promotion_category", lit("null"))\
.withColumn("Promotion_approved", lit("null"))\
.withColumn("Date_as_of",lit("null"))


#df_add_col=df_add_col.drop('Full_Name1','FUNCTION_1','SUB_FUNCTION','SUB_FUNCTION1','ORGANIZATION','Location1','DESIGNATION','JOB_CODE','STARTDATE','CTC','Previous_Year_CTC','Previous_Year_Performance_Bonus___Prorated_','Previous_Year_Sales_Bonus','Previous_Year_Special_Bonus','Total_Bonus_of_Previous_Year','Bonus_Accrued','OTHERS','NUMBER_OF_LEAVES','Employee_Status__Employee___Serving_Notice_','COMPANY','Employee_Number')

#display(df_add_col)

# COMMAND ----------

# DBTITLE 1,Function_tag
from pyspark.sql.functions import col
#year_end_costcenter_functiontag config table
df_year_end_costcenter_functiontag = spark.read.table("kgsonedatadb.config_year_end_costcenter_functiontag");

df_add_col1 = df_add_col.join(df_year_end_costcenter_functiontag, df_add_col.Cost_Center == df_year_end_costcenter_functiontag.Cost_centre,"left").select(df_add_col["*"],df_year_end_costcenter_functiontag["Function_tag"])

display(df_add_col1)
#df.select("Cost_center","Function_Tag").show()

# COMMAND ----------

# DBTITLE 1,New_jobcode
#year_end_jobcode config table
df_year_end_jobcode = spark.read.table("kgsonedatadb.config_year_end_jobcode").withColumnRenamed("Function","Function1").withColumnRenamed("Position","Position1").withColumnRenamed("Job_Name","New_jobcode");

df_add_col2= df_add_col1.join(df_year_end_jobcode, (df_add_col1.Function==df_year_end_jobcode.Function1)&(df_add_col1.Position==df_year_end_jobcode.Position1),'left').select(df_add_col1["*"],df_year_end_jobcode["New_jobcode"])

df_add_col2 = df_add_col2.drop('Function1','Position1')
#df_add_col2  = df_add_col2.dropDuplicates()
display(df_add_col2)

#df.select("Function","Designation","New_jobcode").show())


# COMMAND ----------

# DBTITLE 1,sample_delta_table
df_year_end_test = spark.read.table("kgsonedatadb.config_year_end_test")
df_year_end_test=df_year_end_test.withColumnRenamed("EMP_ID","EMP_ID1").withColumnRenamed("Function_tag","Function_tag1")
#.withColumnRenamed("Promotion_recommendation__Yes_No_?","Promotion_recommendation_Yes_or_No_").withColumnRenamed("Include_in_rating_distribution?","Include_in_rating_distribution").withColumnRenamed("Include_in_rating_distribution?","Include_in_rating_distribution").withColumnRenamed("Proposed_Rating__Oct_'21_-_Sep_'22_","Proposed_Rating_Oct_21_to_Sep_22_").withColumnRenamed("Function_tag","Function_tag1");


df_add_col3 = df_add_col2.join(df_year_end_test, df_add_col2.Function_tag==df_year_end_test.Function_tag1, 'left')

df_add_col3 = df_add_col3.drop('EMP_ID1','Function_tag1')

display(df_add_col3)
#df_add_col3.select("Promotion_recommendation_Yes_or_No_","Include_in_rating_distribution","Proposed_Rating_Oct_21_to_Sep_22_","Function_tag1").show()


# COMMAND ----------

# DBTITLE 1,Rating_Jump_or_Dip
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,lit,array,struct

df=df_add_col3.withColumn("curr_timestamp",current_timestamp()).withColumn("curr_year",date_format(col('curr_timestamp'),'y'))\
.withColumn('prev_date',F.date_add(F.date_trunc('yyyy', 'curr_year'), -1))\
.withColumn("prev_year",date_format(col('prev_date'),'y'))

df = df.withColumn("curr_year_pr",substring(df['curr_year'], -2, 2)).withColumn("prev_year_pr",substring(df['prev_year'], -2, 2))
 

list = df.select("curr_year","prev_year").distinct().rdd.flatMap(lambda x: x).collect()
list1 = df.select("curr_year_pr","prev_year_pr").distinct().rdd.flatMap(lambda x: x).collect()

Rating= [x for x in df.columns if any(x in y or y in x for y in list)]

PR= [x for x in df.columns if any(x in y or y in x for y in list1)]
ratingCol = Rating[0]
prCol = PR[1]
# df = df.withColumn('Rating_Jump_or_Dip', (df[ratingCol] - df[prCol]))

df = df.withColumn('Rating_Jump_or_Dip', (df[prCol] - df[ratingCol]))

df= df.drop('curr_timestamp','curr_year','prev_date','prev_year','curr_year_pr','prev_year_pr')

display(df)

#display(df.select('Rating_Jump_or_Dip'))

#df5 = df.drop('Quarter','Category')

# COMMAND ----------

# DBTITLE 1,New_designation
df_year_end_functiontag_hierarchyflow = spark.read.table("kgsonedatadb.config_year_end_functiontag_hierarchyflow").withColumnRenamed("Function_Tag","Function_Tag1").drop("Designation");

#display(df_year_end_functiontag_hierarchyflow)

# COMMAND ----------

df2 = df.join(df_year_end_functiontag_hierarchyflow, (df.Function_tag==df_year_end_functiontag_hierarchyflow.Function_Tag1)&(df.Team_Band_as_per_CP_document==df_year_end_functiontag_hierarchyflow.Team),'left').select(df['*'],df_year_end_functiontag_hierarchyflow.Rank,df_year_end_functiontag_hierarchyflow.Hierarchy_flow)

df2 = df2.dropDuplicates()
#& (df.Designation == df_year_end_functiontag_hierarchyflow.Hierarchy_flow)
#display(df2)

# COMMAND ----------

import pyspark
from pyspark.sql.functions import lit, col, split, when, sum
from pyspark.sql import functions as F

df2 = df2.withColumn('New_Rank',when(df2.Hierarchy_flow == df2.DESIGNATION, df2.Rank+1)).drop('Rank','Hierarchy_flow')

#display(df2)

# COMMAND ----------

df3 = df2.join(df_year_end_functiontag_hierarchyflow, (df2.Function_tag==df_year_end_functiontag_hierarchyflow.Function_Tag1)&(df2.Team_Band_as_per_CP_document==df_year_end_functiontag_hierarchyflow.Team)& (df2.New_Rank == df_year_end_functiontag_hierarchyflow.Rank),'left')

#display(df3)

# COMMAND ----------

df3 = df3.withColumn('New_Designation_1', df3.Hierarchy_flow)

#df3 = df3.withColumnRenamed("New_Designation_1","New_Designation")

df3 = df3.dropDuplicates()
#display(df3)

# COMMAND ----------

#final = df3.select('Designation','New_Designation_1').filter(df3.New_Designation_1.isNotNull())
#final= final.dropDuplicates()

#final  = final.dropDuplicates()

display(df3)


# COMMAND ----------

# DBTITLE 1,# of days taken in FY22
import datetime
from pyspark.sql.functions import *
df_leave = spark.read.table("kgsonedatadb.trusted_headcount_leave_report")

df=df3.join(df_leave,df3.Emp_ID==df_leave.Employee_Number,'left').select(df3['*'],df_leave.Leave_Start_Date,df_leave.Leave_End_Date)

df = df.withColumn('Year',date_format(col('Leave_Start_Date'),'y'))
list=['2022']
df = df.filter(df.Year.isin(list)) 

#create an array containing all days between start_date and end_date
df = df.withColumn('days', F.expr('sequence(Leave_Start_Date, Leave_End_Date, interval 1 day)'))\
.withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day'))\
.withColumn('no_of_days_taken_in_FY23', F.expr('size(weekdays)'))

df = df.distinct()
df4 = df.drop('days','weekdays','Year','Leave_Start_Date','Leave_End_Date')
display(df4)

#display(df.select('Year','Leave_Start_Date','Leave_End_Date','no_of_days_taken_in_FY23'))
#df.withColumn("curr_timestamp", current_timestamp())\

# COMMAND ----------

# DBTITLE 1,Encore_winner
df_encore = spark.read.table("kgsonedatadb.trusted_employee_engagement_encore_output")
df = df4.join(df_encore, df4.Emp_ID==df_encore.Emp_No, 'left').select(df4['*'],df_encore.Quarter,df_encore.Category)
#Filter IS IN List values
Quarter = ['1','2','3','4']
#Quarter = ['1','2','3','4']
df = df.filter(df.Quarter.isin (Quarter) ) 
df = df.withColumn('Encore_winner',concat_ws('Q', df.Category, df.Quarter))
df5 = df.drop('Quarter','Category')
display(df5)

# COMMAND ----------

# DBTITLE 1,Effective_date
df = df5.withColumn("curr_timestamp", current_timestamp()).withColumn("curr_year",date_format(col('curr_timestamp'),'y')).withColumn('Effective_date1',lit("1/10"))
df = df.withColumn('Effective_date',concat_ws("/",df.Effective_date1,df.curr_year))
df6 = df.drop('curr_timestamp','curr_year','Effective_date1')
display(df6)

# COMMAND ----------

currentDf= df6.select("Emp_ID","Full_name","Function","Employee_Subfunction","Cost_center","Function_tag","Location","Position","Job_Name","Date_as_of","Pitched_at__Current_Positioning_","Date_since_at_current_designation")

list1 = currentDf.columns

current_designation_columns = [column for column in df_com_yec.columns if column.startswith("Years_at")]

list2 = df6.select("DOJ")
list2 = list2.columns

#years_in_the_Firm_columns = [column for column in df_com_yec.columns if column.startswith("Years_in")]

list3 = df6.select("Work_experience_prior_to_KPMG__Years_")
list3 = list3.columns

Total_years_of_work_experience_as_on = [column for column in df_com_yec.columns if column.startswith("TOTAL_WORK")]

ratings_columns = [column for column in df_com_yec.columns if column.startswith("RATING")]

list4=df6.select("Post_Graduation_Highest_Qualification","Graduation","Gender","Email1","Employee_Category","Include_in_rating_distribution?","no_of_days_taken_in_FY23","Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_","Distribution_category","Performance_manager_name")
list4 = list4.columns

Proposed_Rating = [column for column in df_com_yec.columns if column.startswith("Proposed_Rating")]

list5=df6.select("Promotion_recommendation_Yes_or_No_","Encore_winner","Comments_on_Promotion_Exceptions","BP_comments","Rating_Jump_or_Dip","PD_comments","HR_head_comments","Promotion_category","Promotion_approved","New_Designation_1","New_jobcode","New_pitching","Client_Geo","Operating_unit","Effective_date","Team_Band_as_per_CP_document")
list5 = list5.columns

combined = list1+current_designation_columns+list2+list3+Total_years_of_work_experience_as_on+ratings_columns+list4+Proposed_Rating+list5

final = df6.select(combined)


display(final)

# COMMAND ----------


final=final.withColumnRenamed("New_Designation_1","New_Designation").withColumnRenamed("TOTAL_WORK_EXP_INCL_KPMG","Total_years_of_work_experience_as_on_30Sep23").withColumnRenamed("Email1","Email").dropDuplicates()

final=final.withColumnRenamed("Employee_Subfunction","Sub_Function")
final=final.withColumnRenamed("Position","Designation")
final=final.withColumnRenamed("Job_Name","Job_Code")
final=final.withColumnRenamed("Employee_Category","Emp_Category")


# COMMAND ----------

final=final.withColumn("File_Date", lit(FileDate))

display(final)

# COMMAND ----------

# DBTITLE 1,Load into Trusted stg Table
final.write \
.mode("overwrite") \
.format("delta") \
.option("mergeschema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})