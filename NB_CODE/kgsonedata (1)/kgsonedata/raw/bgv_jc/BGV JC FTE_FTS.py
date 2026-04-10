# Databricks notebook source
dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")
print(FileDate)

tableName = "fte_fts"
processName = "bgv_joined_candidate"

# COMMAND ----------

# %run
# /kgsonedata/common_utilities/connection_configuration/

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Import Statement
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace,col, trim, lower
#from pyspark.sql.functions import col, trim, lower
from datetime import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------


fileYear = FileDate[:4]
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
FileDate2=FileDate
FileDate = datetime.strptime(convertedFileDate, '%Y-%m-%d').date()
print(FileDate)

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Reading FTE Table
df_fte_new = spark.sql("select * from kgsonedatadb.trusted_hist_ta_post_hire_fte where file_date ='20240401' ")
#where to_date(File_Date,'yyyyMMdd') = '"+str(FileDate)+"'"+" 
#display(df_fte_new)
df_fte_new = df_fte_new.select("BGV_REFERENCE_NO_","CANDIDATE_NAME",'DEPARTMENT_NAME','GEO','LEVEL','JOINING_LOCATION','EMAIL_ID','ORIGINAL_START_DATE','RECRUITER_NAME','FINAL_JOINING_STATUS','EMPLOYEE_CATEGORY','SOURCE','SUB_SOURCE','BU','PROJECT_CODE','MANDATORY_CHECKS_COMPLETED','CANDIDATE_IDENTIFIER')
#kgsonedatadb.raw_curr_bgv_jc_fte_fts_old1 #trusted_hist_ta_post_hire_fte
df_fte_new=df_fte_new.createOrReplaceTempView("df_fte_new")
df_fte_new= spark.sql(""" select BGV_Reference_No_ as BGV_CASE_REFERENCE_NUMBER, Candidate_Name as CANDIDATE_FULL_NAME , Department_Name as COST_CENTRE , Geo as CLIENT_GEOGRAPHY, Level as DESIGNATION, Joining_Location as LOCATION , Email_ID as CANDIDATE_EMAIL, Original_Start_Date as PLANNED_START_DATE,Recruiter_Name as RECRUITER, Final_Joining_Status as STATUS , EMPLOYEE_CATEGORY, SOURCE, Sub_Source as NAME_OF_SUB_SOURCE, BU, Project_Code as INDIA_PROJECT_CODE, Mandatory_Checks_Completed AS MANDATE_CHECKS_COMPLETED, CANDIDATE_IDENTIFIER, "-" as EMPLOYEE_ID, "-" as OFFER_RELEASE_DATE , "-" as OFFER_ACCEPTANCE_DATE, 'NEW' AS OLD_OR_NEW_DATA   from df_fte_new""")

#trim the column values
df_fte_new=leadtrailremove(df_fte_new)

print(df_fte_new.count())
df_fte_new=df_fte_new.withColumn('BGV_CASE_REFERENCE_NUMBER',trim('BGV_CASE_REFERENCE_NUMBER'))
df_fte_new=df_fte_new.filter(lower(df_fte_new.STATUS).like('%join%')  &(~lower(df_fte_new.STATUS).like('%not%')))

df_fte_new=df_fte_new.withColumn('STATUS',lit('Joined'))
df_fte_new.createOrReplaceTempView('df_fte_new')
print(df_fte_new.count())
display(df_fte_new)

# COMMAND ----------

column_list=['BGV_CASE_REFERENCE_NUMBER', 'CANDIDATE_FULL_NAME' , 'CLIENT_GEOGRAPHY', 'DESIGNATION', 'LOCATION' , 'CANDIDATE_EMAIL', 'PLANNED_START_DATE','RECRUITER', 'STATUS' , 'EMPLOYEE_CATEGORY', 'SOURCE', 'NAME_OF_SUB_SOURCE', 'BU', 'INDIA_PROJECT_CODE', 'MANDATE_CHECKS_COMPLETED', 'CANDIDATE_IDENTIFIER']
for columnName in column_list:
    df_fte_new=df_fte_new.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_fte_new=df_fte_new.withColumn(columnName,ascii_udf(columnName))
    df_fte_new=df_fte_new.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------


dateList = ['PLANNED_START_DATE']
for columnName in df_fte_new.columns:
    if (columnName in dateList):
        print("1")
        df_fte_new = df_fte_new.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_fte_new[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_fte_new[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
        df_fte_new=df_fte_new.withColumn(columnName,changeDateFormat(columnName))
    
#display(df_fte_new)


# COMMAND ----------

# DBTITLE 1,Previous JC_FTE
df_previous=spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_joined_candidate_fte_fts where lower(STATUS) like('%join%') and lower(STATUS) not like('%not%') ")
df_previous.createOrReplaceTempView('tmp_previous')
display(df_previous)

df_prev_jc_fte = spark.sql("select * from (select rank() over(partition by trim(BGV_CASE_REFERENCE_NUMBER) order by file_date desc , PLANNED_START_DATE desc, dated_on desc) as rank, * from  tmp_previous where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+") where rank = 1 ")

#trim the column values
df_prev_jc_fte=leadtrailremove(df_prev_jc_fte)

#display(df_prev_jc_fte)
df_prev_jc_fte=df_prev_jc_fte.withColumn('BGV_CASE_REFERENCE_NUMBER',trim('BGV_CASE_REFERENCE_NUMBER'))
display(df_prev_jc_fte.groupBy("BGV_CASE_REFERENCE_NUMBER").agg(count("BGV_CASE_REFERENCE_NUMBER")).filter(count("BGV_CASE_REFERENCE_NUMBER")>1))    
df_prev_jc_fte.createOrReplaceTempView("jc_fte_hist")

df_prev_jc_fte=spark.sql('''select BGV_CASE_REFERENCE_NUMBER AS BGV_CASE_REFERENCE_NUMBER_PREVIOUS,
EMPLOYEE_ID AS EMPLOYEE_ID_PREVIOUS,PLANNED_START_DATE AS PLANNED_START_DATE_PREVIOUS, STATUS AS STATUS_PREVIOUS,
trim(CANDIDATE_EMAIL) AS CANDIDATE_EMAIL_PREVIOUS,
OFFICIAL_EMAIL_ID_PREVIOUS,HRBP_NAME_PREVIOUS, PERFORMANCE_MANAGER_PREVIOUS,SUPERVISOR_NAME_PREVIOUS,DOI_PREVIOUS,BGV_STATUS_FINAL_PREVIOUS,
FR_PREVIOUS,SR_PREVIOUS,OVER_ALL_PREVIOUS,ALL_CHECKS_COMPLETED_PREVIOUS,WAIVER_APPLICABLE_PREVIOUS,WAIVER_CLOSED_PREVIOUS,
BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS,
BGV_COMPLETION_DATE_PREVIOUS,SUSPICIOUS_COMPANY_PREVIOUS, OFFER_RELEASE_DATE AS OFFER_RELEASE_DATE_PREVIOUS,OFFER_ACCEPTANCE_DATE AS OFFER_ACCEPTANCE_DATE_PREVIOUS,
EMPLOYEE_STATUS AS EMPLOYEE_STATUS_PREVIOUS,COST_CENTRE  AS COST_CENTRE_PREVIOUS, CLIENT_GEOGRAPHY AS CLIENT_GEOGRAPHY_PREVIOUS,DESIGNATION AS DESIGNATION_PREVIOUS,
LOCATION AS LOCATION_PREVIOUS, BU AS BU_PREVIOUS, FILE_DATE AS FILE_DATE_PREVIOUS from jc_fte_hist''')


# df_prev_jc_fte=spark.sql("""SELECT distinct
# CASE WHEN BGV_CASE_REFERENCE_NUMBER  is null THEN '-' ELSE BGV_CASE_REFERENCE_NUMBER END AS BGV_CASE_REFERENCE_NUMBER_PREVIOUS,
# CASE WHEN EMPLOYEE_ID  is null THEN '-' ELSE EMPLOYEE_ID END AS EMPLOYEE_ID_PREVIOUS, 
# CASE WHEN PLANNED_START_DATE is null THEN '-' ELSE PLANNED_START_DATE END AS PLANNED_START_DATE_PREVIOUS,
# CASE WHEN STATUS  is null THEN '-' ELSE STATUS END AS STATUS_PREVIOUS,
# CASE WHEN CANDIDATE_EMAIL is null THEN '-' ELSE trim(CANDIDATE_EMAIL) END AS CANDIDATE_EMAIL_PREVIOUS, 
# CASE WHEN OFFICIAL_EMAIL_ID  is null THEN '-' ELSE OFFICIAL_EMAIL_ID END AS OFFICIAL_EMAIL_ID_PREVIOUS, 
# CASE WHEN HRBP_NAME is null THEN '-' ELSE HRBP_NAME END AS HRBP_NAME_PREVIOUS, 
# CASE WHEN PERFORMANCE_MANAGER  is null THEN '-' ELSE PERFORMANCE_MANAGER END AS PERFORMANCE_MANAGER_PREVIOUS, 
# CASE WHEN SUPERVISOR_NAME  is null THEN '-' ELSE SUPERVISOR_NAME END AS SUPERVISOR_NAME_PREVIOUS, 
# CASE WHEN DOI_NEW is null THEN '-' ELSE DOI_NEW END AS DOI_PREVIOUS, 
# CASE WHEN BGV_STATUS_FINAL  is null THEN '-' ELSE BGV_STATUS_FINAL END AS BGV_STATUS_FINAL_PREVIOUS, 
# CASE WHEN FR_OUTPUT  is null THEN '-' ELSE FR_OUTPUT END AS FR_PREVIOUS, 
# CASE WHEN SR_OUTPUT  is null THEN '-' ELSE SR_OUTPUT END AS SR_PREVIOUS, 
# CASE WHEN OVER_ALL_OUTPUT  is null THEN '-' ELSE OVER_ALL_OUTPUT END AS OVER_ALL_PREVIOUS, 
# CASE WHEN ALL_CHECKS_COMPLETED_NEW  is null THEN '-' ELSE ALL_CHECKS_COMPLETED_NEW END AS ALL_CHECKS_COMPLETED_PREVIOUS,
# CASE WHEN WAIVER_APPLICABLE_NEW  is null THEN '-' ELSE WAIVER_APPLICABLE_NEW END AS WAIVER_APPLICABLE_PREVIOUS, 
# CASE WHEN WAIVER_CLOSED_NEW  is null THEN '-' ELSE WAIVER_CLOSED_NEW END AS WAIVER_CLOSED_PREVIOUS, 
# CASE WHEN BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION is null THEN '-' ELSE BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION END AS BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS, 
# CASE WHEN ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is null THEN '-' ELSE ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER END AS ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS, 
# CASE WHEN BGV_COMPLETION_DATE_NEW is null THEN '-' ELSE BGV_COMPLETION_DATE_NEW END AS BGV_COMPLETION_DATE_PREVIOUS, 
# CASE WHEN SUSPICIOUS_COMPANY_NEW  is null THEN '-' ELSE SUSPICIOUS_COMPANY_NEW END AS SUSPICIOUS_COMPANY_PREVIOUS,

# CASE WHEN OFFER_RELEASE_DATE  is null THEN '-' ELSE OFFER_RELEASE_DATE END AS OFFER_RELEASE_DATE_PREVIOUS,
# CASE WHEN OFFER_ACCEPTANCE_DATE   is null THEN '-' ELSE OFFER_ACCEPTANCE_DATE END AS OFFER_ACCEPTANCE_DATE_PREVIOUS,
# CASE WHEN EMPLOYEE_STATUS is null THEN '-' ELSE EMPLOYEE_STATUS END AS EMPLOYEE_STATUS_PREVIOUS,

# CASE WHEN COST_CENTRE is null THEN '-' ELSE COST_CENTRE END AS COST_CENTRE_PREVIOUS,
# CASE WHEN CLIENT_GEOGRAPHY is null THEN '-' ELSE CLIENT_GEOGRAPHY END AS CLIENT_GEOGRAPHY_PREVIOUS,
# CASE WHEN DESIGNATION is null THEN '-' ELSE DESIGNATION END AS DESIGNATION_PREVIOUS,
# CASE WHEN LOCATION is null THEN '-' ELSE LOCATION END AS LOCATION_PREVIOUS,
# CASE WHEN BU is null THEN '-' ELSE BU END AS BU_PREVIOUS

# FROM jc_fte_hist """)

df_prev_jc_fte=df_prev_jc_fte.withColumn('PLANNED_START_DATE_PREVIOUS',to_date('PLANNED_START_DATE_PREVIOUS', 'yyyy-MM-dd'))

print(df_prev_jc_fte.count()) 
df_prev_jc_fte=df_prev_jc_fte.filter(lower(df_prev_jc_fte.STATUS_PREVIOUS).like('%join%') & (~lower(df_prev_jc_fte.STATUS_PREVIOUS).like('%not%')))
df_prev_jc_fte=df_prev_jc_fte.withColumn('STATUS_PREVIOUS',lit('Joined'))

print(df_prev_jc_fte.count()) 
display(df_prev_jc_fte.groupBy("BGV_CASE_REFERENCE_NUMBER_PREVIOUS").agg(count("BGV_CASE_REFERENCE_NUMBER_PREVIOUS")).filter(count("BGV_CASE_REFERENCE_NUMBER_PREVIOUS")>1))
df_prev_jc_fte=df_prev_jc_fte.dropDuplicates(['BGV_CASE_REFERENCE_NUMBER_PREVIOUS','PLANNED_START_DATE_PREVIOUS','STATUS_PREVIOUS'])
print(df_prev_jc_fte.count())

display(df_prev_jc_fte.filter(df_prev_jc_fte.COST_CENTRE_PREVIOUS=='')) 
# 19279
# 18290
# Query returned no results
# 18290 18291
#display(df_prev_jc_fte.filter(df_prev_jc_fte.BGV_CASE_REFERENCE_NUMBER_PREVIOUS.like('%KPMG%00002111%KGS%2022%')))

# COMMAND ----------

column_list=['BGV_CASE_REFERENCE_NUMBER_PREVIOUS','EMPLOYEE_ID_PREVIOUS','STATUS_PREVIOUS','CANDIDATE_EMAIL_PREVIOUS','OFFICIAL_EMAIL_ID_PREVIOUS','HRBP_NAME_PREVIOUS','PERFORMANCE_MANAGER_PREVIOUS','SUPERVISOR_NAME_PREVIOUS','DOI_PREVIOUS','BGV_STATUS_FINAL_PREVIOUS','FR_PREVIOUS','SR_PREVIOUS','OVER_ALL_PREVIOUS','ALL_CHECKS_COMPLETED_PREVIOUS','WAIVER_APPLICABLE_PREVIOUS','WAIVER_CLOSED_PREVIOUS','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS','ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS','SUSPICIOUS_COMPANY_PREVIOUS','EMPLOYEE_STATUS_PREVIOUS','CLIENT_GEOGRAPHY_PREVIOUS','DESIGNATION_PREVIOUS','LOCATION_PREVIOUS','BU_PREVIOUS']
for columnName in column_list:
    df_prev_jc_fte=df_prev_jc_fte.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_prev_jc_fte=df_prev_jc_fte.withColumn(columnName,ascii_udf(columnName))
    df_prev_jc_fte=df_prev_jc_fte.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

from pyspark.sql.functions import *
df_prev_jc_fte=df_prev_jc_fte
dateList = ['OFFER_RELEASE_DATE_PREVIOUS','OFFER_ACCEPTANCE_DATE_PREVIOUS']
for columnName in df_prev_jc_fte.columns:
    if (columnName in dateList):
        print("1")
        df_prev_jc_fte = df_prev_jc_fte.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_prev_jc_fte[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_prev_jc_fte[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
        df_prev_jc_fte=df_prev_jc_fte.withColumn(columnName,changeDateFormat(columnName))
    
display(df_prev_jc_fte)

# COMMAND ----------

# DBTITLE 1,KGS Joiners Report

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_talent_acquisition_kgs_joiners_report')):
    df_kgs_joiner_report = spark.sql("select * from (select row_number() over(partition by CANDIDATE_EMAIL  order by file_date desc ,dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report  where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
    
    
else:
    print("table doesn't exist")

#RENAMING THE COLUMNS WRT PRITAM TEST FILES
df_kgs_joiner_report=df_kgs_joiner_report.withColumnRenamed("Candidate_Email","CANDIDATE_PERSONAL_EMAIL").withColumnRenamed("EmplNo","EMPLOYEE_NUMBER")

#trim the column values
df_kgs_joiner_report=leadtrailremove(df_kgs_joiner_report)

df_kgs_joiner_report = df_kgs_joiner_report.select('EMPLOYEE_NUMBER','CANDIDATE_PERSONAL_EMAIL','OFFER_ROLE_DATE','OFFER_ACCEPTANCE_DATE')

df_kgs_joiner_report = df_kgs_joiner_report.withColumn('CANDIDATE_PERSONAL_EMAIL',trim('CANDIDATE_PERSONAL_EMAIL'))
df_kgs_joiner_report = df_kgs_joiner_report.withColumn('FIRST_ACCEPTANCE_DATE',col('OFFER_ACCEPTANCE_DATE'))

df_kgs_joiner_report = rename_prefix_columns("JOINERS",df_kgs_joiner_report) 

df_kgs_joiner_report = df_kgs_joiner_report.dropDuplicates()
print(df_kgs_joiner_report.count()) #7901 7890 7879 #11198
df_kgs_joiner_report.createOrReplaceTempView('df_kgs_joiner_report')
display(df_kgs_joiner_report)
display(df_kgs_joiner_report.groupBy("JOINERS_CANDIDATE_PERSONAL_EMAIL").agg(count("JOINERS_CANDIDATE_PERSONAL_EMAIL")).filter(count("JOINERS_CANDIDATE_PERSONAL_EMAIL")>1))

# COMMAND ----------

column_list=['JOINERS_EMPLOYEE_NUMBER','JOINERS_CANDIDATE_PERSONAL_EMAIL']
for columnName in column_list:
    df_kgs_joiner_report=df_kgs_joiner_report.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_kgs_joiner_report=df_kgs_joiner_report.withColumn(columnName,ascii_udf(columnName))
    df_kgs_joiner_report=df_kgs_joiner_report.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

#df_kgs_joiner_report1=df_kgs_joiner_report
dateList = ['JOINERS_OFFER_ROLE_DATE','JOINERS_OFFER_ACCEPTANCE_DATE','JOINERS_FIRST_ACCEPTANCE_DATE']
for columnName in df_kgs_joiner_report.columns:
    if (columnName in dateList):
        
        df_kgs_joiner_report = df_kgs_joiner_report.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_kgs_joiner_report[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_kgs_joiner_report[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
        df_kgs_joiner_report=df_kgs_joiner_report.withColumn(columnName,changeDateFormat(columnName))
    
#display(df_kgs_joiner_report.filter(col('EMPLOYEE_NUMBER').isin('143474','132624')))
#.filter(col('EMPLOYEE_NUMBER').isin()'143474','132624')

# COMMAND ----------

# DBTITLE 1,ED

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_headcount_employee_dump')):
    #df_ed = spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump ")
    df_ed = spark.sql("select * from (select row_number() over(partition by Employee_Number order by file_date desc, dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_date = (select max(File_Date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")
    
else:
    print("table doesn't exist")


#REMOVING THE STATUS FOR TEST DATA. ADD IF REQUIRED ,'Status'
df_ed = df_ed.select('EMAIL_ADDRESS','EMPLOYEE_NUMBER','RM_EMPLOYEE_NAME','PERFORMANCE_MANAGER','SUPERVISOR_NAME','COST_CENTRE','CLIENT_GEOGRAPHY', 'POSITION', 'LOCATION', 'OPERATING_UNIT')

#trim the column values
df_ed=leadtrailremove(df_ed)

df_ed.createOrReplaceTempView("df_ed_temp")
df_ed = rename_prefix_columns("ED",df_ed) 

#trim the column values
df_ed=leadtrailremove(df_ed)

print(df_ed.count())
df_ed = df_ed.dropDuplicates()
display(df_ed.filter(df_ed.ED_COST_CENTRE == ''))

df_ed=df_ed.withColumn("ED_COST_CENTRE",when(df_ed.ED_COST_CENTRE=='',lit(None)).otherwise(df_ed.ED_COST_CENTRE))
display(df_ed.filter(df_ed.ED_COST_CENTRE == ''))
# display(df_ed.filter(df_ed.ED_EMAIL_ADDRESS == 'shreeya764@rediffmail.com'))
#display(df_ed)

display(df_ed.groupBy("ED_EMPLOYEE_NUMBER").agg(count("ED_EMPLOYEE_NUMBER")).filter(count("ED_EMPLOYEE_NUMBER")>1))


# COMMAND ----------

column_list=['ED_EMAIL_ADDRESS','ED_EMPLOYEE_NUMBER','ED_RM_EMPLOYEE_NAME','ED_PERFORMANCE_MANAGER','ED_SUPERVISOR_NAME','ED_CLIENT_GEOGRAPHY', 'ED_POSITION', 'ED_LOCATION', 'ED_OPERATING_UNIT']
for columnName in column_list:
    df_ed=df_ed.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_ed=df_ed.withColumn(columnName,ascii_udf(columnName))
    df_ed=df_ed.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
# display(df_ed)
# display(df_ed.filter(df_ed.ED_EMPLOYEE_NUMBER == '123749'))
display(df_ed.groupBy("ED_EMPLOYEE_NUMBER").agg(count("ED_EMPLOYEE_NUMBER")).filter(count("ED_EMPLOYEE_NUMBER")>1))

# COMMAND ----------

# DBTITLE 1,TD
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_headcount_termination_dump')):
    df_td = spark.sql("select  * from (select row_number() over(partition by EMPLOYEE_NUMBER order by file_date desc,Last_Update_Date desc, dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_termination_dump where File_date = (select max(File_Date) from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")
    #df_td = spark.sql("select * from kgsonedatadb.trusted_headcount_termination_dump")
   
else:
    print("table doesn't exist")


#REMOVING THE STATUS FOR TEST DATA. ADD IF REQUIRED ,'Status'
df_td=df_td.select('EMPLOYEE_NUMBER','EMAIL_ADDRESS','RM_EMPLOYEE_NAME','SUPERVISOR_NAME', 'COST_CENTRE', 'CLIENT_GEOGRAPHY', 'POSITION', 'LOCATION', 'KGS_BUSINESS_UNIT')

#trim the column values
df_td=leadtrailremove(df_td)

df_td = rename_prefix_columns("TD",df_td)  

#trim the column values
df_td=leadtrailremove(df_td)

df_td = df_td.dropDuplicates()
display(df_td.filter(df_td.TD_COST_CENTRE == ''))

df_td=df_td.withColumn("TD_COST_CENTRE",when(df_td.TD_COST_CENTRE=='',lit(None)).otherwise(df_td.TD_COST_CENTRE))
display(df_td.filter(df_td.TD_COST_CENTRE == ''))

print(df_td.count()) #64399 #64398
# display(df_td)
display(df_td.groupBy("TD_EMPLOYEE_NUMBER").agg(count("TD_EMPLOYEE_NUMBER")).filter(count("TD_EMPLOYEE_NUMBER")>1))

# COMMAND ----------

column_list=['TD_EMPLOYEE_NUMBER','TD_EMAIL_ADDRESS','TD_RM_EMPLOYEE_NAME','TD_SUPERVISOR_NAME', 'TD_CLIENT_GEOGRAPHY', 'TD_POSITION', 'TD_LOCATION', 'TD_KGS_BUSINESS_UNIT']
for columnName in column_list:
    df_td=df_td.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_td=df_td.withColumn(columnName,ascii_udf(columnName))
    df_td=df_td.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
# display(df_td)
display(df_td.groupBy("TD_EMPLOYEE_NUMBER").agg(count("TD_EMPLOYEE_NUMBER")).filter(count("TD_EMPLOYEE_NUMBER")>1))

# COMMAND ----------

# DBTITLE 1,RD
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_headcount_talent_konnect_resignation_status_report')):
    df_rd = spark.sql("select * from (select row_number() over(partition by EMPLOYEENUMBER order by file_date desc, dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where File_date = (select max(File_Date) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")

else:
    print("table doesn't exist")

df_rd=df_rd.select('EMPLOYEENUMBER','PERSONAL_EMAIL_ID','HRBP_NAME','PERFORMANCEMANAGERNAME','APPROVED_LWD', 'LOCATION', 'JOB')

df_rd = rename_prefix_columns("RD",df_rd)

#trim the column values
df_rd=leadtrailremove(df_rd) 

df_rd = df_rd.dropDuplicates()
# print(df_rd.count())
display(df_rd)
display(df_rd.groupBy("RD_EMPLOYEENUMBER").agg(count("RD_EMPLOYEENUMBER")).filter(count("RD_EMPLOYEENUMBER")>1))
#11559
#11554]
#11546

# COMMAND ----------

column_list=['RD_EMPLOYEENUMBER','RD_PERSONAL_EMAIL_ID','RD_HRBP_NAME','RD_PERFORMANCEMANAGERNAME', 'RD_LOCATION', 'RD_JOB']

for columnName in column_list:
    df_rd=df_rd.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_rd=df_rd.withColumn(columnName,ascii_udf(columnName))
    df_rd=df_rd.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
display(df_rd.groupBy("RD_EMPLOYEENUMBER").agg(count("RD_EMPLOYEENUMBER")).filter(count("RD_EMPLOYEENUMBER")>1))

# COMMAND ----------

#df_rd1=df_rd
dateList = ['RD_APPROVED_LWD']
for columnName in df_rd.columns:
    if (columnName in dateList):
        print("1")
        df_rd = df_rd.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_rd[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_rd[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
        df_rd=df_rd.withColumn(columnName,changeDateFormat(columnName))
    
display(df_rd)

# COMMAND ----------

# DBTITLE 1,Progress sheet
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_progress_sheet')):
    df_ps =spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_progress_sheet ")
    #trim the column values
    df_ps=leadtrailremove(df_ps)
    
    display(df_ps.filter(df_ps.Personal_Email_ID=='aiswaryakarnati@gmail.com'))
    dateList = ['Case_Initiation_Date','Final_Report_date','Supplimentry_Report_Date']
    for columnName in df_ps.columns:
        if (columnName in dateList):
            df_ps=df_ps.withColumn(columnName,regexp_replace(regexp_replace(df_ps[columnName],'th ','-'),'\'','-'))
            
            df_ps=df_ps.withColumn(columnName,\
            when(to_date(df_ps[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MM-yy'),'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd MM yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd MM yy'),'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MMM-yy'),'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd-MMMM-yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MMMM-yy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd MMM yy'),'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd/MM/yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd/MM/yy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'MM/dd/yy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'MM/dd/yy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'yyyy-MM-dd').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'yyyy-MM-dd'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd/MM/yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd/MM/yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'MM/dd/yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'MM/dd/yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd-MM-yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd-MMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MMM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd-MMMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd-MMMM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd MMMM,yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd MMMM,yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'yyyy/MM/dd').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'yyyy/MM/dd'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'MMMM d,yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'MMMM d,yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'dd.mm.yyyy').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'dd.mm.yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(df_ps[columnName], 'yyyy-MM-dd HH:mm:ss.SSSSSSS').isNotNull(),from_unixtime(unix_timestamp(df_ps[columnName], 'yyyy-MM-dd HH:mm:ss.SSSSSSS'), 'yyyy-MM-dd'))
            .otherwise(df_ps[columnName]))
            
            df_ps=df_ps.withColumn(columnName,substring(columnName,1,10))
            df_ps=df_ps.withColumn(columnName,to_date(columnName,'yyyy-MM-dd'))
            #df_ps=df_ps.withColumn(columnName,changeDateFormat(columnName))
        # df_ps=df_ps.withColumn('FINAL_REPORT_DATE',to_timestamp('FINAL_REPORT_DATE','yyyy-MM-dd HH:mm:ss.SSSSSSS'))
        # df_ps=df_ps.withColumn('FINAL_REPORT_DATE',date_format('FINAL_REPORT_DATE','yyyy-MM-dd'))
        #df_ps=df_ps.withColumn(columnName,to_date(columnName).cast('string'))

    df_ps.createOrReplaceTempView('tmp_ps')
    

    df_progress_sheet = spark.sql("select PERSONAL_EMAIL_ID,OPEN_INSUFF,CASE_INITIATION_DATE,OVER_ALL_STATUS_DROP_DOWN_, FINAL_REPORT_COLOR_CODE_DROP_DOWN_, SUPPLIMENTRY_REPORT_COLOR_CODE_DROP_DOWN_,REFERENCE_NO_,FINAL_REPORT_DATE,SUPPLIMENTRY_REPORT_DATE,EDU_EMP_NAME__SUSPICIOUS_,FILE_DATE  from (select row_number() over(partition by trim(REFERENCE_NO_) order by file_date desc,Dated_On desc ,  Case_Initiation_Date desc) as row_num, * from tmp_ps where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1") 
    
    #display(df_ps.filter(df_ps.Personal_Email_ID=='nikita.golaya@terisas.ac.in'))
    
else:
    print("table doesn't exist")

df_progress_sheet = df_progress_sheet.select('PERSONAL_EMAIL_ID','OPEN_INSUFF','CASE_INITIATION_DATE','OVER_ALL_STATUS_DROP_DOWN_','FINAL_REPORT_COLOR_CODE_DROP_DOWN_','SUPPLIMENTRY_REPORT_COLOR_CODE_DROP_DOWN_','REFERENCE_NO_','FINAL_REPORT_DATE','SUPPLIMENTRY_REPORT_DATE','EDU_EMP_NAME__SUSPICIOUS_','FILE_DATE')


df_progress_sheet = rename_prefix_columns("PS",df_progress_sheet)   

df_progress_sheet=df_progress_sheet.withColumn('PS_REFERENCE_NO_',trim('PS_REFERENCE_NO_'))
df_progress_sheet = df_progress_sheet.dropDuplicates()
display(df_progress_sheet.groupBy("PS_REFERENCE_NO_").agg(count("PS_REFERENCE_NO_")).filter(count("PS_REFERENCE_NO_")>1))
display(df_progress_sheet.filter((df_progress_sheet['PS_OVER_ALL_STATUS_DROP_DOWN_']).like('%process%')))
df_progress_sheet=df_progress_sheet.withColumn("PS_OVER_ALL_STATUS_DROP_DOWN_",when(((lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%process%'))| (lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%progress%'))),"Report Pending").otherwise(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]))

display(df_progress_sheet.filter((df_progress_sheet['PS_OVER_ALL_STATUS_DROP_DOWN_']).like('%process%')))
print(df_progress_sheet.count())
#display(df_progress_sheet.filter(df_progress_sheet.PS_PERSONAL_EMAIL_ID=='ca.anjaliagarwal18@gmail.com'))
#22833 PS_CASE_INITIATION_DATE PS_FINAL_REPORT_DATE
#22290
#22237 23723


# COMMAND ----------

display(df_progress_sheet.filter(df_progress_sheet.PS_PERSONAL_EMAIL_ID.isin('aiswaryakarnati@gmail.com')))

# COMMAND ----------

column_list=['PS_PERSONAL_EMAIL_ID','PS_REFERENCE_NO_']

for columnName in column_list:
    df_progress_sheet=df_progress_sheet.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,ascii_udf(columnName))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

# DBTITLE 1,Waiver Tracker

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_waiver_tracker')):

    df_wt =spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_waiver_tracker ")
    #trim the column values
    df_wt=leadtrailremove(df_wt)
    display(df_wt)
    dateList = ['Report_Date']
    for columnName in df_wt.columns:
        if (columnName in dateList):
            df_wt = df_wt.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_wt[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_wt[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
            df_wt=df_wt.withColumn(columnName,to_date(columnName, 'yyyy-MM-dd'))
    df_wt.createOrReplaceTempView("df_wt")

    df_waiver_tracker = spark.sql("select * from (select row_number() over(partition by trim(BGV_REF_NO) order by file_date desc,Report_date desc, dated_on desc) as row_num, * from df_wt where file_date = (select max(file_date) from df_wt where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")

    

    df_waiver_tracker = df_waiver_tracker.select('BGV_REF_NO','CANDIDATE_EMAIL_ID','WAIVER_RECEIVED_DATE___HRBP_TA_HEAD','CASE_REMARKS','CASE_STATUS')
        
else:
    print("table doesn't exist")


df_waiver_tracker = rename_prefix_columns("WAIVER",df_waiver_tracker) 

df_waiver_tracker=df_waiver_tracker.withColumn("WAIVER_BGV_REF_NO",trim("WAIVER_BGV_REF_NO"))
display(df_waiver_tracker.groupBy("WAIVER_BGV_REF_NO").agg(count("WAIVER_BGV_REF_NO")).filter(count("WAIVER_BGV_REF_NO")>1))

df_waiver_tracker = df_waiver_tracker.dropDuplicates()
print(df_waiver_tracker.count())
display(df_waiver_tracker)
#14470 WAIVER_REPORT_DATE
#13136 WAIVER_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD
#WAIVER_Waiver_Received_Date___HRBP_TA_Head
#13018 WAIVER_Waiver_Sent_Date___HRBP_TA_Head


# COMMAND ----------

column_list=['WAIVER_BGV_REF_NO','WAIVER_CANDIDATE_EMAIL_ID']

for columnName in column_list:
    df_waiver_tracker=df_waiver_tracker.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_waiver_tracker=df_waiver_tracker.withColumn(columnName,ascii_udf(columnName))
    df_waiver_tracker=df_waiver_tracker.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

# DBTITLE 1,Reading Client Check Data

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_client_check_data')):
    df_client_check =spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_client_check_data ")
    #trim the column values
    df_client_check=leadtrailremove(df_client_check)
    #removing some different character which is not in special character and assci values
    df_client_check = df_client_check.withColumn('CASE_REFERENCE_NUMBER', regexp_replace('CASE_REFERENCE_NUMBER', '	', ''))
    #df_client_check = df_client_check.withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '	', ''))
    #display(df_client_check1.filter(df_client_check1.CLIENT_CHECK_EMAIL_ID.contains('shankss91@gmail.com')))
    df_client_check.createOrReplaceTempView("client_temp")

    df_client_check = spark.sql("select * from (select row_number() over(partition by trim(CASE_REFERENCE_NUMBER) order by file_date desc,dated_on desc) as row_num, * from client_temp where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")   
    display(df_client_check) 
else:
    print("table doesn't exist")

#trim the column values
df_client_check=leadtrailremove(df_client_check)

df_client_check = df_client_check.select('CASE_REFERENCE_NUMBER','EMAIL_ID').distinct()
df_client_check = rename_prefix_columns("CLIENT_CHECK",df_client_check) 

df_client_check=df_client_check.withColumn('CLIENT_CHECK_CASE_REFERENCE_NUMBER',trim('CLIENT_CHECK_CASE_REFERENCE_NUMBER'))
df_client_check.createOrReplaceTempView('df_client_check')
df_client_check = df_client_check.dropDuplicates()

display(df_client_check.groupBy("CLIENT_CHECK_CASE_REFERENCE_NUMBER").agg(count("CLIENT_CHECK_CASE_REFERENCE_NUMBER")).filter(count("CLIENT_CHECK_CASE_REFERENCE_NUMBER")>1))

# display(df_client_check.groupBy("CLIENT_CHECK_EMAIL_ID").agg(count("CLIENT_CHECK_EMAIL_ID")).filter(count("CLIENT_CHECK_EMAIL_ID")>1))
print(df_client_check.count()) #4531 4434 4428
# display(df_client_check)
#3913

# COMMAND ----------


if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_client_check_data')):
    df_client_check2 =spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_client_check_data ")
    #trim the column values
    df_client_check2=leadtrailremove(df_client_check2)
    #removing some different character which is not in special character and assci values
    # df_client_check = df_client_check.withColumn('CASE_REFERENCE_NUMBER', regexp_replace('CASE_REFERENCE_NUMBER', '	', ''))
    df_client_check2 = df_client_check2.withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '	', ''))
    #display(df_client_check1.filter(df_client_check1.CLIENT_CHECK_EMAIL_ID.contains('shankss91@gmail.com')))
    df_client_check2.createOrReplaceTempView("client_temp2")

    df_client_check2 = spark.sql("select * from (select row_number() over(partition by trim(EMAIL_ID) order by file_date desc,dated_on desc) as row_num, * from client_temp2 where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")   
    display(df_client_check2) 
else:
    print("table doesn't exist")

#trim the column values
df_client_check2=leadtrailremove(df_client_check2)

df_client_check2 = df_client_check2.select('CASE_REFERENCE_NUMBER','EMAIL_ID').distinct()
df_client_check2 = rename_prefix_columns("CLIENT_CHECK2",df_client_check2) 


df_client_check2.createOrReplaceTempView('df_client_check2')
df_client_check2 = df_client_check2.dropDuplicates()

# display(df_client_check2.groupBy("CLIENT_CHECK_CASE_REFERENCE_NUMBER").agg(count("CLIENT_CHECK_CASE_REFERENCE_NUMBER")).filter(count("CLIENT_CHECK_CASE_REFERENCE_NUMBER")>1))

display(df_client_check2.groupBy("CLIENT_CHECK2_EMAIL_ID").agg(count("CLIENT_CHECK2_EMAIL_ID")).filter(count("CLIENT_CHECK2_EMAIL_ID")>1))
print(df_client_check2.count()) #4531 4461
# display(df_client_check)
#3913

# COMMAND ----------

column_list=['CLIENT_CHECK_CASE_REFERENCE_NUMBER','CLIENT_CHECK_EMAIL_ID']

for columnName in column_list:
    df_client_check=df_client_check.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_client_check=df_client_check.withColumn(columnName,ascii_udf(columnName))
    df_client_check=df_client_check.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

# #removing some different character which is not in special character and assci values
# df_client_check = df_client_check.withColumn('CLIENT_CHECK_CASE_REFERENCE_NUMBER', regexp_replace('CLIENT_CHECK_CASE_REFERENCE_NUMBER', '	', ''))
# df_client_check = df_client_check.withColumn('CLIENT_CHECK_EMAIL_ID', regexp_replace('CLIENT_CHECK_EMAIL_ID', '	', ''))
# #display(df_client_check1.filter(df_client_check1.CLIENT_CHECK_EMAIL_ID.contains('shankss91@gmail.com')))

# COMMAND ----------

# DBTITLE 1,config_hist_cc_bu_sl
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'config_cc_bu_sl')):
    df_bu_sl = spark.sql("select distinct trim(COST_CENTRE) as COST_CENTRE, BU from kgsonedatadb.config_cc_bu_sl")
   
else:
    print("table doesn't exist")
df_bu_sl = rename_prefix_columns("BU",df_bu_sl)

df_bu_sl =leadtrailremove(df_bu_sl)
# print(df_bu_sl.count())
display(df_bu_sl.groupBy("BU_COST_CENTRE").agg(count("BU_COST_CENTRE")).filter(count("BU_COST_CENTRE")>1))

# COMMAND ----------

# from pyspark.sql.functions import *
# column_list=['BU_COST_CENTRE']
# for columnName in column_list:
#     df_bu_sl=df_bu_sl.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
#     df_bu_sl=df_bu_sl.withColumn(columnName,ascii_udf(columnName))
#     df_bu_sl=df_bu_sl.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
# display(df_bu_sl)


# COMMAND ----------

# DBTITLE 1,Reading all the old records except current week records from posthire
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_fte_fts')):
    df_old=spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_joined_candidate_fte_fts where lower(STATUS) like('%join%') and lower(STATUS) not like('%not%') and file_date<='20240401' ")
    df_old.createOrReplaceTempView('tmp_old')
    # display(df_old)


    df_old_fte = spark.sql("select BGV_CASE_REFERENCE_NUMBER,  CANDIDATE_FULL_NAME, COST_CENTRE, CLIENT_GEOGRAPHY,  DESIGNATION,  LOCATION, CANDIDATE_EMAIL, PLANNED_START_DATE,RECRUITER,  STATUS, EMPLOYEE_CATEGORY, SOURCE,  NAME_OF_SUB_SOURCE, BU,  INDIA_PROJECT_CODE,  MANDATE_CHECKS_COMPLETED ,CANDIDATE_IDENTIFIER, EMPLOYEE_ID,OFFER_RELEASE_DATE ,OFFER_ACCEPTANCE_DATE, 'OLD' AS OLD_OR_NEW_DATA from (select rank() over(partition by trim(BGV_CASE_REFERENCE_NUMBER)  order by file_date desc,PLANNED_START_DATE desc,dated_on desc) as rank, * from tmp_old where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+" ) where BGV_CASE_REFERENCE_NUMBER not in (select distinct(BGV_CASE_REFERENCE_NUMBER) from df_fte_new) and rank = 1 order by EMPLOYEE_ID" )

else:
    print("table doesn't exist, reading from the empty hist file")
    filePath="completed/bgv/jc_test/hist_fte_dummy2.csv"
    df_old_fte = spark.read.format("csv").option("inferschema","false").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)
    for col in df_old_fte.columns:
        df_old_fte=df_old_fte.withColumnRenamed(col,replacechar(col))
    df_old_fte = df_old_fte.select("EMPLOYEE_ID","BGV_CASE_REFERENCE_NUMBER",  "CANDIDATE_FULL_NAME", "COST_CENTRE", "CLIENT_GEOGRAPHY",  "DESIGNATION",  "LOCATION", "CANDIDATE_EMAIL", "PLANNED_START_DATE","RECRUITER",  "STATUS", "EMPLOYEE_CATEGORY", "SOURCE",  "NAME_OF_SUB_SOURCE", "BU",  "INDIA_PROJECT_CODE",  "MANDATE_CHECKS_COMPLETED","CANDIDATE_IDENTIFIER","OLD_OR_NEW_DATA")

#trim the column values
df_old_fte=leadtrailremove(df_old_fte)

df_old_fte=df_old_fte.withColumn('BGV_CASE_REFERENCE_NUMBER',trim('BGV_CASE_REFERENCE_NUMBER'))   
# print(df_old_fte.count())    #18291
df_old_fte=df_old_fte.filter(lower(df_old_fte.STATUS).like('%join%')& (~lower(df_old_fte.STATUS).like('%not%')))
df_old_fte=df_old_fte.withColumn('STATUS',lit('Joined'))
# print(df_old_fte.count())    #18291
display(df_old_fte.groupBy("BGV_CASE_REFERENCE_NUMBER").agg(count("BGV_CASE_REFERENCE_NUMBER")).filter(count("BGV_CASE_REFERENCE_NUMBER")>1))
df_old_fte=df_old_fte.dropDuplicates(['BGV_CASE_REFERENCE_NUMBER','PLANNED_START_DATE','STATUS'])
print(df_old_fte.count())   #18291
df_old_fte.createOrReplaceTempView("df_old_fte")
display(df_old_fte)
display(df_old_fte.groupBy("BGV_CASE_REFERENCE_NUMBER").agg(count("BGV_CASE_REFERENCE_NUMBER")).filter(count("BGV_CASE_REFERENCE_NUMBER")>1))

# COMMAND ----------

column_list=['EMPLOYEE_ID','BGV_CASE_REFERENCE_NUMBER',  'CANDIDATE_FULL_NAME', 'CLIENT_GEOGRAPHY',  'DESIGNATION',  'LOCATION', 'CANDIDATE_EMAIL', 'RECRUITER',  'STATUS', 'EMPLOYEE_CATEGORY', 'SOURCE',  'NAME_OF_SUB_SOURCE',  'INDIA_PROJECT_CODE',  'MANDATE_CHECKS_COMPLETED','CANDIDATE_IDENTIFIER','OLD_OR_NEW_DATA']

for columnName in column_list:
    df_old_fte=df_old_fte.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_old_fte=df_old_fte.withColumn(columnName,ascii_udf(columnName))
    df_old_fte=df_old_fte.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

df_old_fte=df_old_fte
dateList = ['OFFER_RELEASE_DATE','OFFER_ACCEPTANCE_DATE']
for columnName in df_old_fte.columns:
    if (columnName in dateList):
        df_old_fte = df_old_fte.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_old_fte[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_old_fte[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
        df_old_fte=df_old_fte.withColumn(columnName,to_date(columnName, 'MM/dd/yyyy'))
# display(df_old_fte)

# COMMAND ----------

df_old_fte_latest = spark.sql("""select * from df_old_fte where CANDIDATE_EMAIL in (select distinct JOINERS_CANDIDATE_PERSONAL_EMAIL from df_kgs_joiner_report )""")
display(df_old_fte_latest)
print(df_old_fte_latest.count()) #8331
df_old_fte_latest.createOrReplaceTempView("df_old_fte_latest")


df_old_fte = spark.sql("""select * from df_old_fte where CANDIDATE_EMAIL not in (select distinct CANDIDATE_EMAIL from df_old_fte_latest )""")

#print(df_old_fte.count()) #11967
#20298


#Union df_fte_new and df_fte_old_latest into df_fte_new
df_fte_new = df_fte_new.union(df_old_fte_latest) 
print(df_fte_new.count())

df_fte_new= df_fte_new.join(df_kgs_joiner_report,(df_fte_new.CANDIDATE_EMAIL == df_kgs_joiner_report.JOINERS_CANDIDATE_PERSONAL_EMAIL),"left")

print(df_fte_new.count())
# display(df_fte_new)
  

# COMMAND ----------

df_fte_new= df_fte_new.withColumn("EMPLOYEE_ID",\
   when((df_fte_new.EMPLOYEE_ID != '-'),df_fte_new.EMPLOYEE_ID)\
       .otherwise(df_fte_new.JOINERS_EMPLOYEE_NUMBER))

df_fte_new= df_fte_new.withColumn("OFFER_RELEASE_DATE",\
   when((df_fte_new.OFFER_RELEASE_DATE != '-'),df_fte_new.OFFER_RELEASE_DATE)\
       .otherwise(df_fte_new.JOINERS_OFFER_ROLE_DATE)) #JOINERS_OFFER_ROLE_DATE OFFER_ROLE_DATE

df_fte_new= df_fte_new.withColumn("OFFER_ACCEPTANCE_DATE",\
   when((df_fte_new.OFFER_ACCEPTANCE_DATE != '-'),df_fte_new.OFFER_ACCEPTANCE_DATE)\
       .otherwise(df_fte_new.JOINERS_FIRST_ACCEPTANCE_DATE))
# display(df_fte_new) 
df_fte_new.createOrReplaceTempView("df_fte_new") 

# COMMAND ----------

# DBTITLE 1,union df_fte_old with df_fte_new
df_fte_new=df_fte_new.drop('JOINERS_EMPLOYEE_NUMBER','JOINERS_CANDIDATE_PERSONAL_EMAIL','JOINERS_OFFER_ROLE_DATE','JOINERS_OFFER_ACCEPTANCE_DATE','JOINERS_FIRST_ACCEPTANCE_DATE')
df_fte = df_fte_new.union(df_old_fte)
print(df_fte.count()) #18351
# display(df_fte)

# COMMAND ----------

# DBTITLE 1,Unioning  Current  data and Hist data
# # union() not required
# df_old_fte=df_old_fte.drop('EMPLOYEE_ID')
# df_fte = df_fte_new.union(df_old_fte) 
# print(df_fte.count())
# from pyspark.sql.functions import col, trim, lower
# df_fte.createOrReplaceTempView("df_fte") 

# COMMAND ----------

#joining Prev JC FTE 

df_fte = df_fte.join(df_prev_jc_fte,df_fte.BGV_CASE_REFERENCE_NUMBER == df_prev_jc_fte.BGV_CASE_REFERENCE_NUMBER_PREVIOUS
,'left')
print(df_fte.count()) #18348

# COMMAND ----------

# a.Staff on probation -> FTE
# b.Fixed term staff -> FTS
#Employee Category
df_fte = df_fte.withColumn('EMPLOYEE_CATEGORY',when(lower(df_fte.EMPLOYEE_CATEGORY).contains('probation'),lit('FTE'))\
    .when(lower(df_fte.EMPLOYEE_CATEGORY).contains('fixed'),lit('FTS'))\
    .otherwise(df_fte.EMPLOYEE_CATEGORY))


# COMMAND ----------

#Campus -> Campus, all others Lateral
#Source
df_fte = df_fte.withColumn('SOURCE',when(lower(trim(df_fte.SOURCE)) == 'campus',lit('Campus'))\
    .otherwise(lit('Lateral')))

# COMMAND ----------

# Joining ED,TD,RD

etr_join_df =df_fte.join(df_ed,df_fte.EMPLOYEE_ID == df_ed.ED_EMPLOYEE_NUMBER ,"left").join(df_td,df_fte.EMPLOYEE_ID == df_td.TD_EMPLOYEE_NUMBER ,"left").join(df_rd,df_fte.EMPLOYEE_ID == df_rd.RD_EMPLOYEENUMBER ,"left")
print(etr_join_df.count())


# COMMAND ----------

#Official_Email_Id

etr_join_df = etr_join_df.withColumn("OFFICIAL_EMAIL_ID",\
     when(etr_join_df.ED_EMAIL_ADDRESS.isNotNull() | (etr_join_df.ED_EMAIL_ADDRESS != '-'),etr_join_df.ED_EMAIL_ADDRESS)\
     .when(etr_join_df.TD_EMAIL_ADDRESS.isNotNull() | (etr_join_df.TD_EMAIL_ADDRESS != '-'),etr_join_df.TD_EMAIL_ADDRESS)\
    .when(((etr_join_df.ED_EMAIL_ADDRESS.isNull()| (etr_join_df.ED_EMAIL_ADDRESS != '-')) & (etr_join_df.TD_EMAIL_ADDRESS.isNull() | (etr_join_df.TD_EMAIL_ADDRESS != '-'))& etr_join_df.OFFICIAL_EMAIL_ID_PREVIOUS.isNotNull()) ,etr_join_df.OFFICIAL_EMAIL_ID_PREVIOUS)\
    .otherwise(lit(None)))

display(etr_join_df)

# COMMAND ----------

#Employee_Status

etr_join_df = etr_join_df.withColumn("EMPLOYEE_STATUS", \
    when((lower(etr_join_df.STATUS)=='joined') & ( etr_join_df.TD_EMPLOYEE_NUMBER.isNotNull()| (etr_join_df.TD_EMPLOYEE_NUMBER != '-' )|  ( ((etr_join_df.RD_EMPLOYEENUMBER != '-') | etr_join_df.RD_EMPLOYEENUMBER.isNotNull()) & ( (etr_join_df.RD_APPROVED_LWD.isNotNull() )& (etr_join_df.RD_APPROVED_LWD!= '1900-01-01' ) & (etr_join_df.RD_APPROVED_LWD < current_date()) ) )   ),lit('Left'))\
    .otherwise(lit('-')))

# COMMAND ----------

# display(df_ed.filter(((df_ed.ED_COST_CENTRE!='')) & (df_ed.ED_EMPLOYEE_NUMBER=='142020')))

# COMMAND ----------

# display(etr_join_df.filter(etr_join_df.EMPLOYEE_ID.isin('142020','142065','117485','143810','143365','143245')))
# #Cost_centre (etr_join_df.ED_COST_CENTRE!='')
# etr_join_df =etr_join_df.withColumn("COST_CENTRE", \
#     when(((etr_join_df.ED_COST_CENTRE!='') | etr_join_df.ED_COST_CENTRE.isNotNull()),etr_join_df.ED_COST_CENTRE)\
#     .when(((etr_join_df.TD_COST_CENTRE!='') | etr_join_df.TD_COST_CENTRE.isNotNull()),etr_join_df.TD_COST_CENTRE)\
#     .when((etr_join_df.COST_CENTRE_PREVIOUS!='')| (etr_join_df.COST_CENTRE_PREVIOUS.isNotNull()) | (etr_join_df.COST_CENTRE_PREVIOUS!='-'),etr_join_df.COST_CENTRE_PREVIOUS)\
#     .otherwise(etr_join_df.COST_CENTRE))
# display(etr_join_df2.filter(etr_join_df2.COST_CENTRE ==''))


# COMMAND ----------

display(etr_join_df.filter(etr_join_df.CANDIDATE_EMAIL=='shreeya764@rediffmail.com'))
#Cost_centre
etr_join_df =etr_join_df.withColumn("COST_CENTRE", \
    when(etr_join_df.ED_COST_CENTRE.isNotNull(),etr_join_df.ED_COST_CENTRE)\
    .when(etr_join_df.TD_COST_CENTRE.isNotNull(),etr_join_df.TD_COST_CENTRE)\
    .when((etr_join_df.COST_CENTRE_PREVIOUS.isNotNull()) | (etr_join_df.COST_CENTRE_PREVIOUS!='-'),etr_join_df.COST_CENTRE_PREVIOUS)\
    .otherwise(etr_join_df.COST_CENTRE))


#    .when((etr_join_df.COST_CENTRE_PREVIOUS.isNotNull()) | (etr_join_df.COST_CENTRE_PREVIOUS!='-'),etr_join_df.COST_CENTRE_PREVIOUS)\
# etr_join_df = etr_join_df.withColumn("COST_CENTRE",\
#      when((etr_join_df.ED_COST_CENTRE != '-') | etr_join_df.ED_COST_CENTRE.isNotNull() ,etr_join_df.ED_COST_CENTRE)\
#      .when( (etr_join_df.TD_COST_CENTRE != '-') | etr_join_df.TD_COST_CENTRE.isNotNull(),etr_join_df.TD_COST_CENTRE)\
#         .otherwise(etr_join_df.COST_CENTRE_PREVIOUS))
# #     .when(((etr_join_df.ED_COST_CENTRE.isNull()| (etr_join_df.ED_COST_CENTRE == '-')) & (etr_join_df.TD_COST_CENTRE.isNull() | (etr_join_df.TD_COST_CENTRE == '-'))& etr_join_df.COST_CENTRE_PREVIOUS.isNotNull()) ,etr_join_df.COST_CENTRE_PREVIOUS)\
# #     .otherwise(lit(None)))
# # #    .otherwise(etr_join_df.COST_CENTRE_PREVIOUS))

#Client Geography
etr_join_df =etr_join_df.withColumn("CLIENT_GEOGRAPHY", \
    when(etr_join_df.ED_CLIENT_GEOGRAPHY.isNotNull(),etr_join_df.ED_CLIENT_GEOGRAPHY)\
    .when(etr_join_df.TD_CLIENT_GEOGRAPHY.isNotNull(),etr_join_df.TD_CLIENT_GEOGRAPHY)\
    .when((etr_join_df.CLIENT_GEOGRAPHY_PREVIOUS.isNotNull()) | (etr_join_df.CLIENT_GEOGRAPHY_PREVIOUS!='-'),etr_join_df.CLIENT_GEOGRAPHY_PREVIOUS)\
    .otherwise(etr_join_df.CLIENT_GEOGRAPHY))

# etr_join_df = etr_join_df.withColumn("CLIENT_GEOGRAPHY",\
#      when( (etr_join_df.ED_CLIENT_GEOGRAPHY != '-') | etr_join_df.ED_CLIENT_GEOGRAPHY.isNotNull() ,etr_join_df.ED_CLIENT_GEOGRAPHY)\
#      .when((etr_join_df.TD_CLIENT_GEOGRAPHY != '-') | etr_join_df.TD_CLIENT_GEOGRAPHY.isNotNull() ,etr_join_df.TD_CLIENT_GEOGRAPHY)\
#     .when(((etr_join_df.ED_CLIENT_GEOGRAPHY.isNull()| (etr_join_df.ED_CLIENT_GEOGRAPHY == '-')) & (etr_join_df.TD_CLIENT_GEOGRAPHY.isNull() | (etr_join_df.TD_CLIENT_GEOGRAPHY == '-'))& etr_join_df.CLIENT_GEOGRAPHY_PREVIOUS.isNotNull()) ,etr_join_df.CLIENT_GEOGRAPHY_PREVIOUS)\
#     .otherwise(lit(None)))

#Designation
etr_join_df =etr_join_df.withColumn("DESIGNATION", \
    when(etr_join_df.ED_POSITION.isNotNull(),etr_join_df.ED_POSITION)\
    .when(etr_join_df.TD_POSITION.isNotNull(),etr_join_df.TD_POSITION)\
    .when(etr_join_df.RD_JOB.isNotNull(),etr_join_df.RD_JOB)\
    .when((etr_join_df.DESIGNATION_PREVIOUS.isNotNull()) | (etr_join_df.DESIGNATION_PREVIOUS!='-'),etr_join_df.DESIGNATION_PREVIOUS)\
    .otherwise(etr_join_df.DESIGNATION))

# etr_join_df = etr_join_df.withColumn("DESIGNATION",\
#      when( (etr_join_df.ED_POSITION != '-') | etr_join_df.ED_POSITION.isNotNull() ,etr_join_df.ED_POSITION)\
#      .when( (etr_join_df.TD_POSITION != '-') | etr_join_df.TD_POSITION.isNotNull(),etr_join_df.TD_POSITION)\
#      .when(etr_join_df.RD_JOB.isNotNull() | (etr_join_df.RD_JOB != '-'),etr_join_df.RD_JOB)\
#     .when(((etr_join_df.ED_POSITION.isNull()| (etr_join_df.ED_POSITION == '-')) & (etr_join_df.TD_POSITION.isNull() | (etr_join_df.TD_POSITION == '-')) & (etr_join_df.RD_JOB.isNull() | (etr_join_df.RD_JOB == '-'))& etr_join_df.DESIGNATION_PREVIOUS.isNotNull()) ,etr_join_df.DESIGNATION_PREVIOUS)\
#     .otherwise(lit(None)))

#Location
etr_join_df =etr_join_df.withColumn("LOCATION", \
    when(etr_join_df.ED_LOCATION.isNotNull(),etr_join_df.ED_LOCATION)\
    .when(etr_join_df.TD_LOCATION.isNotNull(),etr_join_df.TD_LOCATION)\
    .when(etr_join_df.RD_LOCATION.isNotNull(),etr_join_df.RD_LOCATION)\
    .when((etr_join_df.LOCATION_PREVIOUS.isNotNull()) | (etr_join_df.LOCATION_PREVIOUS!='-'),etr_join_df.LOCATION_PREVIOUS)\
    .otherwise(etr_join_df.LOCATION))

# etr_join_df = etr_join_df.withColumn("LOCATION",\
#      when( (etr_join_df.ED_LOCATION != '-') | etr_join_df.ED_LOCATION.isNotNull() ,etr_join_df.ED_LOCATION)\
#      .when( (etr_join_df.TD_LOCATION != '-') | etr_join_df.TD_LOCATION.isNotNull() ,etr_join_df.TD_LOCATION)\
#      .when( (etr_join_df.RD_LOCATION != '-') | etr_join_df.RD_LOCATION.isNotNull() ,etr_join_df.RD_LOCATION)\
#     .when(((etr_join_df.ED_LOCATION.isNull()| (etr_join_df.ED_LOCATION == '-')) & (etr_join_df.TD_LOCATION.isNull() | (etr_join_df.TD_LOCATION == '-')) & (etr_join_df.RD_LOCATION.isNull() | (etr_join_df.RD_LOCATION == '-'))& etr_join_df.LOCATION_PREVIOUS.isNotNull()) ,etr_join_df.LOCATION_PREVIOUS)\
#     .otherwise(lit(None)))

#BU
etr_join_df=etr_join_df.join(df_bu_sl,etr_join_df.COST_CENTRE == df_bu_sl.BU_COST_CENTRE,"left")
print(etr_join_df.count())
etr_join_df =etr_join_df.withColumn("BU", \
    when(etr_join_df.BU_BU.isNotNull(),etr_join_df.BU_BU)\
    .when((etr_join_df.BU_PREVIOUS.isNotNull()) | (etr_join_df.BU_PREVIOUS!='-'),etr_join_df.BU_PREVIOUS)\
    .otherwise(etr_join_df.BU))
#display(etr_join_df.filter(etr_join_df.EMPLOYEE_ID=='110913'))
#display(df_kgs_joiner_report.filter(col('EMPLOYEE_NUMBER').isin('143474','132624')))
#display(etr_join_df.filter(etr_join_df.CANDIDATE_EMAIL=='shreeya764@rediffmail.com'))

# COMMAND ----------

display(etr_join_df.filter(etr_join_df.EMPLOYEE_ID.isin('142020','142065','117485','143810','143365','143245','105433')))

# COMMAND ----------

#HRBP_Name
# etr_join_df =etr_join_df.withColumn("HRBP_NAME", \
#     when(etr_join_df.ED_RM_EMPLOYEE_NAME.isNotNull(),etr_join_df.ED_RM_EMPLOYEE_NAME)\
#     .when(etr_join_df.TD_RM_EMPLOYEE_NAME.isNotNull(),etr_join_df.TD_RM_EMPLOYEE_NAME)\
#     .when(etr_join_df.RD_HRBP_NAME.isNotNull(),etr_join_df.RD_HRBP_NAME)\
#     .when((etr_join_df.RD_HRBP_NAME.isNull() & etr_join_df.ED_RM_EMPLOYEE_NAME.isNull() & etr_join_df.TD_RM_EMPLOYEE_NAME.isNull() & etr_join_df.HRBP_NAME_PREVIOUS.isNotNull()), etr_join_df.HRBP_NAME_PREVIOUS)\
#     .otherwise(lit(None)))

etr_join_df = etr_join_df.withColumn("HRBP_NAME",\
     when(etr_join_df.ED_RM_EMPLOYEE_NAME.isNotNull() | (etr_join_df.ED_RM_EMPLOYEE_NAME != '-'),etr_join_df.ED_RM_EMPLOYEE_NAME)\
     .when(etr_join_df.TD_RM_EMPLOYEE_NAME.isNotNull() | (etr_join_df.TD_RM_EMPLOYEE_NAME != '-'),etr_join_df.TD_RM_EMPLOYEE_NAME)\
     .when(etr_join_df.RD_HRBP_NAME.isNotNull() | (etr_join_df.RD_HRBP_NAME != '-'),etr_join_df.RD_HRBP_NAME)\
    .when(((etr_join_df.ED_RM_EMPLOYEE_NAME.isNull()| (etr_join_df.ED_RM_EMPLOYEE_NAME != '-')) & (etr_join_df.TD_RM_EMPLOYEE_NAME.isNull() | (etr_join_df.TD_RM_EMPLOYEE_NAME != '-')) & (etr_join_df.RD_HRBP_NAME.isNull() | (etr_join_df.RD_HRBP_NAME != '-'))& etr_join_df.HRBP_NAME_PREVIOUS.isNotNull()) ,etr_join_df.HRBP_NAME_PREVIOUS)\
    .otherwise(lit(None)))

# COMMAND ----------

#Performance_Manager 
# Need to confirm with Process Team for TD

# etr_join_df =etr_join_df.withColumn("PERFORMANCE_MANAGER", \
#     when(etr_join_df.ED_PERFORMANCE_MANAGER.isNotNull(),etr_join_df.ED_PERFORMANCE_MANAGER)\
#     .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull(),etr_join_df.TD_SUPERVISOR_NAME)\
#     .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull(),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
#     .when((etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() & etr_join_df.TD_SUPERVISOR_NAME.isNull() & etr_join_df.ED_PERFORMANCE_MANAGER.isNull() & etr_join_df.PERFORMANCE_MANAGER_PREVIOUS.isNotNull()), etr_join_df.PERFORMANCE_MANAGER_PREVIOUS).otherwise(lit(None)))

etr_join_df = etr_join_df.withColumn("PERFORMANCE_MANAGER",\
     when(etr_join_df.ED_PERFORMANCE_MANAGER.isNotNull() | (etr_join_df.ED_PERFORMANCE_MANAGER != '-'),etr_join_df.ED_PERFORMANCE_MANAGER)\
     .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull() | (etr_join_df.TD_SUPERVISOR_NAME != '-'),etr_join_df.TD_SUPERVISOR_NAME)\
     .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull() | (etr_join_df.RD_PERFORMANCEMANAGERNAME != '-'),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
    .when(((etr_join_df.ED_PERFORMANCE_MANAGER.isNull()| (etr_join_df.ED_PERFORMANCE_MANAGER != '-')) & (etr_join_df.TD_SUPERVISOR_NAME.isNull() | (etr_join_df.TD_SUPERVISOR_NAME != '-')) & (etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() | (etr_join_df.RD_PERFORMANCEMANAGERNAME != '-'))& etr_join_df.PERFORMANCE_MANAGER_PREVIOUS.isNotNull()) ,etr_join_df.PERFORMANCE_MANAGER_PREVIOUS)\
    .otherwise(lit(None)))


# COMMAND ----------

#Supervisor_Name

# etr_join_df =etr_join_df.withColumn("SUPERVISOR_NAME", \
#     when(etr_join_df.ED_SUPERVISOR_NAME.isNotNull(),etr_join_df.ED_SUPERVISOR_NAME)\
#     .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull(),etr_join_df.TD_SUPERVISOR_NAME)\
#     .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull(),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
#     .when((etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() & etr_join_df.ED_SUPERVISOR_NAME.isNull() & etr_join_df.TD_SUPERVISOR_NAME.isNull() & etr_join_df.SUPERVISOR_NAME_PREVIOUS.isNotNull()), etr_join_df.SUPERVISOR_NAME_PREVIOUS)\
#     .otherwise(lit(None)))

etr_join_df = etr_join_df.withColumn("SUPERVISOR_NAME",\
     when(etr_join_df.ED_SUPERVISOR_NAME.isNotNull() | (etr_join_df.ED_SUPERVISOR_NAME != '-'),etr_join_df.ED_SUPERVISOR_NAME)\
     .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull() | (etr_join_df.TD_SUPERVISOR_NAME != '-'),etr_join_df.TD_SUPERVISOR_NAME)\
     .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull() | (etr_join_df.RD_PERFORMANCEMANAGERNAME != '-'),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
    .when(((etr_join_df.ED_SUPERVISOR_NAME.isNull()| (etr_join_df.ED_SUPERVISOR_NAME != '-')) & (etr_join_df.TD_SUPERVISOR_NAME.isNull() | (etr_join_df.TD_SUPERVISOR_NAME != '-')) & (etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() | (etr_join_df.RD_PERFORMANCEMANAGERNAME != '-'))& etr_join_df.SUPERVISOR_NAME_PREVIOUS.isNotNull()) ,etr_join_df.SUPERVISOR_NAME_PREVIOUS)\
    .otherwise(lit(None)))

# COMMAND ----------

#progress sheet join
PS_join_df= etr_join_df.join(df_progress_sheet,etr_join_df.BGV_CASE_REFERENCE_NUMBER == df_progress_sheet.
PS_REFERENCE_NO_ , "left")
# print(PS_join_df.count())

# COMMAND ----------

#Insuff Remarks ,  Case_Initiation_Date, BGV Status,  FR_Support_for_New, SR_Support_for_New, Final Report date
PS_join_df=PS_join_df.withColumnRenamed("PS_OPEN_INSUFF","INSUFF_REMARKS")\
    .withColumnRenamed("PS_CASE_INITIATION_DATE","DOI_NEW")\
    .withColumnRenamed("PS_OVER_ALL_STATUS_DROP_DOWN_","BGV_STATUS_PROGRESS_SHEET")\
    .withColumnRenamed("PS_FINAL_REPORT_COLOR_CODE_DROP_DOWN_","FR_SUPPORT_FOR_NEW")\
    .withColumnRenamed("PS_SUPPLIMENTRY_REPORT_COLOR_CODE_DROP_DOWN_","SR_SUPPORT_FOR_NEW")\
    .withColumnRenamed("PS_FINAL_REPORT_DATE","FINAL_REPORT_DATE")


# COMMAND ----------

# DBTITLE 1,DOI_NEW
PS_join_df= PS_join_df.withColumn("DOI_NEW",\
    when((PS_join_df.DOI_NEW.isNull()) | (PS_join_df.DOI_NEW == '-') | (PS_join_df.DOI_NEW == ''), PS_join_df.DOI_PREVIOUS)\
    .otherwise(PS_join_df.DOI_NEW))

display(PS_join_df.filter(PS_join_df.EMPLOYEE_ID.isin('103588','103919','103822','110127','113836','104042')))

# COMMAND ----------

#FR Output, SR Output, Last Green Complete Date

PS_join_df= PS_join_df.withColumn("FR_OUTPUT",\
    when((PS_join_df.FR_SUPPORT_FOR_NEW.isNull()) | (PS_join_df.FR_SUPPORT_FOR_NEW == '-'), PS_join_df.FR_PREVIOUS)\
    .otherwise(PS_join_df.FR_SUPPORT_FOR_NEW))

PS_join_df= PS_join_df.withColumn("SR_OUTPUT",\
    when((PS_join_df.SR_SUPPORT_FOR_NEW.isNull()) | (PS_join_df.SR_SUPPORT_FOR_NEW == '-'), PS_join_df.SR_PREVIOUS)\
    .otherwise(PS_join_df.SR_SUPPORT_FOR_NEW))

PS_join_df= PS_join_df.withColumn("LAST_GREEN_COMPLETE_DATE",\
    when((lower(PS_join_df.FR_OUTPUT) == 'green'), PS_join_df.FINAL_REPORT_DATE)\
    .when(( lower(PS_join_df.FR_OUTPUT) != 'green') & (lower(PS_join_df.SR_OUTPUT) == 'green' ), PS_join_df.PS_SUPPLIMENTRY_REPORT_DATE)\
    .otherwise(lit('-')))


# COMMAND ----------


display(PS_join_df.select(PS_join_df.LAST_GREEN_COMPLETE_DATE,PS_join_df.FR_OUTPUT,PS_join_df.FINAL_REPORT_DATE,PS_join_df.SR_OUTPUT,PS_join_df.PS_SUPPLIMENTRY_REPORT_DATE, PS_join_df.CANDIDATE_EMAIL).filter(PS_join_df.EMPLOYEE_ID.isin('122466','129780','111355')))

# COMMAND ----------

#CEA Latest Report Color
#If SR Output(BJ)<>'-', then SR Output(BJ), else FR Output(BI)
# PS_join_df =PS_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
#     when(((PS_join_df.SR_OUTPUT != '-') | (PS_join_df.SR_OUTPUT.isNotNull())), PS_join_df.SR_OUTPUT)\
#     .when(((PS_join_df.SR_OUTPUT == '-') | (PS_join_df.SR_OUTPUT.isNull() )), PS_join_df.FR_OUTPUT)\
#     .otherwise(lit('-')))
PS_join_df =PS_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
    when((PS_join_df.SR_OUTPUT == '-') | (PS_join_df.SR_OUTPUT.isNull())| (trim(PS_join_df.SR_OUTPUT) == '') | (PS_join_df.SR_OUTPUT  == '0'), PS_join_df.FR_OUTPUT)\
    .otherwise(PS_join_df.SR_OUTPUT))

# waiver_final_join_df =waiver_final_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
#     when((waiver_final_join_df.SR_OUTPUT == '-') | (waiver_final_join_df.SR_OUTPUT.isNull()) | (trim(waiver_final_join_df.SR_OUTPUT) == ''),waiver_final_join_df.FR_OUTPUT).otherwise(waiver_final_join_df.SR_OUTPUT))

# COMMAND ----------

#Waiver tracker join with reference number
waiver_join_df= PS_join_df.join(df_waiver_tracker,PS_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.
WAIVER_BGV_REF_NO, "left")
# print(waiver_join_df.count())

# COMMAND ----------

#Waiver Remarks
waiver_join_df=waiver_join_df.withColumnRenamed("WAIVER_CASE_STATUS","WAIVER_REMARKS")

# COMMAND ----------

#BGV Status (final)
# 1)when  BGV_Status_Progress_Sheet = 'Completed' & CEA_Latest_Report_Color !='Green' & Waiver_Remarks = 'Re-verification', then 'WIP'
# 2)when  BGV_Status_Progress_Sheet = 'Completed' & CEA_Latest_Report_Color !='Green' & Waiver_Remarks = 'Non-Compliant Report', then 'Report Pending'
# 3)if BGV_Status_Progress_Sheet !null or != - then   BGV_Status_Progress_Sheet
# 4)otherwise 'BGV_Status_Prev'

waiver_join_df= waiver_join_df.withColumn("BGV_STATUS_FINAL",\
    when((lower(trim(waiver_join_df.BGV_STATUS_PROGRESS_SHEET)) == 'completed') & (lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)!='green') & (lower(trim(waiver_join_df.WAIVER_REMARKS))=='re-verification'), lit('WIP'))\
    .when((lower(trim(waiver_join_df.BGV_STATUS_PROGRESS_SHEET)) == 'completed') & (lower(trim(waiver_join_df.CEA_LATEST_REPORT_COLOR))!='green') & (lower(trim(waiver_join_df.WAIVER_REMARKS))=='non-compliant report'), lit('Report Pending'))\
    .when((waiver_join_df.BGV_STATUS_PROGRESS_SHEET.isNotNull()) | (waiver_join_df.BGV_STATUS_PROGRESS_SHEET != '-'), waiver_join_df.BGV_STATUS_PROGRESS_SHEET)\
    .otherwise(waiver_join_df.BGV_STATUS_FINAL_PREVIOUS)) 

# COMMAND ----------

#If BGV_Status_Final <> 'Completed', then BGV_Status_Final
#If it is 'completed', if len(SR_Output)<>'-', then SR output(BJ), else FR_Output

#Over all Output
waiver_join_df =waiver_join_df.withColumn('OVER_ALL_OUTPUT',\
    when((lower(trim(waiver_join_df.BGV_STATUS_FINAL) ) != 'completed') , waiver_join_df.BGV_STATUS_FINAL)\
    .when((lower(trim(waiver_join_df.BGV_STATUS_FINAL )) == 'completed') & (waiver_join_df.SR_OUTPUT  != '-') & (waiver_join_df.SR_OUTPUT  != '0') & waiver_join_df.SR_OUTPUT.isNotNull(), waiver_join_df.SR_OUTPUT)\
    .otherwise(waiver_join_df.FR_OUTPUT))


# COMMAND ----------

#Waiver Applicable New
#If Over_all_Output = 'Green', then 'NA'
#Else if CEA_Latest_Report_Color ='CEA' or 0 or '-', then 'NA'
#Else 'Yes'
waiver_join_df=waiver_join_df.withColumn('WAIVER_APPLICABLE_NEW',when(lower(waiver_join_df.OVER_ALL_OUTPUT )== 'green', lit('NA')).when((lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)=='cea') | (lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)=='0') | (waiver_join_df.CEA_LATEST_REPORT_COLOR =='-' ) | (waiver_join_df.CEA_LATEST_REPORT_COLOR.isNull() ), lit('NA')).otherwise(lit('Yes')))


# COMMAND ----------

# If Waiver_Applicable_New = 'NA', then 'NA',
# Else if PrevJC_FTE_Waiver_Closed_Prev='Closed', then 'Closed'
# Else 
# Lookup Waiver Tracker using Waiver_BGV_Ref_no and check if Waiver_CASE_STATUS contains '%Closed%'
# and(or) Lookup waiver tracker using Waiver_Candidate_Email_ID and check if Waiver_CASE_STATUS contains '%Closed%', then 'Closed'
# Else 'Pending'

#Waiver Closed New
waiver_join_df=waiver_join_df.withColumn('WAIVER_CLOSED_NEW',when(lower(waiver_join_df.WAIVER_APPLICABLE_NEW)=='na', lit('NA'))\
.when(lower(waiver_join_df.WAIVER_CLOSED_PREVIOUS)=='closed', lit('Closed'))\
.when( (lower(waiver_join_df.WAIVER_REMARKS).contains("closed"))| (lower(waiver_join_df.WAIVER_REMARKS).contains("closed")), lit('Closed')).otherwise(lit('Pending')))


# COMMAND ----------

#All Checks Completed - Along With Waiver

#If BGV_Status_Final='Completed', then check if Over_all_Output='Green' or Waiver_Closed_New='Closed', then 'Yes' Else 'No'

waiver_join_df =waiver_join_df.withColumn('ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER',\
    when((lower(waiver_join_df.BGV_STATUS_FINAL) == 'completed') & ((lower(waiver_join_df.OVER_ALL_OUTPUT)== 'green') | (lower(waiver_join_df.WAIVER_CLOSED_NEW) =='closed')), lit('Yes'))\
    .otherwise(lit('No')))


# COMMAND ----------

# If Status != 'Joined' or Employee_Status ='left' or  != '-', then Inactive.
# If  ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is "Yes", then '-'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'CEA', then "CEA Pending"
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Completed' and WAIVER_CLOSED_NEW is 'Pending', then 'Waiver Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Insufficiency, then 'Insufficiency'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Report Pending' then 'Report Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'WIP', then 'WIP'
# All else '-'

#BGV Status (BB) BGV_STATUS_SELF
waiver_join_df =waiver_join_df.withColumn('BGV_STATUS_SELF',\
    when( (lower(waiver_join_df.STATUS)!='joined') | ( (lower(waiver_join_df.EMPLOYEE_STATUS) == 'left') | (waiver_join_df.EMPLOYEE_STATUS !='-') ), lit("In-Active"))\
    .when( lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='yes', lit('-'))\
    .when(  (lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(waiver_join_df.BGV_STATUS_FINAL)=='cea'), lit('CEA Pending') )\
    .when( (lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(waiver_join_df.BGV_STATUS_FINAL)=='completed') & (lower(waiver_join_df.WAIVER_CLOSED_NEW).like('%pending%')), lit('Waiver Pending') )\
    .when(  (lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(waiver_join_df.BGV_STATUS_FINAL).like('%insuf%')), lit('Insufficiency') )\
    .when(  (lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(waiver_join_df.BGV_STATUS_FINAL).like('%report pending%')), lit('Report Pending') )\
    .when(  (lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(waiver_join_df.BGV_STATUS_FINAL)=='wip'), lit('WIP') )\
    .otherwise(lit('-')))

# COMMAND ----------

#Remarks
#If BGV Status (BB) is 'Waiver Pending', then fetch Waiver Remarks column (BV)
#BGV_Status_Self , Waiver Remarks

waiver_join_df =waiver_join_df.withColumn('REMARKS',\
    when(lower(waiver_join_df.BGV_STATUS_SELF).like('%waiver pending%'), waiver_join_df.WAIVER_REMARKS)\
    .otherwise(lit('-'))
    )


# COMMAND ----------

# If BGV_Status_Final ='Completed' & Overall Output(BK)='Green', then Yes
# Else if BGV_Status_Final ='Completed', then Yes
# If BGV_Status_Final = 'CEA', then 'Yes except CEA'
# else if FR_Output ='CEA", then 'Yes except CEA'
# else 'No'

#All Checks Completed New
waiver_join_df =waiver_join_df.withColumn('ALL_CHECKS_COMPLETED_NEW',\
    when((lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%') & lower(waiver_join_df.OVER_ALL_OUTPUT).like('%green%')) | lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%') , lit('Yes'))\
    .when(lower(waiver_join_df.BGV_STATUS_FINAL).like('%cea%') | lower(waiver_join_df.FR_OUTPUT).like('%cea%') , lit('Yes except CEA'))
    .otherwise(lit('No'))
    )
# print(waiver_join_df.count())

# COMMAND ----------

# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes' and BU='Tax', then , if PrevJC_FTE_BGV_Completion_Date_PREV <>'-', then 'PrevJC_FTE_BGV_Completion_Date_PREV', 
# else iF BGV_Status_Final='completed' & Over_all_Output ='Green', then Last_Green_Complete_Date, 
# else BGV_Status_Final='completed' & Over_all_Output <>'Green' then Waiver_Waiver_Received_Date___HRBP_TA_Head ='-' OR NULL, then '-' 

# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes' and BU='Tax', then and, if PrevJC_FTE_BGV_Completion_Date_PREV <>'-', then 'PrevJC_FTE_BGV_Completion_Date_PREV', 
# else iF BGV_Status_Final='completed' & Over_all_Output ='Green', then Last_Green_Complete_Date, 
# else BGV_Status_Final='completed' & Over_all_Output <>'Green' then Waiver_Waiver_Received_Date___HRBP_TA_Head (not null or -) on basis of candidate ref num
# '-' OR NULL, then '-' 

#BGV Completion Date New
waiver_join_df =waiver_join_df.withColumn('BGV_COMPLETION_DATE_NEW',\
    when((lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER).like('%yes%')) & (lower(waiver_join_df.BU).like('%tax%')) & ((waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS!='-') | waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS.isNotNull()), waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS )\

    .when(lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%') & lower(waiver_join_df.OVER_ALL_OUTPUT).like('%green%') , waiver_join_df.LAST_GREEN_COMPLETE_DATE)\

    .when((lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%')) & (~lower(waiver_join_df.OVER_ALL_OUTPUT).like('%green%')) & ((waiver_join_df.WAIVER_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD=='-') | waiver_join_df.WAIVER_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD.isNull()) , lit('-'))\

    .otherwise(lit('-'))
    )


# COMMAND ----------

# DBTITLE 1,Insuff remarks starts
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_insuff_component')):
    insuff_df = spark.sql("select distinct INSUFF_COMPONENT as INSUFF_COMPONENTS from kgsonedatadb.trusted_hist_bgv_insuff_component")
else:
    print("table doesn't exist")

insuff_df=leadtrailremove(insuff_df)      

# COMMAND ----------

from pyspark.sql.functions import col,when
insuff_df=insuff_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in insuff_df.columns])
insuff_df = insuff_df.dropna("all")

# COMMAND ----------

insuff_list=insuff_df.select(upper('INSUFF_COMPONENTS')).rdd.flatMap(lambda x: x).collect()
print(insuff_list)

# COMMAND ----------

insuff_df=waiver_join_df.withColumn("INSUFF_REMARKS2",regexp_replace(waiver_join_df["INSUFF_REMARKS"],'[^a-zA-Z0-9:]',''))


# COMMAND ----------

# DBTITLE 1,Insuff component function
def func_insuff(str1):
    res_str = ''
    for i in insuff_list:  
        if i in str1:
            res_str+=i+',' 
            res_str[:-1]
    return res_str[:-1]
func_insuff_udf = udf(func_insuff,  StringType())


# COMMAND ----------

func_insuff("EDU1:PERIYARUNIVERSITYSALEM:AdditionalinformationrequiredAdditionalsupportingdocumentsRequireProvisionalDegreeCertificateEMP2:OGILVYMATHERPRIVATELIMITED:AdditionalinformationrequiredHRcontactdetailsEmailidandContactnumberrequired")
# get_DUE_DATE_working_days('2022-08-30',18)
# EDU1:PERIYARUNIVERSITYSALEM:AdditionalinformationrequiredAdditionalsupportingdocumentsRequireProvisionalDegreeCertificateEMP2:OGILVYMATHERPRIVATELIMITED:AdditionalinformationrequiredHRcontactdetailsEmailidandContactnumberrequired
#'EDU1,EMP2,EDU1,EMP2'

# COMMAND ----------

insuff_nonull = insuff_df.withColumn("INSUFF_REMARKS2", when(insuff_df.INSUFF_REMARKS2.isNull(), "-").otherwise(insuff_df.INSUFF_REMARKS2))


# COMMAND ----------

# DBTITLE 1,Insuff Component Column
insuff_new = insuff_nonull.withColumn("INSUFF_COMPONENTS", func_insuff_udf(upper(insuff_nonull['INSUFF_REMARKS2'])))
# print(str(insuff_new.count()))
display(insuff_new)

# COMMAND ----------

# bgv_jc_insuff_components
# bgv_jc_holiday

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_holiday_list')):
    holiday_df = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_joined_candidate_holiday_list")
else:
    print("table doesn't exist")
holiday_df=leadtrailremove(holiday_df)  

# COMMAND ----------

from pyspark.sql.functions import col,when
holiday_df=holiday_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in holiday_df.columns])
holiday_df = holiday_df.dropna("all")


# COMMAND ----------

holidayList1=holiday_df.select('DATE').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

print(holidayList1)

# COMMAND ----------

# DBTITLE 1,count no. of working days between 2 dates
import datetime
import random
import numpy as np
def get_count_working_days(sdate,edate,label=holidayList1):
    holidayList = holidayList1
    biz_days_count = np.busday_count(sdate,edate,weekmask=[1,1,1,1,1,0,0],holidays=holidayList)
    print(biz_days_count)
    return int(biz_days_count)
# testing the function
# get_count_working_days(['2023-11-14'],['2023-12-14'])
get_count_working_days_udf = udf(get_count_working_days,  IntegerType())

# COMMAND ----------

# DBTITLE 1,Function for calc due date
# import datetime
# import random
# import numpy as np
# import pandas as pd
# def get_due_date_working_days(sdate,daysToBeAdded,label=holidayList1):
#     holidayList = holidayList1
#     workingday_date = np.busday_offset(sdate,daysToBeAdded,roll="modifiedfollowing",weekmask='1111100',holidays=holidayList)
#     ts = pd.to_datetime(workingday_date)
#     print(ts.date())
#     return ts.date()
# # testing the function
# # get_due_date_working_days('2023-11-14',18)
# get_due_date_working_days_udf = udf(get_due_date_working_days, DateType())

# COMMAND ----------

# DBTITLE 1,Function for calc due date
# import numpy as np
# import pandas as pd
def get_DUE_DATE_working_days(sdate,daysToBeAdded,roll="modifiedfollowing",weekmask='1111100',holidays=holidayList1):
    ts = pd.to_datetime(sdate)
    offset = pd.offsets.CustomBusinessDay(weekmask=weekmask,holidays=holidays)
    # due_date=ts+pd.offsets.BDay(daysToBeAdded,weekmask=weekmask,holidays=holidays)
    due_date = ts + offset * daysToBeAdded
    return due_date.date()
# testing the function    
# get_DUE_DATE_working_days('2022-01-25',18)
get_DUE_DATE_working_days_udf = udf(get_DUE_DATE_working_days, DateType())   
get_DUE_DATE_working_days_udf     

# COMMAND ----------

get_DUE_DATE_working_days('2022-08-30',18)

# COMMAND ----------

print(holidayList1)

# COMMAND ----------

# DBTITLE 1,Replacing NULL DOI_New with 1900-01-01
insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("DOI_NEW") == " ") | (col("DOI_NEW") == "0-Jan-00") | (col("DOI_NEW").isNull()) | (col("DOI_NEW") == "") | (col("DOI_NEW") == "#N/A")| (col("DOI_NEW") == "-")),"1900-01-01").otherwise(col("DOI_NEW")))

insuff_new = insuff_new.withColumn("DOI_NEW2",changeDateFormat(col("DOI_NEW2")))


# COMMAND ----------

# DBTITLE 1,calling function  to calculate Due_Date

#DOI_previous -->  DOI_New need confirmation
daysToBeAdded = 18
calendardf = insuff_new.withColumn("DaysToBeAdded",lit(daysToBeAdded).cast(IntegerType()))

calendardf = calendardf.withColumn("Due_Date_n_temp", get_DUE_DATE_working_days_udf(calendardf['DOI_NEW2'],calendardf['DaysToBeAdded']))


# COMMAND ----------

calendardf = calendardf.withColumn("DUE_DATE",
                                         when((col("DOI_NEW2") <= "1901-01-25")| (col("DOI_NEW2").isNull()), "-")
                                        .otherwise(col("Due_Date_n_temp")))
# calendardf.select('DUE_DATE','Due_Date_n_temp','DOI_NEW2').display()

# COMMAND ----------

calendardf = calendardf.withColumn("OFFER_RELEASE_DATE2", when(((col("OFFER_RELEASE_DATE") == " ") | (col("OFFER_RELEASE_DATE") == "0-Jan-00") | (col("OFFER_RELEASE_DATE").isNull()) | (col("OFFER_RELEASE_DATE") == "") | (col("OFFER_RELEASE_DATE") == "#N/A") | (col("OFFER_RELEASE_DATE") == "-")),"1900-01-01").otherwise(col("OFFER_RELEASE_DATE")))
                     
calendardf = calendardf.withColumn("OFFER_RELEASE_DATE2",changeDateFormat(col("OFFER_RELEASE_DATE2")))

display(calendardf.select('OFFER_RELEASE_DATE',"OFFER_RELEASE_DATE2").distinct().orderBy('OFFER_RELEASE_DATE'))

# COMMAND ----------

# DBTITLE 1,Ageing from DOO
# Ageing_from_DOO
# Difference between Offer Release Date and CurrentDate excluding HOLIDAYS , Sat & Sun
calendardf = calendardf.withColumn("Ageing_from_DOO_temp", get_count_working_days_udf(calendardf['OFFER_RELEASE_DATE2'],current_date()))
#print(convertedFileDate)
#print(current_date())

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOO",
                                         when((col("Offer_Release_Date2") <= "1901-01-25")| (col("Offer_Release_Date2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOO_temp")))
calendardf.select('DOI_NEW2','OFFER_RELEASE_DATE2','Ageing_from_DOO_temp','AGEING_FROM_DOO').display()

# COMMAND ----------

# DBTITLE 1,Ageing from DOI
#Difference between Prev Report's DOI and CurrentDate excluding Sat & Sun
calendardf= calendardf.withColumn("Ageing_from_DOI_temp", get_count_working_days_udf(calendardf['DOI_NEW2'],current_date()))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOI",
                                         when((col("DOI_NEW2") <= "1901-01-25")| (col("DOI_NEW2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOI_temp")))
# calendardf.select('DOI_NEW2','Ageing_from_DOI_temp','AGEING_FROM_DOI').display()

# COMMAND ----------

# DBTITLE 1,Ageing from DOJ
#Planned Start Date
calendardf = calendardf.withColumn("PLANNED_START_DATE2", when(((col("PLANNED_START_DATE") == " ") | (col("PLANNED_START_DATE") == "0-Jan-00") | (col("PLANNED_START_DATE").isNull()) | (col("PLANNED_START_DATE") == "") | (col("PLANNED_START_DATE") == "#N/A") | (col("PLANNED_START_DATE") == "-")),"1900-01-01").otherwise(col("PLANNED_START_DATE")))
                     
calendardf = calendardf.withColumn("PLANNED_START_DATE2",changeDateFormat(col("PLANNED_START_DATE2")))

# display(calendardf.select('PLANNED_START_DATE',"PLANNED_START_DATE2").distinct())

# COMMAND ----------

#Difference between Planned Start Date and CurrentDate excluding Sat & Sun
calendardf= calendardf.withColumn("Ageing_from_DOJ_temp", get_count_working_days_udf(calendardf['PLANNED_START_DATE2'],current_date()))
# display(calendardf.select('PLANNED_START_DATE2',"Ageing_from_DOJ_temp"))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ",
                                         when((col("PLANNED_START_DATE2") <= "1901-01-25")| (col("PLANNED_START_DATE2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOJ_temp")))
calendardf.select('PLANNED_START_DATE2','Ageing_from_DOJ_temp','Ageing_from_DOJ').display()

# COMMAND ----------

# DBTITLE 1,Ageing from DOJ (Calender Day)
calendardf = calendardf.withColumn("Ageing_from_DOJ_Calender_Day_temp",datediff(current_date(),col("PLANNED_START_DATE2")))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ_CALENDAR_DAY",
                                         when((col("PLANNED_START_DATE2") <= "1901-01-25")| (col("PLANNED_START_DATE2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOJ_Calender_Day_temp")))
#calendardf.select('PLANNED_START_DATE2','Ageing_from_DOJ_Calender_Day_temp','Ageing_from_DOJ_Calendar_Day').display()

# COMMAND ----------

# DBTITLE 1,Ageing Bucket
calendardf=calendardf.withColumn("AGEING_FROM_DOJ", calendardf['AGEING_FROM_DOJ'].cast('int'))

#If Ageing from DOJ,0-30, 31-60, 61-90, 91-120, 121-180 and Greater than 180 df.value < 1) 
calendardf = calendardf.withColumn("AGEING_BUCKET",
       when(calendardf.AGEING_FROM_DOJ > 180, "Greater than 180")
      .when((180 >= calendardf.AGEING_FROM_DOJ)  & (calendardf.AGEING_FROM_DOJ >=121), "121-180")
      .when((121 > calendardf.AGEING_FROM_DOJ) & (calendardf.AGEING_FROM_DOJ >=91), "91-120")
      .when((91 > calendardf.AGEING_FROM_DOJ) & (calendardf.AGEING_FROM_DOJ >=61), "61-90")
      .when((61 > calendardf.AGEING_FROM_DOJ) & (calendardf.AGEING_FROM_DOJ >=31), "31-60")
      .otherwise("0-30"))
#calendardf.select("AGEING_FROM_DOJ","AGEING_BUCKET").display()


# COMMAND ----------

# DBTITLE 1,Ageing Bucket(Calender Day)
calendardf=calendardf.withColumn("AGEING_FROM_DOJ_CALENDAR_DAY", calendardf['AGEING_FROM_DOJ_CALENDAR_DAY'].cast('int'))


#If Ageing from DOJ,0-30, 31-60, 61-90, 91-120, 121-180 and Greater than 180 df.value < 1) 
calendardf = calendardf.withColumn("AGEING_BUCKET_CALENDAR_DAY",
       when(calendardf.AGEING_FROM_DOJ_CALENDAR_DAY > 180, "Greater than 180")
      .when((180 >= calendardf.AGEING_FROM_DOJ_CALENDAR_DAY)  & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=121), "121-180")
      .when((121 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=91), "91-120")
      .when((91 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=61), "61-90")
      .when((61 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=31), "31-60")
      .otherwise("0-30"))
#calendardf.select("AGEING_FROM_DOJ_CALENDAR_DAY","AGEING_BUCKET_CALENDAR_DAY").display()


# COMMAND ----------

# DBTITLE 1,DOJ Month
#Derive only Month & Year (Aug-23)
calendardf = calendardf.withColumn('DOJ_MONTH', date_format(col('PLANNED_START_DATE2'),"MMM-yy"))

# COMMAND ----------

# DBTITLE 1,BGV Exception (Y/N)
#If Final Report Date (BW)< Planned Start Date (J), then 'Yes', else 'No'
bvg_cal_df = calendardf.withColumn("BGV_EXCEPTION_Y_N",
       when((col("FINAL_REPORT_DATE") < col("PLANNED_START_DATE")), "Yes")
      .otherwise("No"))
#display(bvg_cal_df.select("Final_Report_date","Planned_Start_Date","BGV_Exception_Y_N"))

# COMMAND ----------

bvg_cal_df = bvg_cal_df.withColumn('INSUFF_COMPONENTS_regexp', regexp_replace(col('INSUFF_COMPONENTS'), "[^a-zA-Z0-9\\s,]", ""))
# display(bvg_cal_df.select('INSUFF_COMPONENTS_regexp','INSUFF_COMPONENTS'))

# COMMAND ----------

# DBTITLE 1,BGV Check completd along with Waiver(Except CEA and Campus education)
# bvg_cal_df1 = bvg_cal_df.withColumn("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION1",\
#     when((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='yes'), lit('yes'))\
   
#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('completed')) ) & ((lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('closed'))| (lower(bvg_cal_df.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL)=='completed'))  & (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('pending')) , lit('No'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') &(lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing'))  & (lower(bvg_cal_df.INSUFF_COMPONENTS).contains('edu')), lit('Yes'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (~lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing')) , lit('No'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('wip'))| (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('report pending')), lit('No'))\

#     .when(( ((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)=='left')) |(lower(bvg_cal_df.STATUS)!='joined')) & ((bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS.isNull()) | (bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS == '-' )), lit('-'))\

#     .otherwise(lit("-")) )

bvg_cal_df = bvg_cal_df.withColumn("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION",\
    when((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (trim(lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS))=='yes'), lit('Yes'))\

    .when((lower(calendardf.STATUS)=='joined') & (lower(calendardf.EMPLOYEE_STATUS)=='left') & (lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='yes'), lit('Yes'))\
    .when((lower(calendardf.STATUS)=='joined') & (lower(calendardf.EMPLOYEE_STATUS)=='left') & ((calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS=='-')|
    (lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='no')) , lit('No'))\

   
    .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('completed')) ) & ((lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('closed'))| (lower(bvg_cal_df.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

    .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL)=='completed'))  & (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('pending')) , lit('No'))\

    .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') &(lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing'))  & (lower(bvg_cal_df.INSUFF_COMPONENTS_regexp).contains('edu')) & (length(trim(bvg_cal_df.INSUFF_COMPONENTS_regexp))<=4), lit('Yes'))\

    .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (~lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing')) , lit('No'))\

    .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('wip'))| (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('report pending')), lit('No'))\

    .when(( ((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)=='left')) |(lower(bvg_cal_df.STATUS)!='joined')) & ((bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS.isNull()) | (bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS == '-' )), lit('-'))\

    .otherwise(lit("-")) )

# COMMAND ----------

# DBTITLE 1,Previous Yes
#If BGV Check completd along with Waiver(Except CEA and Campus education) = 'Yes', then 'Yes', else '-'
# bvg_cal_df=bvg_cal_df.withColumn("PREVIOUS_YES", when((lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'yes') & (bvg_cal_df.OLD_OR_NEW_DATA == 'OLD'), lit("Yes")).otherwise(lit('-')) )

bvg_cal_df=bvg_cal_df.withColumn("PREVIOUS_YES",
   when((lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS) == 'yes') ,lit("Yes"))\
   .otherwise(lit('-')) )

display(bvg_cal_df.filter(bvg_cal_df.EMPLOYEE_ID.isin('145311','145273','144616','145027')))

# COMMAND ----------

# DBTITLE 1,Suspicious Company New
# BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education = = 'No' & BU='Consulting' & Suspicious_Company_previous<> ='-' OR !=null, then AK,  
# BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education = = 'No' & BU='Consulting' & Suspicious_Company_previous=='-'or == null, then AK
# else PS_EDU_EMP_Name__Suspicious_
#SUSPICIOUS_COMPANY_NEW
bvg_cal_df=bvg_cal_df.withColumn('SUSPICIOUS_COMPANY_NEW', when( (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(bvg_cal_df.BU).like('%consulting%')) & (bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS != '-') & bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS.isNotNull() , bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS)\
.when( (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(bvg_cal_df.BU).like('%consulting%')) & (bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS == '-') & bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS.isNull() , bvg_cal_df.PS_EDU_EMP_NAME__SUSPICIOUS_ )\
.otherwise(lit('-')))

# COMMAND ----------

# DBTITLE 1,Responsibility
# =IF($BB3="-","-",IF($BB3="In-Active","In-Active",IF(OR($BB3="CEA Pending",$BB3="Insufficiency"),"Colleague",IF(OR($BB3="Report Pending",$BB3="WIP"),"KI Forensic",IF(AND($BB3="Waiver Pending",OR($BV3="Under Review - HRBP",$BV3="Approval Awaited-HRBP/TA Head")),"HRBP Partner",
# IF(AND($BB3="Waiver Pending",$BV3="Approval Awaited-Risk"),"Risk Partner",IF(AND($BB3="Waiver Pending",$BV3="Clarification awaited from candidate"),"Colleague",IF(AND($BB3="Waiver Pending",$BV3="Approval to be secured"),"KGS",IF($BB3="Waiver Pending","KGS")))))))))

# if BGV_Status_Self = '-' then '-' 
# if BGV_Status_Self = 'in-active' then 'in-active'
# if BGV_Status_Self ='CEA Pending' or Insufficiency' then 'Colleague'
# if BGV_Status_Self='Report Pending' or 'WIP' then 'KI Forensic'
# if BGV_Status_Self = 'Wavier Pending' and (Wavier_Remarks = 'Under Review - HRBP' or Wavier_Remarks="Approval Awaited-HRBP/TA Head") then 'HRBP Partner'
# if BGV_Status_Self = 'Wavier Pending' and Wavier_Remarks = 'Approval Awaited-Risk' then 'Risk Partner
# if BGV_Status_Self ='Waiver Pending' and Waiver_Remarks =  'Clarification awaited from ccandidate' then 'Colleague'
# if BGV_Status_Self ='Waiver Pending' and waiver_remarks = 'Approval to be secured' then 'KGS'
# if BGV_Status_Self ='Waiver Pending' the 'KGS'

# BGV_STATUS_SELF ,  Waiver_Remarks
bvg_cal_df = bvg_cal_df.withColumn("RESPONSIBILITY",
    when(bvg_cal_df.BGV_STATUS_SELF == '-', "-")\
    .when(lower(bvg_cal_df.BGV_STATUS_SELF).contains('in-active'),lit('In-Active'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('cea pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('insuff')),lit('Colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('report pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('wip')),lit('KI Forensic'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&((lower(bvg_cal_df.WAIVER_REMARKS).contains('under review - hrbp')) |(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-hrbp/ta head'))) ,lit('HRBP Partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-risk')),lit('Risk Partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('clarification awaited from candidate')),lit('Colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval to be secured')),lit('KGS'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending')),lit('KGS'))\
    .otherwise(lit('-')))

# COMMAND ----------

#joining client check data
# bvg_cal_df1= bvg_cal_df.join(df_client_check,(bvg_cal_df.Candidate_Email == df_client_check.client_check_Case_Reference_number) | client_check_Case_Reference_number,"left")

# bvg_cal_df = bvg_cal_df.join(
#     df_client_check, 
#     (bvg_cal_df.BGV_CASE_REFERENCE_NUMBER == df_client_check.CLIENT_CHECK_CASE_REFERENCE_NUMBER) | (bvg_cal_df.CANDIDATE_EMAIL == df_client_check.CLIENT_CHECK_EMAIL_ID), 
#     'left'
# ).select(
#     bvg_cal_df['*'], 
#     df_client_check.CLIENT_CHECK_CASE_REFERENCE_NUMBER,df_client_check.CLIENT_CHECK_EMAIL_ID)
# print(bvg_cal_df.count())

bvg_cal_df = bvg_cal_df.join(
    df_client_check, 
    ((bvg_cal_df.BGV_CASE_REFERENCE_NUMBER == df_client_check.CLIENT_CHECK_CASE_REFERENCE_NUMBER )), 
    'left'
).select(
    bvg_cal_df['*'], 
    df_client_check.CLIENT_CHECK_CASE_REFERENCE_NUMBER, df_client_check.CLIENT_CHECK_EMAIL_ID)
print(bvg_cal_df.count())

# COMMAND ----------

bvg_cal_df = bvg_cal_df.join(
    df_client_check2, 
    ((bvg_cal_df.CANDIDATE_EMAIL == df_client_check2.CLIENT_CHECK2_EMAIL_ID)), 
    'left'
).select(
    bvg_cal_df['*'], 
    df_client_check2.CLIENT_CHECK2_EMAIL_ID)
print(bvg_cal_df.count())


# COMMAND ----------

# Column name: Add_Checks  LOGIC REQUIRED FOR THIS COLUMN
# Status = 'Joined' and  Employee_Status != 'Left' or ='-' and lookup "candidate ref num" or "email id" with client check data fetch all then Add_Checks  = 'Yes'
# else Add_Checks   ='-'
#df_client_check
bvg_cal_df =  bvg_cal_df.withColumn('ADD_CHECKS',when((lower(bvg_cal_df.STATUS)=='joined')& (bvg_cal_df.CLIENT_CHECK_CASE_REFERENCE_NUMBER.isNotNull() | (bvg_cal_df.CLIENT_CHECK_CASE_REFERENCE_NUMBER!='-') | bvg_cal_df.CLIENT_CHECK_EMAIL_ID.isNotNull() | (bvg_cal_df.CLIENT_CHECK_EMAIL_ID!='-') | bvg_cal_df.CLIENT_CHECK2_EMAIL_ID.isNotNull() | (bvg_cal_df.CLIENT_CHECK2_EMAIL_ID!='-')), lit('Yes')).otherwise(lit('-')))
print(bvg_cal_df.count())


# COMMAND ----------

display(bvg_cal_df.filter(bvg_cal_df.CANDIDATE_EMAIL=='shankss91@gmail.com'))

# COMMAND ----------

# 1)Add_Checks = 'yes' and ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='No' and "BGV Check completd along with Waiver(Except CEA and Campus education)" = 'yes' then ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes'
bvg_cal_df=bvg_cal_df.withColumn('ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER',when( (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') &  (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION)=='yes') & (lower(bvg_cal_df.ADD_CHECKS)=='yes'),lit('Yes')).otherwise(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER))

# COMMAND ----------


#BGV Status (BB) BGV_STATUS_SELF
bvg_cal_df =bvg_cal_df.withColumn('BGV_STATUS_SELF',\
    when( (lower(bvg_cal_df.STATUS)!='joined') | ( (lower(bvg_cal_df.EMPLOYEE_STATUS) == 'left') | (bvg_cal_df.EMPLOYEE_STATUS !='-') ), lit("In-Active"))\
    .when( lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='yes', lit('-'))\
    .when(  (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(bvg_cal_df.BGV_STATUS_FINAL)=='cea'), lit('CEA Pending') )\
    .when( (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(bvg_cal_df.BGV_STATUS_FINAL)=='completed') & (lower(bvg_cal_df.WAIVER_CLOSED_NEW).like('%pending%')), lit('Waiver Pending') )\
    .when(  (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(bvg_cal_df.BGV_STATUS_FINAL).like('%insuf%')), lit('Insufficiency') )\
    .when(  (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(bvg_cal_df.BGV_STATUS_FINAL).like('%report pending%')), lit('Report Pending') )\
    .when(  (lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER)=='no') & (lower(bvg_cal_df.BGV_STATUS_FINAL)=='wip'), lit('WIP') )\
    .otherwise(lit('-')))
    
#Remarks
bvg_cal_df =bvg_cal_df.withColumn('REMARKS',\
    when(lower(bvg_cal_df.BGV_STATUS_SELF).like('%waiver pending%'), bvg_cal_df.WAIVER_REMARKS)\
    .otherwise(lit('-'))
    )
#Responsibility
bvg_cal_df = bvg_cal_df.withColumn("RESPONSIBILITY",
    when(bvg_cal_df.BGV_STATUS_SELF == '-', "-")\
    .when(lower(bvg_cal_df.BGV_STATUS_SELF).contains('in-active'),lit('In-Active'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('cea pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('insuff')),lit('Colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('report pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('wip')),lit('KI Forensic'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&((lower(bvg_cal_df.WAIVER_REMARKS).contains('under review - hrbp')) |(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-hrbp/ta head'))) ,lit('HRBP Partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-risk')),lit('Risk Partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('clarification awaited from candidate')),lit('Colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval to be secured')),lit('KGS'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending')),lit('KGS'))\
    .otherwise(lit('-')))

#BGV Completion Date New
bvg_cal_df =bvg_cal_df.withColumn('BGV_COMPLETION_DATE_NEW',\
    when((lower(bvg_cal_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER).like('%yes%')) & (lower(bvg_cal_df.BU).like('%tax%')) & ((bvg_cal_df.BGV_COMPLETION_DATE_PREVIOUS!='-') | bvg_cal_df.BGV_COMPLETION_DATE_PREVIOUS.isNotNull()), bvg_cal_df.BGV_COMPLETION_DATE_PREVIOUS )\

    .when(lower(bvg_cal_df.BGV_STATUS_FINAL).like('%completed%') & lower(bvg_cal_df.OVER_ALL_OUTPUT).like('%green%') , bvg_cal_df.LAST_GREEN_COMPLETE_DATE)\

    .when((lower(bvg_cal_df.BGV_STATUS_FINAL).like('%completed%')) & (~lower(bvg_cal_df.OVER_ALL_OUTPUT).like('%green%')) & ((bvg_cal_df.WAIVER_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD=='-') | bvg_cal_df.WAIVER_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD.isNull()) , lit('-'))\

    .otherwise(lit('-'))
    )


# COMMAND ----------

#Adding S_NO and current timestamp to Dated_On and File_Date
from datetime import *
currentdatetime= datetime.now()
bvg_cal_df = bvg_cal_df.withColumn("Dated_On",lit(currentdatetime)).withColumn("File_Date", lit(FileDate2))
bvg_cal_df =bvg_cal_df.withColumn("S_NO",row_number().over(Window.orderBy(monotonically_increasing_id())))

# COMMAND ----------

display(bvg_cal_df)

# COMMAND ----------

bvg_cal_df=bvg_cal_df.withColumn("PLANNED_START_DATE",date_format(bvg_cal_df.PLANNED_START_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DOI_PREVIOUS",date_format(bvg_cal_df.DOI_PREVIOUS,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("BGV_COMPLETION_DATE_PREVIOUS",date_format(bvg_cal_df.BGV_COMPLETION_DATE_PREVIOUS,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("OFFER_RELEASE_DATE",date_format(bvg_cal_df.OFFER_RELEASE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("OFFER_ACCEPTANCE_DATE",date_format(bvg_cal_df.OFFER_ACCEPTANCE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DOI_NEW",date_format(bvg_cal_df.DOI_NEW,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DUE_DATE",date_format(bvg_cal_df.DUE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("BGV_COMPLETION_DATE_NEW",date_format(bvg_cal_df.BGV_COMPLETION_DATE_NEW,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("LAST_GREEN_COMPLETE_DATE",date_format(bvg_cal_df.LAST_GREEN_COMPLETE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("FINAL_REPORT_DATE",date_format(bvg_cal_df.FINAL_REPORT_DATE,"dd-MMM-yy"))


# display(bvg_cal_df.select('PLANNED_START_DATE'))
#OFFER_RELEASE_DATE','OFFER_ACCEPTANCE_DATE

# COMMAND ----------

#'S_NO', removed
final_df = bvg_cal_df.select('S_NO','EMPLOYEE_ID','BGV_CASE_REFERENCE_NUMBER','CANDIDATE_FULL_NAME','COST_CENTRE','CLIENT_GEOGRAPHY','DESIGNATION','LOCATION','CANDIDATE_EMAIL','PLANNED_START_DATE','RECRUITER','STATUS','EMPLOYEE_CATEGORY','SOURCE','NAME_OF_SUB_SOURCE','BU','OFFICIAL_EMAIL_ID_PREVIOUS','OFFER_RELEASE_DATE','OFFER_ACCEPTANCE_DATE','INDIA_PROJECT_CODE','HRBP_NAME_PREVIOUS','PERFORMANCE_MANAGER_PREVIOUS','SUPERVISOR_NAME_PREVIOUS','DOI_PREVIOUS','BGV_STATUS_FINAL_PREVIOUS','FR_PREVIOUS','SR_PREVIOUS','OVER_ALL_PREVIOUS','MANDATE_CHECKS_COMPLETED','ALL_CHECKS_COMPLETED_PREVIOUS','WAIVER_APPLICABLE_PREVIOUS','WAIVER_CLOSED_PREVIOUS','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS','CANDIDATE_IDENTIFIER','ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS','BGV_COMPLETION_DATE_PREVIOUS','SUSPICIOUS_COMPANY_PREVIOUS','EMPLOYEE_STATUS','OFFICIAL_EMAIL_ID','HRBP_NAME','PERFORMANCE_MANAGER','SUPERVISOR_NAME','INSUFF_REMARKS','INSUFF_COMPONENTS','DOI_NEW','DUE_DATE','AGEING_FROM_DOO','AGEING_FROM_DOI','AGEING_FROM_DOJ','AGEING_BUCKET','DOJ_MONTH','AGEING_FROM_DOJ_CALENDAR_DAY','AGEING_BUCKET_CALENDAR_DAY','BGV_STATUS_SELF','REMARKS','RESPONSIBILITY','BGV_STATUS_PROGRESS_SHEET','FR_SUPPORT_FOR_NEW','SR_SUPPORT_FOR_NEW','BGV_STATUS_FINAL','FR_OUTPUT','SR_OUTPUT','OVER_ALL_OUTPUT','ALL_CHECKS_COMPLETED_NEW','CEA_LATEST_REPORT_COLOR','WAIVER_APPLICABLE_NEW','WAIVER_CLOSED_NEW','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION' ,'ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER','BGV_COMPLETION_DATE_NEW','LAST_GREEN_COMPLETE_DATE','SUSPICIOUS_COMPANY_NEW','BGV_EXCEPTION_Y_N','WAIVER_REMARKS','FINAL_REPORT_DATE','PREVIOUS_YES','ADD_CHECKS','Dated_On','File_Date' )

# COMMAND ----------

from datetime import datetime
#replacing all null values to '-'
final_df=replacenull(final_df)
print(final_df.count())
# final_df=final_df.withColumn('PLANNED_START_DATE',strftime('PLANNED_START_DATE', '%m-%b-%y'))
# final_df=final_df.withColumn('PLANNED_START_DATE',to_date('PLANNED_START_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('DOI_PREVIOUS',to_date('DOI_PREVIOUS', 'dd-MMM-yy'))
# final_df=final_df.withColumn('BGV_COMPLETION_DATE_PREVIOUS',to_date('BGV_COMPLETION_DATE_PREVIOUS', 'dd-MMM-yy'))
# final_df=final_df.withColumn('OFFER_RELEASE_DATE',to_date('OFFER_RELEASE_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('OFFER_ACCEPTANCE_DATE',to_date('OFFER_ACCEPTANCE_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('DOI_NEW',to_date('DOI_NEW', 'dd-MMM-yy'))
# final_df=final_df.withColumn('DUE_DATE',to_date('DUE_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('BGV_COMPLETION_DATE_NEW',to_date('BGV_COMPLETION_DATE_NEW', 'dd-MMM-yy'))
# final_df=final_df.withColumn('LAST_GREEN_COMPLETE_DATE',to_date('LAST_GREEN_COMPLETE_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('FINAL_REPORT_DATE',to_date('FINAL_REPORT_DATE', 'dd-MMM-yy'))
# final_df=final_df.withColumn('File_Date',to_date('File_Date', 'dd-MMM-yy'))


# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration/

# COMMAND ----------

# tableName = "fte_fts_latest"
# processName = "bgv_joined_candidate"

# COMMAND ----------

print("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)
print(trusted_stg_savepath_url)

# COMMAND ----------

# DBTITLE 1, Loading trusted stg table
# final_df.write \
# .mode("overwrite") \
# .format("delta") \
# .option("overwriteSchema","true") \
# .option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
# .option("compression","snappy") \
# .saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

print("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)
print("kgsonedatadb.trusted_"+ processName + "_" + tableName)
print(trusted_stg_savepath_url+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Loading Trusted Curr and Hist Tables
# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})