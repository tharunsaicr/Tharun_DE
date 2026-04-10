# Databricks notebook source
dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")
print(FileDate)

# COMMAND ----------

tableName = "ki_loaned"
processName = "bgv_joined_candidate"

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Import Statement
from pyspark.sql.functions import *
# regexp_replace,col, trim, lower,upper,lower,lit,when,split, isnull, concat, current_date,trim,from_unixtime, unix_timestamp,to_date,expr,date_format,udf,monotonically_increasing_id,row_number,size,current_date,datediff
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
#from pyspark.sql.functions import col, trim, lower
from datetime import datetime
import random
import numpy as np
import pandas as pd
from datetime import datetime

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------


fileYear = FileDate[:4]
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
FileDate = datetime.strptime(convertedFileDate, '%Y-%m-%d').date()
print(FileDate)

# COMMAND ----------

# DBTITLE 1,Reading KI Loaned Table
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_bgv_ki_loaned_new_joiners_data')):
    # df_ki_new = spark.sql("select * from (select rank() over(partition by CANDIDATE_EMAIL_DON_T_CHANGE_ order by File_Date desc,Dated_On desc) as rank, * from kgsonedatadb.trusted_hist_bgv_ki_loaned_new_joiners_data where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") where rank = 1")   
    df_ki_new = spark.sql("select EMPLOYEE_ID ,BGV_CASE_REFERENCE_NUMBER, CANDIDATE_FULL_NAME,LOCATION, CANDIDATE_EMAIL_DON_T_CHANGE_ as CANDIDATE_EMAIL,PLANNED_START_DATE,RECRUITER, STATUS, EMPLOYEE_CATEGORY,COST_CENTRE,BU,CLIENT_GEOGRAPHY from kgsonedatadb.trusted_hist_bgv_ki_loaned_new_joiners_data where File_Date=='20240405'")

else:
    print("table doesn't exist")

df_ki_new=df_ki_new.withColumn("OLD_OR_NEW_DATA",lit("New"))

print(df_ki_new.count())
df_ki_new.createOrReplaceTempView("df_ki_new")

df_ki_new=df_ki_new.filter((lower(df_ki_new["STATUS"]).like('joined%'))|(lower(df_ki_new["STATUS"]).like('%mid%join%')))
df_ki_new=df_ki_new.withColumn("STATUS",when((lower(df_ki_new["STATUS"]).like('%mid%join%')),"Joined").otherwise(df_ki_new["STATUS"]))
    


print(df_ki_new.count())

# COMMAND ----------

# column_list=['EMPLOYEE_ID','BGV_CASE_REFERENCE_NUMBER','DESIGNATION','CANDIDATE_EMAIL']
# for columnName in column_list:
#     df_ki_new=df_ki_new.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
#     df_ki_new=df_ki_new.withColumn(columnName,ascii_udf(columnName))
#     df_ki_new=df_ki_new.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

# DBTITLE 0,Previous JC_ki_loaned
# Replace table name with Original name as and when table is available

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_ki_loaned')):
    df_prev_jc_ki = spark.sql("select * from (select row_number() over(partition by EMPLOYEE_ID,FILE_DATE order by File_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_joined_candidate_ki_loaned where File_Date = (select max(File_Date) from kgsonedatadb.trusted_hist_bgv_joined_candidate_ki_loaned where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+")) hist where row_num = 1")   
else:
    print("table doesn't exist, reading from the empty hist file")



df_prev_jc_ki.createOrReplaceTempView("jc_ki_hist")

df_prev_jc_ki=spark.sql('''select EMPLOYEE_ID as EMPLOYEE_ID_PREVIOUS,CANDIDATE_EMAIL as CANDIDATE_EMAIL_PREVIOUS,BGV_CASE_REFERENCE_NUMBER as BGV_CASE_REFERENCE_NUMBER_PREVIOUS ,PLANNED_START_DATE as PLANNED_START_DATE_PREVIOUS ,CLIENT_GEOGRAPHY as CLIENT_GEOGRAPHY_PREVIOUS,LOCATION as LOCATION_PREVIOUS,DESIGNATION as DESIGNATION_PREVIOUS,OFFICIAL_EMAIL_ID_PREVIOUS,HRBP_NAME_PREVIOUS,PERFORMANCE_MANAGER_PREVIOUS,SUPERVISOR_NAME_PREVIOUS,DOI_PREVIOUS,BGV_STATUS_FINAL_PREVIOUS,FR_PREVIOUS,SR_PREVIOUS,OVER_ALL_PREVIOUS,ALL_CHECKS_COMPLETED_PREVIOUS,WAIVER_APPLICABLE_PREVIOUS,WAIVER_CLOSED_PREVIOUS,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS,BGV_COMPLETION_DATE_PREVIOUS,SUSPICIOUS_COMPANY_PREVIOUS,File_Date from jc_ki_hist''')

# df_prev_jc_ki=spark.sql("""SELECT 
# CASE WHEN Employee_ID is null THEN '-' ELSE Employee_ID END AS EMPLOYEE_ID_PREVIOUS, 
# CASE WHEN Candidate_Email is null THEN '-' ELSE Candidate_Email END AS CANDIDATE_EMAIL_PREVIOUS, 
# CASE WHEN Official_Email_ID is null THEN '-' ELSE Official_Email_ID END AS OFFICIAL_EMAIL_ID_PREVIOUS, 
# CASE WHEN HRBP_Name is null THEN '-' ELSE HRBP_Name END AS HRBP_NAME_PREVIOUS, 
# CASE WHEN Performance_Manager is null THEN '-' ELSE Performance_Manager END AS PERFORMANCE_MANAGER_PREVIOUS, 
# CASE WHEN Supervisor_Name is null THEN '-' ELSE Supervisor_Name END AS SUPERVISOR_NAME_PREVIOUS, 
# CASE WHEN DOI_New is null THEN '-' ELSE DOI_New END AS DOI_PREVIOUS, 
# CASE WHEN BGV_Status_Final is null THEN '-' ELSE BGV_Status_Final END AS BGV_STATUS_FINAL_PREVIOUS, 
# CASE WHEN FR_Output is null THEN '-' ELSE FR_Output END AS FR_PREVIOUS, 
# CASE WHEN SR_Output is null THEN '-' ELSE SR_Output END AS SR_PREVIOUS, 
# CASE WHEN Over_all_Output is null THEN '-' ELSE Over_all_Output END AS OVER_ALL_PREVIOUS, 
# CASE WHEN All_Checks_Completed_New is null THEN '-' ELSE All_Checks_Completed_New END AS ALL_CHECKS_COMPLETED_PREVIOUS,
# CASE WHEN Waiver_Applicable_New is null THEN '-' ELSE Waiver_Applicable_New END AS WAIVER_APPLICABLE_PREVIOUS, 
# CASE WHEN Waiver_Closed_New is null THEN '-' ELSE Waiver_Closed_New END AS WAIVER_CLOSED_PREVIOUS, 
# CASE WHEN BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education is null THEN '-' ELSE BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education END AS BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS, 
# CASE WHEN ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is null THEN '-' ELSE ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER END AS ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS, 
# CASE WHEN BGV_COMPLETION_DATE_NEW is null THEN '-' ELSE BGV_COMPLETION_DATE_NEW END AS BGV_COMPLETION_DATE_PREVIOUS, 
# CASE WHEN Suspicious_Company_New is null THEN '-' ELSE Suspicious_Company_New END AS SUSPICIOUS_COMPANY_PREVIOUS,
# CASE WHEN PREVIOUS_YES is null THEN '-' ELSE PREVIOUS_YES END AS PREVIOUS_YES_PREVIOUS 
# FROM jc_ki_hist""")

#trim the column values
# df_prev_jc_ki=leadtrailremove(df_prev_jc_ki)

# Prefix_columns=['EMPLOYEE_ID','CANDIDATE_EMAIL','BGV_CASE_REFERENCE_NUMBER','PLANNED_START_DATE','CLIENT_GEOGRAPHY','LOCATION','DESIGNATION','File_Date']
# for column in Prefix_columns:
#     df_prev_jc_ki=df_prev_jc_ki.withColumnRenamed(column,'PrevJC_ki_' + column)

print(df_prev_jc_ki.count())


# df_prev_jc_ki = rename_prefix_columns("PrevJC_ki",df_prev_jc_ki)  
#Waiver_Closed_Prev

# COMMAND ----------

column_list=['EMPLOYEE_ID_PREVIOUS','CANDIDATE_EMAIL_PREVIOUS']
for columnName in column_list:
    df_prev_jc_ki=df_prev_jc_ki.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_prev_jc_ki=df_prev_jc_ki.withColumn(columnName,ascii_udf(columnName))
    df_prev_jc_ki=df_prev_jc_ki.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_prev_jc_ki.select('HRBP_NAME_PREVIOUS').where(df_prev_jc_ki.EMPLOYEE_ID_PREVIOUS == '122194'))

# COMMAND ----------

display(df_prev_jc_ki.where(df_prev_jc_ki.OFFICIAL_EMAIL_ID_PREVIOUS.like('%vaibhavnaik%')))

# COMMAND ----------

display(df_prev_jc_ki.groupBy("EMPLOYEE_ID_PREVIOUS").agg(count("EMPLOYEE_ID_PREVIOUS")).filter(count("EMPLOYEE_ID_PREVIOUS")>1))

# COMMAND ----------

# MAGIC %sql
# MAGIC select EMPLOYEE_NUMBER,RM_Employee_Name,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where EMPLOYEE_NUMBER in('96257') and File_Date < 20240406 order by File_Date desc
# MAGIC -- '110951','120488','124234','121228','136440','101275','137000','139483','141326','105360','137031','128491','130105','122191','90142','104885','115568','122077','121969','113913','116207','122005','122208','113188','112217','122194','115132','118131','128994','99118','128450','103166',

# COMMAND ----------

# DBTITLE 1,ED

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_headcount_employee_dump')):
    df_ed = spark.sql("select * from (select row_number() over(partition by Employee_Number order by file_date desc, Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(file_date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")
    # df_ed_current = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump ")
    
else:
    print("table doesn't exist")


#REMOVING THE STATUS FOR TEST DATA. ADD IF REQUIRED ,'Status'
df_ed = df_ed.select('EMAIL_ADDRESS','EMPLOYEE_NUMBER','RM_EMPLOYEE_NAME','PERFORMANCE_MANAGER','SUPERVISOR_NAME','CLIENT_GEOGRAPHY', 'POSITION', 'LOCATION','COST_CENTRE','File_Date')

df_ed = rename_prefix_columns("ED",df_ed) 

#trim the column values
# df_ed=leadtrailremove(df_ed)
# df_ed=df_ed.withColumn('ED_Employee_Number',ascii_udf('ED_Employee_Number'))
# df_ed_current.createOrReplaceTempView('df_ed')
# display(df_ed)
print(df_ed.count())
display(df_ed.where(df_ed.ED_EMPLOYEE_NUMBER=='137000'))

# COMMAND ----------

column_list=['ED_EMAIL_ADDRESS','ED_EMPLOYEE_NUMBER','ED_RM_EMPLOYEE_NAME','ED_PERFORMANCE_MANAGER','ED_SUPERVISOR_NAME','ED_CLIENT_GEOGRAPHY', 'ED_POSITION', 'ED_LOCATION']

for columnName in column_list:
    df_ed=df_ed.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_ed=df_ed.withColumn(columnName,ascii_udf(columnName))
    df_ed=df_ed.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
# display(df_ed)
# display(df_ed.filter(df_ed.ED_EMPLOYEE_NUMBER == '123749'))

# COMMAND ----------

display(df_ed.groupBy("ED_Employee_Number").agg(count("ED_Employee_Number")).filter(count("ED_Employee_Number")>1))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select EMPLOYEE_NUMBER,RM_Employee_Name,File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump where EMPLOYEE_NUMBER ='96257'and File_Date < 20240406 order by File_Date desc

# COMMAND ----------

# DBTITLE 1,TD
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_headcount_termination_dump')):
    df_td = spark.sql("select * from (select row_number() over(partition by Employee_Number order by file_date desc,dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_termination_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(file_date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")) hist where row_num = 1")
    # df_td_current = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_termination_dump")
    
else:
    print("table doesn't exist")


#REMOVING THE STATUS FOR TEST DATA. ADD IF REQUIRED ,'Status'
df_td=df_td.select('EMPLOYEE_NUMBER','EMAIL_ADDRESS','RM_EMPLOYEE_NAME','SUPERVISOR_NAME','POSITION','LOCATION','COST_CENTRE','CLIENT_GEOGRAPHY')


df_td = rename_prefix_columns("TD",df_td)  

#trim the column values
df_td=leadtrailremove(df_td)

print(df_td.count())
display(df_td.where(df_td.TD_EMPLOYEE_NUMBER=='90142'))

# COMMAND ----------

column_list=['TD_EMPLOYEE_NUMBER','TD_EMAIL_ADDRESS','TD_RM_EMPLOYEE_NAME','TD_SUPERVISOR_NAME','TD_POSITION','TD_LOCATION','TD_CLIENT_GEOGRAPHY']
for columnName in column_list:
    df_td=df_td.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_td=df_td.withColumn(columnName,ascii_udf(columnName))
    df_td=df_td.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_td.groupBy("TD_EMPLOYEE_NUMBER").agg(count("TD_EMPLOYEE_NUMBER")).filter(count("TD_EMPLOYEE_NUMBER")>1))

# COMMAND ----------

display(df_td.select(df_td.TD_EMPLOYEE_NUMBER).where(df_td.TD_EMPLOYEE_NUMBER == '106352'))

# COMMAND ----------

# %sql
# select * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where EMPLOYEENUMBER=='106654'

# COMMAND ----------



# COMMAND ----------

# %sql
# select EMPLOYEENUMBER,HRBP_Name,File_Date from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where EMPLOYEENUMBER ='96257'and File_Date < 20240406 order by File_Date desc

# COMMAND ----------

# DBTITLE 1,RD
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_headcount_talent_konnect_resignation_status_report')):
    df_rd = spark.sql("select * from (select row_number() over(partition by EMPLOYEENUMBER order by File_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") where row_num = 1")
    # df_rd = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report")
    
else:
    print("table doesn't exist")

df_rd=df_rd.select('EMPLOYEENUMBER','PERSONAL_EMAIL_ID','HRBP_NAME','PERFORMANCEMANAGERNAME','APPROVED_LWD')

df_rd = rename_prefix_columns("RD",df_rd)

#trim the column values
df_rd=leadtrailremove(df_rd)
df_rd=df_rd.withColumn('RD_EMPLOYEENUMBER',ascii_udf('RD_EMPLOYEENUMBER'))
df_rd.createOrReplaceTempView('df_rd')
print(df_rd.count())
display(df_rd.where(df_rd.RD_EMPLOYEENUMBER=='90142'))
# select('RD_APPROVED_LWD','RD_EMPLOYEENUMBER')


# COMMAND ----------

column_list=['RD_EMPLOYEENUMBER','RD_PERSONAL_EMAIL_ID','RD_HRBP_NAME','RD_PERFORMANCEMANAGERNAME']

for columnName in column_list:
    df_rd=df_rd.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_rd=df_rd.withColumn(columnName,ascii_udf(columnName))
    df_rd=df_rd.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))
display(df_rd.groupBy("RD_EMPLOYEENUMBER").agg(count("RD_EMPLOYEENUMBER")).filter(count("RD_EMPLOYEENUMBER")>1))

# COMMAND ----------

# dateList = ['RD_APPROVED_LWD']
# for columnName in df_rd.columns:
#     if (columnName in dateList):
#         print("1")
#         df_rd = df_rd.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_rd[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_rd[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
       
#         df_rd=df_rd.withColumn(columnName,changeDateFormat(columnName))

# COMMAND ----------

df_rd = df_rd.withColumn("RD_APPROVED_LWD", when(((col("RD_APPROVED_LWD") == " ") | (col("RD_APPROVED_LWD") == "0-Jan-00") | (col("RD_APPROVED_LWD").isNull()) | (col("RD_APPROVED_LWD") == "") | (col("RD_APPROVED_LWD") == "#N/A") | (col("RD_APPROVED_LWD") == "-") ),"1900-01-01").otherwise(col("RD_APPROVED_LWD")))
               
df_rd= df_rd.withColumn("RD_APPROVED_LWD",changeDateFormat(col("RD_APPROVED_LWD")))

# COMMAND ----------

display(df_rd.groupBy("RD_EMPLOYEENUMBER").agg(count("RD_EMPLOYEENUMBER")).filter(count("RD_EMPLOYEENUMBER")>1))

# COMMAND ----------

display(df_rd.select('RD_APPROVED_LWD').where(df_rd.RD_EMPLOYEENUMBER == '106352'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select CASE_INITIATION_DATE from kgsonedatadb.trusted_hist_bgv_progress_sheet where Reference_No_ like '%KPMG%00026091%KGS%2021%'

# COMMAND ----------

# DBTITLE 1,Progress sheet
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_progress_sheet')):
 
    df_progress_sheet = spark.sql("select * from (select row_number() over(partition by trim(Reference_No_) order by File_Date desc,Case_Initiation_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_progress_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+") hist where row_num = 1")
    # df_progress_sheet_current = spark.sql("select * from kgsonedatadb.trusted_bgv_progress_sheet")
#   to_date(File_Date,'yyyyMMdd')

else:
    print("table doesn't exist")

df_progress_sheet = df_progress_sheet.select('OPEN_INSUFF','CASE_INITIATION_DATE','OVER_ALL_STATUS_DROP_DOWN_','Final_Report_Color_Code_Drop_Down_','Supplimentry_Report_Color_Code_Drop_Down_','REFERENCE_NO_','FINAL_REPORT_DATE','SUPPLIMENTRY_REPORT_DATE','EDU_EMP_NAME__SUSPICIOUS_','PERSONAL_EMAIL_ID')


df_progress_sheet = rename_prefix_columns("PS",df_progress_sheet)  
df_progress_sheet=df_progress_sheet.withColumn("PS_OVER_ALL_STATUS_DROP_DOWN_",when(((lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%process%'))| (lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%progress%'))),"Report Pending").otherwise(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"])) 

print(df_progress_sheet.count())

# COMMAND ----------

column_list=['PS_REFERENCE_NO_']
 
for columnName in column_list:
    df_progress_sheet=df_progress_sheet.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,ascii_udf(columnName))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

df_progress_sheet.select('PS_Over_All_Status_Drop_Down_','PS_Final_Report_Color_Code_Drop_Down_','PS_Supplimentry_Report_Color_Code_Drop_Down_','PS_CASE_INITIATION_DATE','PS_FINAL_REPORT_DATE').where(df_progress_sheet.PS_REFERENCE_NO_.like('%KPMG%00026091%KGS%2021%')).display()

# COMMAND ----------

# DBTITLE 1,Progress_Sheet
# if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_progress_sheet')):
#     df_progress_sheet =spark.sql("select * from  kgsonedatadb.trusted_hist_bgv_progress_sheet ")
#     # display(df_progress_sheet)
#     dateList = ['CASE_INITIATION_DATE','FINAL_REPORT_DATE']
#     for columnName in df_progress_sheet.columns:
#         if (columnName in dateList):
#             df_progress_sheet = df_progress_sheet.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(df_progress_sheet[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(df_progress_sheet[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))
                
    
#     df_progress_sheet.createOrReplaceTempView('tmp_ps')
    

#     df_progress_sheet = spark.sql("select 'Open_Insuff','Case_Initiation_Date','Over_All_Status_Drop_Down_','Final_Report_Color_Code_Drop_Down_','Supplimentry_Report_Color_Code_Drop_Down_','Reference_No_','Final_Report_date','Supplimentry_Report_Date','EDU_EMP_Name__Suspicious_','PERSONAL_EMAIL_ID' from (select row_number() over(partition by trim(REFERENCE_NO_) order by file_date desc,Dated_On desc ,  Case_Initiation_Date desc) as row_num, * from tmp_ps where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1") 
    
#     #display(df_progress_sheet)

# else:
#     print("table doesn't exist")

# df_progress_sheet = rename_prefix_columns("PS",df_progress_sheet) 

# print(df_progress_sheet.count())

# COMMAND ----------

display(df_progress_sheet.groupBy("PS_REFERENCE_NO_").agg(count("PS_REFERENCE_NO_")).filter(count("PS_REFERENCE_NO_")>1))

# COMMAND ----------

# DBTITLE 1,Waiver Tracker
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_waiver_tracker')):
    df_waiver_tracker = spark.sql("select * from (select row_number() over(partition by trim(BGV_Ref_No) order by File_Date desc,REPORT_DATE desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_waiver_tracker where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
#     # df_waiver_tracker = spark.sql("select * from kgsonedatadb.trusted_bgv_waiver_tracker")
else:
    print("table doesn't exist")

# df_waiver_tracker = df_waiver_tracker.select('*')
df_waiver_tracker = df_waiver_tracker.select('BGV_REF_NO','CANDIDATE_EMAIL_ID','WAIVER_RECEIVED_DATE___HRBP_TA_HEAD','CASE_REMARKS','CASE_STATUS')

df_waiver_tracker = rename_prefix_columns("Waiver",df_waiver_tracker) 
# #RENAMING WRT PRITAM TEST FILE
# df_waiver_tracker=df_waiver_tracker.withColumnRenamed("Waiver_BGV_Ref_No","Waiver_BGV_Ref_no")


print(df_waiver_tracker.count())


# COMMAND ----------

display(df_waiver_tracker.groupBy("Waiver_BGV_REF_NO").agg(count("Waiver_BGV_REF_NO")).filter(count("Waiver_BGV_REF_NO")>1))

# COMMAND ----------

# DBTITLE 1,Reading all the old records except current week records from KI-Loaned Data
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_ki_loaned')):
    
    df_ki_old = spark.sql("select EMPLOYEE_ID ,BGV_CASE_REFERENCE_NUMBER, CANDIDATE_FULL_NAME,LOCATION, CANDIDATE_EMAIL,PLANNED_START_DATE,RECRUITER, STATUS, EMPLOYEE_CATEGORY,COST_CENTRE,BU,CLIENT_GEOGRAPHY from(select row_number() over(partition by BGV_CASE_REFERENCE_NUMBER order by File_Date desc,PLANNED_START_DATE desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_joined_candidate_ki_loaned where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+") where BGV_CASE_REFERENCE_NUMBER not in (select distinct(BGV_CASE_REFERENCE_NUMBER) from df_ki_new) and row_num = 1")
   
#  BGV_CASE_REFERENCE_NUMBER not in (select distinct(BGV_CASE_REFERENCE_NUMBER) from df_ki_new) 
else:
    print("table doesn't exist, reading from the empty hist file")

df_ki_old=df_ki_old.withColumn("OLD_OR_NEW_DATA",lit("Old"))
df_ki_old.createOrReplaceTempView("df_ki_old")    

print(df_ki_old.count())

df_ki_old=df_ki_old.filter((lower(df_ki_old["STATUS"]).like('joined%'))|(lower(df_ki_old["STATUS"]).like('%mid%join%')))
df_ki_old=df_ki_old.withColumn("STATUS",when((lower(df_ki_old["STATUS"]).like('%mid%join%')),"Joined").otherwise(df_ki_old["STATUS"]))
print(df_ki_old.count())

# COMMAND ----------

column_list=['BGV_CASE_REFERENCE_NUMBER','CANDIDATE_EMAIL']

for columnName in column_list:
    df_ki_old=df_ki_old.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_ki_old=df_ki_old.withColumn(columnName,ascii_udf(columnName))
    df_ki_old=df_ki_old.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))


# COMMAND ----------

df_ki_old.select('EMPLOYEE_ID').where(df_ki_old.CANDIDATE_EMAIL.like('%priyankakumari20%')).display()

# COMMAND ----------

display(df_ki_old.groupBy("BGV_CASE_REFERENCE_NUMBER").agg(count("BGV_CASE_REFERENCE_NUMBER")).filter(count("BGV_CASE_REFERENCE_NUMBER")>1))

# COMMAND ----------

# DBTITLE 1,Extracting only the records which is present in the depedent IP tables
# df_ki=spark.sql("""select * from df_ki 
# where  
# Employee_ID in (select distinct(ED_Employee_Number) from df_ed_current) or 
# Employee_ID in (select distinct(TD_Employee_Number) from df_td_current) or 
# Employee_ID in (select distinct(RD_EMPLOYEENUMBER) from df_rd_current) or
# BGV_case_reference_Number in (select distinct(PS_Reference_No_) from df_progress_sheet_current) or
# BGV_case_reference_Number in (select distinct(Waiver_BGV_Ref_no) from df_waiver_tracker_current) or
# Candidate_Email in (select distinct(Waiver_CANDIDATE_EMAIL_ID) from df_waiver_tracker_current) 
#                 """)  
# print(df_ki.count())
# display(df_ki)

# COMMAND ----------

# union() 
df_ki = df_ki_new.union(df_ki_old)
print(df_ki.count())
# display(df_ki)

# COMMAND ----------

#replace null values to '-'
from pyspark.sql.functions import col, trim, lower
df_ki=replacenull(df_ki)
df_prev_jc_ki=replacenull(df_prev_jc_ki)


# COMMAND ----------

#joining Prev JC ki 
# df_kii = df_ki.join(df_prev_jc_ki,df_ki.CANDIDATE_EMAIL == df_prev_jc_ki.CANDIDATE_EMAIL_PREVIOUS,'left')
df_kii = df_ki.join(df_prev_jc_ki,df_ki.EMPLOYEE_ID == df_prev_jc_ki.EMPLOYEE_ID_PREVIOUS,'left')
print(str(df_kii.count()))
# display(df_kii.select(df_ki.EMPLOYEE_ID,df_prev_jc_ki.EMPLOYEE_ID_PREVIOUS).where(df_kii.OFFICIAL_EMAIL_ID_PREVIOUS.like('%vaibhavnaik%')))
# display(df_kii.select('CANDIDATE_EMAIL'))

# COMMAND ----------

# Source -> all as Lateral, Offer Release Date, Offer Acceptance Date, India Project Code, Candidate Identifier as '-', Mandate Checks Completed as 'Yes'
df_kii = df_kii.withColumn('Source',lit('Lateral')) \
    .withColumn('NAME_OF_SUB_SOURCE',lit('-'))\
    .withColumn('OFFER_RELEASE_DATE',lit('-'))\
    .withColumn('OFFER_ACCEPTANCE_DATE',lit('-'))\
    .withColumn('INDIA_PROJECT_CODE',lit('-'))\
    .withColumn('MANDATE_CHECKS_COMPLETED',lit('Yes'))\
    .withColumn('CANDIDATE_IDENTIFIER',lit('-'))

# COMMAND ----------

# Joining ED,TD,RD

etr_join_df =df_kii.join(df_ed,df_kii.EMPLOYEE_ID == df_ed.ED_EMPLOYEE_NUMBER ,"left").join(df_td,df_kii.EMPLOYEE_ID == df_td.TD_EMPLOYEE_NUMBER ,"left").join(df_rd,df_kii.EMPLOYEE_ID == df_rd.RD_EMPLOYEENUMBER ,"left")
print(str(etr_join_df.count()))
display(etr_join_df.where(etr_join_df.OFFICIAL_EMAIL_ID_PREVIOUS.like('%vaibhavnaik%')))

# COMMAND ----------

# DBTITLE 1,Four columns condition
etr_join_df=etr_join_df.withColumn('LOCATION',
        when(((etr_join_df.ED_LOCATION.isNotNull())  | (etr_join_df.ED_LOCATION!='-') | (etr_join_df.ED_LOCATION!='')),etr_join_df.ED_LOCATION)
        .when(((etr_join_df.ED_LOCATION.isNull())  | (etr_join_df.ED_POSITION=='-') | (etr_join_df.ED_POSITION=='')),etr_join_df.TD_LOCATION)
        .otherwise(df_ki.LOCATION))

# COMMAND ----------

etr_join_df=etr_join_df.withColumn('DESIGNATION', 
                                   when((etr_join_df.ED_POSITION.isNotNull()),split(col("ED_POSITION"),'\\.'))
                                   .when((etr_join_df.ED_POSITION.isNull()),split(col("TD_POSITION"),'\\.'))
                                   .otherwise(lit(None)))

# display(etr_join_df.select('DESIGNATION').distinct())

etr_join_df=etr_join_df.withColumn('DESIGNATION', 
                                   when((etr_join_df.ED_POSITION.isNotNull()),split(col("ED_POSITION"),'\\.')[size(col("DESIGNATION")) - 1])
                                   .when((etr_join_df.ED_POSITION.isNull()),split(col("TD_POSITION"),'\\.')[size(col("DESIGNATION")) - 1])
                                   .otherwise(lit(None)))

# display(etr_join_df.select('DESIGNATION').distinct())

# COMMAND ----------

#Official_Email_Id

etr_join_df = etr_join_df.withColumn("OFFICIAL_EMAIL_ID",\
     when(((etr_join_df.ED_EMAIL_ADDRESS.isNotNull()) | (etr_join_df.ED_EMAIL_ADDRESS !='-') | (etr_join_df.ED_EMAIL_ADDRESS !='')),etr_join_df.ED_EMAIL_ADDRESS)\
    .when(((etr_join_df.ED_EMAIL_ADDRESS.isNull()) | (etr_join_df.ED_EMAIL_ADDRESS =='-') | (etr_join_df.ED_EMAIL_ADDRESS =='') & (etr_join_df.OFFICIAL_EMAIL_ID_PREVIOUS.isNotNull())) ,etr_join_df.OFFICIAL_EMAIL_ID_PREVIOUS)\
    .otherwise(lit(None)))

# ecp_join_df = ecp_join_df.withColumn("OFFICIAL_EMAIL_ID",\
#     when(ecp_join_df.ED_EMAIL.isNotNull(),ecp_join_df.ED_EMAIL)\
#     .when((ecp_join_df.ED_EMAIL.isNull() & ecp_join_df.PrevJC_ki_OFFICIAL_EMAIL_ID_HISTORY.isNotNull()) ,ecp_join_df.PrevJC_ki_OFFICIAL_EMAIL_ID_HISTORY)\
#     .otherwise(lit(None)))

display(etr_join_df.where(etr_join_df.OFFICIAL_EMAIL_ID.like('%vaibhavnaik%')))

# COMMAND ----------

#Employee_Status
# etr_join_df
# etr_join_df = etr_join_df.withColumn("EMPLOYEE_STATUS", \
#     when((lower(etr_join_df.STATUS)=='joined') & (etr_join_df.TD_Employee_Number.isNotNull()| etr_join_df.RD_EMPLOYEENUMBER.isNotNull()| (etr_join_df.TD_Employee_Number != '-' )|(etr_join_df.RD_EMPLOYEENUMBER != '-')),lit('Left'))\
#     .otherwise(lit('-')))
#Employee_Status

# etr_join_df = etr_join_df.withColumn("EMPLOYEE_STATUS", \
#     when(((etr_join_df.TD_Employee_Number.isNotNull()) | (etr_join_df.TD_Employee_Number != '-' ) | (etr_join_df.RD_EMPLOYEENUMBER != '-') | (etr_join_df.RD_EMPLOYEENUMBER.isNotNull())),lit('Left'))\
#     .otherwise(lit('-')))


etr_join_df = etr_join_df.withColumn("EMPLOYEE_STATUS", \
    when((etr_join_df.ED_EMPLOYEE_NUMBER.isNotNull()),lit('-'))
    .when(( etr_join_df.TD_EMPLOYEE_NUMBER.isNotNull()| (etr_join_df.TD_EMPLOYEE_NUMBER != '-' )|  ( ((etr_join_df.RD_EMPLOYEENUMBER != '-') | etr_join_df.RD_EMPLOYEENUMBER.isNotNull()) & ( (etr_join_df.RD_APPROVED_LWD.isNotNull() )& (etr_join_df.RD_APPROVED_LWD!= '1900-01-01' ) & (etr_join_df.RD_APPROVED_LWD < current_date())) )   ),lit('Left'))\
    .otherwise(lit('-')))

# & (etr_join_df.RD_APPROVED_LWD.isNotNull()))


# COMMAND ----------

display(etr_join_df.select('EMPLOYEE_STATUS').where(etr_join_df.EMPLOYEE_ID == '106654'))

# COMMAND ----------

#HRBP_Name
# etr_join_df =etr_join_df.withColumn("HRBP_NAME", \
#     when(etr_join_df.ED_RM_Employee_Name.isNotNull(),etr_join_df.ED_RM_Employee_Name)\
#     .when(etr_join_df.TD_RM_Employee_Name.isNotNull(),etr_join_df.TD_RM_Employee_Name)\
#     .when(etr_join_df.RD_HRBP_Name.isNotNull(),etr_join_df.RD_HRBP_Name)\
#     .when(((etr_join_df.RD_HRBP_Name.isNull()) | (etr_join_df.RD_HRBP_Name=='-') | (etr_join_df.RD_HRBP_Name=='')) & ((etr_join_df.ED_RM_Employee_Name.isNull()) | (etr_join_df.ED_RM_Employee_Name=='-') | (etr_join_df.ED_RM_Employee_Name=='')) & ((etr_join_df.TD_RM_Employee_Name.isNull() | (etr_join_df.TD_RM_Employee_Name=='-') | (etr_join_df.TD_RM_Employee_Name=='')) & etr_join_df.HRBP_NAME_PREVIOUS.isNotNull()), etr_join_df.HRBP_NAME_PREVIOUS)\
#     .otherwise(lit(etr_join_df.HRBP_NAME_PREVIOUS)))

etr_join_df =etr_join_df.withColumn("HRBP_NAME",
                                   
    when(etr_join_df.ED_RM_EMPLOYEE_NAME.isNotNull(),etr_join_df.ED_RM_EMPLOYEE_NAME)\
    .when(etr_join_df.TD_RM_EMPLOYEE_NAME.isNotNull(),etr_join_df.TD_RM_EMPLOYEE_NAME)\
    .when(etr_join_df.RD_HRBP_NAME.isNotNull(),etr_join_df.RD_HRBP_NAME)\
    .when(etr_join_df.HRBP_NAME_PREVIOUS.isNotNull(), etr_join_df.HRBP_NAME_PREVIOUS)\
    .otherwise(lit(etr_join_df.HRBP_NAME_PREVIOUS)))
    #  


# COMMAND ----------

display(etr_join_df.select('HRBP_NAME','HRBP_NAME_PREVIOUS').where(etr_join_df.EMPLOYEE_ID == '110951'))

# COMMAND ----------

#Performance_Manager 
etr_join_df =etr_join_df.withColumn("PERFORMANCE_MANAGER", \
    when(etr_join_df.ED_PERFORMANCE_MANAGER.isNotNull(),etr_join_df.ED_PERFORMANCE_MANAGER)\
    .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull(),etr_join_df.TD_SUPERVISOR_NAME)\
    .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull(),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
    .when((etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() & etr_join_df.TD_SUPERVISOR_NAME.isNull() & etr_join_df.ED_PERFORMANCE_MANAGER.isNull() & etr_join_df.PERFORMANCE_MANAGER_PREVIOUS.isNotNull()), etr_join_df.PERFORMANCE_MANAGER_PREVIOUS).otherwise(lit(None)))


# COMMAND ----------

#Supervisor_Name

etr_join_df =etr_join_df.withColumn("SUPERVISOR_NAME", \
    when(etr_join_df.ED_SUPERVISOR_NAME.isNotNull(),etr_join_df.ED_SUPERVISOR_NAME)\
    .when(etr_join_df.TD_SUPERVISOR_NAME.isNotNull(),etr_join_df.TD_SUPERVISOR_NAME)\
    .when(etr_join_df.RD_PERFORMANCEMANAGERNAME.isNotNull(),etr_join_df.RD_PERFORMANCEMANAGERNAME)\
    .when((etr_join_df.RD_PERFORMANCEMANAGERNAME.isNull() & etr_join_df.ED_SUPERVISOR_NAME.isNull() & etr_join_df.TD_SUPERVISOR_NAME.isNull() & etr_join_df.SUPERVISOR_NAME_PREVIOUS.isNotNull()), etr_join_df.SUPERVISOR_NAME_PREVIOUS)\
    .otherwise(lit(None)))

# COMMAND ----------

display(etr_join_df.select("EMPLOYEE_ID","EMPLOYEE_STATUS","OFFICIAL_EMAIL_ID","HRBP_NAME","PERFORMANCE_MANAGER","SUPERVISOR_NAME"))

# COMMAND ----------

1#progress sheet join
PS_join_df= etr_join_df.join(df_progress_sheet,etr_join_df.BGV_CASE_REFERENCE_NUMBER == df_progress_sheet.
PS_REFERENCE_NO_ ,"left")
print(str(PS_join_df.count()))

# COMMAND ----------

#Insuff Remarks ,  Case_Initiation_Date, BGV Status,  FR_Support_for_New, SR_Support_for_New, Final Report date
PS_join_df=PS_join_df.withColumnRenamed("PS_Open_Insuff","INSUFF_REMARKS")\
    .withColumnRenamed("PS_Case_Initiation_Date","DOI_NEW")\
    .withColumnRenamed("PS_Over_All_Status_Drop_Down_","BGV_STATUS_PROGRESS_SHEET")\
    .withColumnRenamed("PS_Final_Report_Color_Code_Drop_Down_","FR_SUPPORT_FOR_NEW")\
    .withColumnRenamed("PS_Supplimentry_Report_Color_Code_Drop_Down_","SR_SUPPORT_FOR_NEW")\
    .withColumnRenamed("PS_Final_Report_date","FINAL_REPORT_DATE")


# COMMAND ----------

PS_join_df = PS_join_df.withColumn("FINAL_REPORT_DATE", when(((col("FINAL_REPORT_DATE") == " ") | (col("FINAL_REPORT_DATE") == "0-Jan-00") | (col("FINAL_REPORT_DATE").isNull()) | (col("FINAL_REPORT_DATE") == "") | (col("FINAL_REPORT_DATE") == "#N/A")) | (col("FINAL_REPORT_DATE") == "-"),"1900-01-01").otherwise(col("FINAL_REPORT_DATE")))
#  | (col("FINAL_REPORT_DATE") == "-")                     
PS_join_df= PS_join_df.withColumn("FINAL_REPORT_DATE",changeDateFormat(col("FINAL_REPORT_DATE")))
display(PS_join_df.select('FINAL_REPORT_DATE'))

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
PS_join_df = PS_join_df.withColumn("LAST_GREEN_COMPLETE_DATE", when(((col("LAST_GREEN_COMPLETE_DATE") == " ") | (col("LAST_GREEN_COMPLETE_DATE") == "0-Jan-00") | (col("LAST_GREEN_COMPLETE_DATE").isNull()) | (col("LAST_GREEN_COMPLETE_DATE") == "") | (col("LAST_GREEN_COMPLETE_DATE") == "#N/A") | (col("LAST_GREEN_COMPLETE_DATE") == "-")),"1900-01-01").otherwise(col("LAST_GREEN_COMPLETE_DATE")))
                     
PS_join_df= PS_join_df.withColumn("LAST_GREEN_COMPLETE_DATE",changeDateFormat(col("LAST_GREEN_COMPLETE_DATE")))
# display(PS_join_df.select('LAST_GREEN_COMPLETE_DATE'))

# COMMAND ----------

PS_join_df = PS_join_df.withColumn("LAST_GREEN_COMPLETE_DATE", when((col("LAST_GREEN_COMPLETE_DATE") == "1900-01-01"), "-").otherwise(col("LAST_GREEN_COMPLETE_DATE")))
PS_join_df = PS_join_df.withColumn("FINAL_REPORT_DATE", when((col("FINAL_REPORT_DATE") == "1900-01-01"), "-").otherwise(col("FINAL_REPORT_DATE")))

# COMMAND ----------

#CEA Latest Report Color
#If SR Output(BJ)<>'-', then SR Output(BJ), else FR Output(BI)

# PS_join_df =PS_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
#     when((PS_join_df.SR_OUTPUT != '-') | (PS_join_df.SR_OUTPUT.isNotNull()), PS_join_df.SR_OUTPUT)\
#     .when((PS_join_df.SR_OUTPUT.isNull() | (PS_join_df.SR_OUTPUT == '-')), PS_join_df.FR_OUTPUT)\
#     .otherwise(lit('-')))



PS_join_df =PS_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
    when((PS_join_df.SR_OUTPUT == '-') | (PS_join_df.SR_OUTPUT.isNull()) | (trim(PS_join_df.SR_OUTPUT) == ''),PS_join_df.FR_OUTPUT).otherwise(PS_join_df.SR_OUTPUT))

# COMMAND ----------

# PS_join_df.select('CEA_LATEST_REPORT_COLOR').where(PS_join_df.EMPLOYEE_ID=='46558').display()

# COMMAND ----------

#Waiver tracker join with reference number
waiver_join_df= PS_join_df.join(df_waiver_tracker,PS_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.
Waiver_BGV_REF_NO , "left")
print(str(waiver_join_df.count()))

# COMMAND ----------

#Waiver Remarks
#Lookup waiver tracker using Case Reference No and fetch Case Status (O)
# waiver_join_df=waiver_join_df.withColumnRenamed("Waiver_CASE_STATUS","WAIVER_REMARKS")

# COMMAND ----------

# DBTITLE 1,Waiver_Remarks
waiver_join_df= waiver_join_df.withColumn("WAIVER_REMARKS",\
    when((waiver_join_df.Waiver_CASE_STATUS.isNotNull()) | (trim(waiver_join_df.Waiver_CASE_STATUS) != '') | (waiver_join_df.Waiver_CASE_STATUS != '-'), waiver_join_df.Waiver_CASE_STATUS)\
    .otherwise(lit('-')))

# COMMAND ----------

display(waiver_join_df.select('WAIVER_REMARKS').where(waiver_join_df.CANDIDATE_EMAIL.like('%shriharinaik%')))

# COMMAND ----------

#BGV Status (final)
#1)when  BGV_Status_Progress_Sheet = 'Completed' & CEA_Latest_Report_Color !='Green' & Waiver_Remarks = 'Re-verification', then 'WIP'
# 2)when  BGV_Status_Progress_Sheet = 'Completed' & CEA_Latest_Report_Color !='Green' & Waiver_Remarks = 'Non-Compliant Report', then 'Report Pending'
# 3)if BGV_Status_Progress_Sheet !null or != - then   BGV_Status_Progress_Sheet
# 4)otherwise 'BGV_Status_Prev'

waiver_join_df= waiver_join_df.withColumn("BGV_STATUS_FINAL",\
    when((waiver_join_df.BGV_STATUS_PROGRESS_SHEET.isNotNull()) | (waiver_join_df.BGV_STATUS_PROGRESS_SHEET != '-'), waiver_join_df.BGV_STATUS_PROGRESS_SHEET)\
    .when((lower(waiver_join_df.BGV_STATUS_PROGRESS_SHEET) == 'completed') & (lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)!='green') & (lower(waiver_join_df.WAIVER_REMARKS)=='re-verification'), lit('WIP'))\
    .when((lower(waiver_join_df.BGV_STATUS_PROGRESS_SHEET) == 'completed') & (lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)!='green') & (lower(waiver_join_df.WAIVER_REMARKS)=='non-compliant report'), lit('Report Pending'))\
    .otherwise(waiver_join_df.BGV_STATUS_FINAL_PREVIOUS)) 

# COMMAND ----------

#Waiver tracker join with email id
# waiver_join_df=waiver_join_df.withColumnRenamed('Waiver_BGV_Ref_no','Waiver_BGV_Ref_no_1').withColumnRenamed('Waiver_CANDIDATE_EMAIL_ID','Waiver_Candidate_Email_ID_1')

# waiver_join_df = waiver_join_df.join(
#     df_waiver_tracker, 
#     (waiver_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.Waiver_BGV_Ref_no) | (waiver_join_df.CANDIDATE_EMAIL == df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID), 
#     'left'
# ).select(
#     waiver_join_df['*'], 
#     df_waiver_tracker.Waiver_BGV_Ref_no.alias('Waiver_BGV_Ref_no_2'),df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID.alias('Waiver_Candidate_Email_ID_2'),df_waiver_tracker.Waiver_CASE_STATUS.alias('Waiver_Case_Status_2')
# )
# print(str(waiver_join_df.count()))

# COMMAND ----------

#If BGV_Status_Final <> 'Completed', then BGV_Status_Final
#If it is 'completed', if len(SR_Output)<>'-', then SR output(BJ), else FR_Output

#Over all Output
waiver_join_df =waiver_join_df.withColumn('OVER_ALL_OUTPUT',\
    when((lower(waiver_join_df.BGV_STATUS_FINAL) != 'completed') , waiver_join_df.BGV_STATUS_FINAL)\
    .when((lower(waiver_join_df.BGV_STATUS_FINAL) == 'completed') & (waiver_join_df.SR_OUTPUT != '-') & waiver_join_df.SR_OUTPUT.isNotNull(), waiver_join_df.SR_OUTPUT)\
    .otherwise(waiver_join_df.FR_OUTPUT))


# COMMAND ----------

#Waiver Applicable New
#If Over_all_Output = 'Green', then 'NA'
#Else if CEA_Latest_Report_Color ='CEA' or 0 or '-', then 'NA'
#Else 'Yes'
waiver_join_df=waiver_join_df.withColumn('WAIVER_APPLICABLE_NEW',when(lower(waiver_join_df.OVER_ALL_OUTPUT)== 'green', lit('NA')).when((lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)=='cea') | (lower(waiver_join_df.CEA_LATEST_REPORT_COLOR)=='0') | (waiver_join_df.CEA_LATEST_REPORT_COLOR =='-' ) | (waiver_join_df.CEA_LATEST_REPORT_COLOR.isNull() ), lit('NA')).otherwise(lit('Yes')))


# COMMAND ----------

# If Waiver_Applicable_New = 'NA', then 'NA',
# Else if PrevJC_ki_Waiver_Closed_Prev='Closed', then 'Closed'
# Else 
# Lookup Waiver Tracker using Waiver_BGV_Ref_no and check if Waiver_Case_Status contains '%Closed%'
# and(or) Lookup waiver tracker using Waiver_Candidate_Email_ID and check if Waiver_Case_Status contains '%Closed%', then 'Closed'
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

# If Status != 'Joined' and Employee_Status ='left' or  != '-', then Inactive.
# If  ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is "Yes", then '-'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'CEA', then "CEA Pending"
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Completed' and WAIVER_CLOSED_NEW is 'Pending', then 'Waiver Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Insufficiency, then 'Insufficiency'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Report Pending' then 'Report Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'WIP', then 'WIP'
# All else '-'


# COMMAND ----------

# If Status == 'Joined' and Employee_Status ='left' or  != '-', then Inactive.
# If  ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is "Yes", then '-'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'CEA', then "CEA Pending"
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Completed' and WAIVER_CLOSED_NEW is 'Pending', then 'Waiver Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Insufficiency, then 'Insufficiency'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'Report Pending' then 'Report Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_Status_Final is 'WIP', then 'WIP'
# All else '-'

#BGV Status (BB) BGV_STATUS_SELF
waiver_join_df =waiver_join_df.withColumn('BGV_STATUS_SELF',\
    when( (lower(waiver_join_df.EMPLOYEE_STATUS).like('%left%')), lit("In-Active"))\
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

# COMMAND ----------

waiver_join_df =waiver_join_df.withColumn("S_NO",row_number().over(Window.orderBy(monotonically_increasing_id())))

# COMMAND ----------

# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes' and BU='Tax', then , if PrevJC_ki_BGV_Completion_Date_PREV <>'-', then 'PrevJC_ki_BGV_Completion_Date_PREV', 
# else iF BGV_Status_Final='completed' & Over_all_Output ='Green', then Last_Green_Complete_Date, 
# else BGV_Status_Final='completed' & Over_all_Output <>'Green' then Waiver_Waiver_Received_Date___HRBP_TA_Head ='-' OR NULL, then '-' 

# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes' and BU='Tax', then and, if PrevJC_ki_BGV_Completion_Date_PREV <>'-', then 'PrevJC_ki_BGV_Completion_Date_PREV', 
# else iF BGV_Status_Final='completed' & Over_all_Output ='Green', then Last_Green_Complete_Date, 
# else BGV_Status_Final='completed' & Over_all_Output <>'Green' then Waiver_Waiver_Received_Date___HRBP_TA_Head (not null or -) on basis of candidate ref num
# '-' OR NULL, then '-' 

#BGV Completion Date New
# waiver_join_df =waiver_join_df.withColumn('BGV_COMPLETION_DATE_NEW',\
#     when((lower(waiver_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER).like('%yes%')) & (lower(waiver_join_df.BU).like('%tax%')) & ((waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS!='-') | waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS.isNotNull()), waiver_join_df.BGV_COMPLETION_DATE_PREVIOUS )\

#     .when(lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%') & lower(waiver_join_df.OVER_ALL_OUTPUT).like('%green%') , waiver_join_df.LAST_GREEN_COMPLETE_DATE)\

#     .when((lower(waiver_join_df.BGV_STATUS_FINAL).like('%completed%')) & (~lower(waiver_join_df.OVER_ALL_OUTPUT).like('%green%')) & ((waiver_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD=='-') | waiver_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD.isNull()) , lit('-'))\

#     .otherwise(lit('-'))
#     )

waiver_join_df =waiver_join_df.withColumn('BGV_COMPLETION_DATE_NEW', lit('-'))
#need to confirm for 1st condition

# COMMAND ----------

# DBTITLE 1,Insuff remarks starts
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_insuff_component')):
    insuff_df = spark.sql("select INSUFF_COMPONENT from kgsonedatadb.trusted_hist_bgv_insuff_component")
else:
    print("table doesn't exist")

insuff_df=leadtrailremove(insuff_df)      

# COMMAND ----------

from pyspark.sql.functions import col,when
insuff_df=insuff_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in insuff_df.columns])
insuff_df = insuff_df.dropna("all")

# COMMAND ----------

insuff_list=insuff_df.select(upper('INSUFF_Component')).rdd.flatMap(lambda x: x).collect()
print(insuff_list)

# COMMAND ----------

insuff_df=waiver_join_df.withColumn("Insuff_Remarks2",regexp_replace(waiver_join_df["Insuff_Remarks"],'[^a-zA-Z0-9:]',''))


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

insuff_nonull = insuff_df.withColumn("Insuff_Rema   rks2", when(insuff_df.Insuff_Remarks2.isNull(), "-").otherwise(insuff_df.Insuff_Remarks2))


# COMMAND ----------

# DBTITLE 1,Insuff Component Column
insuff_new = insuff_nonull.withColumn("INSUFF_COMPONENT", func_insuff_udf(upper(insuff_nonull['Insuff_Remarks2'])))
print(str(insuff_new.count()))

# COMMAND ----------

# DBTITLE 1,Calendar columns
# bgv_jc_insuff_component
# bgv_jc_holiday

# COMMAND ----------

# DBTITLE 1,Holiday list
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

holidayList1=holiday_df.select('Date').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

print(holidayList1)

# COMMAND ----------

# DBTITLE 1,convert in date data type to standard format in Holiday list
# from datetime import datetime
# new_date_list = []
# for date_str in holidayList1:
#     date_obj = datetime.strptime(date_str, '%d-%b-%y')
#     new_date_list.append(date_obj.strftime('%Y-%m-%d'))
# print(new_date_list)
# holidayList1=new_date_list

# COMMAND ----------

# DBTITLE 1,count no. of working days between 2 dates
# import datetime
# import random
# import numpy as np
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
def get_due_date_working_days(sdate,daysToBeAdded,roll="modifiedfollowing",weekmask='1111100',holidays=holidayList1):
    ts = pd.to_datetime(sdate)
    offset = pd.offsets.CustomBusinessDay(weekmask=weekmask,holidays=holidays)
    # due_date=ts+pd.offsets.BDay(daysToBeAdded,weekmask=weekmask,holidays=holidays)
    due_date = ts + offset * daysToBeAdded
    return due_date.date()
# testing the function    
# get_DUE_DATE_working_days('2022-01-25',18)
get_due_date_working_days_udf = udf(get_due_date_working_days, DateType())

# COMMAND ----------

# DBTITLE 1,Replacing NULL DOI_New with 1900-01-01
# insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("DOI_NEW") == " ") | (col("DOI_NEW") == "0-Jan-00") | (col("DOI_NEW").isNull()) | (col("DOI_NEW") == "") | (col("DOI_NEW") == "#N/A")| (col("DOI_NEW") == "-")),"1900-01-01").otherwise(col("DOI_NEW")))

insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("DOI_PREVIOUS") == " ") | (col("DOI_PREVIOUS") == "0-Jan-00") | (col("DOI_PREVIOUS").isNull()) | (col("DOI_PREVIOUS") == "") | (col("DOI_PREVIOUS") == "#N/A") | (col("DOI_PREVIOUS") == "-")),lit("1900-01-01")).otherwise(col("DOI_PREVIOUS")))

insuff_new = insuff_new.withColumn("DOI_NEW2",changeDateFormat(col("DOI_NEW2")))


# COMMAND ----------

# DBTITLE 1,calling function  to calculate Due_Date

#DOI_previous -->  DOI_New need confirmation
daysToBeAdded = 18
calendardf = insuff_new.withColumn("DaysToBeAdded",lit(daysToBeAdded).cast(IntegerType()))
# calendar1.display()
calendardf = calendardf.withColumn("Due_Date_n_temp", get_due_date_working_days_udf(calendardf['DOI_New2'],calendardf['DaysToBeAdded']))


# COMMAND ----------

calendardf = calendardf.withColumn("DUE_DATE",
                                         when((col("DOI_NEW2") <= "1901-01-25")| (col("DOI_NEW2").isNull()), "-")
                                        .otherwise(col("Due_Date_n_temp")))
# calendardf.select('DUE_DATE','Due_Date_n_temp','DOI_NEW2').display()

# COMMAND ----------

calendardf.select('DUE_DATE').where(calendardf.EMPLOYEE_ID=='128145').display()

# COMMAND ----------

calendardf = calendardf.withColumn("Offer_Release_Date2", when(((col("OFFER_RELEASE_DATE") == " ") | (col("OFFER_RELEASE_DATE") == "0-Jan-00") | (col("OFFER_RELEASE_DATE").isNull()) | (col("OFFER_RELEASE_DATE") == "") | (col("OFFER_RELEASE_DATE") == "#N/A") | (col("OFFER_RELEASE_DATE") == "-")),"1900-01-01").otherwise(col("OFFER_RELEASE_DATE")))
                     
calendardf = calendardf.withColumn("Offer_Release_Date2",changeDateFormat(col("Offer_Release_Date2")))

# display(calendardf.select('OFFER_RELEASE_DATE',"Offer_Release_Date2").distinct())

# COMMAND ----------

# DBTITLE 1,Ageing from DOO
# Ageing_from_DOO
# Difference between Offer Release Date and CurrentDate excluding Sat & Sun
calendardf = calendardf.withColumn("Ageing_from_DOO_temp", get_count_working_days_udf(calendardf['Offer_Release_Date2'],current_date()))
# print(convertedFileDate)
#print(current_date())

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOO",
                                         when((col("Offer_Release_Date2") <= "1901-01-25")| (col("Offer_Release_Date2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOO_temp")))
# calendardf.select('DOI_NEW2','Offer_Release_Date2','Ageing_from_DOO_temp','AGEING_FROM_DOO').display()

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
calendardf = calendardf.withColumn("Planned_Start_Date2", when(((col("PLANNED_START_DATE") == " ") | (col("PLANNED_START_DATE") == "0-Jan-00") | (col("PLANNED_START_DATE").isNull()) | (col("PLANNED_START_DATE") == "") | (col("PLANNED_START_DATE") == "#N/A") | (col("PLANNED_START_DATE") == "-")),"1900-01-01").otherwise(col("PLANNED_START_DATE")))
                     
calendardf = calendardf.withColumn("Planned_Start_Date2",changeDateFormat(col("Planned_Start_Date2")))

# display(calendardf.select('PLANNED_START_DATE',"Planned_Start_Date2").distinct())

# COMMAND ----------

#Difference between Planned Start Date and CurrentDate excluding Sat & Sun
calendardf= calendardf.withColumn("Ageing_from_DOJ_temp", get_count_working_days_udf(calendardf['Planned_Start_Date2'],current_date()))
# display(calendardf.select('Planned_Start_Date2',"Ageing_from_DOJ_temp"))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ",
                                         when((col("Planned_Start_Date2") <= "1901-01-25")| (col("Planned_Start_Date2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOJ_temp")))
# calendardf.select('Planned_Start_Date2','Ageing_from_DOJ_temp','AGEING_FROM_DOJ').display()

# COMMAND ----------

# DBTITLE 1,Ageing from DOJ (Calender Day)
calendardf = calendardf.withColumn("Ageing_from_DOJ_Calender_Day_temp",datediff(current_date(),col("Planned_Start_Date2")))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ_CALENDAR_DAY",
                                         when((col("Planned_Start_Date2") <= "1901-01-25")| (col("Planned_Start_Date2").isNull()), "-")
                                        .otherwise(col("Ageing_from_DOJ_Calender_Day_temp")))
# calendardf.select('Planned_Start_Date2','Ageing_from_DOJ_Calender_Day_temp','AGEING_FROM_DOJ_CALENDAR_DAY').display()

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
# calendardf.select("AGEING_FROM_DOJ","AGEING_BUCKET").display()


# COMMAND ----------

# DBTITLE 1,Ageing Bucket(Calender Day)
calendardf=calendardf.withColumn("AGEING_FROM_DOJ_CALENDAR_DAY ", calendardf['AGEING_FROM_DOJ_CALENDAR_DAY'].cast('int'))


#If Ageing from DOJ,0-30, 31-60, 61-90, 91-120, 121-180 and Greater than 180 df.value < 1) 
calendardf = calendardf.withColumn("AGEING_BUCKET_CALENDAR_DAY",
       when(calendardf.AGEING_FROM_DOJ_CALENDAR_DAY > 180, "Greater than 180")
      .when((180 >= calendardf.AGEING_FROM_DOJ_CALENDAR_DAY)  & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=121), "121-180")
      .when((121 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=91), "91-120")
      .when((91 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=61), "61-90")
      .when((61 > calendardf.AGEING_FROM_DOJ_CALENDAR_DAY) & (calendardf.AGEING_FROM_DOJ_CALENDAR_DAY >=31), "31-60")
      .otherwise("0-30"))
# calendardf.select("AGEING_FROM_DOJ_CALENDAR_DAY","AGEING_BUCKET_CALENDAR_DAY").display()


# COMMAND ----------

# DBTITLE 1,DOJ Month
#Derive only Month & Year (Aug-23)
calendardf = calendardf.withColumn('DOJ_MONTH', date_format(col('Planned_Start_Date2'),"MMM-yy"))

# COMMAND ----------

# DBTITLE 1,BGV Exception (Y/N)
#If Final Report Date (BW)< Planned Start Date (J), then 'Yes', else 'No'
# bvg_cal_df = calendardf.withColumn("BGV_EXCEPTION_Y_N",
#        when((col("FINAL_REPORT_DATE") < col("PLANNED_START_DATE")), "Yes")
#       .otherwise("NO"))
# display(bvg_cal_df.select("EMPLOYEE_ID","FINAL_REPORT_DATE","PLANNED_START_DATE","BGV_EXCEPTION_Y_N"))
bvg_cal_df = calendardf.withColumn("BGV_EXCEPTION_Y_N",
       when((calendardf.FINAL_REPORT_DATE=='-') | (calendardf.FINAL_REPORT_DATE=='') |
       (calendardf.FINAL_REPORT_DATE=='0'),lit("No"))                                                
       .when(((calendardf.FINAL_REPORT_DATE) < (calendardf.PLANNED_START_DATE)),lit("Yes"))\
      .otherwise("No"))
# display(bvg_cal_df.select('BGV_EXCEPTION_Y_N'))

# COMMAND ----------

# DBTITLE 1,BGV Check completd along with Waiver(Except CEA and Campus education)
# 1) Status == 'Joined' and  Employee_Status = 'Left' and (BGV_Status_Final == 'CEA' or BGV_Status_Final =='Completed')  and Waiver_Closed_New = 'Closed' then 'Yes'
#  Status == 'Joined' and  Employee_Status = 'Left' and (BGV_Status_Final == 'CEA' or BGV_Status_Final =='Completed')  and Waiver_Closed_New = 'Pending' then 'No'

# 2)  BGV_Status_Final  like 'Insuff' and  Insuff_Remarks contains 'Pursuing' and Insuff_Component contains 'EDU' then 'Yes'

# 3) BGV_Status_Final  like 'Insuff' and  Insuff_Remarks not contains 'Pursuing' then 'No'

# 4) BGV_Status_Final contains 'WIP' then 'Yes'

# 5) otherwise 'No'

# bvg_cal_df = bvg_cal_df.withColumn("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION1",\
#     when(((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)=='left')) |(lower(bvg_cal_df.STATUS)!='joined'), lit('-'))\
#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('completed')) ) & ((lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('closed'))| (lower(bvg_cal_df.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL)=='completed'))  & (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('pending')) , lit('No'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') &(lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing'))  & (lower(bvg_cal_df.INSUFF_COMPONENT).contains('edu')), lit('Yes'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (~lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing')) , lit('No'))\

#     .when((lower(bvg_cal_df.STATUS)=='joined')& (lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('wip'))| (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('report pending')), lit('No'))\
        
#     .otherwise(lit("-")) )



# COMMAND ----------

bvg_cal_df = bvg_cal_df.withColumn('INSUFF_COMPONENT_regexp', regexp_replace(col('INSUFF_COMPONENT'), "[^a-zA-Z0-9\\s,]", ""))
# display(bvg_cal_df.select('INSUFF_COMPONENT_regexp','INSUFF_COMPONENT'))

# COMMAND ----------


bvg_cal_df = bvg_cal_df.withColumn("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION",
    when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='yes'), lit('Yes'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)=='left') & (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='yes'), lit('Yes'))
    
   

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('completed')) ) & ((lower(bvg_cal_df.OVER_ALL_OUTPUT).contains('green')) | (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('closed'))| (lower(bvg_cal_df.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\
    
    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)=='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('completed')) ) & ((lower(bvg_cal_df.OVER_ALL_OUTPUT).contains('green')) | (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('closed'))| (lower(bvg_cal_df.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((lower(bvg_cal_df.BGV_STATUS_FINAL).contains('cea')) | (lower(bvg_cal_df.BGV_STATUS_FINAL)=='completed'))  & (lower(bvg_cal_df.WAIVER_CLOSED_NEW).contains('pending')) , lit('No'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') &(lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing'))  & (lower(bvg_cal_df.INSUFF_COMPONENT_regexp).contains('edu')) & (length(trim(bvg_cal_df.INSUFF_COMPONENT_regexp))<=4), lit('Yes'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('insuf')) & (~lower(bvg_cal_df.INSUFF_REMARKS).contains('pursuing')) , lit('No'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('wip'))| (lower(bvg_cal_df.BGV_STATUS_FINAL).contains('report pending')), lit('No'))\

    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)!='left') & ((bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS=='-')|(lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='no')), lit('No'))\



    .when((lower(bvg_cal_df.EMPLOYEE_STATUS)=='left') & ((bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS=='-')|
    (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS)=='no')) , lit('No'))

    # .when(( ((lower(bvg_cal_df.STATUS)=='joined') & (lower(bvg_cal_df.EMPLOYEE_STATUS)=='left')) |(lower(bvg_cal_df.STATUS)!='joined')) & ((bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS.isNull()) | (bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS == '-' )), lit('-'))\

    .otherwise(lit("-")) )



# COMMAND ----------

# bvg_cal_df.select('BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION','EMPLOYEE_ID').display()
# .where(bvg_cal_df.EMPLOYEE_ID=='106094').display()

# COMMAND ----------

# DBTITLE 1,Previous Yes
#If BGV Check completd along with Waiver(Except CEA and Campus education) = 'Yes', then 'Yes', else '-'
bvg_cal_df=bvg_cal_df.withColumn("PREVIOUS_YES", when(lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS) == 'yes', lit("Yes")).otherwise(lit('-')) )
# bvg_cal_df=bvg_cal_df.withColumn("PREVIOUS_YES",
#  when((lower(bvg_cal_df.PREVIOUS_YES_PREVIOUS ) == 'yes'),lit("Yes"))\
#  .when((lower(bvg_cal_df.PREVIOUS_YES_PREVIOUS) == 'no')| (lower(bvg_cal_df.PREVIOUS_YES_PREVIOUS) == '-'),lit("-"))\
#  .otherwise(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) )
# .when(((lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'yes') & (lower(calendardf.OLD_OR_NEW_DATA) == 'old')), lit("Yes"))\
   # .when(((lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION)
   # == 'no') & (lower(calendardf.OLD_OR_NEW_DATA) == 'old')), lit("No"))

# COMMAND ----------

# DBTITLE 1,Suspicious Company New
#BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education = = 'No' & BU='Consulting' & Suspicious_Company_previous<> ='-', then AK,  else PS_EDU_EMP_Name__Suspicious_
#SUSPICIOUS_COMPANY_NEW

# bvg_cal_df=bvg_cal_df.withColumn('SUSPICIOUS_COMPANY_NEW', when( (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(bvg_cal_df.BU).like('%consulting%')) & (bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS != '-') & bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS.isNotNull() , bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS).otherwise(bvg_cal_df.PS_EDU_EMP_Name__Suspicious_))

bvg_cal_df=bvg_cal_df.withColumn('SUSPICIOUS_COMPANY_NEW', when( (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(bvg_cal_df.BU).like('%consulting%')) & (bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS != '-') & bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS.isNotNull() , bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS)\
.when( (lower(bvg_cal_df.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(bvg_cal_df.BU).like('%consulting%')) & (bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS == '-') & bvg_cal_df.SUSPICIOUS_COMPANY_PREVIOUS.isNull() , bvg_cal_df.PS_EDU_EMP_NAME__SUSPICIOUS_)\
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
# if BGV_Status_Self ='Waiver Pending' and Waiver_Remarks =  'Clarification awaited from candidate' then 'Colleague'
# if BGV_Status_Self ='Waiver Pending' and waiver_remarks = 'Approval to be secured' then 'KGS'
# if BGV_Status_Self ='Waiver Pending' the 'KGS'

# BGV_STATUS_SELF ,  Waiver_Remarks
bvg_cal_df = bvg_cal_df.withColumn("RESPONSIBILITY",
    when(bvg_cal_df.BGV_STATUS_SELF == '-', "-")\
    .when(lower(bvg_cal_df.BGV_STATUS_SELF).contains('in-active'),lit('In-Active'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('cea pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('insuff')),lit('Colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('report pending'))|(lower(bvg_cal_df.BGV_STATUS_SELF).contains('wip')),lit('KI Forensic'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&((lower(bvg_cal_df.WAIVER_REMARKS).contains('under review - hrbp')) |(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-hrbp/ta head'))) ,lit('hrbp partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval awaited-risk')),lit('Risk Partner'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('clarification awaited from candidate')),lit('colleague'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending'))&(lower(bvg_cal_df.WAIVER_REMARKS).contains('approval to be secured')),lit('KGS'))\
    .when((lower(bvg_cal_df.BGV_STATUS_SELF).contains('waiver pending')),lit('KGS'))\
    .otherwise(lit('-')))

# COMMAND ----------

bvg_cal_df.select("RESPONSIBILITY").where(bvg_cal_df.EMPLOYEE_ID =='89510').display()

# COMMAND ----------

#Adding current timestamp to Dated_On and File_Date
from datetime import *
currentdatetime= datetime.now()
bvg_cal_df = bvg_cal_df.withColumn("Dated_On",lit(currentdatetime)).withColumn("File_Date", lit(FileDate))

# COMMAND ----------

bvg_cal_df=bvg_cal_df.withColumn("PLANNED_START_DATE",date_format(bvg_cal_df.PLANNED_START_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DOI_PREVIOUS",date_format(bvg_cal_df.DOI_PREVIOUS,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("BGV_COMPLETION_DATE_PREVIOUS",date_format(bvg_cal_df.BGV_COMPLETION_DATE_PREVIOUS,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DOI_NEW",date_format(bvg_cal_df.DOI_NEW,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("DUE_DATE",date_format(bvg_cal_df.DUE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("BGV_COMPLETION_DATE_NEW",date_format(bvg_cal_df.BGV_COMPLETION_DATE_NEW,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("LAST_GREEN_COMPLETE_DATE",date_format(bvg_cal_df.LAST_GREEN_COMPLETE_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("FINAL_REPORT_DATE",date_format(bvg_cal_df.FINAL_REPORT_DATE,"dd-MMM-yy"))
bvg_cal_df=bvg_cal_df.withColumn("File_Date",date_format(bvg_cal_df.File_Date,"dd-MMM-yy"))

# COMMAND ----------

final_df = bvg_cal_df.select('S_NO','EMPLOYEE_ID','BGV_CASE_REFERENCE_NUMBER','CANDIDATE_FULL_NAME','COST_CENTRE','CLIENT_GEOGRAPHY','DESIGNATION','LOCATION','CANDIDATE_EMAIL','PLANNED_START_DATE','RECRUITER','STATUS','EMPLOYEE_CATEGORY','SOURCE','NAME_OF_SUB_SOURCE','BU','OFFICIAL_EMAIL_ID_PREVIOUS','OFFER_RELEASE_DATE','OFFER_ACCEPTANCE_DATE','INDIA_PROJECT_CODE','HRBP_NAME_PREVIOUS','PERFORMANCE_MANAGER_PREVIOUS','SUPERVISOR_NAME_PREVIOUS','DOI_PREVIOUS','BGV_STATUS_FINAL_PREVIOUS','FR_PREVIOUS','SR_PREVIOUS','OVER_ALL_PREVIOUS','MANDATE_CHECKS_COMPLETED','ALL_CHECKS_COMPLETED_PREVIOUS','WAIVER_APPLICABLE_PREVIOUS','WAIVER_CLOSED_PREVIOUS','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS','CANDIDATE_IDENTIFIER','ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS','BGV_COMPLETION_DATE_PREVIOUS','SUSPICIOUS_COMPANY_PREVIOUS','EMPLOYEE_STATUS','OFFICIAL_EMAIL_ID','HRBP_NAME','PERFORMANCE_MANAGER','SUPERVISOR_NAME','INSUFF_REMARKS','INSUFF_COMPONENT','DOI_NEW','DUE_DATE','AGEING_FROM_DOO','AGEING_FROM_DOI','AGEING_FROM_DOJ','AGEING_BUCKET','DOJ_MONTH','AGEING_FROM_DOJ_CALENDAR_DAY','AGEING_BUCKET_CALENDAR_DAY','BGV_STATUS_SELF','REMARKS','RESPONSIBILITY','BGV_STATUS_PROGRESS_SHEET','FR_SUPPORT_FOR_NEW','SR_SUPPORT_FOR_NEW','BGV_STATUS_FINAL','FR_OUTPUT','SR_OUTPUT','OVER_ALL_OUTPUT','ALL_CHECKS_COMPLETED_NEW','CEA_LATEST_REPORT_COLOR','WAIVER_APPLICABLE_NEW','WAIVER_CLOSED_NEW','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION','ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER','BGV_COMPLETION_DATE_NEW','LAST_GREEN_COMPLETE_DATE','SUSPICIOUS_COMPANY_NEW','BGV_EXCEPTION_Y_N','WAIVER_REMARKS','FINAL_REPORT_DATE','PREVIOUS_YES','Dated_On','File_Date' )

print(final_df.count())
display(final_df)

# COMMAND ----------

#replacing all null values to '-'
final_df=replacenull(final_df)
final_df=leadtrailremove(final_df)

# COMMAND ----------

#special character columns
for col in final_df.columns:
     final_df=final_df.withColumn(col,ascii_udf(col))
final_df =leadtrailremove(final_df)
display(final_df)

# COMMAND ----------

# display(final_df)
print(final_df.count())

# COMMAND ----------

# DBTITLE 1, Loading trusted stg table
final_df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# print("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)
# print("kgsonedatadb.trusted_"+ processName + "_" + tableName)
# print(trusted_stg_savepath_url+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Loading Trusted Curr and Hist Tables
# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_stg_bgv_joined_candidate_ki_loaned

# COMMAND ----------

