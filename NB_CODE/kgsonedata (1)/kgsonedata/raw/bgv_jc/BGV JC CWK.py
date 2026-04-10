# Databricks notebook source
tableName = "cwk"
processName = "bgv_joined_candidate"

# COMMAND ----------

from pyspark.sql.functions import *
# lit,col,from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat, current_date,trim
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from datetime import *


# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------


fileYear = FileDate[:4]
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay

FileDate = datetime.strptime(convertedFileDate, '%Y-%m-%d').date()


print(FileDate)

# COMMAND ----------

# from datetime import datetime
# fileDate_new = datetime.strptime(FileDate, '%Y%m%d').strftime('%Y-%m-%d')

# print(fileDate_new)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat, current_date,trim
# from pyspark.sql import Row
# from pyspark.sql import functions as f
# from pyspark.sql.functions import regexp_replace
# from datetime import *
# import datetime
# import random
# import numpy as np
# import pandas as pd

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,col, trim, lower,upper,lower,lit,when,split, isnull, concat, current_date,trim,from_unixtime, unix_timestamp,to_date,expr,date_format,udf,monotonically_increasing_id,row_number
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

# %sql
# select * from kgsonedatadb.trusted_ta_post_hire_cwk --where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+")

# COMMAND ----------

# DBTITLE 1,Reading Post Hire CWK Table
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_ta_post_hire_cwk')):
#    df_cwk_new = spark.sql("select * from (select rank() over(partition by EMAIL order by File_Date,Dated_On desc) as rank, * from kgsonedatadb.trusted_hist_ta_post_hire_cwk where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where rank = 1")  
    df_cwk_new = spark.sql("select * from kgsonedatadb.trusted_hist_ta_post_hire_cwk where File_Date='20240401'")
else:
    print("table doesn't exist")



df_cwk_new  = df_cwk_new.select("BGV_REFERENCE_NO_","CANDIDATE_NAME",'EMAIL','RECRUITER_NAME','HIRING_SOURCE','PROJECT_CODE','CANDIDATE_IDENTIFIER','START_DATE','FINAL_JOINING_STATUS','MANDATORY_CHECKS_COMPLETED','KGS_EMAIL_ID','BU','COST_CENTRE','GEO','JOINING_LOCATION','DESIGNATION')
df_cwk_new=df_cwk_new.withColumn("OLD_OR_NEW_DATA",lit("New"))

df_cwk_new =df_cwk_new.withColumnRenamed("BGV_REFERENCE_NO_","BGV_CASE_REFERENCE_NUMBER").withColumnRenamed("CANDIDATE_NAME","CANDIDATE_FULL_NAME").withColumnRenamed('EMAIL',"CANDIDATE_EMAIL").withColumnRenamed("RECRUITER_NAME","RECRUITER").withColumnRenamed('HIRING_SOURCE','NAME_OF_SUB_SOURCE').withColumnRenamed('PROJECT_CODE',"INDIA_PROJECT_CODE").withColumnRenamed('START_DATE',"PLANNED_START_DATE").withColumnRenamed('MANDATORY_CHECKS_COMPLETED',"MANDATE_CHECKS_COMPLETED").withColumnRenamed('DESIGNATION',"cwk_DESIGNATION")
df_cwk_new=df_cwk_new.withColumn("PLANNED_START_DATE",to_date(df_cwk_new["PLANNED_START_DATE"],"dd-MMM-yy"))
df_cwk_new=df_cwk_new.withColumn("PLANNED_START_DATE",date_format(df_cwk_new["PLANNED_START_DATE"],"yyyy-MM-dd"))
# df_cwk_new =leadtrailremove(df_cwk_new)
df_cwk_new.createOrReplaceTempView("df_cwk_new")
print(df_cwk_new.count())
# display(df_cwk_new)



df_cwk_new=df_cwk_new.filter(lower(df_cwk_new["FINAL_JOINING_STATUS"]).like('joined%')|(lower(df_cwk_new["FINAL_JOINING_STATUS"]).like('%mid%join%')))
df_cwk_new=df_cwk_new.withColumn("FINAL_JOINING_STATUS",when((lower(df_cwk_new["FINAL_JOINING_STATUS"]).like('%mid%join%')),"Joined").otherwise(df_cwk_new["FINAL_JOINING_STATUS"]))


print(df_cwk_new.count())
display(df_cwk_new.select('PLANNED_START_DATE').where(df_cwk_new.BGV_CASE_REFERENCE_NUMBER.like('%KPMG%00040312%KGS%2024%')))

# COMMAND ----------

column_list=['BGV_CASE_REFERENCE_NUMBER','INDIA_PROJECT_CODE','KGS_EMAIL_ID']
 
for columnName in column_list:
    df_cwk_new=df_cwk_new.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_cwk_new=df_cwk_new.withColumn(columnName,ascii_udf(columnName))
    df_cwk_new=df_cwk_new.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

# DBTITLE 1,Previous JC_CWK
# Replace table name with Original name as and when table is available

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_cwk')):
   df_prev_jc_cwk = spark.sql("select * from (select rank() over(partition by EMPLOYEE_ID,FILE_DATE order by FILE_DATE desc,PLANNED_START_DATE desc,Dated_On desc) as rank, * from kgsonedatadb.trusted_hist_bgv_joined_candidate_cwk where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+") hist where rank = 1")


else:
    print("table doesn't exist, reading from the empty hist file")



df_prev_jc_cwk.createOrReplaceTempView("df_prev_jc_cwk_temp")

df_prev_jc_cwk=spark.sql('''select EMPLOYEE_ID,CANDIDATE_EMAIL,BGV_CASE_REFERENCE_NUMBER,PLANNED_START_DATE,CLIENT_GEOGRAPHY,LOCATION,DESIGNATION,
PrevJC_CWK_OFFICIAL_EMAIL_ID_HISTORY,PrevJC_CWK_HRBP_NAME_HISTORY,PrevJC_CWK_PERFORMANCE_MANAGER_HISTORY,PrevJC_CWK_SUPERVISOR_NAME_HISTORY,PrevJC_CWK_DOI_HISTORY,PrevJC_CWK_BGV_STATUS_HISTORY,PrevJC_CWK_FR_HISTORY,PrevJC_CWK_SR_HISTORY,PrevJC_CWK_OVER_ALL_HISTORY,PrevJC_CWK_ALL_CHECKS_COMPLETED_HISTORY,PrevJC_CWK_WAIVER_APPLICABLE_HISTORY,PrevJC_CWK_WAIVER_CLOSED_HISTORY,PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY,PrevJC_CWK_ALL_CHECKS_COMPLETED___ALONG_WITH_WAIVER_HISTORY,PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY,PrevJC_CWK_SUSPICIOUS_COMPANY_HISTORY,File_Date from df_prev_jc_cwk_temp''')

# df_prev_jc_cwk=spark.sql("""SELECT distinct CANDIDATE_EMAIL,BGV_CASE_REFERENCE_NUMBER,PLANNED_START_DATE,CLIENT_GEOGRAPHY,LOCATION,DESIGNATION,
                          
# CASE WHEN OFFICIAL_EMAIL_ID is null THEN '-' ELSE OFFICIAL_EMAIL_ID END AS OFFICIAL_EMAIL_ID_HISTORY,

# CASE WHEN HRBP_NAME is null THEN '-' ELSE HRBP_NAME END AS HRBP_NAME_HISTORY,

# CASE WHEN PERFORMANCE_MANAGER is null THEN '-' ELSE PERFORMANCE_MANAGER END AS PERFORMANCE_MANAGER_HISTORY,

# CASE WHEN SUPERVISOR_NAME is null THEN '-' ELSE SUPERVISOR_NAME END AS SUPERVISOR_NAME_HISTORY,

# CASE WHEN DOI_NEW is null THEN '-' ELSE DOI_NEW END AS DOI_HISTORY,

# CASE WHEN BGV_STATUS_OUTPUT is null THEN '-' ELSE BGV_STATUS_OUTPUT END AS BGV_STATUS_HISTORY,

# CASE WHEN FR_OUTPUT is null THEN '-' ELSE FR_OUTPUT END AS FR_HISTORY,

# CASE WHEN SR_OUTPUT is null THEN '-' ELSE SR_OUTPUT END AS SR_HISTORY,

# CASE WHEN OVER_ALL_OUTPUT is null THEN '-' ELSE OVER_ALL_OUTPUT END AS OVER_ALL_HISTORY,

# CASE WHEN ALL_CHECKS_COMPLETED_NEW is null THEN '-' ELSE ALL_CHECKS_COMPLETED_NEW END AS ALL_CHECKS_COMPLETED_HISTORY,

# CASE WHEN WAIVER_APPLICABLE_NEW is null THEN '-' ELSE WAIVER_APPLICABLE_NEW END AS WAIVER_APPLICABLE_HISTORY,

# CASE WHEN WAIVER_CLOSED_NEW is null THEN '-' ELSE WAIVER_CLOSED_NEW END AS WAIVER_CLOSED_HISTORY,

# CASE WHEN BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION is null THEN '-' ELSE BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION END AS BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY,

# CASE WHEN ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER is null THEN '-' ELSE ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER END AS ALL_CHECKS_COMPLETED___ALONG_WITH_WAIVER_HISTORY,

# CASE WHEN BGV_COMPLETION_DATE_NEW is null THEN '-' ELSE BGV_COMPLETION_DATE_NEW END AS BGV_COMPLETION_DATE_HISTORY,

# CASE WHEN SUSPICIOUS_COMPANY_NEW is null THEN '-' ELSE SUSPICIOUS_COMPANY_NEW END AS SUSPICIOUS_COMPANY_HISTORY,

# CASE WHEN EMPLOYEE_ID is null THEN '-' ELSE EMPLOYEE_ID END AS EMPLOYEE_ID,

# CASE WHEN PREVIOUS_YES is null THEN '-' ELSE PREVIOUS_YES END AS PREVIOUS_YES,File_Date


# FROM df_prev_jc_cwk_temp""")

# df_prev_jc_cwk=leadtrailremove(df_prev_jc_cwk)



Prefix_columns=['EMPLOYEE_ID','CANDIDATE_EMAIL','BGV_CASE_REFERENCE_NUMBER','PLANNED_START_DATE','CLIENT_GEOGRAPHY','LOCATION','DESIGNATION','File_Date']
for column in Prefix_columns:
    df_prev_jc_cwk=df_prev_jc_cwk.withColumnRenamed(column,'PrevJC_CWK_' + column)
    

# df_prev_jc_cwk = rename_prefix_columns("PrevJC_CWK",df_prev_jc_cwk)
df_prev_jc_cwk.select('PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY','PrevJC_CWK_BGV_CASE_REFERENCE_NUMBER','PrevJC_CWK_EMPLOYEE_ID','PrevJC_CWK_File_Date').where(df_prev_jc_cwk.PrevJC_CWK_EMPLOYEE_ID=='611867').display()

# KPMG\00000829\KGS\2024


# COMMAND ----------

column_list=['PrevJC_CWK_BGV_CASE_REFERENCE_NUMBER']
 
for columnName in column_list:
    df_prev_jc_cwk=df_prev_jc_cwk.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_prev_jc_cwk=df_prev_jc_cwk.withColumn(columnName,ascii_udf(columnName))
    df_prev_jc_cwk=df_prev_jc_cwk.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_prev_jc_cwk.groupBy("PrevJC_CWK_EMPLOYEE_ID").agg(count("PrevJC_CWK_EMPLOYEE_ID")).filter(count("PrevJC_CWK_EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,CWK_ED

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_cwk_employee_dump')):
    df_ed = spark.sql("select * from (select row_number() over(partition by CANDIDATE_ID order by File_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_cwk_employee_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
    
    # df_ed = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_cwk_employee_dump")
    # where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_cwk_employee_dump
    
else:
    print("table doesn't exist")

  



df_ed= df_ed.select('EMAIL','CANDIDATE_ID','FULL_NAME')


df_ed =leadtrailremove(df_ed) 
df_ed = rename_prefix_columns("ED",df_ed)
print(df_ed.count())#1251


# COMMAND ----------

display(df_ed.groupBy("ED_CANDIDATE_ID").agg(count("ED_CANDIDATE_ID")).filter(count("ED_CANDIDATE_ID")>1))

# COMMAND ----------

# DBTITLE 1,CWK_TD
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_cwk_termination_dump')):
    df_td = spark.sql("select * from (select row_number() over(partition by trim(CANDIDATE_ID) order by File_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_cwk_termination_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
    # df_td = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_cwk_termination_dump")
    
else:
    print("table doesn't exist")



df_td=df_td.select('CANDIDATE_ID','OFFICIAL_EMAIL_ID')
df_td=df_td.withColumn('CANDIDATE_ID',trim(col("CANDIDATE_ID")))


df_td=leadtrailremove(df_td) 
df_td = rename_prefix_columns("TD",df_td)
print(df_td.count())
# display(df_td)



# COMMAND ----------

column_list=['TD_CANDIDATE_ID']
 
for columnName in column_list:
    df_td=df_td.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_td=df_td.withColumn(columnName,ascii_udf(columnName))
    df_td=df_td.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

df_td.select('TD_CANDIDATE_ID').where(df_td.TD_CANDIDATE_ID=='586223').display()

# COMMAND ----------

display(df_td.groupBy("TD_CANDIDATE_ID").agg(count("TD_CANDIDATE_ID")).filter(count("TD_CANDIDATE_ID")>1))

# COMMAND ----------

# MAGIC %sql
# MAGIC select Case_Initiation_Date,REFERENCE_NO_,OVER_ALL_STATUS_DROP_DOWN_,Final_Report_Color_Code_Drop_Down_,Supplimentry_Report_Color_Code_Drop_Down_,FINAL_REPORT_DATE,File_Date from kgsonedatadb.trusted_hist_bgv_progress_sheet where Reference_No_ like('%KPMG%00019795%KGS%2022%') order by File_Date desc

# COMMAND ----------

# DBTITLE 1,Progress sheet
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_progress_sheet')):
    df_progress_sheet = spark.sql("select * from (select row_number() over(partition by trim(REFERENCE_NO_) order by File_Date desc,Case_Initiation_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_progress_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
 
    # df_progress_sheet = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_progress_sheet")
#     # Case_Initiation_Date desc,
# to_date(File_Date,'yyyyMMdd')

else:
    print("table doesn't exist")




df_progress_sheet = df_progress_sheet.select('OPEN_INSUFF','CASE_INITIATION_DATE','OVER_ALL_STATUS_DROP_DOWN_','Final_Report_Color_Code_Drop_Down_','Supplimentry_Report_Color_Code_Drop_Down_','REFERENCE_NO_','FINAL_REPORT_DATE','SUPPLIMENTRY_REPORT_DATE','EDU_EMP_NAME__SUSPICIOUS_','PERSONAL_EMAIL_ID')


df_progress_sheet = rename_prefix_columns("PS",df_progress_sheet) 
# df_progress_sheet=leadtrailremove(df_progress_sheet)
print(df_progress_sheet.count())

df_progress_sheet.select('PS_CASE_INITIATION_DATE','PS_FINAL_REPORT_DATE','PS_OVER_ALL_STATUS_DROP_DOWN_','PS_Supplimentry_Report_Color_Code_Drop_Down_','PS_OPEN_INSUFF').where(df_progress_sheet.PS_REFERENCE_NO_.like('%KPMG%00022746%KGS%2022%')).display()
df_progress_sheet=df_progress_sheet.withColumn("PS_OVER_ALL_STATUS_DROP_DOWN_",when(((lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%process%'))| (lower(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]).like('%progress%'))),"Report Pending").otherwise(df_progress_sheet["PS_OVER_ALL_STATUS_DROP_DOWN_"]))

# COMMAND ----------

column_list=['PS_REFERENCE_NO_']
 
for columnName in column_list:
    df_progress_sheet=df_progress_sheet.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,ascii_udf(columnName))
    df_progress_sheet=df_progress_sheet.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_progress_sheet.groupBy("PS_REFERENCE_NO_").agg(count("PS_REFERENCE_NO_")).filter(count("PS_REFERENCE_NO_")>1))

# COMMAND ----------

# DBTITLE 1,Waiver Tracker

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_waiver_tracker')):
    df_waiver_tracker = spark.sql("select * from (select row_number() over(partition by trim(BGV_REF_NO) order by File_Date desc,Report_Date desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_waiver_tracker where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") hist where row_num = 1")
    # df_waiver_tracker = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_waiver_tracker")
else:
    print("table doesn't exist")
    

df_waiver_tracker = df_waiver_tracker.select('BGV_REF_NO','CANDIDATE_EMAIL_ID','WAIVER_RECEIVED_DATE___HRBP_TA_HEAD','CASE_REMARKS','CASE_STATUS')

# 'Waiver_Open_Closed'

df_waiver_tracker = rename_prefix_columns("Waiver",df_waiver_tracker) 

# df_waiver_tracker=leadtrailremove(df_waiver_tracker)
print(df_waiver_tracker.count())

# COMMAND ----------

df_waiver_tracker.select('Waiver_CASE_STATUS').where(df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID.like('%tsirigineedi%gmail%com%')).display()

# COMMAND ----------

# DBTITLE 1,To check duplicates
display(df_waiver_tracker.groupBy("Waiver_BGV_REF_NO").agg(count("Waiver_BGV_REF_NO")).filter(count("Waiver_BGV_REF_NO")>1))

# COMMAND ----------

# DBTITLE 1,HOLIDAY_LIST
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_bgv_joined_candidate_holiday_list')):
    holiday_df = spark.sql("select * from kgsonedatadb.trusted_hist_bgv_joined_candidate_holiday_list")
else:
    print("table doesn't exist")
print(holiday_df.count())
# holiday_df=leadtrailremove(holiday_df)  

# COMMAND ----------

# display(holiday_df.groupBy("DATE").agg(count("DATE")).filter(count("DATE")>1))

# COMMAND ----------

# DBTITLE 1,INSUFF_COMPONENT
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_bgv_insuff_component')):
    insuff_df = spark.sql("select INSUFF_COMPONENT from kgsonedatadb.trusted_bgv_insuff_component")
else:
    print("table doesn't exist")

insuff_df=leadtrailremove(insuff_df)      

# COMMAND ----------

column_list=['INSUFF_COMPONENT']
for columnName in column_list:
    insuff_df=insuff_df.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
    insuff_df=insuff_df.withColumn(columnName,ascii_udf(columnName))
    insuff_df=insuff_df.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(insuff_df.groupBy("INSUFF_COMPONENT").agg(count("INSUFF_COMPONENT")).filter(count("INSUFF_COMPONENT")>1))

# COMMAND ----------

# DBTITLE 1,CWK Rekonnect Sheet
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_cwk_rekonnect_sheet')):
    # df_CWK_Rekonnect_Sheet= spark.sql("select * from kgsonedatadb.trusted_hist_bgv_cwk_rekonnect_sheet")
    df_CWK_Rekonnect_Sheet= spark.sql("select * from (select row_number() over(partition by CANDIDATE_ID order by File_Date desc,dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_cwk_rekonnect_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(FileDate)+"'"+") where row_num= 1")
else:
    print("table doesn't exist")

for col in df_CWK_Rekonnect_Sheet.columns:
        df_CWK_Rekonnect_Sheet=df_CWK_Rekonnect_Sheet.withColumnRenamed(col,ascii_ignore(col))


df_CWK_Rekonnect_Sheet = df_CWK_Rekonnect_Sheet.select('CANDIDATE_ID','EMPLOYEE_NUMBER','FULL_NAME','OFFICIAL_EMAIL_ID','LOCATION','CLIENT_GEO','ORGANIZATION','POSITION','ASSIGNMENT_CATEGORY','RM_TREE_NAME','PERFORMANCE_MANAGER_NAME','SUPERVISOR_EMPLOYEE_NAME')

df_CWK_Rekonnect_Sheet = rename_prefix_columns("Rekonnect",df_CWK_Rekonnect_Sheet)
df_CWK_Rekonnect_Sheet = df_CWK_Rekonnect_Sheet.withColumnRenamed('Rekonnect_CANDIDATE_ID','EMPLOYEE_ID')

df_CWK_Rekonnect_Sheet=leadtrailremove(df_CWK_Rekonnect_Sheet)
print(df_CWK_Rekonnect_Sheet.count())

# COMMAND ----------

# column_list=['EMPLOYEE_ID','Rekonnect_OFFICIAL_EMAIL_ID','LOCATION','CLIENT_GEOGRAPHY','Rekonnect_ORGANIZATION','Rekonnect_POSITION','Rekonnect_ASSIGNMENT_CATEGORY','Rekonnect_RM_TREE_NAME','Rekonnect_PERFORMANCE_MANAGER_NAME','Rekonnect_SUPERVISOR_EMPLOYEE_NAME']
# for columnName in column_list:
#     df_CWK_Rekonnect_Sheet=df_CWK_Rekonnect_Sheet.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
#     df_CWK_Rekonnect_Sheet=df_CWK_Rekonnect_Sheet.withColumn(columnName,ascii_udf(columnName))
#     df_CWK_Rekonnect_Sheet=df_CWK_Rekonnect_Sheet.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_CWK_Rekonnect_Sheet.groupBy("EMPLOYEE_ID").agg(count("EMPLOYEE_ID")).filter(count("EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,Reading all the old cwk records except current week posthire cwk records
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_joined_candidate_cwk')):
   
    df_old_cwk = spark.sql("select BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,CANDIDATE_EMAIL,RECRUITER,NAME_OF_SUB_SOURCE,INDIA_PROJECT_CODE,EMPLOYEE_ID as CANDIDATE_IDENTIFIER,PLANNED_START_DATE,STATUS as FINAL_JOINING_STATUS,MANDATE_CHECKS_COMPLETED,KGS_EMAIL_ID,BU,COST_CENTRE,CLIENT_GEOGRAPHY as GEO,LOCATION as JOINING_LOCATION,DESIGNATION as cwk_DESIGNATION from (select row_number() over(partition by trim(BGV_CASE_REFERENCE_NUMBER) order by File_Date desc,PLANNED_START_DATE desc,Dated_On desc) as row_num, * from kgsonedatadb.trusted_hist_bgv_joined_candidate_cwk where to_date(File_Date,'yyyyMMdd') < '"+str(FileDate)+"'"+") where BGV_CASE_REFERENCE_NUMBER not in (select distinct(BGV_CASE_REFERENCE_NUMBER) from df_cwk_new) and row_num = 1")
   
else:
    print("table doesn't exist, reading from the empty hist file")
  
# df_old_cwk.createOrReplaceTempView("df_old_cwk")
print(df_old_cwk.count())
# df_old_cwk.select('PLANNED_START_DATE','BGV_CASE_REFERENCE_NUMBER').where(df_old_cwk.BGV_CASE_REFERENCE_NUMBER.like('%KPMG%00040312%KGS%2024%')).display()
df_old_cwk=df_old_cwk.withColumn("OLD_OR_NEW_DATA",lit("Old"))


df_old_cwk=df_old_cwk.filter(lower(df_old_cwk["FINAL_JOINING_STATUS"]).like('joined%')|(lower(df_old_cwk["FINAL_JOINING_STATUS"]).like('%mid%join%')))
df_old_cwk=df_old_cwk.withColumn("FINAL_JOINING_STATUS",when((lower(df_old_cwk["FINAL_JOINING_STATUS"]).like('%mid%join%')),"Joined").otherwise(df_old_cwk["FINAL_JOINING_STATUS"]))
print(df_old_cwk.count())
# display(df_old_cwk)


# COMMAND ----------

# from pyspark.sql.functions import col, when, lit
# column_list=['BGV_CASE_REFERENCE_NUMBER','INDIA_PROJECT_CODE','KGS_EMAIL_ID']
# for columnName in column_list:
#     df_old_cwk=df_old_cwk.withColumn(columnName, when(col(columnName).isNull() | (col(columnName)==''),"-").otherwise(col(columnName)))
#     df_old_cwk=df_old_cwk.withColumn(columnName,ascii_udf(col(columnName)))
#     df_old_cwk=df_old_cwk.withColumn(columnName,when(col(columnName)=='-',lit(None)).otherwise(col(columnName)))

# COMMAND ----------

display(df_old_cwk.groupBy("BGV_CASE_REFERENCE_NUMBER").agg(count("BGV_CASE_REFERENCE_NUMBER")).filter(count("BGV_CASE_REFERENCE_NUMBER")>1))

# COMMAND ----------

# DBTITLE 1,Union of Current  data and Hist data
# union() 
# # df_old_cwk=df_old_cwk.drop('Employee_ID')
df_cwk = df_cwk_new.union(df_old_cwk)
print(df_cwk.count())
# display(df_old_cwk)
display(df_cwk.select('PLANNED_START_DATE').where(df_cwk.BGV_CASE_REFERENCE_NUMBER.like('%KPMG%00040312%KGS%2024%')))

# COMMAND ----------

# DBTITLE 1,CWK Rekonnect sheet ,CWK_ED join , CWK_TD join & CC BU SL join
# ec_join_df=df_ed.join(df_CWK_Rekonnect_Sheet,df_ed.ED_CANDIDATE_ID == df_CWK_Rekonnect_Sheet.EMPLOYEE_ID ,"left")
ec_join_df=df_CWK_Rekonnect_Sheet.join(df_ed,df_CWK_Rekonnect_Sheet.EMPLOYEE_ID == df_ed.ED_CANDIDATE_ID ,"left").join(df_td,df_CWK_Rekonnect_Sheet.EMPLOYEE_ID == df_td.TD_CANDIDATE_ID,"left")
ec_join_df=ec_join_df.dropDuplicates(["EMPLOYEE_ID"])
# .join(df_bu_sl,df_CWK_Rekonnect_Sheet.COST_CENTRE == df_bu_sl.BU_COST_CENTRE,"left")

print(ec_join_df.count())


# COMMAND ----------

# display(ec_join_df.groupBy("CANDIDATE_ID").agg(count("CANDIDATE_ID")).filter(count("CANDIDATE_ID")>1))


# COMMAND ----------

# DBTITLE 1,Joining with Prev JC CWK
ecp_join_df=ec_join_df.join(df_prev_jc_cwk,ec_join_df.EMPLOYEE_ID == df_prev_jc_cwk.PrevJC_CWK_EMPLOYEE_ID ,"left")
print(ecp_join_df.count())
ecp_join_df=ecp_join_df.dropDuplicates(['EMPLOYEE_ID'])
print(ecp_join_df.count())


# COMMAND ----------

display(ecp_join_df.groupBy("EMPLOYEE_ID").agg(count("EMPLOYEE_ID")).filter(count("EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,#Source
# If Hiring SOURCE contains campus then  in SOURCE column it will have campus ,Otherwise,it is lateral

# ecp_join_df = ecp_join_df.withColumn("SOURCE",\
#      when(lower(ecp_join_df.NAME_OF_SUB_SOURCE).like('%campus%'),ecp_join_df.NAME_OF_SUB_SOURCE).otherwise(lit('lateral')))

# COMMAND ----------

# DBTITLE 1,Employee Status

# Search in "Employee ID" from "BGV Dashboard Working CWK" sheet in "CWK_TD" sheet with column name "Candidate Id". If found then Left else "-" 

# ecp_join_df = ecp_join_df.withColumn("Employee_Status", \
#   when((ecp_join_df.TD_CANDIDATE_ID !='-') & (ecp_join_df.TD_CANDIDATE_ID.isNotNull()),lit('Left'))\
#   .otherwise(lit('-')))
ecp_join_df = ecp_join_df.withColumn("EMPLOYEE_STATUS", \
  when((ecp_join_df.EMPLOYEE_ID == ecp_join_df.TD_CANDIDATE_ID),lit('Left'))\
  .otherwise(lit('-')))
# df5=ecp_join_df.where(ecp_join_df.Employee_Status=='Left')


# COMMAND ----------

#Official_EMAIL_ID
# IFERROR(VLOOKUP($B3,'[For KGS One data project.xlsb]CWK_ED'!$A:$X,24,0),$Q3)

ecp_join_df = ecp_join_df.withColumn("OFFICIAL_EMAIL_ID",\
    when(ecp_join_df.Rekonnect_OFFICIAL_EMAIL_ID.isNotNull(),ecp_join_df.Rekonnect_OFFICIAL_EMAIL_ID)\
    .when((ecp_join_df.Rekonnect_OFFICIAL_EMAIL_ID.isNull() & ecp_join_df.PrevJC_CWK_OFFICIAL_EMAIL_ID_HISTORY.isNotNull()) ,ecp_join_df.PrevJC_CWK_OFFICIAL_EMAIL_ID_HISTORY)\
    .otherwise(lit(None)))

# ecp_join_df = ecp_join_df.withColumn("Official_EMAIL_ID",\
#      when(ecp_join_df.Rekonnect_Official_EMAIL_ID.isNotNull(),ecp_join_df.Rekonnect_Official_EMAIL_ID)\
#     .when((ecp_join_df.Rekonnect_Official_EMAIL_ID.isNull() & ecp_join_df.PrevJC_CWK_Official_EMAIL_ID_HISTORY.isNotNull()) ,ecp_join_df.PrevJC_CWK_Official_EMAIL_ID_HISTORY)\
#     .otherwise(lit(None)))

# COMMAND ----------

#HRBP_NAME
# IFERROR(VLOOKUP('[For KGS One data project.xlsb]BGV Dashboard Working CWK'!$B3,'[For KGS One data project.xlsb]CWK_Rekonnnect'!$A:$AW,49,0),$U3)
ecp_join_df =ecp_join_df.withColumn("HRBP_NAME", \
  when(ecp_join_df.Rekonnect_RM_TREE_NAME.isNotNull(),ecp_join_df.Rekonnect_RM_TREE_NAME)\
 .when((ecp_join_df.Rekonnect_RM_TREE_NAME.isNull() & ecp_join_df.PrevJC_CWK_HRBP_NAME_HISTORY.isNotNull()), ecp_join_df.PrevJC_CWK_HRBP_NAME_HISTORY)\
 .otherwise(lit(None)))

# COMMAND ----------

#PERFORMANCE_MANAGER 
# for TD,Supervisor name has been taken

ecp_join_df =ecp_join_df.withColumn("PERFORMANCE_MANAGER", \
     when(ecp_join_df.Rekonnect_PERFORMANCE_MANAGER_NAME.isNotNull(),ecp_join_df.Rekonnect_PERFORMANCE_MANAGER_NAME)\
     .when((ecp_join_df.Rekonnect_PERFORMANCE_MANAGER_NAME.isNull() & ecp_join_df.PrevJC_CWK_PERFORMANCE_MANAGER_HISTORY.isNotNull()), ecp_join_df.PrevJC_CWK_PERFORMANCE_MANAGER_HISTORY).otherwise(lit(None)))

#    .when(ecp_join_df.TD_SUPERVISOR_EMPLOYEE_NAME.isNotNull(),ecp_join_df.TD_SUPERVISOR_EMPLOYEE_NAME)\
#    .when(ecp_join_df.ED_PERFORMANCE_MANAGER.isNotNull(),ecp_join_df.ED_PERFORMANCE_MANAGER)\


# COMMAND ----------

#SUPERVISOR_NAME

ecp_join_df =ecp_join_df.withColumn("SUPERVISOR_NAME", \
    when(ecp_join_df.Rekonnect_SUPERVISOR_EMPLOYEE_NAME.isNotNull(),ecp_join_df.Rekonnect_SUPERVISOR_EMPLOYEE_NAME)
    .when((ecp_join_df.Rekonnect_SUPERVISOR_EMPLOYEE_NAME.isNull()) & (ecp_join_df.PrevJC_CWK_SUPERVISOR_NAME_HISTORY.isNotNull()), ecp_join_df.PrevJC_CWK_SUPERVISOR_NAME_HISTORY)\
    .otherwise(lit(None)))


    # .when(ecp_join_df.TD_SUPERVISOR_EMPLOYEE_NAME.isNotNull(),ecp_join_df.TD_SUPERVISOR_EMPLOYEE_NAME)\
    # .when(ecp_join_df.ED_SUPERVISOR_NAME.isNotNull(),ecp_join_df.ED_SUPERVISOR_NAME)\

# COMMAND ----------

# DBTITLE 1,Assignment Category
# Fill the ASSIGNMENT_CATEGORY column using Rekonnect_ASSIGNMENT_CATEGORY against CWK Rekonnect emp ids

ecp_join_df=ecp_join_df.withColumn('ASSIGNMENT_CATEGORY',when((ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY.isNotNull()) | (trim(ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY) != '') | (ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY != '-'),ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY).otherwise(lit('Not Found')))
# ecp_join_df.EMPLOYEE_ID == ecp_join_df.TD_CANDIDATE_ID
# ecp_join_df=ecp_join_df.withColumn('ASSIGNMENT_CATEGORY',when((ecp_join_df.Official_EMAIL_ID.isNotNull()),ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY).otherwise(lit('Not Found')))

# COMMAND ----------

display(ecp_join_df.groupBy("EMPLOYEE_ID").agg(count("EMPLOYEE_ID")).filter(count("EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,Progress Sheet Join with Post Hire CWK sheet 
PS_join_df=df_cwk.join(df_progress_sheet,df_cwk.BGV_CASE_REFERENCE_NUMBER == df_progress_sheet.PS_REFERENCE_NO_,"left")
print(PS_join_df.count())
# display(PS_join_df)


# COMMAND ----------

display(PS_join_df.groupBy("PS_REFERENCE_NO_").agg(count("PS_REFERENCE_NO_")).filter(count("PS_REFERENCE_NO_")>1))

# COMMAND ----------

# DBTITLE 1,Join B/w ecp_join_df & pc_join_df
# progress sheet & CWK join with ecp
# PS_join_df= ecp_join_df.join(df_progress_sheet,ecp_join_df.BGV_CASE_REFERENCE_NUMBER == df_progress_sheet.
# PS_REFERENCE_NO_ , "left")

# PS_join_df= pc_join_df.join(ecp_join_df,pc_join_df.CANDIDATE_IDENTIFIER == ecp_join_df.ED_CANDIDATE_ID, "left")

# PS_join_df= Rekonnect_join_df.join(df_progress_sheet,Rekonnect_join_df.BGV_CASE_REFERENCE_NUMBER == df_progress_sheet.
# PS_REFERENCE_NO_ , "left")

# COMMAND ----------

# DBTITLE 1,Source
# Hiring SOURCE contains campus then  in SOURCE column it will have campus & for rest,it is lateral

PS_join_df = PS_join_df.withColumn("SOURCE",\
     when(lower(PS_join_df.NAME_OF_SUB_SOURCE).like('%campus%'),PS_join_df.NAME_OF_SUB_SOURCE).otherwise(lit('lateral')))

# COMMAND ----------

# DBTITLE 1,Employee Category
PS_join_df =PS_join_df.withColumn('EMPLOYEE_CATEGORY',lit("CWK"))


# COMMAND ----------

# #Insuff Remarks ,DOI New, BGV Status for New, FR_SUPPORT_FOR_NEW, SR_SUPPORT_FOR_NEW
# PS_join_df=PS_join_df.withColumnRenamed("PS_OPEN_INSUFF","INSUFF_REMARKS")\
#     .withColumnRenamed("PS_CASE_INITIATION_DATE","DOI_NEW")\
# PS_join_df=PS_join_df.withColumnRenamed("PS_OVER_ALL_STATUS_DROP_DOWN_","BGV_STATUS_FOR_NEW")\
#     .withColumnRenamed("PS_Final_Report_Color_Code_Drop_Down_","FR_SUPPORT_FOR_NEW")\
#     .withColumnRenamed("PS_Supplimentry_Report_Color_Code_Drop_Down_","SR_SUPPORT_FOR_NEW")\
#     # .withColumnRenamed("PS_FINAL_REPORT_DATE","FINAL_REPORT_DATE")



# COMMAND ----------

# DBTITLE 1,Insuff Remarks
PS_join_df=PS_join_df.withColumn("INSUFF_REMARKS",\
    when((PS_join_df.PS_OPEN_INSUFF.isNotNull()) | (trim(PS_join_df.PS_OPEN_INSUFF) != '') | (PS_join_df.PS_OPEN_INSUFF != '-'),PS_join_df.PS_OPEN_INSUFF)\
    .otherwise(lit('-')))


# COMMAND ----------

# DBTITLE 1,DOI NEW
PS_join_df=PS_join_df.withColumn("DOI_NEW",\
    when((PS_join_df.PS_CASE_INITIATION_DATE.isNotNull()) | (trim(PS_join_df.PS_CASE_INITIATION_DATE) != '') | (PS_join_df.PS_CASE_INITIATION_DATE != '-'),PS_join_df.PS_CASE_INITIATION_DATE)\
    .otherwise(lit('-')))  

# COMMAND ----------

# DBTITLE 1,Replacing Null PS_Case_Initiation_date with 1900-01-01
# DOI_NEW
# # from pyspark.sql.functions import col,when
# PS_join_df = PS_join_df.withColumn("DOI_NEW", when((PS_join_df.PS_CASE_INITIATION_DATE == " ") | (PS_join_df.PS_CASE_INITIATION_DATE == "0-Jan-00") | (PS_join_df.PS_CASE_INITIATION_DATE.isNull()) | (PS_join_df.PS_CASE_INITIATION_DATE == "") | (PS_join_df.PS_CASE_INITIATION_DATE == "#N/A") | (PS_join_df.PS_CASE_INITIATION_DATE == "-"),"1900-01-01").otherwise(PS_join_df.PS_CASE_INITIATION_DATE))

# # insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("PrevJC_CWK_DOI_HISTORY") == " ") | (col("PrevJC_CWK_DOI_HISTORY") == "0-Jan-00") | (col("PrevJC_CWK_DOI_HISTORY").isNull()) | (col("PrevJC_CWK_DOI_HISTORY") == "") | (col("PrevJC_CWK_DOI_HISTORY") == "#N/A") | (col("PrevJC_CWK_DOI_HISTORY") == "None")),lit("1900-01-01")).otherwise(col("PrevJC_CWK_DOI_HISTORY")))
                     
# PS_join_df = PS_join_df.withColumn("DOI_NEW",changeDateFormat(PS_join_df.DOI_NEW))

# COMMAND ----------

# DBTITLE 1,BGV_Status_for_New
PS_join_df=PS_join_df.withColumn("BGV_STATUS_FOR_NEW",\
    when((PS_join_df.PS_OVER_ALL_STATUS_DROP_DOWN_.isNotNull()) | (trim(PS_join_df.PS_OVER_ALL_STATUS_DROP_DOWN_) != '') | (PS_join_df.PS_OVER_ALL_STATUS_DROP_DOWN_ != '-'),PS_join_df.PS_OVER_ALL_STATUS_DROP_DOWN_)\
    .otherwise(lit('-')))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,FR_Support_for_New
PS_join_df=PS_join_df.withColumn("FR_SUPPORT_FOR_NEW",\
    when((PS_join_df.PS_Final_Report_Color_Code_Drop_Down_.isNotNull()) | (trim(PS_join_df.PS_Final_Report_Color_Code_Drop_Down_) != '') | (PS_join_df.PS_Final_Report_Color_Code_Drop_Down_ != '-'),PS_join_df.PS_Final_Report_Color_Code_Drop_Down_)\
    .otherwise(lit('-')))

# COMMAND ----------

# DBTITLE 1,SR_Support_for_New
PS_join_df=PS_join_df.withColumn("SR_SUPPORT_FOR_NEW",\
    when((PS_join_df.PS_Supplimentry_Report_Color_Code_Drop_Down_.isNotNull()) | (trim(PS_join_df.PS_Supplimentry_Report_Color_Code_Drop_Down_) != '') | (PS_join_df.PS_Supplimentry_Report_Color_Code_Drop_Down_ != '-'),PS_join_df.PS_Supplimentry_Report_Color_Code_Drop_Down_)\
    .otherwise(lit('-'))) 

# COMMAND ----------

# PS_join_df=PS_join_df.withColumn("FINAL_REPORT_DATE",\
#     when((PS_join_df.PS_FINAL_REPORT_DATE.isNotNull()) | (trim(PS_join_df.PS_FINAL_REPORT_DATE) != '') | (PS_join_df.PS_FINAL_REPORT_DATE != '-'),PS_join_df.PS_FINAL_REPORT_DATE)\
#     .otherwise(lit('-'))) 

# COMMAND ----------

# DBTITLE 1,Final_Report_Date
from pyspark.sql.functions import col
#FINAL_REPORT_DATE
# Lookup Progress Sheet using BGV Reference No and fetch Final Report Date (M) from PS. If no value, '-'


PS_join_df=PS_join_df.withColumnRenamed("PS_FINAL_REPORT_DATE","FINAL_REPORT_DATE")
PS_join_df = PS_join_df.withColumn("FINAL_REPORT_DATE", when(((col("FINAL_REPORT_DATE") == " ") | (col("FINAL_REPORT_DATE") == "0-Jan-00") | (col("FINAL_REPORT_DATE").isNull()) | (col("FINAL_REPORT_DATE") == "") | (col("FINAL_REPORT_DATE") == "#N/A") | (col("FINAL_REPORT_DATE") == "-") | (col("FINAL_REPORT_DATE") == "NA") ),"1900-01-01").otherwise(col("FINAL_REPORT_DATE")))





# #  | (col("FINAL_REPORT_DATE") == "-")                     
PS_join_df= PS_join_df.withColumn("FINAL_REPORT_DATE",changeDateFormat(col('FINAL_REPORT_DATE')))

# display(PS_join_df.select('FINAL_REPORT_DATE','PS_REFERENCE_NO_'))


# PS_join_df=PS_join_df.withColumnRenamed("PS_FINAL_REPORT_DATE","WAIVER_REMARKS")

# COMMAND ----------

# PS_join_df_filt=PS_join_df.filter(~lower(PS_join_df["CANDIDATE_IDENTIFIER"]).isin('-','tbc','na'))

# COMMAND ----------

# DBTITLE 1,Join B/w ecp_join_df & PS_join_df
PS_ecp_join_df= PS_join_df.join(ecp_join_df,(PS_join_df.CANDIDATE_IDENTIFIER==ecp_join_df.EMPLOYEE_ID)|((PS_join_df.KGS_EMAIL_ID == ecp_join_df.Rekonnect_OFFICIAL_EMAIL_ID)&(lower(PS_join_df.CANDIDATE_IDENTIFIER).isin('-','tbc','na'))), "left")
# display(PS_ecp_join_df.select('EMPLOYEE_ID','CANDIDATE_IDENTIFIER','KGS_EMAIL_ID'))

from pyspark.sql.functions import coalesce

PS_ecp_join_df=PS_ecp_join_df.withColumn('final_EMPLOYEE_ID',coalesce(PS_ecp_join_df['EMPLOYEE_ID'],PS_ecp_join_df['CANDIDATE_IDENTIFIER']))
# display(PS_ecp_join_df.select('final_EMPLOYEE_ID','EMPLOYEE_ID','CANDIDATE_IDENTIFIER'))
print(PS_ecp_join_df.count())
display(PS_ecp_join_df.groupBy("final_EMPLOYEE_ID").agg(count("final_EMPLOYEE_ID")).filter(count("final_EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,Join B/w ecp_join_df & PS_join_df

# PS_ecp_join_df= ecp_join_df.join(PS_join_df,(ecp_join_df.Rekonnect_OFFICIAL_EMAIL_ID==PS_join_df.KGS_EMAIL_ID) | (ecp_join_df.EMPLOYEE_ID==PS_join_df.CANDIDATE_IDENTIFIER), "right")

# display(PS_ecp_join_df.groupBy("EMPLOYEE_ID").agg(count("EMPLOYEE_ID")).filter(count("EMPLOYEE_ID")>1))



# PS_ecp_join_df=PS_ecp_join_df.dropDuplicates(['CANDIDATE_IDENTIFIER'])


# print(PS_ecp_join_df.count())

# display(PS_ecp_join_df.groupBy("EMPLOYEE_ID").agg(count("EMPLOYEE_ID")).filter(count("EMPLOYEE_ID")>1))

# COMMAND ----------

# DBTITLE 1,FR Output
#FR Output
# If FR Support for New (BF) is '-', then column Z (previous FR), else BF itself

# PS_ecp_join_df= PS_ecp_join_df.withColumn("FR_OUTPUT",\
#     when((PS_ecp_join_df.FR_SUPPORT_FOR_NEW.isNotNull()) | (trim(PS_ecp_join_df.FR_SUPPORT_FOR_NEW) !='') | (PS_ecp_join_df.FR_SUPPORT_FOR_NEW != '-'), PS_ecp_join_df.FR_SUPPORT_FOR_NEW)\
#     .otherwise(PS_ecp_join_df.PrevJC_CWK_FR_HISTORY))

PS_ecp_join_df= PS_ecp_join_df.withColumn("FR_OUTPUT",\
    when((PS_ecp_join_df.FR_SUPPORT_FOR_NEW.isNull()) | (trim(PS_ecp_join_df.FR_SUPPORT_FOR_NEW) == '') | (PS_ecp_join_df.FR_SUPPORT_FOR_NEW == '-'), PS_ecp_join_df.PrevJC_CWK_FR_HISTORY)\
    .otherwise(PS_ecp_join_df.FR_SUPPORT_FOR_NEW))

# COMMAND ----------

# DBTITLE 1,SR Output
PS_ecp_join_df= PS_ecp_join_df.withColumn("SR_OUTPUT",\
    when((PS_ecp_join_df.SR_SUPPORT_FOR_NEW.isNull()) | (trim(PS_ecp_join_df.SR_SUPPORT_FOR_NEW) == '') | (PS_ecp_join_df.SR_SUPPORT_FOR_NEW == '-'), PS_ecp_join_df.PrevJC_CWK_SR_HISTORY)\
    .otherwise(PS_ecp_join_df.SR_SUPPORT_FOR_NEW))

# PS_ecp_join_df= PS_ecp_join_df.withColumn("SR_OUTPUT",\
#     when((PS_ecp_join_df.SR_SUPPORT_FOR_NEW.isNotNull()) | (trim(PS_ecp_join_df.SR_SUPPORT_FOR_NEW) != '') | (PS_ecp_join_df.SR_SUPPORT_FOR_NEW != '-'), PS_ecp_join_df.SR_SUPPORT_FOR_NEW)\
#     .otherwise(PS_ecp_join_df.PrevJC_CWK_SR_HISTORY))    

# COMMAND ----------

# DBTITLE 1,Last_Green_Complete_Date
PS_ecp_join_df= PS_ecp_join_df.withColumn("LAST_GREEN_COMPLETE_DATE",\
    when((lower(PS_ecp_join_df.FR_OUTPUT) == 'green'), PS_ecp_join_df.FINAL_REPORT_DATE)\
    .when(( lower(PS_ecp_join_df.FR_OUTPUT) != 'green') & (lower(PS_ecp_join_df.SR_OUTPUT) == 'green' ), PS_ecp_join_df.PS_SUPPLIMENTRY_REPORT_DATE)\
    .otherwise(lit('-')))
# display(PS_ecp_join_df.select('LAST_GREEN_COMPLETE_DATE'))    

PS_ecp_join_df = PS_ecp_join_df.withColumn("LAST_GREEN_COMPLETE_DATE", when(((col("LAST_GREEN_COMPLETE_DATE") == " ") | (col("LAST_GREEN_COMPLETE_DATE") == "0-Jan-00") | (col("LAST_GREEN_COMPLETE_DATE").isNull()) | (col("LAST_GREEN_COMPLETE_DATE") == "") | (col("LAST_GREEN_COMPLETE_DATE") == "#N/A") | (col("LAST_GREEN_COMPLETE_DATE") == "-")),"1900-01-01").otherwise(col("LAST_GREEN_COMPLETE_DATE")))
              
PS_ecp_join_df= PS_ecp_join_df.withColumn("LAST_GREEN_COMPLETE_DATE",changeDateFormat(col("LAST_GREEN_COMPLETE_DATE")))
# display(PS_ecp_join_df.select('LAST_GREEN_COMPLETE_DATE'))

# COMMAND ----------

PS_ecp_join_df = PS_ecp_join_df.withColumn("LAST_GREEN_COMPLETE_DATE", when((col("LAST_GREEN_COMPLETE_DATE") == "1900-01-01"), "-").otherwise(col("LAST_GREEN_COMPLETE_DATE")))
PS_ecp_join_df = PS_ecp_join_df.withColumn("FINAL_REPORT_DATE", when((col("FINAL_REPORT_DATE") == "1900-01-01"), "-").otherwise(col("FINAL_REPORT_DATE")))
                                        

# COMMAND ----------

# DBTITLE 1,Designation
PS_ecp_join_df=PS_ecp_join_df.withColumn('DESIGNATION',
    when((PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY.isNotNull()) | (trim(PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY) != '') | (PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY != '-'),PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY)\
    .when((PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY.isNull()) | (trim(PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY) == '') | (PS_ecp_join_df.Rekonnect_ASSIGNMENT_CATEGORY == '-'),PS_ecp_join_df.PrevJC_CWK_DESIGNATION)\
    .otherwise(PS_ecp_join_df.cwk_DESIGNATION))

# COMMAND ----------

# DBTITLE 1,CLient Geography
PS_ecp_join_df = PS_ecp_join_df.withColumn("CLIENT_GEOGRAPHY",\
 when(((PS_ecp_join_df.Rekonnect_CLIENT_GEO.isNotNull()) | (PS_ecp_join_df.Rekonnect_CLIENT_GEO !='-') | (PS_ecp_join_df.Rekonnect_CLIENT_GEO !='')),PS_ecp_join_df.Rekonnect_CLIENT_GEO) 
 .when(((PS_ecp_join_df.Rekonnect_CLIENT_GEO.isNull()) | (PS_ecp_join_df.Rekonnect_CLIENT_GEO =='-') | (PS_ecp_join_df.Rekonnect_CLIENT_GEO =='')),PS_ecp_join_df.PrevJC_CWK_CLIENT_GEOGRAPHY)
 .otherwise(PS_ecp_join_df.GEO))
PS_ecp_join_df.select('CLIENT_GEOGRAPHY').where(PS_ecp_join_df.EMPLOYEE_ID =='649101').display()

# COMMAND ----------

# DBTITLE 1,Location
PS_ecp_join_df = PS_ecp_join_df.withColumn("LOCATION",
    when(((PS_ecp_join_df.Rekonnect_LOCATION.isNotNull()) | (PS_ecp_join_df.Rekonnect_LOCATION !='-') | (PS_ecp_join_df.Rekonnect_LOCATION !='')),PS_ecp_join_df.Rekonnect_LOCATION)
    .when(((PS_ecp_join_df.Rekonnect_LOCATION.isNull()) | (PS_ecp_join_df.Rekonnect_LOCATION =='-') | (PS_ecp_join_df.Rekonnect_LOCATION =='')),PS_ecp_join_df.PrevJC_CWK_LOCATION)
    .otherwise(PS_ecp_join_df.JOINING_LOCATION))

PS_ecp_join_df.select('LOCATION').where(PS_ecp_join_df.EMPLOYEE_ID =='649101').display()

# COMMAND ----------

waiver_final_join_df = PS_ecp_join_df.join(df_waiver_tracker,(PS_ecp_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.Waiver_BGV_REF_NO),"left")
# | (PS_ecp_join_df.CANDIDATE_EMAIL == df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID)
print(waiver_final_join_df.count())

# COMMAND ----------

#Waiver tracker join with reference number | EMAIL_id
# waiver_join_df= PS_join_df.join(df_waiver_tracker,PS_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.
# Waiver_BGV_REF_NO , "left")
# waiver_final_join_df = PS_ecp_join_df.join(df_waiver_tracker,(PS_ecp_join_df.BGV_CASE_REFERENCE_NUMBER == df_waiver_tracker.Waiver_BGV_REF_NO) | (PS_ecp_join_df.CANDIDATE_EMAIL == df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID),"left")
# | (PS_ecp_join_df.CANDIDATE_EMAIL == df_waiver_tracker.Waiver_CANDIDATE_EMAIL_ID)
# print(waiver_final_join_df.count())

# COMMAND ----------

# DBTITLE 1,Waiver Remarks
#Waiver Remarks
# waiver_final_join_df=waiver_final_join_df.withColumnRenamed("Waiver_CASE_STATUS","WAIVER_REMARKS")
waiver_final_join_df= waiver_final_join_df.withColumn("WAIVER_REMARKS",\
    when((waiver_final_join_df.Waiver_CASE_STATUS.isNotNull()) | (trim(waiver_final_join_df.Waiver_CASE_STATUS) != '') | (waiver_final_join_df.Waiver_CASE_STATUS != '-'), waiver_final_join_df.Waiver_CASE_STATUS)\
    .otherwise(lit('-')))

# COMMAND ----------

waiver_final_join_df.select('WAIVER_REMARKS').where(waiver_final_join_df.EMPLOYEE_ID =='602363').display()

# COMMAND ----------

from pyspark.sql.functions import date_format,to_date,col,when
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,BGV Exception (Y/N)
# # BGV Exception (Y/N)
# If Final Report Date (BW)< Planned Start Date (J), then 'Yes', else 'No'

waiver_final_join_df = waiver_final_join_df.withColumn("BGV_EXCEPTION__Y_N",
       when((waiver_final_join_df.FINAL_REPORT_DATE=='-') | (waiver_final_join_df.FINAL_REPORT_DATE=='') |
       (waiver_final_join_df.FINAL_REPORT_DATE=='0'),lit("No"))                                                
       .when(((waiver_final_join_df.FINAL_REPORT_DATE) < (waiver_final_join_df.PLANNED_START_DATE)),lit("Yes"))\
      .otherwise("No"))

# COMMAND ----------

# display(waiver_final_join_df.select('FINAL_REPORT_DATE','PLANNED_START_DATE','BGV_EXCEPTION__Y_N').where(waiver_final_join_df.EMPLOYEE_ID=='79623'))

# COMMAND ----------

#CEA Latest Report Color
#If SR Output(BL)<>'-', then SR Output(BL), else FR Output(BK)
waiver_final_join_df =waiver_final_join_df.withColumn('CEA_LATEST_REPORT_COLOR',\
    when((waiver_final_join_df.SR_OUTPUT == '-') | (waiver_final_join_df.SR_OUTPUT.isNull()) | (trim(waiver_final_join_df.SR_OUTPUT) == '') | (waiver_final_join_df.SR_OUTPUT == '0') ,waiver_final_join_df.FR_OUTPUT).otherwise(waiver_final_join_df.SR_OUTPUT))

# COMMAND ----------

waiver_final_join_df.select('CEA_LATEST_REPORT_COLOR').where(waiver_final_join_df.EMPLOYEE_ID=='662783').display()

# COMMAND ----------

#  BGV_STATUS_OUTPUT 
# PS_OVER_ALL_status_drop_down renamed as BGV_STATUS_FOR_NEW
# If BE (Progress Sheet - BGV Status) is '-', then consider Y3 (previous Progress Sheet -BGV Status), else if not '-', then consider BE3((Progress Sheet - BGV Status) only and if this is 'Completed' and if BM (CEA Latest Report Color Code) is not 'Green' and BV(Waiver Remarks) is 'Re-verification', then 'WIP'

# If 'Completed' and not 'Green' and 'Non-Compliant Report', then 'Report Pending'
# Else BE or Y (depending on if BE is '-' or not)




waiver_final_join_df= waiver_final_join_df.withColumn("BGV_STATUS_OUTPUT",\
    when((waiver_final_join_df.BGV_STATUS_FOR_NEW == '-' ) | ( waiver_final_join_df.BGV_STATUS_FOR_NEW.isNull()) ,waiver_final_join_df.PrevJC_CWK_BGV_STATUS_HISTORY)\
  
    .when((lower(waiver_final_join_df.BGV_STATUS_FOR_NEW)== 'completed') & (lower(waiver_final_join_df.CEA_LATEST_REPORT_COLOR)!='green') & (lower(waiver_final_join_df.WAIVER_REMARKS) == "re-verification"), lit('WIP'))\
    .when((lower(waiver_final_join_df.BGV_STATUS_FOR_NEW) == 'completed' ) & (lower(waiver_final_join_df.CEA_LATEST_REPORT_COLOR)!='green') &( lower(waiver_final_join_df.WAIVER_REMARKS)=='non-compliant report'), lit('Report Pending'))\
   .otherwise(waiver_final_join_df.BGV_STATUS_FOR_NEW))

#  .when((waiver_final_join_df.BGV_STATUS_FOR_NEW.isNotNull()) | ((waiver_final_join_df.BGV_STATUS_FOR_NEW) != '-'), waiver_final_join_df.BGV_STATUS_FOR_NEW).

# COMMAND ----------

#OVER_ALL_OUTPUT
#If BGV_STATUS_OUTPUT <> 'Completed', then BGV_STATUS_OUTPUT
#If it is 'completed', if len(SR_OUTPUT)<>'-', then SR output(BJ), else FR_OUTPUT

#Over all Output
# waiver_final_join_df =waiver_final_join_df.withColumn('OVER_ALL_OUTPUT',\
#     when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT) != 'completed') , waiver_final_join_df.BGV_STATUS_OUTPUT)\
#     .when((waiver_final_join_df.SR_OUTPUT != '-') & (waiver_final_join_df.SR_OUTPUT.isNotNull()), waiver_final_join_df.SR_OUTPUT)\
#     .otherwise(waiver_final_join_df.FR_OUTPUT))

waiver_final_join_df =waiver_final_join_df.withColumn('OVER_ALL_OUTPUT',\
    when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT) != 'completed'),waiver_final_join_df.BGV_STATUS_OUTPUT)\
    .when((waiver_final_join_df.SR_OUTPUT != '-') & (waiver_final_join_df.SR_OUTPUT.isNotNull()) & (waiver_final_join_df.SR_OUTPUT != '0'), waiver_final_join_df.SR_OUTPUT)\
    
    .otherwise(waiver_final_join_df.FR_OUTPUT))
waiver_final_join_df.select('OVER_ALL_OUTPUT').where(waiver_final_join_df.EMPLOYEE_ID=='662783').display()        
# (lower(waiver_final_join_df.BGV_STATUS_OUTPUT) == 'completed') & 

# COMMAND ----------

#Waiver Applicable New
#If OVER_ALL_OUTPUT = 'Green', then 'NA'
#Else if CEA_LATEST_REPORT_COLOR ='CEA' or 0 or '-', then 'NA'
#Else 'Yes'
waiver_final_join_df=waiver_final_join_df.withColumn('WAIVER_APPLICABLE_NEW',
    
    when(lower(waiver_final_join_df.OVER_ALL_OUTPUT)== 'green', lit('NA'))\
    .when(((waiver_final_join_df.CEA_LATEST_REPORT_COLOR == '-') | (lower(waiver_final_join_df.CEA_LATEST_REPORT_COLOR) == 'cea') | (waiver_final_join_df.CEA_LATEST_REPORT_COLOR == '0')),lit('NA'))\
    .when(((waiver_final_join_df.CEA_LATEST_REPORT_COLOR != '-') | (lower(waiver_final_join_df.CEA_LATEST_REPORT_COLOR) != 'cea') | (waiver_final_join_df.CEA_LATEST_REPORT_COLOR != '0')),lit('Yes'))\
   
    .otherwise(lit('NA')))

# COMMAND ----------

waiver_final_join_df.select('WAIVER_APPLICABLE_NEW').where(waiver_final_join_df.EMPLOYEE_ID=='659349').display()

# COMMAND ----------

#Waiver Closed New

# AF--	Waiver Closed
# BP--	Waiver Applicable New

#Waiver case status renamed as Waiver Remarks
# If WAIVER_APPLICABLE_New = 'NA', then 'NA',
# Else if PrevJC_CWK_WaiverClosed='Closed', then 'Closed'
# Else 
# Lookup Waiver Tracker using Waiver_BGV_REF_NO and check if Waiver_CASE_STATUS contains '%Closed%'
# and(or) Lookup waiver tracker using Waiver_CANDIDATE_EMAIL_ID and check if Waiver_CASE_STATUS contains '%Closed%', then 'Closed'
# Else 'Pending'



waiver_final_join_df=waiver_final_join_df.withColumn('WAIVER_CLOSED_NEW',
    when((lower(waiver_final_join_df.WAIVER_APPLICABLE_NEW)=='na'), lit('NA'))\
    .when((lower(waiver_final_join_df.PrevJC_CWK_WAIVER_CLOSED_HISTORY).like('%closed%')), lit('Closed'))\
    .when((lower(waiver_final_join_df.Waiver_CASE_STATUS).like("%closed%")), lit('Closed')).otherwise(lit('Pending')))



#need to check logic (& or |)
# Waiver_Waiver_Open_Closed & PrevJC_CWK_WAIVER_CLOSED

# COMMAND ----------

waiver_final_join_df.select('WAIVER_CLOSED_NEW').where(waiver_final_join_df.EMPLOYEE_ID=='632441').display()

# COMMAND ----------

# If BGV_STATUS_OUTPUT ='Completed' & Overall Output(BK)='Green', then Yes
# Else if BGV_STATUS_OUTPUT ='Completed', then Yes
# If BGV_STATUS_OUTPUT = 'CEA', then 'Yes except CEA'
# else if FR_OUTPUT ='CEA", then 'Yes except CEA'
# # else 'No'

# #All Checks Completed New
waiver_final_join_df =waiver_final_join_df.withColumn('ALL_CHECKS_COMPLETED_NEW',\
    when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%')) & (lower(waiver_final_join_df.OVER_ALL_OUTPUT).like('%green%')) | (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%')) , lit('Yes'))\
    .when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%cea%')) | (lower(waiver_final_join_df.FR_OUTPUT)).like('%cea%') , lit('Yes except CEA'))\
    .otherwise(lit('No'))) 

# COMMAND ----------

# DBTITLE 1,#All Checks Completed - Along With Waiver
#All Checks Completed - Along With Waiver

# IF(AND($BJ3="Completed",OR($BM3="Green",$BQ3="Closed")),"Yes","No")
# BJ=BGV Status Output
# BM=Over all Output
# BQ=Waiver Closed New


waiver_final_join_df =waiver_final_join_df.withColumn('ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER',\
   when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%')) & ((lower(waiver_final_join_df.OVER_ALL_OUTPUT).like('%green%')) | (lower(waiver_final_join_df.WAIVER_CLOSED_NEW).like('%closed%'))),lit('Yes')).otherwise(lit('No')))

# COMMAND ----------

# If Post Hire final joining Status is  joined and Employee_Status is not = '-', then Inactive.
# If  ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is "Yes", then '-'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_STATUS_OUTPUT is 'CEA', then "CEA Pending"
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_STATUS_OUTPUT is 'Completed' and WAIVER_CLOSED_New is 'Pending', then 'Waiver Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_STATUS_OUTPUT is 'Insufficiency, then 'Insufficiency'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_STATUS_OUTPUT is 'Report Pending' then 'Report Pending'
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER is 'No' and BGV_STATUS_OUTPUT is 'WIP', then 'WIP'
# All else '-'

#BGV Status (BB) BGV_STATUS
waiver_final_join_df =waiver_final_join_df.withColumn('BGV_STATUS',\
    when((lower(waiver_final_join_df.EMPLOYEE_STATUS).like('%left%')),lit("In-Active"))\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER)).like('%yes%'), lit('-'))\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%no%')) & (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%cea%')), lit('CEA Pending') )\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%no%')) & (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%')) & (lower(waiver_final_join_df.WAIVER_CLOSED_NEW).like('%pending%')), lit('Waiver Pending') )\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%no%')) & (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%insufficiency%')), lit('Insufficiency') )\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%no%')) & (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%report pending%')), lit('Report Pending') )\
    .when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%no%')) & (lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%wip%')), lit('WIP') )\
    .otherwise(lit('-')))

# (waiver_final_join_df.Employee_Status =='-') | (waiver_final_join_df.FINAL_JOINING_STATUS=='-')
# (~lower(waiver_final_join_df["FINAL_JOINING_STATUS"]).like('joined%'))|(~lower(waiver_final_join_df["FINAL_JOINING_STATUS"]).like('%mid%join%')) | 



# COMMAND ----------

waiver_final_join_df.select('BGV_STATUS').where(waiver_final_join_df.EMPLOYEE_ID=='80552').display()  

# COMMAND ----------

#Remarks
#If BGV Status (BB) is 'Waiver Pending', then fetch Waiver Remarks column (BV)
#BGV_STATUS_Self , Waiver Remarks

waiver_final_join_df =waiver_final_join_df.withColumn('REMARKS',\
    when((lower(waiver_final_join_df.BGV_STATUS).like('%waiver pending%')), waiver_final_join_df.WAIVER_REMARKS)\
    .otherwise(lit('-')))

# COMMAND ----------

# DBTITLE 1,Responsibility
# BGV Status (BD)
# Waiver Remarks (BX)

# IF($BD3="-","-",IF($BD3="In-Active","In-Active",IF(OR($BD3="CEA Pending",$BD3="Insufficiency"),"Colleague",IF(OR($BD3="Report Pending",$BD3="WIP"),"KI Forensic",IF(AND($BD3="Waiver Pending",OR($BX3="Under Review - HRBP",$BX3="Approval Awaited-HRBP/TA Head")),"HRBP Partner",IF(AND($BD3="Waiver Pending",$BX3="Approval Awaited-Risk"),"Risk Partner",IF(AND($BD3="Waiver Pending",$BX3="Clarification awaited from candidate"),"Colleague",IF(AND($BD3="Waiver Pending",$BX3="Approval to be secured"),"KGS",IF($BD3="Waiver Pending","KGS")))))))))

# if BGV_STATUS = '-' then '-' 
# if BGV_STATUS = 'in-active' then 'in-active'
# if BGV_STATUS ='CEA Pending' or Insufficiency' then 'Colleague'
# if BGV_STATUS='Report Pending' or 'WIP' then 'KI Forensic'
# if BGV_STATUS = 'Wavier Pending' and (Wavier_Remarks = 'Under Review - HRBP' or Wavier_Remarks="Approval Awaited-HRBP/TA Head") then 'HRBP Partner'
# if BGV_STATUS = 'Wavier Pending' or Wavier_Remarks = 'Approval Awaited-Risk' then 'Risk Partner
# if BGV_STATUS ='Waiver Pending' and WAIVER_REMARKS =  'Clarification awaited from ccandidate' then 'Colleague'
#  if BGV_STATUS ='Waiver Pending' and WAIVER_REMARKS = 'Approval to be secured' then 'KGS'
# if BGV_STATUS ='Waiver Pending' the 'KGS'


waiver_final_join_df = waiver_final_join_df.withColumn("RESPONSIBILITY",when((waiver_final_join_df.BGV_STATUS == '-'),lit('-'))\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='in-active'),'In-Active')\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='cea pending') | (lower(waiver_final_join_df.BGV_STATUS)=='insufficiency'),"Colleague")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='report pending') | (lower(waiver_final_join_df.BGV_STATUS)=='wip'),"KI Forensic")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='waiver pending') & ((lower(waiver_final_join_df.WAIVER_REMARKS)=='under review - hrbp') | (lower(waiver_final_join_df.WAIVER_REMARKS)=='approval awaited-hrbp/ta head')),"HRBP Partner")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='waiver pending') & (lower(waiver_final_join_df.WAIVER_REMARKS)=='approval awaited-risk'),"Risk Partner")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='waiver pending') & (lower(waiver_final_join_df.WAIVER_REMARKS)=='clarification awaited from candidate'),"Colleague")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='waiver pending') & (lower(waiver_final_join_df.WAIVER_REMARKS)=='approval to be secured'),"KGS")\
        .when((lower(waiver_final_join_df.BGV_STATUS)=='waiver pending'),"KGS").otherwise(lit('-')))    

# COMMAND ----------

# DBTITLE 1,BGV_COMPLETION_DATE_NEW
# If ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER='Yes' and BU='Tax', and PrevJC_CWK_BGV_COMPLETION_DATE <>'-', then 'PrevJC_CWK_BGV_COMPLETION_DATE', 
# else iF BGV_STATUS_OUTPUT='completed' & OVER_ALL_OUTPUT ='Green', then LAST_GREEN_COMPLETE_DATE, 
# else BGV_STATUS_OUTPUT='completed' & OVER_ALL_OUTPUT <>'Green' then lookup using case refernce number in waiver tracker,then,Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD values will be filled
# Else,Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD ='-' OR NULL, then '-' 

#BGV Completion Date New
waiver_final_join_df =waiver_final_join_df.withColumn('BGV_COMPLETION_DATE_NEW',\
 when((lower(waiver_final_join_df.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER).like('%yes%')) & (lower(waiver_final_join_df.BU).like('%tax%')) & (waiver_final_join_df.PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY !='-') | (waiver_final_join_df.PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY.isNotNull()), waiver_final_join_df.PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY )\
 .when(lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%') & (lower(waiver_final_join_df.OVER_ALL_OUTPUT).like('%green%')),waiver_final_join_df.LAST_GREEN_COMPLETE_DATE)\
 .when((lower(waiver_final_join_df.BGV_STATUS_OUTPUT).like('%completed%')) & (lower(waiver_final_join_df.OVER_ALL_OUTPUT) != 'green'),waiver_final_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD)\
 .when((waiver_final_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD=='-') & (trim(waiver_final_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD)=='') | (waiver_final_join_df.Waiver_WAIVER_RECEIVED_DATE___HRBP_TA_HEAD.isNull()),lit('-'))\
 .otherwise(lit('-')))

# waiver_final_join_df =waiver_final_join_df.withColumn('BGV_COMPLETION_DATE_NEW',lit('-'))
   

# COMMAND ----------

# DBTITLE 1,Sr No.
# from pyspark.sql.window import Window
# from pyspark.sql.functions import monotonically_increasing_id,row_number
waiver_final_join_df =waiver_final_join_df.withColumn("S_NO",row_number().over(Window.orderBy(monotonically_increasing_id())))

# COMMAND ----------

from pyspark.sql.functions import col,when
insuff_df=insuff_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in insuff_df.columns])
insuff_df = insuff_df.dropna("all")

# COMMAND ----------

insuff_list=insuff_df.select(upper('INSUFF_COMPONENT')).rdd.flatMap(lambda x: x).collect()
print(insuff_list)

# COMMAND ----------

insuff_df=waiver_final_join_df.withColumn("INSUFF_REMARKS2",regexp_replace(waiver_final_join_df["INSUFF_REMARKS"],'[^a-zA-Z0-9:]',''))

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

insuff_df = insuff_df.withColumn("INSUFF_REMARKS2", when(insuff_df.INSUFF_REMARKS2.isNull(), lit('-')).otherwise(insuff_df.INSUFF_REMARKS2))


# COMMAND ----------

# DBTITLE 1,Insuff Component Column
insuff_new = insuff_df.withColumn("INSUFF_COMPONENT", func_insuff_udf(upper(insuff_df['INSUFF_REMARKS2'])))

# COMMAND ----------

# DBTITLE 1,Calendar columns
# bgv_joined_candidate_INSUFF_COMPONENT
# bgv_joined_candidate_holiday

# COMMAND ----------

from pyspark.sql.functions import *
holiday_df=holiday_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in holiday_df.columns])
holiday_df = holiday_df.dropna("all")
# display(holiday_df)

# COMMAND ----------

# from pyspark.sql.functions import date_format,to_date
# spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
# holiday_df= holiday_df.withColumn("DATE",date_format(to_date(holiday_df.DATE,"dd-MMM-yy"),"yyyy-MM-dd"))

# COMMAND ----------


holidayList1=holiday_df.select('DATE').rdd.flatMap(lambda x: x).collect()
# for row in holidayList1:
#     print(row)



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
# from datetime import datetime
# import random
# import numpy as np
# from pyspark.sql.functions import udf
# from pyspark.sql.types import IntegerType

def get_count_working_days(sdate,edate,label=holidayList1):
    holidayList = holidayList1
    biz_days_count = np.busday_count(sdate,edate,weekmask=[1,1,1,1,1,0,0],holidays=holidayList)
    print(biz_days_count)
    return int(biz_days_count)
# testing the function
# get_count_working_days(['2023-03-08'],['2023-03-04'])
get_count_working_days_udf = udf(get_count_working_days,  IntegerType())

# COMMAND ----------

# DBTITLE 1,Function for calc due date
# import datetime
# import random
# import numpy as np
# import pandas as pd
# def get_DUE_DATE_working_days(sdate,daysToBeAdded,label=holidayList1):
#     holidayList = holidayList1
#     workingday_date = np.busday_offset(sdate,daysToBeAdded,roll="modifiedfollowing",weekmask='1111100',holidays=holidayList)
#     ts = pd.to_datetime(workingday_date)
#     print(ts.date())
#     return ts.date()
# # testing the function
# get_DUE_DATE_working_days('2023-03-08',18)
# get_DUE_DATE_working_days_udf = udf(get_DUE_DATE_working_days, DateType())

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

# COMMAND ----------

# from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Replacing NULL DOI_New with 1900-01-01
# from pyspark.sql.functions import date_format,to_date
# spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("DOI_NEW") == " ") | (col("DOI_NEW") == "0-Jan-00") | (col("DOI_NEW").isNull()) | (col("DOI_NEW") == "") | (col("DOI_NEW") == "#N/A") | (col("DOI_NEW") == "-")),"1900-01-01").otherwise(col("DOI_NEW")))

insuff_new = insuff_new.withColumn("DOI_NEW2", when(((col("PrevJC_CWK_DOI_HISTORY") == " ") | (col("PrevJC_CWK_DOI_HISTORY") == "0-Jan-00") | (col("PrevJC_CWK_DOI_HISTORY").isNull()) | (col("PrevJC_CWK_DOI_HISTORY") == "") | (col("PrevJC_CWK_DOI_HISTORY") == "#N/A") | (col("PrevJC_CWK_DOI_HISTORY") == "-")),lit("1900-01-01")).otherwise(col("PrevJC_CWK_DOI_HISTORY")))
                     
insuff_new_cal = insuff_new.withColumn("DOI_NEW2",changeDateFormat(col("DOI_NEW2")))



# insuff_new_cal= insuff_new.withColumn("DOI_NEW2",date_format(to_date(insuff_new.DOI_NEW2,"dd-MMM-yy"),"yyyy-MM-dd"))


# COMMAND ----------

# DBTITLE 1,calling function  to calculate Due_Date
#DOI_previous -->  DOI_NEW need confirmation
daysToBeAdded = 18
calendardf = insuff_new_cal.withColumn("DaysToBeAdded",lit(daysToBeAdded).cast(IntegerType()))

calendardf = calendardf.withColumn("DUE_DATE_n_temp", get_DUE_DATE_working_days_udf(calendardf['DOI_NEW2'],calendardf['DaysToBeAdded']))

# COMMAND ----------


calendardf = calendardf.withColumn("DUE_DATE",
                                         when((col("DOI_NEW2") <= "1901-01-25")| (col("DOI_NEW2").isNull()), lit("-"))
                                        .otherwise(col("DUE_DATE_n_temp")))

# COMMAND ----------

# DBTITLE 1,BGV_Check_completed_along_with_Waiver_Except_CEA_and_Campus_education
# 1.Select Joined from post hire CWK final joining status & (Employee status column is not equal to left  and IF BGV Status output is either completed/CEA & Waiver Closed New is closed/ na then 'yes'.

# 2.Select Joined from post hire CWK final joining status & (Employee status column is not equal to left  and If BGV Status output is either completed/CEA & Waiver Closed New is pending, then 'No'

# 3.Select Joined from post hire CWK final joining status & (Employee status column is not equal to left and If BGV status output =insufficiency & Insuff remarks contains/like'Pursuing' and Insuff_Component contains 'EDU' then 'yes' 

#  4.Select Joined from post hire CWK final joining status & (Employee status column is not equal to left and If BGV status output=insufficiency & Insuff remarks not contain/like 'Pursuing' then 'no'.
# 5.Select Joined from post hire CWK final joining status & (Employee status column is not equal to left and If BGV status output(BJ)=wip /report pending then 'no'
# .otherwise('no')




# COMMAND ----------

calendardf = calendardf.withColumn('INSUFF_COMPONENT_regexp', regexp_replace(col('INSUFF_COMPONENT'), "[^a-zA-Z0-9\\s,]", ""))
# display(calendardf.select('INSUFF_COMPONENT_regexp','INSUFF_COMPONENT'))

# COMMAND ----------

calendardf = calendardf.withColumn("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION",
                                   
    when((lower(calendardf.EMPLOYEE_STATUS)!='left') & (lower(calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY)=='yes'), lit('Yes'))\

    

    
    .when((lower(calendardf.EMPLOYEE_STATUS)=='left') & (lower(calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY)=='yes'), lit('Yes'))
    
    



    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') &((lower(calendardf.BGV_STATUS_OUTPUT).contains('cea')) | (lower(calendardf.BGV_STATUS_OUTPUT).contains('completed')) ) & ((lower(calendardf.OVER_ALL_OUTPUT).contains('green')) | (lower(calendardf.WAIVER_CLOSED_NEW).contains('closed'))| (lower(calendardf.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)=='left') &((lower(calendardf.BGV_STATUS_OUTPUT).contains('cea')) | (lower(calendardf.BGV_STATUS_OUTPUT).contains('completed')) ) & ((lower(calendardf.OVER_ALL_OUTPUT).contains('green')) | (lower(calendardf.WAIVER_CLOSED_NEW).contains('closed'))| (lower(calendardf.WAIVER_CLOSED_NEW)=='na')) , lit('Yes'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') & ((lower(calendardf.BGV_STATUS_OUTPUT).contains('cea')) | (lower(calendardf.BGV_STATUS_OUTPUT)=='completed'))  & (lower(calendardf.WAIVER_CLOSED_NEW).contains('pending')) , lit('No'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') &(lower(calendardf.BGV_STATUS_OUTPUT).contains('insuf')) & (lower(calendardf.INSUFF_REMARKS).contains('pursuing'))  & (lower(calendardf.INSUFF_COMPONENT_regexp).contains('edu')) & (length(trim(calendardf.INSUFF_COMPONENT_regexp))<=4),lit('Yes'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') & (lower(calendardf.BGV_STATUS_OUTPUT).contains('insuf')) & (~lower(calendardf.INSUFF_REMARKS).contains('pursuing')) , lit('No'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') & (lower(calendardf.BGV_STATUS_OUTPUT).contains('wip'))| (lower(calendardf.BGV_STATUS_OUTPUT).contains('report pending')), lit('No'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)!='left') & ((calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY=='-') | (lower(calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY)=='no')), lit('No'))\

    .when((lower(calendardf.EMPLOYEE_STATUS)=='left') & ((calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY=='-')|
    (lower(calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY)=='no')) , lit('No'))

    # .when( ((lower(calendardf.FINAL_JOINING_STATUS)=='joined') & (lower(calendardf.EMPLOYEE_STATUS)=='left')) |((calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY.isNull()) | (calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY == '-' )),lit('No'))\

    

    .otherwise(lit("No")))


# COMMAND ----------

display(calendardf.select('BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION').where(calendardf.EMPLOYEE_ID =='662782'))

# COMMAND ----------

# DBTITLE 1,Suspicious Company New
#Suspicious Company New

#BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION = = 'No' & BU='Consulting' & SUSPICIOUS_COMPANY_previous<> ='-', then AK,  else else lookup Progress Sheet using BGV Case reference no and fetch EDU/EMP Name (Suspicious) (T)

calendardf=calendardf.withColumn('SUSPICIOUS_COMPANY_NEW', when( (lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no') & (lower(calendardf.BU).like('%consulting%')) & (calendardf.PrevJC_CWK_SUSPICIOUS_COMPANY_HISTORY != '-') & (calendardf.PrevJC_CWK_SUSPICIOUS_COMPANY_HISTORY.isNotNull()) , calendardf.PrevJC_CWK_SUSPICIOUS_COMPANY_HISTORY).otherwise(calendardf.PS_EDU_EMP_NAME__SUSPICIOUS_))

# COMMAND ----------

# DBTITLE 1,Previous_Yes
# If BGV Check completd along with Waiver(Except CEA and Campus education) is '-' or no , then Previous Yes ='-'.
# If BGV Check completd along with Waiver(Except CEA and Campus education) is 'yes' then previous yes ='yes'
# if any new records for any columns then previous yes will be '-'

# calendardf =calendardf.withColumn('PREVIOUS_YES',\
#    when(col("KGS_EMAIL_ID").isin(df_cwk_new.KGS_EMAIL_ID),lit('-'))
#    .when((lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'no'),lit('-')).otherwise(col("BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION")))
calendardf=calendardf.withColumn("PREVIOUS_YES",
   when((lower(calendardf.PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY) == 'yes') ,lit("Yes"))\
   
   .otherwise(lit('-')) )
# .when((lower(calendardf.PrevJC_CWK_PREVIOUS_YES) == 'no')| (lower(calendardf.PrevJC_CWK_PREVIOUS_YES) == '-'),lit("-"))\
   # .when(((lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION) == 'yes') & (lower(calendardf.OLD_OR_NEW_DATA) == 'old')), lit("Yes"))\
   # .when(((lower(calendardf.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION)
   # == 'no') & (lower(calendardf.OLD_OR_NEW_DATA) == 'old')), lit("No"))

# calendardf.select('PREVIOUS_YES','PrevJC_CWK_PREVIOUS_YES').where(calendardf.EMPLOYEE_ID=='659606').display()
   
   

# COMMAND ----------

calendardf.select('PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION','PrevJC_CWK_BGV_CASE_REFERENCE_NUMBER','PREVIOUS_YES').where(calendardf.EMPLOYEE_ID=='662782').display()
# df_prev_jc_cwk.select('PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY','PrevJC_CWK_File_Date','PrevJC_CWK_BGV_CASE_REFERENCE_NUMBER').where(df_prev_jc_cwk.PrevJC_CWK_EMPLOYEE_ID=='662782').display()

# COMMAND ----------

# DBTITLE 1,#Offer Release Date(Not required as per Pritam)
# calendardf = calendardf.withColumn("Offer_Release_Date2", when(((col("Offer_Release_Date") == " ")  | (col("Offer_Release_Date") == "-")| (col("Offer_Release_Date") == "0-Jan-00") | (col("Offer_Release_Date").isNull()) | (col("Offer_Release_Date") == "") | (col("Offer_Release_Date") == "#N/A") | (col("Offer_Release_Date") == "None")),lit("1900-01-01")).otherwise(col("Offer_Release_Date")))

                     
# calendardf = calendardf.withColumn("Offer_Release_Date2",changeDateFormat(col("Offer_Release_Date2")))

# COMMAND ----------

# DBTITLE 1,Ageing from DOO(Not required as per Pritam)
# AGEING_FROM_DOO
# Difference between Offer Release Date and CurrentDate excluding Sat & Sun
# calendardf = calendardf.withColumn("AGEING_FROM_DOO_temp", get_count_working_days_udf(calendardf['Offer_Release_Date2'],current_date()))

# COMMAND ----------

# DBTITLE 1,Ageing from DOI
#Difference between Prev Report's DOI and CurrentDate excluding Sat & Sun
calendardf= calendardf.withColumn("AGEING_FROM_DOI_temp", get_count_working_days_udf(calendardf['DOI_NEW2'],current_date()))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOI",
                                         when((col("DOI_NEW2") <= "1901-01-25")| (col("DOI_NEW2").isNull()), "-")
                                        .otherwise(col("AGEING_FROM_DOI_temp")))

# COMMAND ----------

# DBTITLE 1,Ageing from DOJ
#Planned Start Date
calendardf = calendardf.withColumn("PLANNED_START_DATE2", when(((col("PLANNED_START_DATE") == " ") | (col("PLANNED_START_DATE") == "0-Jan-00") | (col("PLANNED_START_DATE").isNull()) | (col("PLANNED_START_DATE") == "") | (col("PLANNED_START_DATE") == "#N/A")),"1900-01-01").otherwise(col("PLANNED_START_DATE")))
                     
calendardf = calendardf.withColumn("PLANNED_START_DATE2",changeDateFormat(col("PLANNED_START_DATE2")))
# calendardf= calendardf.withColumn("PLANNED_START_DATE2",date_format(to_date(calendardf.PLANNED_START_DATE2,"yyyyMMdd"),"yyyy-MM-dd"))



# COMMAND ----------

#Difference between Planned Start Date and CurrentDate excluding Sat & Sun
calendardf= calendardf.withColumn("AGEING_FROM_DOJ_temp", get_count_working_days_udf(calendardf['PLANNED_START_DATE2'],current_date()))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ",
                                         when((col("PLANNED_START_DATE2") <= "1901-01-25")| (col("PLANNED_START_DATE2").isNull()), "-")
                                        .otherwise(col("AGEING_FROM_DOJ_temp")))

# COMMAND ----------

# DBTITLE 1,Ageing from DOJ (Calender Day)
calendardf = calendardf.withColumn("AGEING_FROM_DOJ_CALENDER_DAY_temp",datediff(current_date(),col("PLANNED_START_DATE2")))

# COMMAND ----------

calendardf = calendardf.withColumn("AGEING_FROM_DOJ_CALENDER_DAY",
                                         when((col("PLANNED_START_DATE2") <= "1901-01-25")| (col("PLANNED_START_DATE2").isNull()), "-")
                                        .otherwise(col("AGEING_FROM_DOJ_CALENDER_DAY_temp")))

# COMMAND ----------

# DBTITLE 1,Ageing Bucket
calendardf=calendardf.withColumn("AGEING_FROM_DOJ", calendardf['AGEING_FROM_DOJ'].cast('int'))

#If Ageing from DOJ,0-30, 31-60, 61-90, 91-120, 121-180 and Greater than 180 df.value < 1) 
calendardf = calendardf.withColumn("AGEING_BUCKET",
       when(calendardf.AGEING_FROM_DOJ > 180, "Greater than 180")
      .when((calendardf.AGEING_FROM_DOJ >120) & (calendardf.AGEING_FROM_DOJ <=180), "121-180")
      .when((calendardf.AGEING_FROM_DOJ >=91) & (calendardf.AGEING_FROM_DOJ < 121), "91-120")
      .when((calendardf.AGEING_FROM_DOJ >=61) & (calendardf.AGEING_FROM_DOJ <=90), "61-90")
      .when((calendardf.AGEING_FROM_DOJ >=31) & (calendardf.AGEING_FROM_DOJ < 61), "31-60")
      .otherwise("0-30"))

# COMMAND ----------

# DBTITLE 1,Ageing Bucket(Calender Day)
calendardf=calendardf.withColumn("AGEING_FROM_DOJ_CALENDER_DAY", calendardf['AGEING_FROM_DOJ_CALENDER_DAY'].cast('int'))


#If Ageing from DOJ,0-30, 31-60, 61-90, 91-120, 121-180 and Greater than 180 df.value < 1) 
calendardf = calendardf.withColumn("AGEING_BUCKET_CALENDER_DAY",
       when(calendardf.AGEING_FROM_DOJ_CALENDER_DAY > 180, "Greater than 180")
      .when((calendardf.AGEING_FROM_DOJ_CALENDER_DAY >=120) & (calendardf.AGEING_FROM_DOJ_CALENDER_DAY <=180), "121-180")
      .when((calendardf.AGEING_FROM_DOJ_CALENDER_DAY >91) & (calendardf.AGEING_FROM_DOJ_CALENDER_DAY < 121), "91-120")
      .when((calendardf.AGEING_FROM_DOJ_CALENDER_DAY >=61) & (calendardf.AGEING_FROM_DOJ_CALENDER_DAY <=90), "61-90")
      .when((calendardf.AGEING_FROM_DOJ_CALENDER_DAY >=31) & (calendardf.AGEING_FROM_DOJ_CALENDER_DAY < 61), "31-60")
      .otherwise("0-30"))

# COMMAND ----------

# DBTITLE 1,DOJ Month
#Derive only Month & Year (Aug-23)
calendardf = calendardf.withColumn('DOJ_MONTH', date_format(col('PLANNED_START_DATE2'),"MMM-yy"))

# COMMAND ----------

calendardf=calendardf.withColumn("OFFER_RELEASE_DATE",lit('-'))
calendardf=calendardf.withColumn("OFFER_ACCEPTANCE_DATE",lit('-'))
calendardf=calendardf.withColumn("AGEING_FROM_DOO",lit('-'))

# COMMAND ----------

#Adding current timestamp to Dated_On and File_Date
from datetime import *
currentdatetime= datetime.now()
calendardf = calendardf.withColumn("File_Date", lit(FileDate)).withColumn("Dated_On",lit(currentdatetime))
calendardf = calendardf.withColumnRenamed('FINAL_JOINING_STATUS','STATUS')



# COMMAND ----------

calendardf=calendardf.withColumn("PLANNED_START_DATE",date_format(calendardf.PLANNED_START_DATE,"dd-MMM-yy"))
calendardf=calendardf.withColumn("PrevJC_CWK_DOI_HISTORY",date_format(calendardf.PrevJC_CWK_DOI_HISTORY,"dd-MMM-yy"))
calendardf=calendardf.withColumn("PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY",date_format(calendardf.PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY,"dd-MMM-yy"))
calendardf=calendardf.withColumn("DOI_NEW",date_format(calendardf.DOI_NEW,"dd-MMM-yy"))
calendardf=calendardf.withColumn("DUE_DATE",date_format(calendardf.DUE_DATE,"dd-MMM-yy"))
calendardf=calendardf.withColumn("BGV_COMPLETION_DATE_NEW",date_format(calendardf.BGV_COMPLETION_DATE_NEW,"dd-MMM-yy"))
calendardf=calendardf.withColumn("LAST_GREEN_COMPLETE_DATE",date_format(calendardf.LAST_GREEN_COMPLETE_DATE,"dd-MMM-yy"))
calendardf=calendardf.withColumn("FINAL_REPORT_DATE",date_format(calendardf.FINAL_REPORT_DATE,"dd-MMM-yy"))
calendardf=calendardf.withColumn("File_Date",date_format(calendardf.File_Date,"dd-MMM-yy"))



# display(calendardf.select('PLANNED_START_DATE'))


# COMMAND ----------

final_df = calendardf.select('S_NO','EMPLOYEE_ID','BGV_CASE_REFERENCE_NUMBER','CANDIDATE_FULL_NAME','COST_CENTRE','CLIENT_GEOGRAPHY','DESIGNATION','LOCATION','CANDIDATE_EMAIL','PLANNED_START_DATE','RECRUITER','STATUS','EMPLOYEE_CATEGORY','SOURCE','NAME_OF_SUB_SOURCE','BU','PrevJC_CWK_OFFICIAL_EMAIL_ID_HISTORY','OFFER_RELEASE_DATE','OFFER_ACCEPTANCE_DATE','INDIA_PROJECT_CODE','PrevJC_CWK_HRBP_NAME_HISTORY','PrevJC_CWK_PERFORMANCE_MANAGER_HISTORY','PrevJC_CWK_SUPERVISOR_NAME_HISTORY','PrevJC_CWK_DOI_HISTORY','PrevJC_CWK_BGV_STATUS_HISTORY','PrevJC_CWK_FR_HISTORY','PrevJC_CWK_SR_HISTORY','PrevJC_CWK_OVER_ALL_HISTORY','MANDATE_CHECKS_COMPLETED','PrevJC_CWK_ALL_CHECKS_COMPLETED_HISTORY','PrevJC_CWK_WAIVER_APPLICABLE_HISTORY','PrevJC_CWK_WAIVER_CLOSED_HISTORY','PrevJC_CWK_BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_HISTORY','CANDIDATE_IDENTIFIER','PrevJC_CWK_ALL_CHECKS_COMPLETED___ALONG_WITH_WAIVER_HISTORY','PrevJC_CWK_BGV_COMPLETION_DATE_HISTORY','PrevJC_CWK_SUSPICIOUS_COMPANY_HISTORY','ASSIGNMENT_CATEGORY','EMPLOYEE_STATUS','OFFICIAL_EMAIL_ID','HRBP_NAME','PERFORMANCE_MANAGER','SUPERVISOR_NAME','INSUFF_REMARKS','INSUFF_COMPONENT','DOI_NEW','DUE_DATE','AGEING_FROM_DOO','AGEING_FROM_DOI','AGEING_FROM_DOJ','AGEING_BUCKET','DOJ_MONTH','AGEING_FROM_DOJ_CALENDER_DAY','AGEING_BUCKET_CALENDER_DAY','BGV_STATUS','REMARKS','RESPONSIBILITY','BGV_STATUS_FOR_NEW','FR_SUPPORT_FOR_NEW','SR_SUPPORT_FOR_NEW','BGV_STATUS_OUTPUT','FR_OUTPUT','SR_OUTPUT','OVER_ALL_OUTPUT','ALL_CHECKS_COMPLETED_NEW','CEA_LATEST_REPORT_COLOR','WAIVER_APPLICABLE_NEW','WAIVER_CLOSED_NEW','BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION','ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER','BGV_COMPLETION_DATE_NEW','LAST_GREEN_COMPLETE_DATE','SUSPICIOUS_COMPANY_NEW', 'BGV_EXCEPTION__Y_N','WAIVER_REMARKS','FINAL_REPORT_DATE','PREVIOUS_YES','File_Date')

# COMMAND ----------

display(final_df.select('PLANNED_START_DATE').where(final_df.BGV_CASE_REFERENCE_NUMBER.like('%KPMG%00040312%KGS%2024%')))

# COMMAND ----------

# final_df = final_df.withColumnRenamed('FINAL_JOINING_STATUS','STATUS')

# COMMAND ----------

#replace null values to '-'

final_df=replacenull(final_df)
final_df=leadtrailremove(final_df)


# COMMAND ----------

#special character columns

for col in final_df.columns:
     final_df=final_df.withColumn(col,ascii_udf(col))
# final_df =leadtrailremove(final_df)
print(final_df.count())

# COMMAND ----------



#Adding current timestamp to Dated_On and File_Date

# # from datetime import *
# currentdatetime= datetime.now()
# final_df = final_df.withColumn("Dated_On",lit(currentdatetime)).withColumn("File_Date", lit(FileDate))
# display(final_df)

# COMMAND ----------

# display(final_df)

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

# DBTITLE 1,Loading Trusted Curr and Hist Tables
# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_stg_bgv_joined_candidate_cwk

# COMMAND ----------

