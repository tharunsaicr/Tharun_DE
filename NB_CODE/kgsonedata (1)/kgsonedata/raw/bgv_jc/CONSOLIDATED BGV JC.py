# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace,col, trim, lower
#from pyspark.sql.functions import col, trim, lower
from datetime import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

tableName = "consolidated"
processName = "bgv_joined_candidate" 

# COMMAND ----------

# DBTITLE 1,Reading FTE_FTS Current Table
fte_fts = spark.sql("""select EMPLOYEE_ID,BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,COST_CENTRE,CLIENT_GEOGRAPHY,DESIGNATION,LOCATION,PLANNED_START_DATE,RECRUITER,EMPLOYEE_CATEGORY,BU,MANDATE_CHECKS_COMPLETED,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER  as ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION,HRBP_NAME,PERFORMANCE_MANAGER,INSUFF_REMARKS,INSUFF_COMPONENTS,AGEING_BUCKET,BGV_COMPLETION_DATE_NEW,BGV_STATUS_SELF as BGV_STATUS,REMARKS,RESPONSIBILITY,SUSPICIOUS_COMPANY_NEW,SOURCE,BGV_EXCEPTION_Y_N,Dated_On,File_Date
 from  kgsonedatadb.trusted_bgv_joined_candidate_fte_fts where lower(EMPLOYEE_STATUS) != 'left'  """)
print(fte_fts.count())
display(fte_fts)
#14301

# COMMAND ----------

# DBTITLE 1,Reading KI Loaned Current Table
ki_loaned = spark.sql("""select EMPLOYEE_ID,BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,COST_CENTRE,CLIENT_GEOGRAPHY,DESIGNATION,LOCATION,PLANNED_START_DATE,RECRUITER,EMPLOYEE_CATEGORY,BU,MANDATE_CHECKS_COMPLETED,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER  as ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION,HRBP_NAME,PERFORMANCE_MANAGER,INSUFF_REMARKS,INSUFF_COMPONENT as INSUFF_COMPONENTS,AGEING_BUCKET,BGV_COMPLETION_DATE_NEW,BGV_STATUS_SELF as BGV_STATUS,REMARKS,RESPONSIBILITY,SUSPICIOUS_COMPANY_NEW,SOURCE, BGV_EXCEPTION_Y_N,Dated_On,File_Date
 from  kgsonedatadb.trusted_bgv_joined_candidate_ki_loaned where lower(EMPLOYEE_STATUS) != 'left' """)
display(ki_loaned)#386
print(ki_loaned.count())

# COMMAND ----------

# DBTITLE 1,Reading CWK Current Table
cwk = spark.sql(""" select EMPLOYEE_ID,BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,COST_CENTRE,CLIENT_GEOGRAPHY,DESIGNATION,LOCATION,PLANNED_START_DATE,RECRUITER,EMPLOYEE_CATEGORY,BU,MANDATE_CHECKS_COMPLETED,ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION,HRBP_NAME,PERFORMANCE_MANAGER,INSUFF_REMARKS,INSUFF_COMPONENT as INSUFF_COMPONENTS,AGEING_BUCKET,BGV_COMPLETION_DATE_NEW,BGV_STATUS ,REMARKS,RESPONSIBILITY,SUSPICIOUS_COMPANY_NEW,SOURCE,BGV_EXCEPTION__Y_N as BGV_EXCEPTION_Y_N, Dated_On,File_Date from  kgsonedatadb.trusted_bgv_joined_candidate_cwk where lower(EMPLOYEE_STATUS)!= 'left' """)
print(cwk.count()) #232 1037
display(cwk) #107

# COMMAND ----------

# DBTITLE 1,Unioning above 3 dataframes
#Union fte_fts and KI loaned
print(fte_fts.count())
print(ki_loaned.count())
print(cwk.count())
fte_ki_con = fte_fts.union(ki_loaned) 
print(fte_ki_con.count())
# display(fte_ki_con.filter(fte_ki_con.REPORT_NAME=='KI_LOANED'))
con_overall = fte_ki_con.union(cwk) 
#display(con_overall)
print(con_overall.count())

#14301
#386
#107
#14687

# COMMAND ----------

# DBTITLE 1,Reading CEA current table
cea_raw = spark.sql("""select *  from  kgsonedatadb.trusted_bgv_cea """)
# dateList = ['CASE_INITIATION_DATE']
#cea=cea.withColumn('CASE_INITIATION_DATE',changeDateFormat('CASE_INITIATION_DATE'))
print(cea_raw.count())
dateList = ['CASE_INITIATION_DATE','UPDATED_DOJ','CEA_STOP_CHECK_INITIATION_DATE']
for columnName in cea_raw.columns:
    if (columnName in dateList):
        cea_raw = cea_raw.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(cea_raw[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(cea_raw[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))      
        cea_raw=cea_raw.withColumn(columnName,changeDateFormat(columnName))
cea_raw.createOrReplaceTempView('cea_temp')  

cea = spark.sql("select CASE_REFERENCE_NUMBER,STATUS from (select rank() over(partition by CASE_REFERENCE_NUMBER  order by Updated_DOJ desc ) as rn, * from cea_temp ) hist where rn = 1")  

display(cea.groupBy("CASE_REFERENCE_NUMBER").agg(count("CASE_REFERENCE_NUMBER")).filter(count("CASE_REFERENCE_NUMBER")>1))
print(cea.count())
cea=cea.dropDuplicates(['CASE_REFERENCE_NUMBER','STATUS'])
print(cea.count())
display(cea.groupBy("CASE_REFERENCE_NUMBER").agg(count("CASE_REFERENCE_NUMBER")).filter(count("CASE_REFERENCE_NUMBER")>1))
#display(df_fte_new)


# COMMAND ----------

# DBTITLE 1,Reading Insufficiency current table
insuff_raw = spark.sql("""select * from  kgsonedatadb.trusted_hist_bgv_insufficiency where FILE_DATE='20240412' """)

print(insuff_raw.count())
dateList = ['CASE_INITIATION_DATE','INSUFFICIENCY_CLOSURE_DATE']
for columnName in insuff_raw.columns:
    if (columnName in dateList):
        insuff_raw = insuff_raw.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(insuff_raw[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(insuff_raw[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA")),"1900-01-01").otherwise(col(columnName)))      
        insuff_raw=insuff_raw.withColumn(columnName,changeDateFormat(columnName))
insuff_raw.createOrReplaceTempView('insuff_temp')  

insuff = spark.sql("select * from (select rank() over(partition by REFERENCE_NO_  order by INSUFFICIENCY_CLOSURE_DATE desc ) as rn, * from insuff_temp ) hist where rn = 1") 

display(insuff.groupBy("REFERENCE_NO_").agg(count("REFERENCE_NO_")).filter(count("REFERENCE_NO_")>1))
print(insuff.count())
insuff=insuff.dropDuplicates(['REFERENCE_NO_','KGS_STATUS'])
print(insuff.count())
display(insuff.groupBy("REFERENCE_NO_").agg(count("REFERENCE_NO_")).filter(count("REFERENCE_NO_")>1))
# display(insuff)

# COMMAND ----------

insuff_raw = spark.sql("""select * from  kgsonedatadb.trusted_hist_bgv_insufficiency where upper(INSUFFICIENCY_COMPONENT) ='EDU' and KGS_STATUS !='-' and KGS_STATUS is not null""")
#display(insuff_raw)
print(insuff_raw.count())
display(insuff.groupBy("REFERENCE_NO_").agg(count("REFERENCE_NO_")).filter(count("REFERENCE_NO_")>1))
insuff=insuff_raw.dropDuplicates(['REFERENCE_NO_','KGS_STATUS'])
#display(insuff.groupBy("REFERENCE_NO_").agg(count("REFERENCE_NO_")).filter(count("REFERENCE_NO_")>1))
insuff.createOrReplaceTempView('insuff_temp')
print(insuff.count())


# COMMAND ----------

#Creating priorty dataframe

# Drop - 1
# Open - 2
# Closed - 3
# Stop-check - 4

df_priority = spark.createDataFrame([ 
    Row(STATUS='drop', PRIORITY=1),
    Row(STATUS='open', PRIORITY=2),
    Row(STATUS='closed', PRIORITY=3),
    Row(STATUS='stop-check', PRIORITY=4)     
]) 

display(df_priority)

# COMMAND ----------

# DBTITLE 1,Joining Insuff and df_priority
insuff_join = insuff.join(df_priority,(lower(insuff.KGS_STATUS) == df_priority.STATUS ),'left')


# COMMAND ----------

insuff_join=insuff_join.withColumn('STATUS',when(insuff_join.STATUS.isNull(),lit('open')).otherwise(insuff_join.STATUS)).withColumn('PRIORITY',when(insuff_join.PRIORITY.isNull(),lit(2)).otherwise(insuff_join.PRIORITY))

#creating temp view
insuff_join.createOrReplaceTempView('insuff_join_temp')

# COMMAND ----------

display(insuff_join.select(insuff_join.REFERENCE_NO_, insuff_join.KGS_STATUS,insuff_join.STATUS,insuff_join.PRIORITY))
#.filter(insuff_join.KGS_STATUS.isNull()))

# COMMAND ----------

# After this, do a group by on EmpId,Status,Rank and get count
# This will give which employee_id has what count..
# Now do the below transformation for only the Employee_Id where  count>1 and status = open
# drop duplicates only these employeeId and also update the status to "Education Documents Pending"

insuff = spark.sql("select * from (select rank() over(partition by REFERENCE_NO_  order by PRIORITY desc ) as rn, * from insuff_join_temp ) hist where rn = 1") 
display(insuff.count())

insuff=insuff.dropDuplicates(['REFERENCE_NO_','KGS_STATUS'])
display(insuff.count())
display(insuff.groupBy("REFERENCE_NO_").agg(count("REFERENCE_NO_")).filter(count("REFERENCE_NO_")>1))
# display(insuff.select(insuff.REFERENCE_NO_, insuff.KGS_STATUS).filter(insuff.REFERENCE_NO_).like('KPMG%00110795%KGS%2023'))
# display(df_ed.groupBy("ED_EMPLOYEE_NUMBER").agg(count("ED_EMPLOYEE_NUMBER")).filter(count("ED_EMPLOYEE_NUMBER")>1))
# df_fte_new.filter(lower(df_fte_new.STATUS).like('%join%')  &(~lower(df_fte_new.STATUS).like('%not%')))

# COMMAND ----------


con_overall = con_overall.withColumn("AGEING_BUCKET", \
    when((lower(con_overall.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER)=='yes'),lit(''))\
    .otherwise(con_overall.ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER))
display(con_overall)

# COMMAND ----------

# DBTITLE 1,Joining CEA
con_overall = con_overall.join(cea,(con_overall.BGV_CASE_REFERENCE_NUMBER == cea.CASE_REFERENCE_NUMBER ),"left")
print((con_overall).count())

# COMMAND ----------

# DBTITLE 1,Joining Insufficiency
con_overall = con_overall.join(insuff,(con_overall.BGV_CASE_REFERENCE_NUMBER == insuff.REFERENCE_NO_ ),"left")
print(con_overall.count())

# COMMAND ----------

display(con_overall)

# COMMAND ----------

# DBTITLE 1,CEA Dependancy Logic
# If BGV Status is “CEA Pending” and Output is “CEA initiated” then “Remarks” should be “CEA WIP” and “Responsibility “should be “KI Forensic “, if BGV Status is “CEA Pending” and Output is not “CEA initiated” then “Remarks” should be “CEA Documents Pending” and responsibility should be “Colleague”

con_overall = con_overall.withColumn("REMARKS", \
    when((lower(trim(con_overall.BGV_STATUS))=='cea pending') &  (lower(trim(con_overall.CEA_KGS_STATUS))=='cea initiated'),lit('CEA WIP'))\
    .when((lower(trim(con_overall.BGV_STATUS))=='cea pending') & (lower(trim(con_overall.CEA_KGS_STATUS))!='cea initiated'),lit('CEA Documents Pending'))
    .otherwise(con_overall.REMARKS))


con_overall = con_overall.withColumn("RESPONSIBILITY", \
    when((lower(trim(con_overall.BGV_STATUS))=='cea pending') &  (lower(trim(con_overall.CEA_KGS_STATUS))=='cea initiated') ,lit('KI Forensic'))\
    .when((lower(trim(con_overall.BGV_STATUS))=='cea pending') & (lower(trim(con_overall.CEA_KGS_STATUS))!='cea initiated') ,lit('Colleague'))
    .otherwise(con_overall.RESPONSIBILITY))



# COMMAND ----------

# DBTITLE 1,Insufficiency Dependancy Logic
# If BGV Status is “Insufficiency” and Output is “Closed”   and insuff_component contains ‘EDUCATION’ then “Remarks” should be “Education WIP” and “Responsibility “should be “KI Forensic “, 
# if BGV Status is “Insufficiency” and Output is not “Closed”  and insuff_component contains ‘EDUCATION’ then “Remarks” should be “Education Documents Pending” and responsibility should be “Colleague”.

con_overall = con_overall.withColumn("REMARKS", \
    when((lower(trim(con_overall.BGV_STATUS))=='insufficiency') & (lower(trim(con_overall.INSUFF_KGS_STATUS))=='closed') & (lower(trim(con_overall.INSUFF_COMPONENTS)).contains('education')),lit('Education WIP'))\
    .when((lower(trim(con_overall.BGV_STATUS))=='insufficiency') & (lower(trim(con_overall.INSUFF_KGS_STATUS))!='closed') & (lower(trim(con_overall.INSUFF_COMPONENTS)).contains('education')),lit('Education Documents Pending'))
    .otherwise(con_overall.REMARKS))

con_overall = con_overall.withColumn("RESPONSIBILITY", \
    when((lower(trim(con_overall.BGV_STATUS))=='insufficiency') & (lower(trim(con_overall.INSUFF_KGS_STATUS))=='closed') & (lower(trim(con_overall.INSUFF_COMPONENTS)).contains('education')),lit('KI Forensic'))\
    .when((lower(trim(con_overall.BGV_STATUS))=='insufficiency') & (lower(trim(con_overall.INSUFF_KGS_STATUS))!='closed') & (lower(trim(con_overall.INSUFF_COMPONENTS)).contains('education')) ,lit('Colleague'))
    .otherwise(con_overall.REMARKS))

display(con_overall)


# COMMAND ----------

con_overall=con_overall.drop('CEA_KGS_STATUS','INSUFF_KGS_STATUS','CASE_REFERENCE_NUMBER','REFERENCE_NO_')

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration/

# COMMAND ----------

print("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)
print("kgsonedatadb.trusted_"+ processName + "_" + tableName)
print(trusted_stg_savepath_url+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1, Loading trusted stg table
con_overall.write \
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
# MAGIC select count(*) from kgsonedatadb.trusted_stg_bgv_joined_candidate_consolidated --20217 14413