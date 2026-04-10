-- Databricks notebook source
-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC delta_table_path = "dbfs:/mnt/configmount/bu_mapping_list"
-- MAGIC
-- MAGIC # DeltaTable.forPath(spark, delta_table_path).toDF().createOrReplaceTempView("test_employee_details")
-- MAGIC df = DeltaTable.forPath(spark, delta_table_path).toDF()
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table kgsonedatadb_badrecords.trusted_hist_lnd_kva_details_bad

-- COMMAND ----------

-- select Employee_Number,file_date, dated_on, count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by Employee_Number,file_date, dated_on having count(1)>1

-- select EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD,count(1) from (select distinct EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where APPROVED_LWD <> '1900-01-01') group by EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD having count(1)>1

-- where employeenumber = '103983' 

-- select * from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where approved_lwd <> '1900-01-01' 
-- and employeenumber = '72788'

-- select count(distinct EMPLOYEENUMBER,REQUESTSTATUS,APPROVED_LWD) from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where APPROVED_LWD <> '1900-01-01'

-- select * from (select row_number() over(partition by employeenumber order by resignationdate desc) as tk_rownum,* from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report where APPROVED_LWD <> '1900-01-01') where tk_rownum = 1

-- select Employee_Number,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,'' as Current_Base_Location_of_the_Candidate,'' as Office_Location_in_the_KGS_Offer_Letter,'' as WFA_Option__Permanent_12_Months_,Dated_On,File_Date,Business_Category,left(File_Date,8) as Month_Key from kgsonedatadb.trusted_headcount_employee_details

select * from kgsonedatadb.trusted_hist_employee_engagement_year_end

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_compensation_yec

-- COMMAND ----------

-- select distinct file_date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details

-- select distinct date_format(cast(resignationdate as date),'yyyyMMdd') from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report

select EMPLOYEENUMBER,FULLNAME,CONTACTNUMBER,Personal_Email_ID,REQUESTSTATUS,FUNCTION,SUB_FUNCTION,ORGANIZATIONNAME,COMPANY_NAME,JOB,LOCATION,RESIGNATIONDATE,POLICYLWD,No_of_days_Waved,
case when APPROVED_LWD = '1900-01-01' then LWD_DESIREDLWD else APPROVED_LWD end as LWD,
case when APPROVED_LWD = '1900-01-01' then null else APPROVED_LWD end as APPROVED_LWD,
LWD_DESIREDLWD,CLEARANCE_COMPLETE_DATE,TERMINATION_DATE,HCM_TERMINATION__DATE,PM_EMP_CODE,PERFORMANCEMANAGERNAME,Reporting_Partner,Reporting_Partner_Approval_Date,WITHDRAWAL_PENDING_WITHPM,LWD_PENDING_WITH_PARTNER_NAME,NO_DUES_CERTIFICATE_SENT,NO_DUES_CERTIFICATE_SENT_DATE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE_DATE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE_DATE,RESIGNATION_TYPE,EXIT_REASON,ONBEHALF_RESIGNATION,HRBP_EMP_Code_,HRBP_Name,Dated_On,File_Date,date_format(cast(resignationdate as date),'yyyyMMdd') as Month_Key from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report

-- COMMAND ----------


select * from kgsonedatadb.trusted_hist_compensation_additional_pay

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_compensation_additional_pay

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.raw_hist_headcount_employee_dump group by File_Date order by file_date desc

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where `Candidate_Email/Candidate_Email_Personal_Email` = 'anuradhaneelakandan@outlook.com';

select distinct BGV_Status from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

select * from kgsonedatadb.trusted_hist_bgv_offer_release where Candidate_Email = '19poonamrawat@gmail.com'

select * from (select rank() over(partition by `Candidate_Email/Candidate_Email_Personal_Email` order by File_Date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where `Candidate_Email/Candidate_Email_Personal_Email` = 'anuradhaneelakandan@outlook.com' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where to_date(File_Date,'yyyyMMdd') < to_date('20230608','yyyyMMdd'))) hist where rank = 1

select * from (select rank() over(partition by Email order by Initiated_On desc,`Inv#` desc) as rank, * from kgsonedatadb.trusted_hist_bgv_kcheck where email = 'anuradhaneelakandan@outlook.com' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_kcheck where to_date(File_Date,'yyyyMMdd') <= to_date('20230608','yyyyMMdd'))) hist where rank = 1

select `Inv#`,count(1) from kgsonedatadb.trusted_hist_bgv_kcheck 
where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_kcheck where to_date(File_Date,'yyyyMMdd') <= to_date('20230608','yyyyMMdd'))
group by `Inv#` having count(1) > 1

select * from kgsonedatadb.trusted_hist_bgv_progress_sheet

select * from (select rank() over(partition by Personal_Email_ID order by Case_Initiation_Date desc, Dated_On desc) as rank, * from kgsonedatadb.trusted_hist_bgv_progress_sheet where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_progress_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1

-- COMMAND ----------

-- select `Candidate_Email/Candidate_Email_Personal_Email` as Prev_File_Candidate_Mail 
select Candidate_Identifier,BU,Candidate_Name,`Candidate_Email/Candidate_Email_Personal_Email`,Candidate_Alternate_Email_Candiddate_Official_Email,
CASE when (trim(Start_Date) == '' or trim(Start_Date) == '-')  then NULL else Start_Date END as Start_Date,
Recruiter_Name,BGV_Reference_No,
CASE when (trim(BGV_Initiation_Date) == '' or trim(BGV_Initiation_Date) == '-')  then NULL else BGV_Initiation_Date END as BGV_Initiation_Date,
BGV_Status,Report_Colour,Mandatory_Checks_Completed,All_Checks_Completed,Can_be_Onboarded_Yes_or_No,Waiver_Applicable,Waiver_Status_Closed_Yes_or_No,Insuff_Remarks,UTV_Remarks,Discrepancy_remarks,Found_in_Suspicious_List,Work_Location,Cost_Center,Client_Geography,Designation,CRI1,CRI2,CRI3,CRI4,CRI5,DTB_1,DTB_2,DTB_3,DTB_4,File_Date,Dated_On,concat(year(Start_Date),date_format(Start_Date,"MM")) as Month_Key from (select rank() over(partition by `Candidate_Email/Candidate_Email_Personal_Email` order by File_Date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners) hist where rank = 1

-- COMMAND ----------



-- COMMAND ----------

select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where file_date = '20230517' group by file_date,Dated_On order by file_date desc

select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned group by file_date,Dated_On order by file_date desc

-- delete from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where File_Date = '20230517' and dated_on = (select max(Dated_On) from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where file_date = '20230517')

describe kgsonedatadb.trusted_hist_headcount_employee_details

-- COMMAND ----------

-- select * from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')

select distinct REQUESTSTATUS,resignationdate where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and resignationdate is null

select distinct REQUESTSTATUS,resignationdate from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where coalesce(resignationdate,'NA') != 'NA'

-- COMMAND ----------

select 'trusted_hist_headcount_employee_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_details where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_academic_trainee where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_maternity_cases where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_resigned_and_left where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_secondee_outward where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki where File_Date = '20230517' group by File_Date,Dated_on

-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where to_date(Start_Date) > to_date('2023-05-31')

-- alter table kgsonedatadb.trusted_hist_bgv_upcoming_joiners alter column rename Candidate_Email_Candidate_Email_Perosnal_Email

ALTER TABLE kgsonedatadb.trusted_hist_bgv_upcoming_joiners SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');

-- alter table kgsonedatadb.trusted_hist_bgv_upcoming_joiners RENAME COLUMN `Candidate_Email/Candidate_Email_Perosnal_Email` TO `Candidate_Email/Candidate_Email_Personal_Email`

-- COMMAND ----------

select 'trusted_hist_headcount_employee_dump' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump union 
select 'trusted_hist_headcount_termination_dump' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump union 
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report union 
select 'trusted_hist_headcount_leave_report' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report union 
select 'trusted_hist_headcount_contingent' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_contingent union 
select 'trusted_hist_headcount_employee_details' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_details union 
select 'trusted_hist_headcount_resigned_and_left' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_resigned_and_left union 
select 'trusted_hist_headcount_sabbatical' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_sabbatical union 
select 'trusted_hist_headcount_maternity_cases' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_maternity_cases union 
select 'trusted_hist_headcount_secondee_outward' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_secondee_outward union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki union 
select 'trusted_hist_headcount_loaned_staff_resigned' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned union 
select 'trusted_hist_headcount_contingent_worker' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_contingent_worker union
select 'trusted_hist_headcount_contingent_worker_resigned' as table_name,max(file_date) from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned
-- delete from kgsonedatadb.raw_hist_headcount_employee_dump where file_date = '20230529'
-- trusted hist ED 42205
-- raw hist ED 42205
-- delete from kgsonedatadb.trusted_hist_headcount_termination_dump where file_date = '20230529'
-- trusted hist ED 62153
-- raw hist ED 62153
-- delete from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where file_date = '20230529'
-- trusted hist tksr 26072
-- raw hist tksr 26072
-- delete from kgsonedatadb.raw_hist_headcount_resigned_and_left where file_date = '20230529'
-- trusted hist tksr 328
-- raw hist tksr 0
-- delete from kgsonedatadb.raw_hist_headcount_secondee_outward where file_date = '20230529'
-- trusted hist tksr 111
-- raw hist tksr 0
-- delete from kgsonedatadb.raw_hist_headcount_maternity_cases where file_date = '20230526'
-- trusted hist tksr 100
-- raw hist tksr 0
-- delete from kgsonedatadb.trusted_hist_headcount_sabbatical where file_date = '20230526'
-- trusted hist tksr 11
-- raw hist tksr 0

-- COMMAND ----------

select distinct file_date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details order by file_date desc

-- COMMAND ----------

select Start_Date from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

select * from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump

-- COMMAND ----------


-- delete from kgsonedatadb.raw_hist_lnd_glms_details where File_Date in ('20230228','20230331','20230430')
-- delete from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date in ('20230228','20230331','20230430')

-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where File_Date in ('20230228','20230331','20230430')
-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430')

-- delete from kgsonedatadb.raw_hist_lnd_glms_details where File_Date in ('20230228')
-- delete from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date in ('20230228')

-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where File_Date in ('20230228')
-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date in ('20230228')

-- show tables in kgsonedatadb like 'raw_hist*'

select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

-- COMMAND ----------

select distinct Local_HR_ID,Cost_Center,Level_Wise,BU,File_Date from kgsonedatadb.trusted_hist_lnd_glms_details A where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430') and
--  (BU is null or trim(BU) = '')
-- (Cost_Centre is null or trim(Cost_Centre) = '')
(Level_Wise is null or trim(Level_Wise) = '')  
order by A.Local_HR_ID

-- COMMAND ----------

-- select count(1)  from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430') and (trim(coalesce(Level_Wise,'NA')) is null or trim(coalesce(Level_Wise,'NA')) != '')



select distinct Local_HR_ID,Cost_Center,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_details A where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430') and
-- and (BU is null or trim(BU) = '')
(Cost_Centre is null or trim(Cost_Centre) = '')
--((Level_Wise is null or trim(Level_Wise) = '') or 
order by A.Local_HR_ID

-- select distinct Local_HR_ID  from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430')
-- and (Cost_Center is null or trim(Cost_Center) = '')
-- and TrainingCategory is null; 
-- --38717 

-- 131899
-- 132368

-- 132368
-- 131085
-- 132836

-- select distinct Item_Title  from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date in ('20230228','20230331','20230430')
-- and (Training_Category is null or trim(Training_Category) ='')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # -- import pyodbc
-- MAGIC
-- MAGIC jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")  
-- MAGIC jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")  
-- MAGIC jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")  
-- MAGIC username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")  
-- MAGIC password = password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")  
-- MAGIC jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)  
-- MAGIC connectionProperties = {  
-- MAGIC   "user" : username,  
-- MAGIC   "password" : password,  
-- MAGIC   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
-- MAGIC }
-- MAGIC
-- MAGIC # delete from trusted_hist_headcount_maternity_cases_test;
-- MAGIC
-- MAGIC delete_query = f"DELETE from trusted_hist_headcount_leave_report where 1 = 2"
-- MAGIC
-- MAGIC # sampleDF.write \
-- MAGIC # .format("jdbc") \
-- MAGIC # .mode("delete") \
-- MAGIC # .option("url",jdbcUrl) \
-- MAGIC # .option("dbtable","trusted_hist_headcount_maternity_cases_test") \
-- MAGIC # .option("user", username) \
-- MAGIC # .option("password", password) \
-- MAGIC # .save()
-- MAGIC
-- MAGIC spark.sql(delete_query).write.jdbc(url = jdbcUrl, table = "trusted_hist_headcount_leave_report", mode = "delete",properties = connectionProperties)
-- MAGIC # -- conn.execute('DELETE from trusted_hist_headcount_maternity_cases_test WHERE 1=1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC finalDf.write \
-- MAGIC .mode("overwrite") \
-- MAGIC .format("delta") \
-- MAGIC .option("overwriteSchema", "True") \
-- MAGIC .option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
-- MAGIC .option("compression","snappy") \
-- MAGIC .saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

-- COMMAND ----------

insert into kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023
select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC # select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_type = 'GLMS' and file_date in ('20230201','20230301','20230401')
-- MAGIC
-- MAGIC from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat
-- MAGIC
-- MAGIC fileYear = '2023'
-- MAGIC fileMonth = '04'
-- MAGIC
-- MAGIC query = "select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where concat(year(File_Date),date_format(File_Date,\"MM\")) <= \""+fileYear+fileMonth+"\")) ed_hist where rank = 1"
-- MAGIC
-- MAGIC print(query)
-- MAGIC
-- MAGIC df_emp_dump=spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where date_format(to_Date(File_Date,'yyyyMMdd'),'yyyyMM') <= '"+fileYear+fileMonth+"')) ed_hist where rank = 1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))
-- MAGIC
-- MAGIC display(df_emp_dump)
-- MAGIC
-- MAGIC # --delete count below for glms feb, mar, apr for glms
-- MAGIC # --211526 trusted hist glms details
-- MAGIC # --446014 raw hist glms details
-- MAGIC # --211526 trusted hist glms kva details where file_type = 'glms'

-- COMMAND ----------

select date_format(to_Date(File_Date,'yyyyMMdd'),'yyyyMM') from kgsonedatadb.trusted_hist_headcount_employee_dump

-- COMMAND ----------


select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where date_format(to_Date(File_Date,'yyyyMMdd'),'yyyyMM') <= '202304')) ed_hist where rank = 1

-- COMMAND ----------


select concat(date_format(File_Date,"YYYY"),date_format(File_Date,"MM")) from kgsonedatadb.trusted_hist_headcount_employee_dump 
--where concat(year(File_Date),date_format(File_Date,"MM")) <= "202304")

-- COMMAND ----------

select file_date,dated_on,count(1) from kgsonedatadb.raw_hist_lnd_glms_details group by File_Date,dated_on

select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_lnd_glms_details group by File_Date,dated_on

-- COMMAND ----------

show tables in kgsonedatadb_badrecords

-- COMMAND ----------

select Employee_Number,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,Status,File_Date from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') < to_date('2023-05-29'))) ed_hist where rank = 1

-- COMMAND ----------

select distinct Report_Date from kgsonedatadb.trusted_hist_bgv_waiver_tracker

-- COMMAND ----------

show tables in kgsonedatadb like 'trusted_hist*';

-- COMMAND ----------

select count(position, Job_Name,Mapping) from kgsonedatadb.config_dim_level_wise;

select * from kgsonedatadb.config_dim_training_category;

select Original_Levelwise_Mapping from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023

-- COMMAND ----------

select Cost_Center,CC,BU,Position,Job_Name,Geo,
Level_Wise,Training_Category,
Global_Function,SL(GLMS,KVA),Local_Service_Line(KVA)
from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023;

-- From Employee_Details - Cost_Center,CC,BU,Position,Job_Name,Geo
select Local_HR_ID,Position from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID = '131899';
--(position is null or Position ='') and to_date(File_Date,'yyyyMMdd') > to_date('20221001','yyyyMMdd');

-- COMMAND ----------

--KVA - update CC with Cost_Center, update Local_Service_Line, SL - both from Service_Line
create table temp_lnd_glms_ed_data
select A.local_HR_ID as LND_Local_HR_ID,B.Employee_Number as ED_Employee_Number,left(A.File_Date,6) as LND_Month_Key,left(B.File_Date,6) as ED_Month_Key,A.Position as LND_Position,B.Position as ED_Position,A.Job_Name as LND_Job_Name,B.Job_Name as ED_Job_Name,A.BU as LND_BU,B.Bu as ED_BU,A.Geo as LND_GEo,B.Client_Geography as ED_Cli_Geo,A.Cost_Center as LND_CostCenter,B.Cost_centre as ED_Cost_Centre,A.CC as LND_CC 
from (select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and upper(File_Type) = 'GLMS' and 
((Position is null or trim(Position) = '') or (Job_Name is null or trim(Job_Name) = '') or (BU is null or trim(BU) = '') or (Cost_Center is null or trim(Cost_Center) = '') or (CC is null or trim(CC) = '') or (Geo is null or trim(Geo) = ''))
) A left outer join kgsonedatadb.trusted_hist_headcount_monthly_employee_details B on A.Local_HR_ID = B.Employee_Number and left(A.File_Date,6) = left(B.File_Date,6);


select distinct LND_Local_HR_ID,ED_Employee_Number,LND_Month_Key,ED_Month_Key,LND_CostCenter,ED_Job_Name from temp_lnd_glms_ed_data


select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and upper(File_Type) = 'KVA' and 
(Geo is null or trim(Geo) = '')

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
USING (select Employee_Number,Location,File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump) AS B
ON A.Local_HR_ID = B.Employee_Number and left(A.File_Date,6) = left(B.File_Date,6) and B.Employee_Number IS NOT NULL and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (A.Location is null or trim(A.Location) = '')
WHEN MATCHED THEN
UPDATE SET A.Location = B.Location

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 as A
USING (select distinct LND_Local_HR_ID,ED_Employee_Number,LND_Month_Key,ED_Month_Key,LND_Geo,ED_Cli_Geo from temp_lnd_glms_ed_data) AS B
ON A.Local_HR_ID = B.ED_Employee_Number and left(A.File_Date,6) = B.ED_Month_Key and upper(A.File_Type) = 'GLMS' and B.ED_Employee_Number IS NOT NULL and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (A.Geo is null or trim(A.Geo) = '')
WHEN MATCHED THEN
UPDATE SET A.Geo = B.ED_Cli_Geo

-- COMMAND ----------


MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
USING (select distinct Cost_centre,Final_BU,BU,Service_Line from kgsonedatadb.config_cc_bu_sl) AS B
ON A.CC = B.Cost_Centre and A.BU=B.Final_BU and upper(A.File_Type) = 'KVA' and A.SL IS NULL and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (A.CC is not null and trim(A.CC) != '')
WHEN MATCHED THEN
UPDATE SET A.SL = B.Service_Line

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 as A
USING (select distinct Cost_centre,Final_BU,Service_Line from kgsonedatadb.config_cc_bu_sl) AS B
ON A.Cost_Center = B.Cost_centre and A.BU = B.Final_BU and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (A.SL is null or trim(A.SL) = '')
WHEN MATCHED 
THEN UPDATE SET A.SL = B.Service_Line

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 set Level_Wise = 'AD and Above' where Level_Wise in ('Senior Associate Director','Associate Director','Director +')

-- select concat(Cost_centre,Client_Geography,BU,Final_BU) from kgsonedatadb.config_cc_bu_sl group by Cost_centre,Client_Geography,BU,Final_BU having count(1) >1

-- select * from kgsonedatadb.config_cc_bu_sl where concat(Cost_centre,Client_Geography,BU,Final_BU) in (select concat(Cost_centre,Client_Geography,BU,Final_BU) from kgsonedatadb.config_cc_bu_sl group by Cost_centre,Client_Geography,BU,Final_BU having count(1) >1)

-- delete from kgsonedatadb.config_cc_bu_sl where Cost_centre = 'DA Core-CF-M&A-East'and Client_Geography ='Canada' and BU ='DAS' and Final_BU= 'DAS' and service_line = 'Infrastructure'

-- select * from kgsonedatadb.config_cc_bu_sl where concat(Cost_centre,Final_BU) in
-- (select distinct concat(cost_center,BU) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (SL is null or trim(SL) = ''))
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (SL is null or trim(SL) = '')

-- COMMAND ----------

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and upper(File_Type) = 'KVA' and Position is null or trim(position) = '' group by File_Date

select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and ((Position is null or trim(Position) = '') or (Job_Name is null or trim(Job_Name) = '') or (BU is null or trim(BU) = '') or (Cost_Center is null or trim(Cost_Center) = '') or (CC is null or trim(CC) = '') or (Geo is null or trim(Geo) = ''))

-- COMMAND ----------

create table kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023_bckp
select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023_bckp

-- COMMAND ----------

select A.Department,min(A.Leave_start_Date) from kgsonedatadb.trusted_hist_headcount_leave_report A where A.BU is null and to_date(A.Leave_Start_Date,'yyyyMMdd') > to_date('20220930','yyyyMMdd') group by A.Department;

-- select Cost_centre,count(1) from kgsonedatadb.config_cost_center_business_unit group by Cost_centre having count(1) >1

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_hist_headcount_leave_report as A
USING kgsonedatadb.config_cost_center_business_unit AS B
ON A.Department = B.Cost_centre and A.BU is null and to_date(A.Leave_Start_Date,'yyyyMMdd') > to_date('20220930','yyyyMMdd')
WHEN MATCHED THEN
UPDATE SET A.BU = B.BU

select rank() over(partition by cost_center order by file_date desc) as cc_bu_rank,* from kgsonedatadb.config_hist_cost_center_business_unit

select * from (select rank() over(partition by cost_center order by file_date desc,dated_on desc) as cc_bu_rank, * from kgsonedatadb.config_hist_cost_center_business_unit where file_date = (select max(file_date) from kgsonedatadb.config_hist_cost_center_business_unit where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+fileDate+"'"+"))) cc_bu_hist where cc_bu_rank = 1;

select max(Leave_Start_Date) from kgsonedatadb.trusted_hist_headcount_leave_report where BU = 'Advisory'

select File_date,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by File_date

update kgsonedatadb.trusted_hist_headcount_leave_report set BU = 'Cap-Hubs' where BU = 'KGS Capability Hubs'

-- select concat(year(A.Leave_Start_Date),date_format(A.Leave_Start_Date,"MM")),BU,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report A group by concat(year(A.Leave_Start_Date),date_format(A.Leave_Start_Date,"MM")),BU

-- COMMAND ----------

select * from kgsonedatadb.config_bu_mapping_list

-- COMMAND ----------

select min(Leave_Start_Date) from kgsonedatadb.trusted_hist_headcount_leave_report;
select min(FIle_Date) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details;
select min(FIle_Date) from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left;

select A.Employee_Number,concat(year(A.Leave_Start_Date),date_format(A.Leave_Start_Date,"MM")) as LR_Month_Key,A.Department,left(B.File_Date,6) as ED_Month_Key,B.Cost_centre,B.BU from kgsonedatadb.trusted_hist_headcount_leave_report A left join kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left B on A.Employee_Number = B.Employee_Number and concat(year(A.Leave_Start_Date),date_format(A.Leave_Start_Date,"MM")) = left(B.File_Date,6)
where B.Employee_Number is not null

-- select * from kgsonedatadb.config_cc_bu_sl where Cost_centre like '%Tax%Tech%TTP%'

-- select File_Date,BU,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by File_Date,BU

-- COMMAND ----------

--GLMS - update only cost_center, don't touch CC - it is from source, update only SL and not Local_Service_Line as this is from source
select A.local_HR_ID,A.Position,B.Position from (select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where to_date(File_Date,'yyyyMMdd') > to_date('20221001','yyyyMMdd') and upper(File_Type) = 'GLMS' and ((Position is null or trim(Position) = '') or (Job_Name is null or trim(Job_Name) = '') or (BU is null or trim(BU) = '') or (Cost_Center is null or trim(Cost_Center) = '')  or (Geo is null or trim(Geo) = ''))) A inner join kgsonedatadb.trusted_hist_headcount_monthly_employee_details B on A.Local_HR_ID = B.Employee_Number and left(A.File_Date,6) = left(B.File_Date,6);

-- COMMAND ----------

select distinct Leave_Type from kgsonedatadb.trusted_hist_headcount_leave_report where coalesce(upper(Cancel_Status),"NO") not like "YES%" and (coalesce(upper(leave_type),"NA") not like "%KI%") and coalesce(upper(leave_type),"NA") not in ("ILLNESS OR INCAPACITY LEAVE","AUDIT SQL LEAVE") 

union all

select count(1) as count from kgsonedatadb.trusted_hist_headcount_leave_report where (Cancel_Status is null or trim(upper(Cancel_Status)) = "") and leave_type not like 'KI%'

-- (upper(Cancel_Status) is null or trim(upper(Cancel_Status)) = "") 
-- and leave_type not like 'KI%'

-- select count(1) from kgsonedatadb.trusted_hist_headcount_leave_report where Employee_Number in (select distinct Employee_Number from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS');

truncate table kgsonedatadb.raw_stg_headcount_leave_report;
truncate table kgsonedatadb.raw_curr_headcount_leave_report;
truncate table kgsonedatadb.raw_hist_headcount_leave_report;
truncate table kgsonedatadb.trusted_stg_headcount_leave_report;
truncate table kgsonedatadb.trusted_headcount_leave_report;
truncate table kgsonedatadb.trusted_hist_headcount_leave_report;

-- select File_Date,min(Leave_Start_Date),max(Leave_Start_Date),Dated_On from kgsonedatadb.trusted_hist_headcount_leave_report group by File_Date,Dated_On;

-- select file_date,* from kgsonedatadb.trusted_hist_headcount_leave_report where Employee_Number = '101551' and Leave_Type = 'KGS Annual Leave' and Leave_Start_Date = '2021-12-27' order by Date_of_Approved desc;

-- select Employee_Number,Leave_Type,Leave_Start_Date,count(1) from (select rank() over(partition by Employee_Number,Leave_Type,Leave_Start_Date order by Date_of_Approved desc) as leave_rank,* from kgsonedatadb.trusted_hist_headcount_leave_report where (cancel_status is null or cancel_status = '')) where leave_rank = 1 group by Employee_Number,Leave_Type,Leave_Start_Date having count(1) > 1;

-- COMMAND ----------


select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by file_date

-- select BU, count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by BU;

-- select CC, count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by CC;

-- select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (CC is null or CC = '') and (Cost_Center is null or Cost_Center = '')

-- select concat(year(Leave_Start_Date),date_format(Leave_Start_Date,"MM")),count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by concat(year(Leave_Start_Date),date_format(Leave_Start_Date,"MM"))

-- select Leave_Start_Date,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by Leave_Start_Date

-- select file_date,count(1) from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump group by file_date

-- delete from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where file_date = '20230222'

-- select count(1) from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump

-- select distinct file_Date from kgsonedatadb.trusted_hist_employee_engagement_encore_output

-- update kgsonedatadb.trusted_hist_headcount_leave_report set Approver_s_Name = `______Approver’s_Name` where Approver_s_Name is null and `______Approver’s_Name` is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set `______Approver’s_Name` = Approver_s_Name where `______Approver’s_Name` is null and Approver_s_Name is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set Date_of_Approved = _Date_of_Approved where Date_of_Approved is null and _Date_of_Approved is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set _Date_of_Approved = Date_of_Approved where _Date_of_Approved is null and Date_of_Approved is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set Sub_Function1 = Sub_Function_1 where Sub_Function1 is null and Sub_Function_1 is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set Sub_Function_1 = Sub_Function1 where Sub_Function_1 is null and Sub_Function1 is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set Sub_Function_2 = Sub_Function2 where Sub_Function_2 is null and Sub_Function2 is not null
-- update kgsonedatadb.trusted_hist_headcount_leave_report set Sub_Function2 = Sub_Function_2 where Sub_Function2 is null and Sub_Function_2 is not null

-- select count(1) from kgsonedatadb.trusted_hist_headcount_leave_report A where concat(employee_number, File_Date) in (select concat(employee_number,max(file_Date)) from kgsonedatadb.trusted_hist_headcount_leave_report where employee_number = A.employee_number group by Employee_Number)

-- select min(Leave_Start_Date) from kgsonedatadb.trusted_hist_headcount_leave_report where File_Date = '20230522'

-- select concat(year(leave_start_date),date_format(leave_start_date,"MM")),count(1) from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by file_date desc,date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report) hist where rank = 1 and file_Date = '20230522' and cancel_status is null group by concat(year(leave_start_date),date_format(leave_start_date,"MM"))

-- select concat(year(leave_start_date),date_format(leave_start_date,"MM")) as Month_Key from kgsonedatadb.trusted_hist_headcount_leave_report where file_Date = '20230522'

select * from kgsonedatadb.trusted_hist_headcount_leave_report

-- select leave_start_date, * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by file_date desc,date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report) hist where rank = 1 and employee_number = 108293
-- select count(distinct(Employee_Number,leave_type,leave_start_date)) from kgsonedatadb.trusted_hist_headcount_leave_report

-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by file_date
-- 2020-10-07
-- 2022-04-22

-- COMMAND ----------

-- MAGIC %run
-- MAGIC /kgsonedata/common_utilities/connection_configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ALTER TABLE kgsonedatadb.trusted_hist_employee_engagement_encore_output SET LOCATION = 'dbfs:/mnt/trustedlayermount/history/employee_engagement/encore_output'
-- MAGIC
-- MAGIC processName = 'employee_engagement'
-- MAGIC tableName = 'encore_output'
-- MAGIC
-- MAGIC df = spark.sql("select * from kgsonedatadb.encore_output_backup")
-- MAGIC
-- MAGIC df.write \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .format("delta") \
-- MAGIC     .option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
-- MAGIC     .option("compression","snappy") \
-- MAGIC     .saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

-- COMMAND ----------


-- create table kgsonedatadb.encore_output_backup
-- select distinct File_Date from kgsonedatadb.trusted_hist_employee_engagement_encore_output

select * from kgsonedatadb.config_data_type_cast where Delta_Table_Name = 'encore_output'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql import functions
-- MAGIC from pyspark.sql.functions import dense_rank
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
-- MAGIC
-- MAGIC currentDf = spark.sql('''select "27 Sep 19" as LWD from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned where File_Date = "20200219"''')
-- MAGIC
-- MAGIC columnName = "LWD"
-- MAGIC columnNew = columnName+'_New'
-- MAGIC
-- MAGIC currentDf=currentDf.withColumn(columnName,regexp_replace(regexp_replace(currentDf[columnName],'th ','-'),'\'','-'))
-- MAGIC
-- MAGIC currentDf=currentDf.withColumn(columnNew,\
-- MAGIC when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yy'),'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd MM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MM yy'),'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yy'),'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd-MMMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMMM-yy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMM yy'),'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd/MM/yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd/MM/yy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'MM/dd/yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'MM/dd/yy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'yyyy-MM-dd'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd/MM/yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'MM/dd/yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMMM-yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'dd MMMM,yyyy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMMM,yyyy'), 'yyyy-MM-dd'))\
-- MAGIC .when(to_date(currentDf[columnName], 'yyyy/MM/dd').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'yyyy/MM/dd'), 'yyyy-MM-dd'))\
-- MAGIC .otherwise(currentDf[columnName]))
-- MAGIC
-- MAGIC
-- MAGIC # currentDf=currentDf.withColumn(columnNew,\
-- MAGIC # when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yy'),'yyyy-MM-dd'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yy'),'yyyy-MM-dd'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMM yy'),'yyyy-MM-dd'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),to_date(currentDf[columnName], 'yyyy-MM-dd'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),to_date(currentDf[columnName], 'dd/MM/yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),to_date(currentDf[columnName], 'MM/dd/yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MM-yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMM-yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMMM-yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd MMMM, yyyy').isNotNull(),to_date(currentDf[columnName], 'dd MMMM,yyyy'))\
-- MAGIC # .otherwise(currentDf[columnName]))
-- MAGIC
-- MAGIC
-- MAGIC # currentDf=currentDf.withColumn(columnNew,\
-- MAGIC # when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yy'),'dd-MM-yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yy'),'dd-MM-yyyy'))\
-- MAGIC # .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMM yy'),'dd-MM-yyyy'))\
-- MAGIC # .otherwise(currentDf[columnName]))
-- MAGIC
-- MAGIC display(currentDf.select(columnName,columnNew).distinct())

-- COMMAND ----------

-- select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and Employee_Number in ('113059','121205','121209','121217','121258','121263','121272','121279','121295','121347','121388','121438','123759','130990','131076','131090','131314','131340','131467','131474','131480','131545','131546','131573','131576','131579','131582','131587','131588','131590','131591','27449','48687','77333','87334','98494') and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date('20230101','yyyymmdd'))) ed_hist where rank = 1 and ((`Function` is not null and employee_subfunction is not null and cost_centre is not null) and (not(`Function` = '' and employee_subfunction = '' and cost_centre = '')))

-- select distinct `Position`,Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where entity = 'KGS'

-- select distinct `Position`,Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where entity = 'KGS'

-- select * from (select distinct `Position`,Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where entity = 'KGS') A where `Position` = 'Senior'

-- select File_Date,count(1) from kgsonedatadb.raw_hist_lnd_glms_kva_details group by File_Date order by File_Date

select coalesce(B.Cost_centre,C.Cost_Centre) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A left join kgsonedatadb.trusted_hist_headcount_employee_dump B on A.Local_HR_ID = B.Employee_Number
left join kgsonedatadb.trusted_hist_headcount_termination_dump C on A.Local_HR_ID = C.Employee_Number group by

-- COMMAND ----------

-- select * from kgsonedatadb.config_data_type_cast where process_name in ('compensation') order by Process_Name, Delta_Table_Name

-- select file_date,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (period like '%22%' and period not in ('Oct 2022','Nov 2022','Dec 2022')) or period in ('Oct\'21','Nov\'21','Dec\'21') group by file_date
-- drop table kgsonedatadb_badrecords.trusted_hist_lnd_glms_kva_details

-- select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by File_Date,Dated_On order by file_date

select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- describe kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- 'Item_Revision_Date', 'Completion_Date', 'Scheduled_Offering_Start_Date', 'Last_Hire_Date', 'Class_End_Date'
-- select dated_on,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by Dated_On

-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (period like '%22%' and period not in ('Oct 2022','Nov 2022','Dec 2022')) or period in ('Oct\'21','Nov\'21','Dec\'21')
-- Item_Revision_Date

-- Completion_Date
-- select * from kgsonedatadb.config_data_type_cast_bckp_05182023

-- select Type_Cast_To,count(1) from kgsonedatadb.config_data_type_cast where Process_Name = 'lnd' group by Type_Cast_To
-- select * from kgsonedatadb.config_data_type_cast where Process_Name = 'lnd' and column_name in ('Total_Hours', 'Credit_Hours', 'Contact_Hours', 'CPD_CPE_Hours__Item_')

-- update kgsonedatadb.config_data_type_cast set type_cast_to = 'string' where Process_Name = 'lnd' and Type_Cast_To = 'date' and column_name in ('Scheduled_Offering_Start_Date', 'Last_Hire_Date', 'Class_End_Date');

-- update kgsonedatadb.config_data_type_cast set type_cast_to = 'string' where Process_Name = 'lnd' and Type_Cast_To = 'timestamp' and column_name in ('Last_Update_Timestamp');

-- update kgsonedatadb.config_data_type_cast set type_cast_to = 'string' where Process_Name = 'lnd' and Type_Cast_To = 'float' and column_name in ('Total_Hours', 'Credit_Hours', 'Contact_Hours', 'CPD_CPE_Hours__Item_');

-- update kgsonedatadb.config_data_type_cast set type_cast_to = 'double' where Process_Name = 'lnd' and Type_Cast_To = 'float' and column_name in ('CPD_CPE_Hours_Awarded');

-- select * from kgsonedatadb.config_data_type_cast where Process_Name = 'lnd'  and column_name in ('CPD_CPE_Hours_Awarded')

-- COMMAND ----------

-- DBTITLE 1,a
-- select * from kgsonedatadb.config_job_code_distribution_category_mapping where processname = 'Employee_Engagement' and tablename = 'Year_End' and `Position` = 'Senior-GDC' and Job_Name = '680.Senior-GDC'

-- select * from kgsonedatadb.trusted_hist_headcount_employee_dump

-- select * from kgsonedatadb.config_year_end_jobcode order by `function`,`position`

-- select file_date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end group by file_date

-- select employeenumber,count(1) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report group by employeenumber

select employeenumber,count(1) from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report group by EmployeeNumber having count(1)>1
-- select count(distinct employeenumber) from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report
-- select distinct new_cost_center from kgsonedatadb.trusted_hist_employee_engagement_year_end
-- truncate table kgsonedatadb.trusted_hist_employee_engagement_year_end

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump where employee_number in ('113059','121205','121209','121217','121258','121263','121272','121279','121295','121347','121388','121438','123759','130990','131076','131090','131314','131340','131467','131474','131480','131545','131546','131573','131576','131579','131582','131587','131588','131590','131591','27449','48687','77333','87334','98494') group by File_Date

-- COMMAND ----------

-- select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by file_date,Dated_On
-- select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_headcount_contingent group by file_date,Dated_On
-- select distinct dated_on from kgsonedatadb.trusted_hist_headcount_employee_dump 

-- delete from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20230309' and dated_on > '2023-05-04T10:37:08.204+0000'
-- select count(1) from kgsonedatadb.trusted_hist_headcount_contingent where file_date = '20230309' and dated_on > '2023-05-04T07:04:15.387+0000'

select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by file_date,Dated_On

-- COMMAND ----------

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by file_date

-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- select * from kgsonedatadb.trusted_hist_compensation_yec where EMP_ID in ('23943','41011') order by EMP_ID

-- select file_date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end group by file_date

select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rn from kgsonedatadb.trusted_headcount_leave_report where 
-- cast(Leave_Start_Date as date)>= to_date('2022-10-01','yyyy-MM-dd') and cast(Leave_End_Date as date)<= to_date('2023-09-30','yyyy-MM-dd') and 
lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where company_name in (select Company_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where entity = 'KGS')

-- COMMAND ----------


-- select file_date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_gps group by file_date

-- select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where upper(approval_status) = 'APPROVED' and upper(cancel_status) is null) leave_report where rank = 1 and employee_number = '91134'
--group by employee_number having count(1) > 10

-- select * from kgsonedatadb.trusted_hist_headcount_leave_report where employee_number = '91134'

-- select distinct Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- select distinct Level_Wise from kgsonedatadb.raw_stg_lnd_glms_kva_details

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Level_Wise = 'AD and Above' where Level_Wise in  ('Director +','Associate Director')

-- select A.Employee_Number,A.Tenure_Length_of_Service,B.Gratuity_Date, A.File_Date from kgsonedatadb.trusted_hist_employee_engagement_gps A left join kgsonedatadb.trusted_headcount_employee_dump B on A.Employee_Number = B.Employee_Number where A.Tenure_Length_of_Service < 0

-- select distinct to_date(File_Date,'yyyyMMdd') from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') < '2023-01-01'

-- select A.Employee_Number,A.File_Date,count(1) from kgsonedatadb.raw_hist_headcount_employee_dump A inner join (select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Gratuity_Date is null) B on A.Employee_Number = B.Employee_Number group by A.Employee_Number,A.File_Date having count(1)>1
--Employee_Number = '67591'
-- select Gratuity_Date,File_Date,Dated_On from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '117085' --and file_date = '20230118'

-- select * from kgsonedatadb.trusted_hist_headcount_employee_dump where concat(employee_number,file_date) in (select concat(Employee_Number,File_Date) from kgsonedatadb.trusted_hist_headcount_employee_dump where Gratuity_Date is null) and gratuity_date is not null

-- select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Gratuity_Date is null

-- select * from kgsonedatadb.config_adhoc_convert_column_to_date where DeltaTableName = 'employee_dump'

-- select * from kgsonedatadb.config_data_type_cast where delta_table_name = 'yec'


-- select count(1) from (select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230118' and Entity = 'KGS') A left join (select * from kgsonedatadb.trusted_hist_compensation_yec where File_Date = '20230101') B on A.Employee_Number = B.EMP_ID where B.Date_since_at_current_designation is null

-- Jan - 590
-- Feb - 419
-- select Date_since_at_current_designation from kgsonedatadb.trusted_hist_compensation_yec where concat(EMP_ID,File_Date) in 
-- (select concat(Emp_ID,File_Date) from kgsonedatadb.trusted_hist_employee_engagement_year_end where Date_since_at_current_designation is null)
-- select File_Date,* from kgsonedatadb.trusted_hist_compensation_yec where emp_id in (select Emp_ID from kgsonedatadb.trusted_hist_employee_engagement_year_end where Date_since_at_current_designation is null)

-- select File_Date,* from kgsonedatadb.trusted_hist_compensation_yec where emp_id in (
-- select Emp_ID from kgsonedatadb.trusted_hist_employee_engagement_year_end where Date_since_at_current_designation is null
-- )

-- COMMAND ----------

select distinct TD_Postion from kgsonedatadb.ln_test_1

-- COMMAND ----------


-- delete from kgsonedatadb.raw_hist_headcount_contingent where file_date = '20230309'

-- select distinct business_category from kgsonedatadb.trusted_hist_headcount_contingent
-- alter table kgsonedatadb.trusted_stg_headcount_contingent SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
alter table kgsonedatadb.trusted_stg_headcount_contingent drop column Business_Category

-- truncate table kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned;
-- truncate table kgsonedatadb.raw_stg_headcount_monthly_loaned_staff_resigned;
-- truncate table kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned;
-- truncate table kgsonedatadb.trusted_headcount_monthly_loaned_staff_resigned;

-- truncate table kgsonedatadb.raw_hist_headcount_monthly_maternity_cases;
-- truncate table kgsonedatadb.raw_stg_headcount_monthly_maternity_cases;
-- truncate table kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases;
-- truncate table kgsonedatadb.trusted_headcount_monthly_maternity_cases;

-- truncate table kgsonedatadb.raw_hist_headcount_monthly_sabbatical;
-- truncate table kgsonedatadb.raw_stg_headcount_monthly_sabbatical;
-- truncate table kgsonedatadb.trusted_hist_headcount_monthly_sabbatical;
-- truncate table kgsonedatadb.trusted_headcount_monthly_sabbatical;

-- truncate table kgsonedatadb.raw_hist_headcount_monthly_employee_details;
-- truncate table kgsonedatadb.raw_stg_headcount_monthly_employee_details;
-- truncate table kgsonedatadb.trusted_hist_headcount_monthly_employee_details;
-- truncate table kgsonedatadb.trusted_headcount_monthly_employee_details;

-- truncate table kgsonedatadb.raw_hist_headcount_monthly_academic_trainee;
-- truncate table kgsonedatadb.raw_stg_headcount_monthly_academic_trainee;
-- truncate table kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee;
-- truncate table kgsonedatadb.trusted_headcount_monthly_academic_trainee;

-- COMMAND ----------


-- select left(File_Date,6),count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end group by left(File_Date,6) order by left(File_Date,6)

-- select distinct First_Acceptance_Date from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report

select Gratuity_Date,count(1) from kgsonedatadb.trusted_headcount_employee_dump where Employee_Number = '15411' group by Gratuity_Date

-- COMMAND ----------


-- select  count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end
-- select * from kgsonedatadb.trusted_hist_global_mobility_secondment_details
-- select  `Function`,* from kgsonedatadb.trusted_hist_employee_engagement_gps

-- update kgsonedatadb.trusted_hist_employee_engagement_gps A set `Function` = (select max(BU) from kgsonedatadb.config_cost_center_business_unit B where B.Cost_centre = A.Cost_Centre)

-- select BU from kgsonedatadb.config_cost_center_business_unit

update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'CF' where `Function` = 'CF'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'CF' where `Function` = 'Corporate Functions'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Cap-Hubs' where `Function` = 'RAK'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Cap-Hubs' where `Function` = 'KGS Capability Hubs'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Cap-Hubs' where `Function` = 'CH'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Cap-Hubs' where `Function` = 'Cap-Hubs'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Consulting' where `Function` = 'Consulting'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Consulting' where `Function` = 'MC'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'DAS' where `Function` = 'DAS'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'DAS' where `Function` = 'DA'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'DAS' where `Function` = 'DA&S'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Digital Nexus' where `Function` = 'Digital Nexus'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'GDC' where `Function` = 'GDC'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'KRC' where `Function` = 'KRC'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'MS' where `Function` = 'MS'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'RS' where `Function` = 'RS'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'RS' where `Function` = 'Risk Services'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'RS' where `Function` = 'RC'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'RS' where `Function` = 'RAS'
update kgsonedatadb.trusted_hist_employee_engagement_gps set `Function` = 'Tax' where `Function` = 'Tax'

-- COMMAND ----------

-- update kgsonedatadb.config_cost_center_business_unit set BU = 'CF' where BU in ('CF','Corporate Functions')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'Cap-Hubs' where BU in ('Cap-Hubs','KGS Capability Hubs','CH','RAK')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'Consulting' where BU in ('Consulting','MC')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'DAS' where BU in ('DA','DA&S','DAS')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'Digital Nexus' where BU in ('Digital Nexus')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'GDC' where BU in ('GDC')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'KRC' where BU in ('KRC')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'MS' where BU in ('MS')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'RS' where BU in ('Risk Services','RS','RAS','RC')
-- update kgsonedatadb.config_cost_center_business_unit set BU = 'Tax' where BU in ('Tax')

ALTER TABLE kgsonedatadb.trusted_hist_employee_engagement_gps ADD COLUMN Gratuity_Date date
-- alter table kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report RENAME COLUMN _Requisition_Number TO Requisition_Number
-- alter table kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report RENAME COLUMN Leave_type_1 TO Leave_Type
-- alter table kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')

-- select * from kgsonedatadb_badrecords.trusted_hist_compensation_finance_metrics_bad

-- select * from kgsonedatadb.trusted_hist_employee_engagement_rock

-- select * from kgsonedatadb.trusted_headcount_leave_report

-- truncate table kgsonedatadb.trusted_hist_employee_engagement_year_end

-- COMMAND ----------


select distinct Delta_Table_Name from kgsonedatadb.config_data_type_cast
-- alter table kgsonedatadb.trusted_hist_headcount_sabbatical alter column Leave_Start_Date DATE
-- drop table kgsonedatadb.trusted_hist_headcount_sabbatical_test
-- create table kgsonedatadb.trusted_hist_global_mobility_secondment_tracker_test select Assignment_Start_Date from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker

-- select * from kgsonedatadb.trusted_hist_headcount_sabbatical
-- select * from kgsonedatadb.trusted_hist_headcount_sabbatical_test

-- INSERT OVERWRITE TABLE kgsonedatadb.trusted_hist_global_mobility_secondment_tracker_test SELECT cast(Assignment_Start_Date as date) FROM kgsonedatadb.trusted_hist_global_mobility_secondment_tracker_test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from kgsonedatadb.config_data_type_cast")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ALTER TABLE table_name
-- MAGIC    { RENAME TO clause |
-- MAGIC      ADD COLUMN clause |
-- MAGIC      ALTER COLUMN clause |
-- MAGIC      DROP COLUMN clause |
-- MAGIC      RENAME COLUMN clause |
-- MAGIC      ADD CONSTRAINT clause |
-- MAGIC      DROP CONSTRAINT clause |
-- MAGIC      ADD PARTITION clause |
-- MAGIC      DROP PARTITION clause |
-- MAGIC      RENAME PARTITION clause |
-- MAGIC      RECOVER PARTITIONS clause |
-- MAGIC      SET TBLPROPERTIES clause |
-- MAGIC      UNSET TBLPROPERTIES clause |
-- MAGIC      SET SERDE clause |
-- MAGIC      SET LOCATION clause |
-- MAGIC      SET OWNER TO clause }

-- COMMAND ----------

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_rock group by File_Date

select * from kgsonedatadb.trusted_hist_employee_engagement_year_end where Emp_ID = '23943'

-- select * from kgsonedatadb.trusted_hist_headcount_leave_report where LEave_Type = 'KGS Maternity Leave'

-- COMMAND ----------

-- %sql
-- select Employee_Number,Full_Name,Function,Sub_Function,Cost_centre,Location,Position,Employee_Category,Gratuity_Date,Date_First_Hired,Email_Address,Company_Name,desired_LWD,Approved_LWD,RM_Employee_Name,CAST(Rock_Tenure as DECIMAL),CAST(Amount as decimal),File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.trusted_hist_employee_engagement_rock

-- create database kgsonedatadb
-- drop table kgsonedatadb.trusted_hist_headcount_leave_report --RENAME COLUMN Leave_type TO Leave_Type2;
-- ALTER TABLE kgsonedatadb.trusted_hist_employee_engagement_rock RENAME COLUMN Leave_type TO Leave_Type;
-- ALTER TABLE kgsonedatadb.raw_stg_headcount_leave_report RENAME COLUMN Leave_type TO Leave_Type;

ALTER TABLE kgsonedatadb.trusted_hist_employee_engagement_rock ALTER COLUMN Amount double;

-- ALTER TABLE SET TBLPROPERTIES (
--  'delta.columnMapping.mode' = 'name',
--  'delta.minReaderVersion' = '2',
--  'delta.minWriterVersion' = '5')

-- select count(1) from kgsonedatadb.raw_hist_compensation_additional_pay where File_Date = '20221101';
-- select count(1) from kgsonedatadb.raw_hist_compensation_paysheet where File_Date = '20221101';
-- select count(1) from kgsonedatadb.raw_hist_compensation_yec where File_Date = '20221101';
-- select count(1) from kgsonedatadb.trusted_hist_compensation_additional_pay where File_Date = '20221101';
-- select count(1) from kgsonedatadb.trusted_hist_compensation_paysheet where File_Date = '20221101';
-- select count(1) from kgsonedatadb.trusted_hist_compensation_yec where File_Date = '20221101';

-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by file_date
-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_resigned_and_left group by file_date
-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by file_date

-- Select distinct File_Date from kgsonedatadb.employee_golden_data
-- select file_date,count(1) from kgsonedatadb.trusted_hist_lnd_kva_details group by file_date
-- select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by file_date,dated_on

-- delete from kgsonedatadb.trusted_hist_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_hist_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_hist_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_hist_compensation_finance_metrics where File_Date = '20221101'

-- delete from kgsonedatadb.trusted_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_compensation_finance_metrics where File_Date = '20221101'

-- delete from kgsonedatadb.trusted_stg_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_stg_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_stg_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.trusted_stg_compensation_finance_metrics where File_Date = '20221101'

-- delete from kgsonedatadb.raw_hist_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.raw_hist_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.raw_hist_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.raw_hist_compensation_finance_metrics where File_Date = '20221101'

-- delete from kgsonedatadb.raw_stg_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.raw_stg_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.raw_stg_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.raw_stg_compensation_finance_metrics where File_Date = '20221101'

-- delete from kgsonedatadb.raw_curr_compensation_paysheet where File_Date = '20221101'
-- delete from kgsonedatadb.raw_curr_compensation_additional_pay where File_Date = '20221101'
-- delete from kgsonedatadb.raw_curr_compensation_yec where File_Date = '20221101'
-- delete from kgsonedatadb.raw_curr_compensation_finance_metrics where File_Date = '20221101'

-- COMMAND ----------


-- select reward_date from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump
-- select file_date,count(1) from kgsonedatadb.trusted_hist_jml_germany_joiner group by file_date
-- select file_date,count(1) from kgsonedatadb.trusted_hist_jml_us_leaver group by file_date
-- select file_date,count(1) from kgsonedatadb.trusted_hist_jml_us_mover group by file_date
-- select * from kgsonedatadb.trusted_hist_jml_germany_joiner where SAP_ID is null

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_bgv_progress_sheet where cast(S_No as int) is not null and S_No is not null group by File_Date order by file_date asc

select file_date,count(1) from kgsonedatadb.trusted_hist_bgv_progress_sheet group by file_date--where cast(S_No as int) is null

-- COMMAND ----------


select * from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker where Emp_ID is null

-- COMMAND ----------


select distinct Offered_Created_Date from kgsonedatadb.raw_stg_bgv_offer_release union
select distinct Start_Date from kgsonedatadb.raw_stg_bgv_offer_release union
select distinct BATCH_DATE from kgsonedatadb.raw_stg_compensation_additional_pay union
select distinct BIRTH_DATE from kgsonedatadb.raw_stg_compensation_paysheet union
select distinct Date_since_at_current_designation from kgsonedatadb.raw_stg_compensation_yec union
select distinct DOJ from kgsonedatadb.raw_stg_compensation_yec union
select distinct STARTDATE from kgsonedatadb.raw_stg_compensation_yec union
select distinct Date_of_Joining from kgsonedatadb.raw_stg_headcount_academic_trainee union
select distinct Date_of_Joining from kgsonedatadb.raw_stg_headcount_contingent union
select distinct LWD from kgsonedatadb.raw_stg_headcount_contingent union
select distinct Date_of_Joining from kgsonedatadb.raw_stg_headcount_contingent_worker union
select distinct Date_of_Joining from kgsonedatadb.raw_stg_headcount_contingent_worker_resigned union
select distinct LWD from kgsonedatadb.raw_stg_headcount_contingent_worker_resigned union
select distinct Date_First_Hired from kgsonedatadb.raw_stg_headcount_employee_details union
select distinct Date_First_Hired from kgsonedatadb.raw_stg_headcount_employee_dump union
select distinct Termination_Date from kgsonedatadb.raw_stg_headcount_employee_dump union
select distinct Leave_Start_Date from kgsonedatadb.raw_stg_headcount_leave_report union
select distinct Leave_End_Date from kgsonedatadb.raw_stg_headcount_leave_report union
select distinct Date_of_Joining from kgsonedatadb.trusted_headcount_monthly_academic_trainee union
select distinct Date_of_Joining from kgsonedatadb.trusted_headcount_monthly_contingent_worker union
select distinct Date_of_Joining from kgsonedatadb.trusted_headcount_monthly_contingent_worker_resigned union
select distinct LWD from kgsonedatadb.trusted_headcount_monthly_contingent_worker_resigned union
select distinct Date_First_Hired from kgsonedatadb.trusted_headcount_monthly_employee_details union
select distinct End_Date from kgsonedatadb.trusted_headcount_monthly_employee_details union
select distinct Start_Date from kgsonedatadb.trusted_headcount_monthly_loaned_staff_from_ki union
select distinct End_Date from kgsonedatadb.trusted_headcount_monthly_loaned_staff_from_ki union
select distinct Start_Date from kgsonedatadb.trusted_headcount_monthly_loaned_staff_resigned union
select distinct End_Date from kgsonedatadb.trusted_headcount_monthly_loaned_staff_resigned union
select distinct LWD from kgsonedatadb.trusted_headcount_monthly_loaned_staff_resigned union
select distinct Date_First_Hired from kgsonedatadb.trusted_headcount_monthly_maternity_cases union
select distinct Start_Date from kgsonedatadb.trusted_headcount_monthly_maternity_cases union
select distinct End_Date from kgsonedatadb.trusted_headcount_monthly_maternity_cases union
select distinct Date_First_Hired from kgsonedatadb.trusted_headcount_monthly_resigned_and_left union
select distinct Termination_Date from kgsonedatadb.trusted_headcount_monthly_resigned_and_left union
select distinct Date_First_Hired from kgsonedatadb.trusted_headcount_monthly_sabbatical union
select distinct Leave_Start_Date from kgsonedatadb.trusted_headcount_monthly_sabbatical union
select distinct Leave_End_Date from kgsonedatadb.trusted_headcount_monthly_sabbatical union
select distinct Date_First_Hired from kgsonedatadb.trusted_headcount_monthly_secondee_outward union
select distinct APPROVED_LWD from kgsonedatadb.raw_stg_headcount_talent_konnect_resignation_status_report union
select distinct Date_First_Hired from kgsonedatadb.raw_stg_headcount_termination_dump union
select distinct Termination_Date from kgsonedatadb.raw_stg_headcount_termination_dump union
select distinct Request_Received_on from kgsonedatadb.raw_stg_jml_germany_joiner union
select distinct Form_sent_to_Germany_Team from kgsonedatadb.raw_stg_jml_germany_joiner union
select distinct SAP_ID_Created_On from kgsonedatadb.raw_stg_jml_germany_joiner union
select distinct LWD_of_the_employee from kgsonedatadb.raw_stg_jml_germany_leaver union
select distinct Request_Received_on from kgsonedatadb.raw_stg_jml_germany_leaver union
select distinct Form_sent_to_Germany_Team from kgsonedatadb.raw_stg_jml_germany_leaver union
select distinct SAP_ID_Terminated_On from kgsonedatadb.raw_stg_jml_germany_leaver union
select distinct Request_Received_on from kgsonedatadb.raw_stg_jml_germany_mover union
select distinct Form_sent_to_Germany_Team from kgsonedatadb.raw_stg_jml_germany_mover union
select distinct Request_completed_on from kgsonedatadb.raw_stg_jml_germany_mover union
select distinct LWD_of_the_user from kgsonedatadb.raw_stg_jml_nl_leaver union
select distinct Start_Date from kgsonedatadb.raw_stg_jml_uk_joiner union
select distinct Date_of_Birth from kgsonedatadb.raw_stg_jml_uk_joiner union
select distinct Leave_Date from kgsonedatadb.raw_stg_jml_uk_leaver union
select distinct HR_Date from kgsonedatadb.raw_stg_jml_uk_leaver union
select distinct JML_Date from kgsonedatadb.raw_stg_jml_uk_leaver union
select distinct People_Center_Date from kgsonedatadb.raw_stg_jml_uk_leaver union
select distinct Effective_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct Absence_End_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct Secondment_End_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct HR_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct JML_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct People_Center_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct MI_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct ITS_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct GM_Date from kgsonedatadb.raw_stg_jml_uk_mover union
select distinct US_Login_details_date from kgsonedatadb.raw_stg_jml_us_joiner union
select distinct Date_sent_to_Reconnect_team from kgsonedatadb.raw_stg_jml_us_joiner union
select distinct Date_sent_to_Reconnect_team from kgsonedatadb.raw_stg_jml_us_leaver union
select distinct Candidate_Creation_Date from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Candidate_Last_Modified_Date from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Date_of_Birth18 from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Latest_Acceptance_Date from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Planned_Start_Date from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Contract_End_Date from kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report union
select distinct Latest_Fully_Approved_Date from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct _Req_Creation_Date from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct _First_Fully_Approved_Date from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct _First_Approval_Rejection_Date from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct Req_Created_By from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct Date_on_Hold_ from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump

-- COMMAND ----------


-- select distinct [Date_of_Un-Hold] from kgsonedatadb.raw_stg_talent_acquisition_requisition_dump union
select distinct (Date:__BU_HR_to_JML_) from kgsonedatadb.raw_stg_jml_nl_joiner union
select distinct [Date:__JML_to_NL_] from kgsonedatadb.raw_stg_jml_nl_joiner union
select distinct [Date:__NL_to_JML_] from kgsonedatadb.raw_stg_jml_nl_joiner union
select distinct [Date:__BU_HR_to_JML_] from kgsonedatadb.raw_stg_jml_nl_leaver union
select distinct [Date:__JML_to_NL_Team_] from kgsonedatadb.raw_stg_jml_nl_leaver union
select distinct [Date:__NL_to_JML_Team_] from kgsonedatadb.raw_stg_jml_nl_leaver union
select distinct [Date:__BU_HR_to_JML_] from kgsonedatadb.raw_stg_jml_nl_mover union
select distinct [Date:__JML_to_NL_] from kgsonedatadb.raw_stg_jml_nl_mover union
select distinct [Date:__NL_to_JML_] from kgsonedatadb.raw_stg_jml_nl_mover union
select distinct [Date:__BU_HR_to_JML_] from kgsonedatadb.raw_stg_jml_us_joiner union
select distinct [Date:__JML_to_US_] from kgsonedatadb.raw_stg_jml_us_joiner union
select distinct [Date:__US_to_JML_] from kgsonedatadb.raw_stg_jml_us_joiner

-- COMMAND ----------

-- select count(1) from kgsonedatadb.trusted_hist_global_mobility
-- select count(1) from kgsonedatadb_badrecords.trusted_hist_compensation_paysheet_bad
-- select count(1) from kgsonedatadb.trusted_hist_compensation_additional_pay
-- select * from kgsonedatadb.config_data_type_cast where process_name = 'compensation' and Delta_Table_Name = 'paysheet'
-- select * from kgsonedatadb.config_bad_record_check where process_name = 'compensation'

show tables in kgsonedatadb like '*global_mobility*'

-- COMMAND ----------


-- select Employee_Number,count(1) from kgsonedatadb.trusted_hist_global_mobility_secondment_details group by Employee_Number having count(1)>1
-- A LEFT JOIN
-- (select * from kgsonedatadb.trusted_headcount_employee_dump where Employee_Category in ('Secondee-Outward-With Pay','Secondee-Inward-With Pay','Secondee-Inward-Without Pay','Secondee-Outward-Without Pay')) B
-- on A.Employee_No = B.Employee_Number
-- select * from kgsonedatadb.trusted_global_mobility_secondment_details where Home_Country is not null

-- update kgsonedatadb.raw_curr_global_mobility_secondment_tracker set Employee_No = cast(Employee_No as int)

-- drop * from kgsonedatadb.trusted_hist_global_mobility_secondment_details

-- COMMAND ----------


select A.Emp_No, A.Final_Function, B.Employee_Number, B.`Function` from (select * from kgsonedatadb.trusted_employee_engagement_encore_output 
-- where file_date = '20230131'
) A LEFT JOIN (select * from kgsonedatadb.trusted_headcount_employee_dump 
-- where File_Date = '20230118'
) B on A.Emp_No = B.Employee_Number where A.Emp_No is not null and B.Employee_Number is not null and A.Final_Function <> B.`Function`

-- select left(file_date,6) from kgsonedatadb.trusted_hist_headcount_employee_dump

-- COMMAND ----------


-- select Reward_Date,file_date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump group by Reward_Date,file_date

-- select Nominee_s_Employee_ID,count(1) from kgsonedatadb.trusted_employee_engagement_thanks_dump where upper(Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED') group by Nominee_s_Employee_ID having count(1) > 1

select * from kgsonedatadb.trusted_employee_engagement_thanks_dump where Nominee_s_Employee_ID = '82971'
-- select left(right('20230118',4),2)

-- select A.Final_Function,B.`Function`,* from kgsonedatadb.trusted_employee_engagement_encore_output A left join kgsonedatadb.trusted_hist_headcount_employee_details B on A.Emp_No = B.Employee_Number and left(A.File_Date,6) = left(B.File_Date,6) where B.Employee_Number is not null and A.Final_Function <> B.`Function`
--where Nominee_s_Employee_ID = '79055'

-- COMMAND ----------


-- select File_Date,`Function`,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '83261' order  by File_Date

-- select File_date, Gratuity_Date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_rock group by File_date, Gratuity_Date--where Gratuity_Date is null

-- select File_date, Gratuity_Date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by File_date, Gratuity_Date

-- select Function_tag,count(1) from kgsonedatadb.trusted_employee_engagement_year_end group by Function_tag
-- select count(1) from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS' --or Entity is null
-- select * from kgsonedatadb.trusted_compensation_yec

-- select distinct A.Cost_centre from kgsonedatadb.trusted_headcount_employee_dump A left join kgsonedatadb.config_year_end_costcenter_functiontag B on A.Cost_centre=B.Cost_centre where A.Entity = 'KGS' and B.Function_Tag is  null

select `Quarter`,count(1) from kgsonedatadb.trusted_employee_engagement_encore_output group by `Quarter`

-- COMMAND ----------


-- select distinct Status from kgsonedatadb.trusted_hist_headcount_employee_dump A inner join kgsonedatadb.trusted_hist_headcount_employee_details B on A.Employee_Number = B.Employee_Number

-- select distinct Nomination_Date,file_date,dated_on from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump

-- select count(distinct employee_number) from (select * from kgsonedatadb.trusted_headcount_employee_dump where Entity != 'KI') A left join kgsonedatadb.config_cost_center_business_unit B on A.Cost_Centre = B.Cost_centre

-- df_emp_dump = spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump where Entity != 'KI'")
-- print(df_emp_dump.count())

-- select cost_centre,BU from kgsonedatadb.config_cost_center_business_unit where Cost_centre in (select distinct Cost_centre from kgsonedatadb.trusted_headcount_employee_dump where Employee_Number in (select A.Employee_Number from kgsonedatadb.trusted_headcount_employee_dump A left join kgsonedatadb.config_cost_center_business_unit B on A.Cost_Centre = B.Cost_centre where A.Entity != 'KI' group by A.Employee_Number having count(1) > 1)) --order by Cost_centre

-- select A.Employee_Number from kgsonedatadb.trusted_headcount_employee_dump A left join kgsonedatadb.config_cost_center_business_unit B on A.Cost_Centre = B.Cost_centre where A.Entity != 'KI' group by A.Employee_Number having count(1) > 1

-- select distinct bu from kgsonedatadb.config_cost_center_business_unit

-- select * from kgsonedatadb.trusted_headcount_employee_dump ed left join (select * from kgsonedatadb.trusted_employee_engagement_thanks_dump where upper(Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED')) thd on ed.Employee_Number = thd.Nominee_s_Employee_ID where ed.Entity = 'KGS' or (ed.Entity='KI' and thd.Nominator_s_Employee_ID in (select Employee_Number from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS'))

-- select count(1) from kgsonedatadb.trusted_employee_engagement_thanks_dump where Nominee_s_Employee_ID in (select Employee_Number from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KI') and Nominator_s_Employee_ID in (select Employee_Number from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS')

-- select distinct File_Date from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS' --and !(Approved_LWD < to_date(File_Date))

-- select cast(cast('20230127' as string) as date)
-- SELECT     CONVERT(DATE, CAST(CAST(@input AS INT) AS CHAR(8)), 112)

-- select * from (select case when quarter = 1 then 2 when quarter = 2 then 3 when quarter = 3 then 4 when quarter = 4 then 1 end as kgs_quarter, year as normal_year, * from kgsonedatadb.trusted_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%') A where (A.normal_year = 2023 and A.kgs_quarter in (2,3,4)) or (A.normal_year = 2022 and A.kgs_quarter = 1)