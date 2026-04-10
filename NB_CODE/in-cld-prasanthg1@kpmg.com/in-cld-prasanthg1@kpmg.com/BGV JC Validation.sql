-- Databricks notebook source
select distinct Client_Geography from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KI';
select * from kgsonedatadb.config_position_jobname where Job_Name = (select Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '86278' and File_Date = '20240129');

select * from kgsonedatadb.config_hist_cc_bu_sl where cost_centre like '%Digital%Strategy%';



-- COMMAND ----------

select File_Date,Dated_On,Case_Initiation_Date,* from kgsonedatadb.trusted_hist_bgv_progress_sheet where Reference_No_ like '%KPMG%00085415%KGS%2022%' order by File_Date desc, Dated_On desc;

select File_Date,Email_Address,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '136093' order by File_Date desc;

-- COMMAND ----------

select File_Date,Email_Address,* from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number = '136093' order by File_Date desc;

select * from kgsonedatadb.

-- COMMAND ----------

select File_Date,Termination_Date from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number in ('103002','130103','87164') order by file_date desc;

select File_Date,* from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where EMPLOYEENUMBER = '103002' order by file_date desc;

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_bgv_progress_sheet where Candidate_s_Name like '%barman%'
--  Personal_Email_ID = 'souvik.barman2013@gmail.com'
--Reference_No_ like '%KPMG%00133543%KGS%2021%'

-- COMMAND ----------

select distinct DateFirstHired from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report 
-- where Candidate_Email = 'prakashlatkar2013@gmail.com'