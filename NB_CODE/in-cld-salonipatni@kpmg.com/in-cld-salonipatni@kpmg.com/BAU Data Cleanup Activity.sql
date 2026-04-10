-- Databricks notebook source
select * from kgsonedatadb.trusted_hist_employee_engagement_year_end

-- COMMAND ----------

talent_konnect_resignation_status_report
Employee_Details
employee_dump
termination_dump
headcount_report_weekly_ed
Maternity_Cases
Resigned_And_Left
Sabbatical
Secondee_Outward
uk_joiner
uk_leaver
uk_mover
us_joiner
us_leaver
us_mover

'Employee_Dump' - 6,12
,'Talent_Konnect_Resignation_Status_Report' 5,12
,'Termination_Dump'  - 6,12

talent_konnect_resignation_status_report
employee_dump
termination_dump
headcount_report_weekly_ed
Maternity_Cases
Employee_Details
Resigned_And_Left
Sabbatical
Secondee_Outward
uk_joiner
uk_leaver
uk_mover
us_joiner
us_leaver
us_mover

-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_uk_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_uk_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_uk_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_us_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_us_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd') union 
select count(1) from kgsonedatadb.raw_hist_jml_us_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_uk_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_uk_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_uk_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_us_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_us_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_jml_us_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_maternity_cases where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_resigned_and_left where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_sabbatical where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
select * from kgsonedatadb.trusted_hist_headcount_secondee_outward where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');


-- COMMAND ----------

-- delete from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_uk_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_uk_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_uk_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_us_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_us_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_jml_us_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_maternity_cases where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_resigned_and_left where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_sabbatical where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.trusted_hist_headcount_secondee_outward where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');

-- COMMAND ----------

-- delete from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_uk_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_uk_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_uk_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_us_joiner where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_us_leaver where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_jml_us_mover where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_employee_details where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_maternity_cases where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_resigned_and_left where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_sabbatical where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');
-- delete from kgsonedatadb.raw_hist_headcount_secondee_outward where to_date(File_Date,'yyyyMMdd') >= to_date('20230605','yyyyMMdd');

-- COMMAND ----------

select * from kgsonedatadb.