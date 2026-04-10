-- Databricks notebook source
select sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date >'2022-09-30' and Completion_Date <'2023-10-01'

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.test_raw_stg_lnd_lnd_final group by BU
order by BU

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
where Completion_Date >'2023-09-30'
group by BU
order by BU

-- COMMAND ----------

select distinct Local_HR_ID,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.test_raw_stg_lnd_lnd_final where BU='Tax' group by Local_HR_ID

-- COMMAND ----------

select * from kgsonedatadb.test_raw_stg_lnd_lnd_final where Local_HR_ID is null

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Email_Address__User_ ='karishmagoenka@kpmg.com'
and Completion_Date >'2023-09-30'
--Local_HR_ID='67694'  --and BU ='consulting'

-- COMMAND ----------


select distinct Local_HR_ID,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details
where Completion_Date >'2023-09-30' and BU='Tax' 
group by Local_HR_ID

-- COMMAND ----------

select CC,count(CPD_CPE_Hours_Awarded) as count from kgsonedatadb.test_raw_stg_lnd_lnd_final
where BU='Digital Nexus'
group by CC

-- COMMAND ----------

select Cost_Center,count(CPD_CPE_Hours_Awarded) as count from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
where Completion_Date >'2023-09-30' and BU='Digital Nexus' 
group by Cost_Center

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
where Completion_Date >'2023-09-30' and Local_HR_ID='140577'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number='126245'

-- COMMAND ----------

select * from kgsonedatadb.config_hist_cc_bu_sl where Cost_centre ='Nexus-Management'

-- COMMAND ----------

