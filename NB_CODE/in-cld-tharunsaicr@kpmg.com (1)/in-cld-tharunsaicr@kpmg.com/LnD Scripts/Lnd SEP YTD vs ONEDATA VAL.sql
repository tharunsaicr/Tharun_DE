-- Databricks notebook source
select left(completion_date,7),count(completion_date) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' group by left(completion_date,7)

-- COMMAND ----------

select left(completion_date,7),count(completion_date) from kgsonedatadb.lnd_sep_ytd where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' group by left(completion_date,7)

-- COMMAND ----------

select Local_HR_ID,count(Local_HR_ID),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' and left(completion_date,7)='2022-10' group by Local_HR_ID

-- COMMAND ----------

select Local_HR_ID,count(Local_HR_ID),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.lnd_sep_ytd where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' and left(completion_date,7)='2023-02' group by Local_HR_ID

-- COMMAND ----------

select count(*) from
(select Local_HR_ID,left(completion_date,7),count(Local_HR_ID) from kgsonedatadb.lnd_sep_ytd where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' group by Local_HR_ID,left(completion_date,7)
minus
select Local_HR_ID,left(completion_date,7),count(Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' group by Local_HR_ID,left(completion_date,7))

-- COMMAND ----------

select Item_Title from kgsonedatadb.lnd_sep_ytd where Local_HR_ID='39296' and Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01'
minus
select Item_Title from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID='39296' and Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01' 

--and left(completion_date,7)='2022-10' Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' and Item_Title in('KVA - Test Automation with Python: 6 Elements and Selectors','KVA - Mastering Communications as a Leader','KVA - Leadership Foundations')

-- COMMAND ----------

select * from kgsonedatadb.lnd_sep_ytd where Local_HR_ID='39296' and Item_Title in('KVA - Leading with Emotional Intelligence',"KVA - What's the So What: Writing Clearly for a Business Audience","KVA - What Is Generative AI?","KVA - Test Automation with Python: 6 Elements and Selectors",'KVA - Learning to Be Assertive','KVA - Network Concepts and Protocols','KVA - Server Administration Essential Training','KVA - Leadership Foundations','KVA - Project Management Foundations: Communication')

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID='39296' and Item_Title in('KVA - Leading with Emotional Intelligence',"KVA - What's the So What: Writing Clearly for a Business Audience","KVA - What Is Generative AI?","KVA - Test Automation with Python: 6 Elements and Selectors",'KVA - Learning to Be Assertive','KVA - Network Concepts and Protocols','KVA - Server Administration Essential Training','KVA - Leadership Foundations','KVA - Project Management Foundations: Communication')

-- COMMAND ----------

select count(*) from kgsonedatadb.lnd_sep_ytd where Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01'  --834393 full count 


-- COMMAND ----------

select count(*) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01' and CPD_CPE_Hours_Awarded <>'0'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01' and Local_HR_ID='29082' and CPD_CPE_Hours_Awarded <>'0' --and BU <>'GDC'

select * from kgsonedatadb.lnd_sep_ytd where Completion_Date > '2022-09-30' and Completion_Date <'2023-10-01' and Local_HR_ID='29082'

-- COMMAND ----------

select Local_HR_ID,left(completion_date,7),count(Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and CPD_CPE_Hours_Awarded <> 0 and Completion_Date <'2023-10-01' group by Local_HR_ID,left(completion_date,7)) '39296'
