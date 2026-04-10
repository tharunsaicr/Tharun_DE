-- Databricks notebook source
select sum(CPD_CPE_Hours_Awarded),count(2),min(Completion_Date),max(Completion_Date) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date >'2023-07-31' and Completion_Date <'2023-09-01'

-- COMMAND ----------

-- DBTITLE 1,one data aug hours
select sum(CPD_CPE_Hours_Awarded),File_Type,Dated_On from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date >'2023-07-31' and Completion_Date <'2023-09-01' group by File_Type,Dated_On;
--GLMS 74347.91007936001 after removing KVA data 
--KVA  7045.18
--GLMS 81022.85007745214

select sum(CPD_CPE_Hours_Awarded)from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date >'2023-07-31' and Completion_Date <'2023-09-01';

--kgsonedatadb.test_trusted_stg_lnd_kva_details
select sum(CPD_Hours)from kgsonedatadb.test_trusted_stg_lnd_kva_details where Completion_Date >'2023-07-31' and Completion_Date <'2023-09-01';

-- COMMAND ----------

select (88068.03729737992-6674.93999809213)

-- COMMAND ----------

81393.09729928779-81452.11

-- COMMAND ----------

-- count - 79247
-- sum hours 81452.11

-- deepti kva - 7104.2 , glms - 74347.91
select (74347.91+7104.2)

-- COMMAND ----------

select 81452.11+7045

-- COMMAND ----------

select left(Completion_Date,7),sum(CPE_Hours),count(Completion_Date) from kgsonedatadb.test_trusted_stg_lnd_kva_details group by left(Completion_Date,7)

-- COMMAND ----------

select left(Completion_Date,7),sum(CPD_CPE_Hours_Awarded),count(Completion_Date) from kgsonedatadb.test_trusted_stg_lnd_glms_details where Item_Title not like 'KVA%'  group by left(Completion_Date,7)

-- COMMAND ----------

select left(Completion_Date,7),sum(CPD_CPE_Hours_Awarded),count(Completion_Date) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where (Item_Title not like 'KVA%' and file_type='GLMS') or file_type='KVA' and (CPD_CPE_Hours_Awarded = 0 or CPD_CPE_Hours_Awarded is null) group by left(Completion_Date,7)

-- COMMAND ----------

select CPD_CPE_Hours_Awarded,left(Completion_Date,7),File_Type,count(CPD_CPE_Hours_Awarded) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details group by CPD_CPE_Hours_Awarded,left(Completion_Date,7),File_Type

-- COMMAND ----------

-- count - 79247
-- sum hours 81452.11
-- count difference 2797
-- hours diff - -60

-- COMMAND ----------

select Employee_Id,sum(CPE_Hours) from kgsonedatadb.test_trusted_hist_lnd_kva_details where Completion_Date >'2023-07-31' and Completion_Date <'2023-09-01' group by Employee_Id

-- COMMAND ----------

select sum (CPE_Hours)from kgsonedatadb.test_trusted_hist_lnd_kva_details 
where (Completion_Date >'2022-09-30' and Completion_Date <'2023-09-01') 


-- COMMAND ----------

in('24066',
'34880',
'36285',
'37819',
'38935',
'42247',
'42348',
'54138',
'60223',
'63302',
'63356',
'68752',
'73181',
'73773',
'74105',
'77444',
'77666',
'80643',
'80822',
'80870',
'81296',
'81459',
'82112',
'82298',
'82855',
'83086',
'83883',
'84715',
'84734',
'84955',
'85376',
'85382',
'85573',
'85906',
'86323',
'86623',
'87028',
'87423',
'87514',
'89057',
'89912',
'90476',
'90789',
'91539',
'91775',
'92544',
'93627',
'93851',
'94191',
'97299',
'98788',
'100644',
'101557',
'102156',
'103520',
'103571',
'103919',
'106416',
'106955',
'107216',
'108456',
'108620',
'110073',
'111629',
'112741',
'113008',
'113836',
'113839',
'115422',
'115908',
'116011',
'116489',
'116922',
'117415',
'118395',
'118759',
'119114',
'119162',
'120598',
'120653',
'121337',
'121762',
'123563',
'124160',
'124773',
'125897',
'125965',
'126261',
'126282',
'126490',
'126830',
'128147',
'128661',
'128666',
'128677',
'128937',
'129760',
'130058',
'130363',
'130436',
'130749',
'131314',
'131519',
'133832',
'134451',
'135209',
'135228',
'137881',
'138437',
'138579',
'138583',
'138587',
'138592',
'138595',
'138601',
'138604',
'138605',
'138612',
'139076')

-- COMMAND ----------

select * from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Local_HR_ID = '12723' and Completion_Date = '2023-08-28';

select * from kgsonedatadb.test_raw_stg_lnd_kva_details where Employee_Id = '12723';

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.test_trusted_hist_lnd_glms_details where Local_HR_ID = '12723' and Completion_Date = '2023-08-28';

select sum(cpe_hours) from kgsonedatadb.test_trusted_hist_lnd_kva_details where Employee_Id = '12723' and Completion_Date = '2023-08-28';

select sum(CPE_hours),sum(cpe_hours/count) from (
select Local_HR_ID,Item_Title,CPD_CPE_Hours_Awarded,count(1) as count,sum(CPD_CPE_Hours_Awarded) as CPE_hours from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details group by Local_HR_ID,Item_Title,CPD_CPE_Hours_Awarded having count(1)>1 and sum(CPD_CPE_Hours_Awarded) >0) A

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded),min(Completion_Date),max(Completion_Date) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date >'2022-09-30' and Completion_Date <'2023-09-01'

-- COMMAND ----------

select (1425228.9202557486+74417.03)- 1387674.52

-- COMMAND ----------

