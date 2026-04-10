-- Databricks notebook source
select distinct file_date,dated_on from kgsonedatadb.trusted_lnd_glms_kva_details where Completion_Date > '2023-09-30' 
-- trusted_lnd_glms_kva_details
-- create table kgsonedatadb.trusted_hist_lnd_glms_kva_details_backup_02212024 as
-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details





-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and file_date='20240410' and left(dated_on,10)='2024-04-10';
-- delete from kgsonedatadb.trusted_hist_lnd_glms_details where Completion_Date > '2023-09-30' and file_date='20240410' and left(dated_on,10)='2024-04-10';
-- delete from kgsonedatadb.trusted_lnd_glms_details where Completion_Date > '2023-09-30' and file_date='20240410' and left(dated_on,10)='2024-04-10';
-- delete from kgsonedatadb.trusted_lnd_glms_kva_details where Completion_Date > '2023-09-30' and file_date='20240410' and left(dated_on,10)='2024-04-10';


-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and left(Dated_On,10) ='2024-04-10';

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and left(Dated_On,10) ='2024-03-11' group by BU;

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as Total_Hours,sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID) as Per_Person_Learning_Hours,count(distinct Local_HR_ID) as HC from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and BU <> 'GDC' and CPD_CPE_Hours_Awarded <> 0 group by BU;

-- COMMAND ----------

select distinct A.Item_Title,A.Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and left(A.Dated_On,10) ='2024-02-21'

where File_Date='20240215' and completion_date >'2023-09-30'

select distinct A.Item_Title,A.Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and File_Date='20240210' and left(A.Dated_On,10) ='2024-02-21'

select distinct A.Item_Title,A.Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and File_Date='20240210' and left(A.Dated_On,10) ='2024-02-21'
and A.Item_Title like '%Teamwork%'

-- COMMAND ----------

-- join the two tables on the 'item_title' column with new line characters replaced
joined_table = table1.join(table2, 
                           regexp_replace(table1['item_title'], '\n', '') == regexp_replace(table2['item_title'], '\n', ''),
                           'inner')

-- COMMAND ----------

select distinct Item_Title,Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Item_Title like '%WD HCM%' and Item_Title not like 'KVA%'

-- COMMAND ----------

select * from kgsonedatadb.config_dim_training_category where ItemTitle like '%WD HCM%' and ItemTitle not like 'KVA%'

-- COMMAND ----------

 select count(Training_Category),Training_Category,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and left(Dated_On,10) ='2024-02-19' group by Training_Category

-- COMMAND ----------

select Item_Type_Category,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_stg_lnd_glms_debug_details where Completion_Date > '2023-09-30' group by Item_Type_Category

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_stg_lnd_glms_debug_details as A
USING kgsonedatadb.config_dim_training_category AS B
ON trim(A.Item_Title) = trim(B.ItemTitle) and (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and left(A.Dated_On,10) ='2024-01-10'
WHEN MATCHED THEN
UPDATE SET A.Training_Category = B.TrainingCategory

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

select count(Completion_Date) from kgsonedatadb.raw_hist_lnd_glms_details where File_Date='20231105'

-- COMMAND ----------

select * from kgsonedatadb.trusted_lnd_glms_kva_details where Local_HR_ID in
('119119',
'121066',
'122537',
'129641',
'129769',
'129770')
order by Local_HR_ID


-- COMMAND ----------

Emp Dump
Term Dump
Trainng category
CC BU
BU and GF
CC BU SL

-- COMMAND ----------

select distinct Cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_employee_details 
--where Cost_centre in ('DA Core-Strategy P&SC','Adv Technology-Product Strategy and Innovation','Capability Hubs-BOI-GAMG','MC-GDN-Microsoft-ERP') and File_Date ='20240304'
where Employee_Number in('91071','106873','137088','129875','122726','122960','137072','130301','133270',
'133834','105859','90415','119926','132209','137077','118794','112939','113886','96203','129203','77333','129895',
'91349','57230','125590','130594') and File_Date='20240220'

-- COMMAND ----------


select Employee_Number,Cost_centre,BU,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details 
--where Cost_centre in ('DA Core-Strategy P&SC','Adv Technology-Product Strategy and Innovation','Capability Hubs-BOI-GAMG','MC-GDN-Microsoft-ERP') and File_Date ='20240304'
where Employee_Number in('91071','106873','137088','129875','122726','122960','137072','130301','133270',
'133834','105859','90415','119926','132209','137077','118794','112939','113886','96203','129203','77333','129895',
'91349','57230','125590','130594')

select distinct Employee_Number,Cost_centre,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump 
where employee_number in ('91071','106873','137088','129875','122726','122960','137072','130301','133270',
'133834','105859','90415','119926','132209','137077','118794','112939','113886','96203','129203','77333','129895',
'91349','57230','125590','130594') order by File_Date desc
--where Cost_centre in ('DA Core-Strategy P&SC','Adv Technology-Product Strategy and Innovation','Capability Hubs-BOI-GAMG','MC-GDN-Microsoft-ERP') and File_Date ='20240304'
--Employee_Number in('143245','142020','36568','32233','142065','50789','143365') and File_Date ='20240304'

select * from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number='129769' and File_Date ='20231031'
'91071','106873','137088','129875','122726','122960','137072','130301','133270','133834','105859','90415','119926','132209','137077','118794','112939',
'113886','96203','129203','77333','129895','91349','57230','125590','130594'


-- COMMAND ----------

select * from kgsonedatadb.config_dim_level_wise where Position ='Partner-LT' and Job_Name like '%Partner-LT%'

-- COMMAND ----------

select * from kgsonedatadb.config_dim_training_category where ItemTitle =''

-- COMMAND ----------

select * from kgsonedatadb.config_hist_cc_bu_sl where File_Date ='20231018' and Cost_centre ='Capability Hubs-Corporate MGT'

-- COMMAND ----------

select * from kgsonedatadb.config_dim_global_function where Final_BU='Cap-Hubs'

-- COMMAND ----------

select * from kgsonedatadb.config_hist_cc_bu_sl where File_Date ='20231018' and Cost_centre ='Capability Hubs-Corporate MGT' and Client_Geography= 'CORPORATE' and Final_BU='Cap-Hubs'

-- COMMAND ----------

select count(1) ,sum(cpd_cpe_hours_awarded),training_category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2023-09-30' and left(dated_on,10)='2024-01-10' and BU != '
GDC' group by training_category

-- COMMAND ----------

select Level_Wise,count(Level_Wise),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2023-09-30' and left(dated_on,10)='2024-01-10' and BU != '
GDC' and Training_Category='Enabling'
group by Level_Wise

-- COMMAND ----------

