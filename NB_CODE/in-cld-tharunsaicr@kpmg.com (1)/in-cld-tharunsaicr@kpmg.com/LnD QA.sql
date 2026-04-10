-- Databricks notebook source
select distinct Item_Title from kgsonedatadb.trusted_hist_lnd_glms_kva_details  where Item_Title like 'Own Your Voice: Improve%Presentations and Executive Presence'

-- COMMAND ----------

select Local_HR_ID,left(Completion_Date,7),count(1),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2023-08-01' and file_type = 'GLMS' group by Local_HR_ID,left(Completion_Date,7) order by Local_HR_ID,left(Completion_Date,7)

-- COMMAND ----------

select distinct file_date,Dated_On from kgsonedatadb.trusted_hist_lnd_glms_kva_details order by file_date desc
--2024-02-21T11:16:36.000+0000  20240215

-- COMMAND ----------

select distinct file_date,Dated_On from kgsonedatadb.trusted_lnd_glms_kva_details order by file_date desc
select sum(CPD_CPE_Hours_Awarded) as total_hours , BU from kgsonedatadb.trusted_lnd_glms_kva_details where File_Date='20240215' group by BU

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240110' and left(Dated_On,10) ='2024-02-21' 
and BU !='GDC'
and Email_Address__User_ like '%karishmagoenka%'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) as total_hours , BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and Completion_Date > '2023-09-30'
and left(Dated_On,10) ='2024-04-15' 
group by BU

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as total_hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and BU <> 'GDC' and left(Dated_On,10) ='2024-02-15' group by BU;

select sum(CPD_CPE_Hours_Awarded) as total_hours , BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
where File_Date='20240310' 
--and BU is null
and BU != 'GDC' 
group by BU
--,File_Date having BU <> 'GDC' and File_Date='20240215'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240215' and BU <>'GDC'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pip install unidecode

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql("select distinct Item_Title,training_category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_date>'2023-09-30' and File_Date='20240410' and (Training_Category is null or Training_Category ='') ")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from unidecode import unidecode
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql.types import StringType

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define a UDF to apply unidecode to each element in a column
-- MAGIC unidecode_udf = udf(lambda x: unidecode(str(x)), StringType())
-- MAGIC
-- MAGIC # Apply the UDF to a column called 'column_name'
-- MAGIC df = df.withColumn('Item_Title', unidecode_udf('Item_Title'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- %python
-- from pyspark.sql.functions import regexp_replace

-- df = df.withColumn('Item_Title', regexp_replace('Item_Title', '[^a-zA-Z0-9: - _?().,$%#@ \\s]+', ''))
-- display(df)

-- COMMAND ----------



-- COMMAND ----------

select distinct File_Date,dated_on from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

-- DBTITLE 1,Select Item title , training category
select count(Item_Title) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and (Training_Category is null or Training_Category ='') --6128
select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and (Training_Category is null or Training_Category ='')-- 18217.16

-- update kgsonedatadb.trusted_lnd_glms_kva_details
-- set Item_Title='TBD'
-- where File_Date='20240215' and (Item_Title is null or Item_Title ='')

select distinct Item_Title,training_category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_date>'2023-09-30' and File_Date='20240410' and (Training_Category is null or Training_Category ='') 

select distinct item_title,Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_date>'2023-09-30' and File_Date='20240310' 
and (Training_Category is null or Training_Category ='') 
and item_title like '%Training%'

-- COMMAND ----------

select * from kgsonedatadb.config_dim_training_category where ItemTitle like '%Alteryx Foundational%' and ItemTitle not like 'KVA%' 

-- COMMAND ----------

-- DBTITLE 1,Position Job Name Level Wise
select Local_HR_ID,Position,Job_Name,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where 
left(Dated_On,10) ='2024-04-15'
-- File_Date='20240410' 
and (Job_Name is null or trim(Job_Name)='' or Job_Name='null');

select distinct Local_HR_ID,Email_Address__User_,Position,Job_Name,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where 
-- File_Date='20240410' 
left(Dated_On,10) ='2024-04-15'
and (Level_Wise is null or trim(Level_Wise)='' or Level_Wise='null')
and Local_HR_ID in ('74400' ,'137384','137382');

select distinct Local_HR_ID,Position,Job_Name from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410'
and left(Dated_On,10) ='2024-04-15' and level_wise is null or Position is null or Job_name is null;

select distinct Position,Job_Name from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240310' and Level_Wise ='AD and Above'

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set Job_Name ='Secretary'
where completion_date >'2023-09-30' and left(Dated_On,10) ='2024-04-15' and Local_HR_ID in('139078') and Job_Name is null

Associate Senior Secretary
120.Secretary

--select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240110' and Level_Wise is null and job_name is null
-- 142580 soniakurmi@kpmg.com  74400 pranayvij@kpmg.com ,Position ='Consultant' and Job_Name ='Senior'

-- COMMAND ----------

select * from kgsonedatadb.config_dim_level_wise where Position like '%Associate Senior Secretary%' --and Job_Name like '%080%'

-- COMMAND ----------

-- DBTITLE 1,cost centre update
--select distinct Employee_Number,Cost_centre,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in('123749') order by File_Date desc
-- select * from kgsonedatadb.trusted_hist_headcount_termination_dump where File_Date='20240102' and Employee_Number in('123749','142065','142020','32233','50789','36568') File_Date='20231114' and 

select * from kgsonedatadb.trusted_lnd_glms_kva_details where completion_date >'2023-09-30' and File_Type = 'GLMS' and left(Dated_On,10) ='2024-01-10' and (Cost_Center is null or trim(Cost_Center)='') and Local_HR_ID in ('145235','143810','143097','144413','144604','142020','36568','144584','144008','140571','32233','144621','142065','143532','50789','144594','145047','144409','142916','145048','144810',
'144772')

select distinct employee_Number from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date='20240408' and 
cost_centre in ('MC-GDN-SAP-EPM-INV','MC-GDN-Cloud-Architecture-INV','MC-GDN-SAP-Tech Dev-INV','MC-ASE-SAP GGA','Tax-USGMS','Tax-BTS Corp Velocity','Tax-USCT')
-- employee_Number
-- in ('145235','143810','143097','144413','144604','142020','36568','144584','144008','140571','32233','144621','142065','143532','50789','144594','145047','144409','142916','145048','144810',
-- '144772')
-- 142580 Position=Consultant,Job_Name=080.Senior --> 74400 Position=Associate Consultant,Job_Name=085.Executive file_date=20231128 20231120 20231114 20231114 20231106
--('123749','142065','142020','32233','50789','36568')

-- 123749- DA Core - Infra Commercial Advisory - 20231114
-- 36568,50789,32233 Audit-DSG-KASP - 20230925
-- no cost center for '142065','142020'

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set Cost_Center ='MC-GDN-Cyber-Transformation Archer-INV'
where completion_date >'2023-09-30' and left(Dated_On,10) ='2024-04-15' and (Cost_Center is null or trim(Cost_Center)='') and CC in ('MC-GDN-Cyber-Transformation-Archer') and Local_HR_ID in 
('143810')




select distinct local_hr_id,Cost_Center,CC,BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' 
and (Cost_Center is null or trim(Cost_Center)='') order by Local_HR_ID
-- and Local_HR_ID in ('142020','142065')

select distinct Cost_Center,CC,Geo,BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and Completion_Date > '2023-09-30' and left(Dated_On,10) ='2024-04-10' and(BU is null or trim(BU)='');

-- '142020','36568','123749','32233','142065','143245','143365','50789'
-- '142020','36568','123749','32233','142065','50789'


-- COMMAND ----------

-- DBTITLE 1,BU Update
select 
-- count(CPD_CPE_Hours_Awarded)
distinct Cost_Center,BU 
from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and (BU is null or trim(BU)='' )--or Cost_Center is null or trim(Cost_Center)='')
select * from kgsonedatadb.trusted_lnd_glms_kva_details where File_Date='20240110' and (BU is null or trim(BU)='') and Local_HR_ID ='123749'

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set BU ='TBD'
where completion_date >'2023-09-30' and left(Dated_On,10) ='2024-04-15' and (BU is null or trim(BU)='') and 
Cost_Center in ('MC-GDN-SAP-EPM-INV','MC-GDN-Cloud-Architecture-INV','MC-GDN-SAP-Tech Dev-INV','MC-ASE-SAP GGA','Tax-USGMS','Tax-BTS Corp Velocity','Tax-USCT')

-- COMMAND ----------

select distinct Cost_centre,BU from kgsonedatadb.config_hist_cc_bu_sl
where File_Date='20240320' 
and Cost_centre in 
('MC-GDN-SAP-EPM-INV',
'MC-GDN-Cloud-Architecture-INV',
'MC-GDN-SAP-Tech Dev-INV',
'MC-ASE-SAP GGA',
'Tax-USGMS',
'Tax-BTS Corp Velocity',
'Tax-USCT')



select distinct file_date from kgsonedatadb.trusted_hist_headcount_employee_dump

select distinct position,Cost_centre,employee_number,Client_Geography from kgsonedatadb.trusted_hist_headcount_employee_dump
where file_date='20240122' and employee_number in ('138866','141826')
--('142020','36568','123749','32233','142065','143245','143365','50789')
--('32262','130289','36827','130023','129855')
--('142020','36568','123749','32233','142065','143245','143365','50789')
--Cost_centre ='MC-KAU-ASOS'
'32262','130289','36827','130023','129855'

-- COMMAND ----------

-- DBTITLE 1,Global Function
select distinct BU,Global_Function from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' 
and Global_Function is null 
group by Global_Function;

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set Global_Function ='Advisory'
where completion_date >'2023-09-30' and File_Date='20240410' and left(Dated_On,10) ='2024-04-15'  and BU ='Consulting' and Global_Function is null

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- set Global_Function ='Advisory'
-- and Local_HR_ID in ('142065','142020','32233','50789','36568')
-- where completion_date >'2023-09-30' and File_Type = 'GLMS' and left(Dated_On,10) ='2024-01-10' and Local_HR_ID ='123749' and Cost_Center='DA Core - Infra Commercial Advisory' and BU ='DAS'

select * from kgsonedatadb.config_dim_global_function; KRC-->Audit Consulting--> Advisory

-- COMMAND ----------

-- DBTITLE 1,Service Line
select distinct Local_HR_ID,Cost_Center,BU,Geo,Local_Service_Line from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240110' and SL is null group by Local_HR_ID,Cost_Center,BU,Geo

select distinct Cost_Center,BU,Geo,Local_Service_Line,SL from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and (SL is null or trim(SL)='')
and BU !='TBD'
-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- set SL='Audit-ACE'
-- where completion_date >'2023-09-30' and File_Type = 'GLMS' and left(Dated_On,10) ='2024-01-10' and Local_HR_ID ='74400' and (SL is null or trim(SL)='')

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240215' and SL is null and Cost_center='MC-TE-Data&Tech'
and BU='Consulting'
and Geo='UK'

select File_Date,BU,Cost_centre,Service_Line from kgsonedatadb.config_hist_cc_bu_sl --where File_Date='20240118'
where Cost_centre in ('Capability Hubs-QRM-Support Echo')
--where Cost_centre like '%MC-%'
-- ='20240118'
-- and Cost_centre='Consulting'
-- and Final_BU='Consulting'
-- and Client_Geography='KDN'
order by File_Date desc

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set SL ='QRM'
where completion_date >'2023-09-30' and left(Dated_On,10) ='2024-04-15' 
and Cost_Center in ('Capability Hubs-QRM-Support Echo')
and BU ='Cap-Hubs' 
-- and Geo ='CORPORATE' 
and SL is null 
--and Local_HR_ID in ('141717')

-- COMMAND ----------

select * from kgsonedatadb.config_hist_cc_bu_sl where Cost_Centre='BIS/Gen Research' or Service_Line='BIS/Gen Research'

-- COMMAND ----------

select distinct Cost_centre,Client_Geography from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date='20240108' and Employee_Number in
('110868',
'112151',
'141826',
'142580',
'134735',
'54388',
'74400',
'138866',
'140577')
order by Cost_centre

-- COMMAND ----------

select distinct Employee_Number,Cost_centre,Client_Geography,BU from kgsonedatadb.trusted_hist_headcount_employee_details where File_Date='20231218' and Employee_Number in
('110868',
'112151',
'141826',
'142580',
'134735',
'54388',
'74400',
'138866',
'140577')
order by Cost_centre

-- COMMAND ----------

select distinct Cost_Center,BU,Geo,SL from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240110' and Local_HR_ID in
('110868',
'112151',
'141826',
'142580',
'134735',
'54388',
'74400',
'138866',
'140577') group by Cost_Center,BU,Geo,SL order by Cost_Center

-- COMMAND ----------

select Item_Type_Category,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' group by Item_Type_Category

-- COMMAND ----------

select Level_Wise,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' group by Level_Wise

-- COMMAND ----------

select Training_Category,sum(CPD_CPE_Hours_Awarded) as total_hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and completion_date >'2023-09-30' group by Training_Category order by Training_Category

-- select distinct Training_Category,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240210' and completion_date >'2023-09-30'
-- and Training_Category ='Enabling' and Level_Wise='AD and Above'

-- COMMAND ----------

select 
count(CPD_CPE_Hours_Awarded)
-- distinct level_wise,Training_Category 
from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date='20240410' and completion_date >'2023-09-30' and Training_Category ='Enabling' and Level_Wise='AD and Above' and left(Dated_On,10) ='2024-04-10' 

-- COMMAND ----------

-- DBTITLE 1,Training Category Update
MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
USING kgsonedatadb.config_dim_training_category AS B
ON trim(A.Item_Title_decode) = trim(B.ItemTitle) and (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and left(A.Dated_On,10) ='2024-04-15'
WHEN MATCHED THEN
UPDATE SET A.Training_Category = B.TrainingCategory

--6128

-- COMMAND ----------

MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
USING kgsonedatadb.config_dim_training_category AS B
ON regexp_replace(A.Item_Title,'\n', ' ') = regexp_replace(B.ItemTitle,'\n', ' ') and (A.Training_Category is null or trim(A.Training_Category) ='') and completion_date >'2023-09-30' and A.File_Type = 'GLMS' and left(A.Dated_On,10) ='2024-03-11'
WHEN MATCHED THEN
UPDATE SET A.Training_Category = B.TrainingCategory

-- COMMAND ----------

update kgsonedatadb.trusted_hist_lnd_glms_kva_details
set Training_Category='Technology/ Techno-functional'
where (Training_Category is null or trim(Training_Category) ='') and completion_date >'2023-09-30' and File_Type = 'GLMS' and left(Dated_On,10) ='2024-04-10'
and Item_Title like '%UK Payroll Technical%'

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- set Training_Category='Leadership'
-- where Training_Category ='Enabling' and Level_Wise='AD and Above' and completion_date >'2023-09-30' and left(Dated_On,10) ='2024-02-21'

-- COMMAND ----------

