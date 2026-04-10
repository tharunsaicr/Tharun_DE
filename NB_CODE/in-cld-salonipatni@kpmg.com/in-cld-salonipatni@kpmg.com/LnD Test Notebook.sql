-- Databricks notebook source
-- select Local_HR_ID,Cost_Centre,Geo,BU,File_Date from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where File_Date = '20230430' and Local_HR_ID in ('134437','27824')

select * from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where File_Date = '20230430' and File_Type = 'GLMS'
and Remarks = 'Training Category is Null' --and Local_HR_ID in ('134437')
-- 'Level Wise is Null'
-- 'BU is Null'


-- COMMAND ----------

-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING (select distinct Cost_Centre,Final_BU, Service_Line from kgsonedatadb.config_cc_bu_sl) AS B
-- ON A.Cost_Center = B.Cost_Centre and A.BU = B.Final_BU  and
-- (A.BU is not null or A.BU != '' ) and (A.SL is null or trim(A.SL)='') and A.File_Type='GLMS'and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.SL =B.service_line

-- COMMAND ----------

select File_Date,Dated_On,count(1) from (select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430') and File_Type = 'GLMS') group by Dated_On,File_Date


-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430') and File_Type = ('GLMS') and Dated_On = (select min(Dated_On) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430')) and Local_HR_ID in ('134437')

-- select from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430') and File_Type = ('GLMS') and Dated_On = (select max(Dated_On) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430'))

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details  set Dated_On = (select distinct dated_on from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on = (select min(Dated_On) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430') and  File_Type = ('GLMS'))) where File_Date in ('20230430') and File_Type = ('GLMS') and Dated_On = (select max(Dated_On) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430'))

-- select distinct dated_on from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on = (select min(Dated_On) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230430') and  File_Type = ('GLMS'))

-- COMMAND ----------


select count(1) from kgsonedatadb.trusted_lnd_glms_details

-- COMMAND ----------

select distinct CC from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and File_Type = 'KVA' -- 26

-- local_HR_Id,Cost_Center,BU,SL,File_Date


-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING kgsonedatadb.config_dim_training_category AS B
-- ON A.Item_Title = B.ItemTitle and (A.Training_Category is null or trim(A.Training_Category) ='') and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and A.File_Type = 'GLMS'
-- WHEN MATCHED THEN
-- UPDATE SET A.Training_Category = B.TrainingCategory


-- select Local_HR_ID,Cost_Center,BU,SL from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Cost_Center = 'MC-OneData-QE' and (BU is null or BU ='')

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set BU ='Consulting' where Cost_Center = 'MC-OneData-QE' and (BU is null or BU ='')

-- delete from  kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID in ('134437','27824') and File_Type ='GLMS' and File_Date = '20230430'

-- COMMAND ----------


select Cost_Centre,Final_BU,Client_Geography,Service_Line from kgsonedatadb.config_cc_bu_sl where Cost_Centre = 'DA Core-Modeling'

-- select Cost_centre,BU,count(1) from kgsonedatadb.config_cc_bu_sl group by Cost_centre,BU

-- COMMAND ----------


-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Cost_Center = 'null' and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')

select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

-- COMMAND ----------


select * from 
-- kgsonedatadb.trusted_hist_bgv_kcheck
-- kgsonedatadb.trusted_hist_bgv_offer_release
-- kgsonedatadb.trusted_hist_bgv_progress_sheet
-- kgsonedatadb.trusted_hist_bgv_upcoming_joiners
-- kgsonedatadb.trusted_hist_bgv_waiver_tracker

-- kgsonedatadb.raw_hist_bgv_kcheck
-- kgsonedatadb.raw_hist_bgv_offer_release
-- kgsonedatadb.raw_hist_bgv_progress_sheet
-- kgsonedatadb.raw_hist_bgv_waiver_tracker


-- where File_Date not in ('20230228')
-- group by File_Date

-- COMMAND ----------

-- CPD/CPE Hours Awarded	-from source
-- Local Service Line	-source


-- Period	-Done
-- Global Function - Done
-- Training Category	
-- Level Wise	
-- CC	
-- BU	
-- Geo	
-- SL	
-- Location	-Done
-- Item Type category

-- COMMAND ----------


select distinct Cost_Center,BU,Global_Function,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Global_Function is Null or trim(SL) = '' or SL = 'null') and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')

-- COMMAND ----------

-- Training Category

select distinct Item_Title,Training_Category,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Training_Category is Null or trim(Training_Category) = '' or Training_Category = 'null') and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')


-- COMMAND ----------

-- select Item_Title,count(1)  from ItemTitle_TrainingCategory  group by Item_Title having count(1)>1

select ItemTitle,count(1)  from kgsonedatadb.config_dim_training_category  group by ItemTitle having count(1)>1

-- COMMAND ----------

-- DBTITLE 1,Update Training Category
-- create view ItemTitle_TrainingCategory as 
select distinct Item_Title,TrainingCategory from (select distinct Item_Title,Training_Category,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Training_Category is Null or trim(Training_Category) = '') a join kgsonedatadb.config_dim_training_category b  on a.Item_Title = b.ItemTitle

-- select * from ItemTitle_TrainingCategory

-- select distinct Item_Title from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Training_Category is Null or trim(Training_Category) = '') and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') --61240

-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING ItemTitle_TrainingCategory AS B
-- ON A.Item_Title = B.Item_Title and (A.Training_Category is null or trim(A.Training_Category) ='') and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.Training_Category = B.TrainingCategory


-- COMMAND ----------

select * from (select distinct Item_Title from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Training_Category is NULL) or(trim(Training_Category) = '') and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430'))

-- COMMAND ----------

-- Levelwise

select distinct Position,Job_Name,Original_Levelwise_Mapping,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is Null and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')



-- COMMAND ----------

-- create view emp_location as select * from (select distinct Employee_Number, Location,File_Date,row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_employee_dump) t where Location is not null and rn=1;


-- COMMAND ----------

-- create view termination_location as select * from (select distinct Employee_Number, Location,File_Date,row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_termination_dump) t where Location is not null and rn=1;

select * from termination_location 

-- COMMAND ----------

-- create table kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023
select distinct Location from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')

-- insert into kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023

-- COMMAND ----------

-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING termination_location AS B
-- ON A.Local_HR_ID = B.Employee_Number and A.Location is null and A.Local_HR_ID IS NOT NULL and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.Location = B.Location

-- COMMAND ----------

-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING emp_location AS B
-- ON A.Local_HR_ID = B.Employee_Number and A.Location is null and A.Local_HR_ID IS NOT NULL and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.Location = B.Location



-- COMMAND ----------

-- Global Function

select distinct BU,Global_Function,File_Date,File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Global_Function is null or Global_Function='') and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')


-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Global_Function = 'Advisory' where (Global_Function is null or Global_Function='') and File_Date in ('20230430')

-- COMMAND ----------

-- select * from (select distinct BU,Global_Function from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (trim(Global_Function) = '' or Global_Function is Null) and (BU is not null or BU !='') and File_Date in (
-- '20221001',
-- '20221031',
-- '20221101',
-- '20221130',
-- '20221201',
-- '20221231',
-- '20230101',
-- '20230131',
-- '20230201',
-- '20230228',
-- '20230301',
-- '20230331',
-- '20230401',
-- '20230430')) a left join kgsonedatadb.config_dim_global_function b on a.BU=b.Final_BU


-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details 

-- -- set Global_Function ='Advisory' where BU ='RS' and
-- -- set Global_Function ='Advisory' where BU ='Digital Nexus' and
-- -- set Global_Function ='Advisory' where BU ='MS' and
-- -- set Global_Function ='Advisory' where BU ='Consulting' and
-- -- set Global_Function ='Advisory' where BU ='DAS' and
-- -- set Global_Function ='Audit' where BU ='GDC' and
-- -- set Global_Function ='Audit' where BU ='KRC' and
-- -- set Global_Function ='Infrastructure & Central Support' where BU ='Cap-Hubs' and
-- -- set Global_Function ='Infrastructure & Central Support' where BU ='CF' and
-- set Global_Function ='Tax' where BU ='Tax' and

-- (trim(Global_Function) = '' or Global_Function is Null) and (BU is not null or BU !='') and File_Date in (
-- '20221001',
-- '20221031',
-- '20221101',
-- '20221130',
-- '20221201',
-- '20221231',
-- '20230101',
-- '20230131',
-- '20230201',
-- '20230228',
-- '20230301',
-- '20230331',
-- '20230401',
-- '20230430')

-- COMMAND ----------

select distinct Local_HR_ID,Position,Job_Name,File_Date,File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (Position is null or Position = '') and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430')


-- 118732 - Not available in TD or ED
-- 131899-  Update manually

-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number='131899'

-- select Entity,Position,Job_Name,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number='131899'

-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID='131899'

select * from kgsonedatadb.config_cost_center_business_unit

-- COMMAND ----------

-- Cost_centre,BU,SL - KVA

select distinct CC,BU from (select distinct Local_HR_ID,Email_Address__User_,CC,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='KVA')

-- Add 
-- Capability Hubs-QRM-KBS-Assurance Support
-- Audit-Quality and Risk


-- COMMAND ----------

select Employee_number,Cost_centre,Position,Job_Name,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('27449','131591','133781') order by Employee_Number,File_Date

-- COMMAND ----------

-- Cost_centre,BU,SL -- GLMS

-- select distinct Cost_center,BU,SL,File_Date from (select distinct Local_HR_ID,Email_Address__User_,Cost_Center,BU,SL,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and File_Type='GLMS')

select distinct Cost_center,Geo,BU,SL,File_Date from (select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and File_Type='GLMS');


-- select * from kgsonedatadb.config_cc_bu_sl where Cost_centre in (select distinct Cost_center from (select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and File_Type='GLMS'))

-- COMMAND ----------

select distinct Cost_centre from (select Employee_number,Cost_centre,Position,Job_Name,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('133320',
'117737',
'133781',
'122297',
'131692',
'133540',
'87023',
'113425',
'111557',
'87023',
'131692',
'122297',
'117737',
'133071',
'133781',
'123240',
'117754',
'113431',
'132063',
'103120',
'87023',
'131591',
'133540',
'131899',
'133320',
'126910'
) order by Employee_Number,File_Date)


-- COMMAND ----------

select distinct * from (select distinct CC as Cost_Centre,BU from (select distinct Local_HR_ID,Email_Address__User_,CC,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='KVA')

union

select distinct Cost_center,BU from (select distinct Local_HR_ID,Email_Address__User_,Cost_Center,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='GLMS')) a left join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Centre = b.Cost_Centre

-- COMMAND ----------

select distinct * from (select distinct CC as Cost_Centre,BU from (select distinct Local_HR_ID,Email_Address__User_,CC,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='KVA')

union

select distinct Cost_center,BU from (select distinct Local_HR_ID,Email_Address__User_,Cost_Center,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='GLMS')) a left join kgsonedatadb.config_cc_bu_sl b on a.Cost_Centre = b.Cost_Centre

-- COMMAND ----------

select distinct POsition,Job_Name from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Position in ('Audit Associate 2','Audit Associate 1')

-- COMMAND ----------

select distinct Function,Employee_Subfunction,Employee_Subfunction_1,a.Cost_centre,Client_Geography,b.BU from (select distinct CC as Cost_Centre,BU from (select distinct Local_HR_ID,Email_Address__User_,CC,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='KVA')

union

select distinct Cost_center,BU from (select distinct Local_HR_ID,Email_Address__User_,Cost_Center,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where ((BU is null) or  (BU = '')) and (Local_HR_ID IS NOT NULL) and File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and File_Type='GLMS')) a left join kgsonedatadb.trusted_hist_headcount_monthly_employee_details b on a.Cost_Centre = b.Cost_Centre

-- COMMAND ----------

select distinct Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography from kgsonedatadb.trusted_hist_headcount_employee_dump where Cost_centre in
('MC-ESG-Climate Data & Tech-Business',
'MC-ESG-Climate Data & Tech-Engg','Tax-T&L Ops') --and BU is not null

-- COMMAND ----------

select * from (select a.Local_HR_ID,b.location,b.File_Date from (select distinct Local_HR_ID,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and Location is null) a left join (select * from (select distinct Employee_Number, Location,File_Date,row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_employee_dump) t where Location is not null and rn=1) b on a.Local_HR_ID = b.Employee_Number) where Location is null

-- COMMAND ----------

select distinct Local_Hr_Id,Cost_Center,Position,Job_Name,BU,Global_Function,Geo,SL  from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and (Cost_Center is null or trim(Cost_Center) ='') and File_Type='GLMS'

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- set BU ='Consulting',
-- SL = 'PMO, Source & Testing',
-- Global_Function ='Advisory',
-- Cost_Center='MC-ASE-Proc Support' where Local_Hr_Id ='131899' and File_Date in (
-- '20221001',
-- '20221031',
-- '20221101',
-- '20221130',
-- '20221201',
-- '20221231',
-- '20230101',
-- '20230131',
-- '20230201',
-- '20230228',
-- '20230301',
-- '20230331',
-- '20230401',
-- '20230430') and Bu is null

-- COMMAND ----------

-- create view glms_cc_bu_sl as
-- select a.*,b.BU,c.Global_Function,d.service_line from (select * from (select Employee_Number, Cost_Centre,Client_Geography,Position,Job_Name, row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in 
-- ('133540',
-- '133781',
-- '87023',
-- '122297',
-- '133320',
-- '111557',
-- '117737',
-- '131692',
-- '113425',
-- '103120',
-- '126910',
-- '132063',
-- '117754',
-- '113431',
-- '133071',
-- '131591',
-- '123240') and Cost_centre is not null and trim(Cost_centre) != '') where rn=1) a join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Centre = b.Cost_Centre join kgsonedatadb.config_dim_global_function c on b.BU = c.Final_BU join kgsonedatadb.config_cc_bu_sl d on a.cost_centre = d.Cost_centre and a.Client_Geography = d.Client_Geography and b.BU=d.Final_BU



select * from glms_cc_bu_sl



-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in (
-- '20221001',
-- '20221031',
-- '20221101',
-- '20221130',
-- '20221201',
-- '20221231',
-- '20230101',
-- '20230131',
-- '20230201',
-- '20230228',
-- '20230301',
-- '20230331',
-- '20230401',
-- '20230430') and (Cost_Center is null or trim(Cost_Center) ='') and (Global_Function is null) and (BU is null) and (SL is null) and File_Type='GLMS'




-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING glms_cc_bu_sl AS B
-- ON A.Local_HR_ID = B.Employee_Number and A.Local_HR_ID IS NOT NULL and
-- (A.Cost_Center is null or trim(A.Cost_Center) = '') and (A.Global_Function is null) and (A.BU is null) and (A.SL is null) and A.File_Type='GLMS'and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.Cost_Center = B.Cost_Centre, A.Global_Function = B.Global_Function,A.BU = B.BU, A.SL =B.service_line

-- COMMAND ----------

-- create view glms_cc_bu_sl as
select a.*,b.BU,c.Global_Function,d.service_line from (select * from (select Employee_Number, Cost_Centre,Client_Geography,Position,Job_Name, row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in 
('133540',
'133781',
'87023',
'122297',
'133320',
'111557',
'117737',
'131692',
'113425',
'103120',
'126910',
'132063',
'117754',
'113431',
'133071',
'131591',
'123240') and Cost_centre is not null and trim(Cost_centre) != '') where rn=1) a join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Centre = b.Cost_Centre join kgsonedatadb.config_dim_global_function c on b.BU = c.Final_BU join kgsonedatadb.config_cc_bu_sl d on a.cost_centre = d.Cost_centre and a.Client_Geography = d.Client_Geography and b.BU=d.Final_BU

-- COMMAND ----------

-- create view glms_cc_bu_sl as
select a.*,b.BU,c.Global_Function from (select * from (select Employee_Number, Cost_Centre,Client_Geography,Position,Job_Name, row_number() over (partition by employee_number order by File_Date desc)rn from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in 
('133540',
'133781',
'133320') and Cost_centre is not null and trim(Cost_centre) != '') where rn=1) a join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Centre = b.Cost_Centre join kgsonedatadb.config_dim_global_function c on b.BU = c.Final_BU 


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in 
('133540',
'133781',
'133320')

-- only available in April ED without Cost_Centre

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in (
'20221001',
'20221031',
'20221101',
'20221130',
'20221201',
'20221231',
'20230101',
'20230131',
'20230201',
'20230228',
'20230301',
'20230331',
'20230401',
'20230430') and (trim(CC) ='' or CC is null )and File_Type = 'KVA' and Email_Address__User_ is not null and Email_Address__User_ not in ('kpmgindiaadmin@skillton.degreed.com')

-- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set 
-- Cost_Center = 'Audit-Quality and Risk',
-- Geo = 'CORPORATE',
-- Global_Function = 'Audit',
-- BU = 'KRC',
-- SL = 'Audit-Quality and Risk'
-- where Local_HR_ID in ('113431') and File_Date in (
-- '20221001',
-- '20221031',
-- '20221101',
-- '20221130',
-- '20221201',
-- '20221231',
-- '20230101',
-- '20230131',
-- '20230201',
-- '20230228',
-- '20230301',
-- '20230331',
-- '20230401',
-- '20230430') and trim(Cost_Center) ='' and File_Type = 'GLMS'




-- Global_Function
-- CC
-- BU
-- Local_Service_Line
-- SL


-- rishishukla@kpmg.com -  Available in April Dump and Cost_Center is null

-- COMMAND ----------

-- create view kva_cc_bu_sl as 

-- select a.*,b.Final_BU,c.Global_Function,d.service_line from (select distinct Employee_Number,Email_Address,Cost_centre,Client_Geography from kgsonedatadb.trusted_hist_headcount_employee_dump where Email_Address in (
-- 'akhilbaby1@kpmg.com',
-- 'rishishukla@kpmg.com',
-- 'dharshikgopan@kpmg.com',
-- 'mandadapuhimab@kpmg.com',
-- 'sakshimalik@kpmg.com'
-- ) and trim(Cost_centre) != '') a join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Centre = b.Cost_Centre join kgsonedatadb.config_dim_global_function c on b.BU = c.Final_BU join kgsonedatadb.config_cc_bu_sl d on a.cost_centre = d.Cost_centre and a.Client_Geography = d.Client_Geography and b.BU=d.Final_BU 


-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as A
-- USING kva_cc_bu_sl AS B
-- ON (A.Email_Address__User_ = B.Email_Address) and (A.Email_Address__User_ IS NOT NULL) and (Email_Address__User_ not in ('kpmgindiaadmin@skillton.degreed.com')) and
-- (A.CC is null or trim(A.CC) = '') and ((A.Global_Function is null ) or trim(A.Global_Function) = '') and ((A.BU is null )or (trim(A.BU)) = '') and (A.SL is null) and A.File_Type='KVA'and A.File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
-- WHEN MATCHED THEN
-- UPDATE SET A.CC = B.Cost_Centre, A.Global_Function = B.Global_Function,A.BU = B.Final_BU, A.SL =B.service_line,A.Local_Service_Line = B.service_line


-- COMMAND ----------

