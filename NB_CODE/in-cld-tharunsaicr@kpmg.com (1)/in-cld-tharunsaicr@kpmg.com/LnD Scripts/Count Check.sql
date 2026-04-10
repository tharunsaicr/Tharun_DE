-- Databricks notebook source
select count(*) from kgsonedatadb.test_raw_curr_headcount_employee_dump; --43297
select count(*) from kgsonedatadb.test_raw_curr_headcount_termination_dump ;--65002
select count(*) from kgsonedatadb.test_config_cc_bu_sl; --1107

-- COMMAND ----------

select count(*) from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date= '20230911'
--20230904 = 43142 , 20230911='43297'
select count(*) from kgsonedatadb.trusted_hist_headcount_termination_dump where file_date= '20230911'
--20230904 = 64824, 20230911='65002'
select count(*) from kgsonedatadb.config_hist_cc_bu_sl where file_date='20230831' --9216

-- COMMAND ----------

--select count(*) from kgsonedatadb.config_hist_cc_bu_sl where file_date='20230831' --9216
select distinct File_Date from kgsonedatadb.config_hist_cc_bu_sl

-- COMMAND ----------

select File_Type,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where left((cast(Dated_On as string)),10) = '2023-09-14' group by File_Type

-- sep 5 1351796.9803277664
--sep 14 114340.83730327408

-- COMMAND ----------

select left((cast(Dated_On as string)),10),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by left((cast(Dated_On as string)),10)


select left((cast(completion_date as string)),7),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by left((cast(completion_date as string)),7)

-- COMMAND ----------

select 1469126.63 -1466137.8176310405  --Aug Ytd comparison
select 1387674.52 - 1384685.9403311536  -- OCt to July YTD Comparison

-- COMMAND ----------

select count(*) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date ='20231013'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) as Till_Aug_CPE_Hours from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on = '2023-09-14T21:30:29.000+0000' and BU <>'GDC'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) as Sep_CPE_Hours from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date ='20231013' and BU <>'GDC'

-- COMMAND ----------

select 358742 as GDC_Hours

-- COMMAND ----------

select 358742+95310.88697208092+1466370.8804149907

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date ='20231013' and BU <>'GDC' and Local_HR_ID ='134208'

-- COMMAND ----------

select BU,Cost_Center,sum(CPD_CPE_Hours_Awarded) as Total_Hours,sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID) as Per_Person_Learning_Hours,count(distinct Local_HR_ID) as HC from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and BU <> 'GDC' and BU ='Consulting' and dated_on = '2023-09-14T21:30:29.000+0000' group by BU,Cost_Center

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as Total_Hours,sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID) as Per_Person_Learning_Hours,count(distinct Local_HR_ID) as HC from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and BU <> 'GDC' and File_Date ='20231013' group by BU 

select Training_Category,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and BU <> 'GDC' and File_Date ='20231013' group by Training_Category 

select Item_Type_Category,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and BU <> 'GDC' and File_Date ='20231013' group by Item_Type_Category 

select level_wise,sum(CPD_CPE_Hours_Awarded) as Total_Hours from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and BU <> 'GDC' and File_Date ='20231013' group by level_wise 


-- COMMAND ----------

select Dated_On,File_Type,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by Dated_On,File_Type

-- COMMAND ----------

select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl_master where Month_Key='202309' and BU ='Consulting'-- and 

-- COMMAND ----------

select count(Function) from kgsonedatadb.config_test_lnd_cc_bu_sl

-- COMMAND ----------

create table kgsonedatadb.test_lnd_glms_kva_details_20231016
select * from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

select count(*) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

select count(*) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date='20231013'

-- COMMAND ----------

delete from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date='20231013'
delete from kgsonedatadb.test_trusted_hist_lnd_glms_details where File_Date='20231013'
delete from kgsonedatadb.test_trusted_hist_lnd_kva_details where File_Date='20231013'


-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Dated_On='2023-09-14T21:30:29.000+0000' and BU <>'GDC'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date='20231013' and BU <>'GDC'

-- COMMAND ----------

select distinct Level_Wise,Original_Levelwise_Mapping,Position,Job_Name from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date='20231013' --and BU <>'GDC'

select distinct Level_Wise,Original_Levelwise_Mapping,Position,Job_Name from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details where File_Date='20231013' and Level_Wise is null

-- COMMAND ----------

select * from kgsonedatadb.config_hist_dim_level_wise where position in ('Team Lead','Partner-LT')

-- COMMAND ----------

select Dated_On,* from kgsonedatadb.raw_hist_lnd_kva_details where Employee_Id= '100823' and Content_Provider !='SuccessFactors'

-- COMMAND ----------

select Dated_On,* from kgsonedatadb.trusted_hist_lnd_kva_details where Employee_Id= '100823' and Content_Provider !='SuccessFactors'

-- COMMAND ----------

select distinct * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on='2023-09-14T21:30:29.000+0000' and Local_HR_ID ='100823' and left(Completion_Date,7) ='2022-10' and CPD_CPE_Hours_Awarded !='0'-- and Item_Title like '%Strengthening%'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on='2023-10-13T15:14:02.000+0000' and Local_HR_ID ='100823'
--where dated_on='2023-09-14T21:30:29.000+0000' and Local_HR_ID ='100823' and left(Completion_Date,7) ='2022-10' and CPD_CPE_Hours_Awarded !='0' and Item_Title like '%Strengthening%'

-- COMMAND ----------

select distinct dated_on from kgsonedatadb.trusted_hist_lnd_glms_kva_details

-- COMMAND ----------

select count(*),left(Completion_Date,7) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on='2023-09-14T21:30:29.000+0000' and CPD_CPE_Hours_Awarded !='0' group by left(Completion_Date,7)

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on='2023-10-19T18:46:32.000+0000' and BU <>'GDC' group by BU

-- COMMAND ----------

select 1566790.2405427285 +  358742 


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where dated_on='2023-09-14T21:30:29.000+0000' and (Local_HR_ID = '106986' or Local_HR_ID ='113645')  and CPD_CPE_Hours_Awarded !='0'
--Local_HR_ID in ('106986','113645')

select Local_HR_ID,left(Completion_Date,7),* from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (dated_on='2023-10-13T15:14:02.000+0000' or dated_on='2023-10-13T15:20:38.000+0000') and Local_HR_ID in ('106986','113645')  and CPD_CPE_Hours_Awarded !='0'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Period