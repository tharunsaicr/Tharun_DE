-- Databricks notebook source
select BU,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and BU <> 'GDC' group by BU;

-- COMMAND ----------

select BU,sum(CPD_CPE_Hours_Awarded) as Total_Hours,sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID) as Per_Person_Learning_Hours,count(distinct Local_HR_ID) as HC from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and BU <> 'GDC' and CPD_CPE_Hours_Awarded <> 0 group by BU;

-- COMMAND ----------

select Training_Category,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' group by Training_Category

-- COMMAND ----------

select Item_Type_Category,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2023-09-30' and File_Date='20240310' group by Item_Type_Category

-- COMMAND ----------

select distinct Item_Title,Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details

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

select distinct Employee_Number,Cost_centre,BU,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Cost_centre in ('MC-GDN-SAP-EPM-INV',
'MC-GDN-Cloud-Architecture-INV',
'MC-GDN-SAP-Tech Dev-INV',
'MC-ASE-SAP GGA',
'Tax-USGMS',
'Tax-BTS Corp Velocity',
'Tax-USCT')
-- File_Date ='20240320' and
-- Employee_Number in (
-- '145235','145276')

-- '143810','143097','144413','144604','142020','36568','144584','144008','140571','32233','144621','142065','143532','50789','144594','145047','144409','142916','145048','144810',
-- '144772') order by Employee_Number

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('145235','145276') and File_Date ='20231031'

select distinct Cost_centre,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where Cost_centre in ('MC-GDN-SAP-EPM-INV','MC-GDN-Cloud-Architecture-INV','MC-GDN-SAP-Tech Dev-INV','MC-ASE-SAP GGA','Tax-USGMS','Tax-BTS Corp Velocity','Tax-USCT') order by File_Date 

select * from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number='129769' and File_Date ='20231031'

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

