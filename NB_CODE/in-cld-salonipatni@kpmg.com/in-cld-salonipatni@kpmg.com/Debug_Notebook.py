# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_data_type_cast where Process_Name ='lnd'

# COMMAND ----------

from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
print(currentdatetime)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct dated_on from kgsonedatadb.trusted_it_talent_connect_daily_exit_report where FILE_DATE ='20230801' order by dated_on desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(1),FILE_DATE,Dated_On
# MAGIC   FROM kgsonedatadb.raw_hist_it_talent_connect_daily_exit_report group by FILE_DATE,Dated_On order by Dated_On desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --delete FROM kgsonedatadb.raw_hist_it_talent_connect_daily_exit_report where Dated_On >= '2023-07-31T13:52:18.982+0000'
# MAGIC   --in ('2023-07-31 14:01:37.490','2023-07-31 13:54:34.407')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.tye

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by FIle_Date,Dated_On

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bad_record_check
# MAGIC -- kgsonedatadb.config_convert_column_to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC --End_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details
# MAGIC employee_Number from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_mapping_list

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC BU
# MAGIC --cost_centre 
# MAGIC from kgsonedatadb.config_cost_center_business_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_compensation_finance_metrics

# COMMAND ----------

# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'CF' where BU in ('CF','Corporate Functions')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'Cap-Hubs' where BU in ('Cap-Hubs','KGS Capability Hubs','CH','RAK')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'Consulting' where BU in ('Consulting','MC')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'DAS' where BU in ('DA','DA&S','DAS')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'Digital Nexus' where BU in ('Digital Nexus')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'GDC' where BU in ('GDC')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'KRC' where BU in ('KRC')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'MS' where BU in ('MS')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'RS' where BU in ('Risk Services','RS','RAS','RC')
# update dbo.trusted_hist_bgv_upcoming_joiners set BU = 'Tax' where BU in ('Tax')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_update_table_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- truncate table kgsonedatadb.trusted_hist_bgv_upcoming_joiners
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- create table kgsonedatadb.trusted_hist_bgv_upcoming_joiners_bck
# MAGIC -- select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners--
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners_bck

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_mapping_list 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners --where bu is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where cost_center is null or cost_center == 'null'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners a inner join kgsonedatadb.config_cost_center_business_unit b on a.Cost_Center = b.Cost_centre 
# MAGIC --where b.Cost_centre is null
# MAGIC --a.Cost_Center is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_offer_release where cost_center is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.config_cost_center_business_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.config_cc_bu_sl

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Cost_Center,BU from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where BU is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_bu_update_table_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_cost_center_business_unit 
# MAGIC --where cost_centre like '%Procurement%'
# MAGIC where Cost_centre in 
# MAGIC --('Nexus-ServiceNow')
# MAGIC
# MAGIC ('Capability Hubs-BOI-Procurement F&A','Capability Hubs-Marketing-G&S-Analytics','Capability Hubs-Pursuits-G&S-IFMS','Capability Hubs-Pursuits-RAS','Capability Hubs-Research-RAS','GDC GTS - Data Engineering Services')
# MAGIC
# MAGIC -- select * from kgsonedatadb.config_bu_mapping_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.raw_stg_global_mobility_secondment_tracker

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_maternity_cases where Employee_Number in (select Emp_No from kgsonedatadb.trusted_hist_compensation_paysheet where Emp_No not in (select Employee_Number from kgsonedatadb.trusted_hist_headcount_monthly_employee_details))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_compensation_paysheet
# MAGIC where Emp_No not in (select Employee_Number from kgsonedatadb.employee_golden_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.trusted_hist_compensation_paysheet where Emp_No not in (select Employee_Number from kgsonedatadb.trusted_hist_headcount_monthly_employee_details)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_compensation_paysheet where Emp_No not in (select Employee_Number from kgsonedatadb.trusted_hist_headcount_monthly_employee_details)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_Date from kgsonedatadb.trusted_hist_compensation_paysheet

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC --distinct File_Date 
# MAGIC *
# MAGIC from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number = '130295'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.employee_golden_data where Employee_Number in ('16383','17792','18392','19250')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_Date from kgsonedatadb.trusted_hist_compensation_paysheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_compensation_paysheet where Emp_No not in (select Employee_number from kgsonedatadb.employee_golden_data)
# MAGIC
# MAGIC select distinct FIle_Date from kgsonedatadb.trusted_hist_compensation_paysheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_compensation_finance_metrics where Cost_Centre is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(*) from kgsonedatadb.trusted_hist_jml_us_leaver
# MAGIC group by File_Date order by File_Date asc
# MAGIC
# MAGIC --select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_compensation_additional_pay group by File_Date order by File_Date asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(*) from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned group by File_Date
# MAGIC --select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(*) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki group by File_Date
# MAGIC --select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(*) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned group by File_Date
# MAGIC --select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(*) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by File_Date
# MAGIC --select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details

# COMMAND ----------

# %sql
# select * from kgsonedatadb.trusted_hist_employee_engagement_year_end

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Joining_Date,File_Date from kgsonedatadb.trusted_hist_compensation_paysheet order by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select HR_ from kgsonedatadb.trusted_hist_lnd_glms_kva_details where BU is null
# MAGIC
# MAGIC
# MAGIC select BU from kgsonedatadb.trusted_hist_headcount_employee_details where Employee_Number = '34226'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_employee_details
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward
# MAGIC -- select distinct Emp_BU from kgsonedatadb.trusted_hist_lnd_glms_details
# MAGIC -- select distinct Emp_BU from kgsonedatadb.trusted_hist_lnd_kva_details
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC -- select distinct Business_Unit_-_Name from kgsonedatadb.trusted_hist_bgv_offer_release
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_bgv_upcoming_joiners
# MAGIC -- select distinct Business_Unit from kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker
# MAGIC -- select distinct Business_Unit from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker
# MAGIC -- select distinct Business_Unit from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_jml_nl_joiner
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_jml_nl_leaver
# MAGIC -- select distinct BU from kgsonedatadb.trusted_hist_jml_nl_mover
# MAGIC -- select distinct Business_Unit from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_adhoc_convert_column_to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_convert_column_to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_serialdateconversioncolumns

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from kgsonedatadb.trusted_hist_compensation_paysheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from kgsonedatadb.trusted_hist_compensation_additional_pay
# MAGIC
# MAGIC -- select `EMPLOYEE_NO**`, count(*) from kgsonedatadb.trusted_hist_compensation_additional_pay where File_Date = '20221001' group by `EMPLOYEE_NO**` having count(1)>1
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_compensation_additional_pay where `EMPLOYEE_NO**` ='106986' and File_Date = '20221001'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Emp_No, count(*) from kgsonedatadb.trusted_hist_compensation_paysheet where File_Date = '20221001' group by Emp_No having count(1)>1 

# COMMAND ----------

# MAGIC %sql
# MAGIC select Emp_Id, count(*) from kgsonedatadb.trusted_hist_compensation_yec where File_Date = '20221001' group by Emp_Id having count(1)>1 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners

# COMMAND ----------

