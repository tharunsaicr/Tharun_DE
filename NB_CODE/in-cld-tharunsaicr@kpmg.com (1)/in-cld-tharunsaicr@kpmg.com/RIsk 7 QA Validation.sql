-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC  
-- MAGIC # Create a Spark session
-- MAGIC spark = SparkSession.builder.appName("DeltaTableList").getOrCreate()
-- MAGIC  
-- MAGIC # List all tables in the Delta database
-- MAGIC db_name = "kgsonedatadb_badrecords"
-- MAGIC tables = spark.catalog.listTables(db_name)
-- MAGIC  
-- MAGIC # Filter tables starting with ''
-- MAGIC filtered_tables = [table.name for table in tables if table.name.startswith('trusted_hist_risk')]
-- MAGIC  
-- MAGIC print(filtered_tables)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in filtered_tables:
-- MAGIC     print("select * from kgsonedatadb_badrecords."+i+" where File_Date>'20230919';")

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_employee_details where Full_Name like '%K P, Prajwal Madhyastha%' order by File_Date desc limit 1

-- COMMAND ----------

select count(*),File_Date,Dated_On from kgsonedatadb.trusted_hist_risk_ipf group by File_Date,dated_on
select count(*),File_Date,Dated_On from kgsonedatadb.trusted_hist_risk_dp group by File_Date,dated_on
delete from kgsonedatadb.trusted_hist_risk_dp where dated_on='2024-01-11T11:20:13.000+0000'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_risk_local_admin_users_kgs where File_Date=20231219

-- update kgsonedatadb.trusted_hist_risk_local_admin_users_kgs
-- set ACCOUNT_CONTAINED_WITHIN_THE_GROUP='rishabhsahni1'
-- where File_Date='20231219' and ACCOUNT_CONTAINED_WITHIN_THE_GROUP='rishabh sahni'

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_risk_applications_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_cloud_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_dp_bad where File_Date>'20230919';  --22 pass
select * from kgsonedatadb_badrecords.trusted_hist_risk_euda_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_exit_report_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_gsoc_bad where File_Date>'20230919';  --355 check
select * from kgsonedatadb_badrecords.trusted_hist_risk_in_dl_allow_user_outbound_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_in_dl_kgs_allow_user_outbound_bad where File_Date>'20230919'; --565 check
--select * from kgsonedatadb_badrecords.trusted_hist_risk_independence_violations_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_intune_compliance_bad where File_Date>'20230919';  --951 pass
select * from kgsonedatadb_badrecords.trusted_hist_risk_ipf_bad where File_Date>'20230919'; --22 pass
-- select * from kgsonedatadb_badrecords.trusted_hist_risk_kgs_india_mandatory_training_bad where File_Date>'20230919';
-- select * from kgsonedatadb_badrecords.trusted_hist_risk_kgs_mandatory_training_ki_led_bad where File_Date>'20230919';
-- select * from kgsonedatadb_badrecords.trusted_hist_risk_kgs_uk_mandatory_training_bad where File_Date>'20230919';
-- select * from kgsonedatadb_badrecords.trusted_hist_risk_kgs_us_mandatory_training_bad where File_Date>'20230919';
-- select * from kgsonedatadb_badrecords.trusted_hist_risk_kin_data_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_laptop_ageing_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_laptop_bu_mapping_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_laptop_recovery_bad where File_Date>'20230919'; --4 check
select * from kgsonedatadb_badrecords.trusted_hist_risk_late_arrival_to_it_bin_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_level_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_local_admin_users_kgs_bad where File_Date>'20230919';  --51 check
select * from kgsonedatadb_badrecords.trusted_hist_risk_m_applications_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_m_bu_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_m_job_name_bad where File_Date>'20230919';
select * from kgsonedatadb_badrecords.trusted_hist_risk_usb_access_bad where File_Date>'20230919';

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_risk_dp_bad where File_Date>'20230919';

-- COMMAND ----------

select * from kgsonedatadb.config_bad_record_check where Process_Name ='risk'

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_risk_in_dl_kgs_allow_user_outbound_bad where File_Date>'20230919'; --565 check

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_risk_laptop_recovery_bad where File_Date>'20230919'; --4 check

-- COMMAND ----------

delete from  kgsonedatadb_badrecords.trusted_hist_risk_dp_bad where File_Date>'20230919';  --51 check

-- COMMAND ----------

delete from kgsonedatadb.trusted_hist_risk_dp where File_Date>'20230919'; 
delete from kgsonedatadb.trusted_hist_risk_ipf where File_Date>'20230919'; 

delete from kgsonedatadb.raw_hist_risk_ipf where File_Date>'20230919'; 

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_risk_dp where File_Date>'20230919'; 
select * from kgsonedatadb.trusted_hist_risk_ipf where File_Date>'20230919'; 

-- COMMAND ----------

