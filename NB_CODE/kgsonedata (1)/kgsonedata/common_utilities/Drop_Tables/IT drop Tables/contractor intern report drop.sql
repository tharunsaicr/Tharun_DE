-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_contractor_intern_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_contractor_intern_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_contractor_intern_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_contractor_intern_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_contractor_intern_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/contractor_intern_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/contractor_intern_daily_exit_report