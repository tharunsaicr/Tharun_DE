-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_daily_exit_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/daily_exit_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/daily_exit_report

-- COMMAND ----------


drop table kgsonedatadb_badrecords.it_daily_exit_report_bad

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/badfiles/it/daily_exit_report