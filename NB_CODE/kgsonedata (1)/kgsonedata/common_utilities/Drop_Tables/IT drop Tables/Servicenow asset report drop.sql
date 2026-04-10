-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_servicenow_asset_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_servicenow_asset_report

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_servicenow_asset_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_servicenow_asset_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_servicenow_asset_report

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/servicenow_asset_report

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/servicenow_asset_report

-- COMMAND ----------


drop table kgsonedatadb_badrecords.servicenow_asset_report_bad

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/badfiles/it/servicenow_asset_report