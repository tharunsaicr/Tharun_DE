-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_software_approval_request

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_software_approval_request

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_software_approval_request

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_software_approval_request

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_software_approval_request

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/software_approval_request

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/software_approval_request

-- COMMAND ----------


drop table kgsonedatadb_badrecords.trusted_hist_it_software_approval_request_bad

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/badfiles/it/software_approval_request_bad

-- COMMAND ----------

