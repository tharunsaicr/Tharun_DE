-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_project_code_dump

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_project_code_dump

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_project_code_dump

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_project_code_dump

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_project_code_dump

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/project_code_dump

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/project_code_dump