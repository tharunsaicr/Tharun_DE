-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_optional_business_application_installation

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_optional_business_application_installation

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_optional_business_application_installation

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_optional_business_application_installation

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_optional_business_application_installation

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/optional_business_application_installation

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/optional_business_application_installation