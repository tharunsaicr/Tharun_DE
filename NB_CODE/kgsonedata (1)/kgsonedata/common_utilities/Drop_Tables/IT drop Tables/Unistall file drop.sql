-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_uninstall_an_application

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_uninstall_an_application

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_uninstall_an_application

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_uninstall_an_application

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_uninstall_an_application

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/uninstall_an_application

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/uninstall_an_application