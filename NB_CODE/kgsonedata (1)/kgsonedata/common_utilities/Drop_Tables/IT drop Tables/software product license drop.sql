-- Databricks notebook source

drop table  kgsonedatadb.raw_curr_it_software_product_license_inventory

-- COMMAND ----------


drop table  kgsonedatadb.raw_hist_it_software_product_license_inventory

-- COMMAND ----------


drop table  kgsonedatadb.raw_stg_it_software_product_license_inventory

-- COMMAND ----------


drop table  kgsonedatadb.trusted_hist_it_software_product_license_inventory

-- COMMAND ----------


drop table  kgsonedatadb.trusted_stg_it_software_product_license_inventory

-- COMMAND ----------


drop table  kgsonedatadb.trusted_it_software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/it/software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/it/software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/it/software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/it/software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/it/software_product_license_inventory

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/it/software_product_license_inventory