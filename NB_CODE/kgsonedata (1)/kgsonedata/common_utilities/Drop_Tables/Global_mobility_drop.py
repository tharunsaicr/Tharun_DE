# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,l1_visa_tracker
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_global_mobility_l1_visa_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_global_mobility_l1_visa_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_global_mobility_l1_visa_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_global_mobility_l1_visa_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_global_mobility_l1_visa_tracker

# COMMAND ----------

# DBTITLE 1,non_us_tracker
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_global_mobility_non_us_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_global_mobility_non_us_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_global_mobility_non_us_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_global_mobility_non_us_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_global_mobility_non_us_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_global_mobility_non_us_tracker

# COMMAND ----------

# DBTITLE 1,Secondment_tracker
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_global_mobility_secondment_tracker

# COMMAND ----------

# DBTITLE 1,Opportunity_dump
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_global_mobility_opportunity_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_global_mobility_opportunity_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_global_mobility_opportunity_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_global_mobility_opportunity_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_global_mobility_opportunity_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_global_mobility_opportunity_dump

# COMMAND ----------

# DBTITLE 1,Secondment_details
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_global_mobility_secondment_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_global_mobility_secondment_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_global_mobility_secondment_details

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/global_mobility

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/global_mobility

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/global_mobility

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/global_mobility

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/global_mobility

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/global_mobility