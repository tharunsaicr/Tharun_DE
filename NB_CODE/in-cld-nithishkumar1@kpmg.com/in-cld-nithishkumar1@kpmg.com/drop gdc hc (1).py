# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/absconding

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/absconding

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/active_hc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/attrition

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/attrition_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/long_leave

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/long_leave

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/secondee

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/secondee

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/transfers

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/transfers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC select *,row_number() over(partition by PSID order by PSID)rownum from kgsonedatadb.trusted_history_gdc_onboarding_master) where rownum >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC select *,row_number() over(partition by PSID order by PSID)rownum from kgsonedatadb.raw_hist_gdc_analytics) where rownum >1

# COMMAND ----------

