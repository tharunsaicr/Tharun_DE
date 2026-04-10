# Databricks notebook source
# MAGIC %sql
# MAGIC Truncate table kgsonedatadb_badrecords.trusted_hist_gdc_onboarding_master_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/gdc/onboarding_master_bad

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/onboarding_master

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/onboarding_master

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_onboarding_master

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_onboarding_master

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/onboarding_master

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_onboarding_master

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/onboarding_master

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(TO_DATE('20231115','yyyyMMdd'),'yyyy-MM-dd') as date

# COMMAND ----------

# MAGIC %sql
# MAGIC select Dated_on,File_date from kgsonedatadb.trusted_hist_bgv_progress_sheet group by Dated_on,File_Date order by Dated_on desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb_badrecords.trusted_hist_gdc_onboarding_master_bad where Dated_on='2024-02-19T18:40:54.000+0000'

# COMMAND ----------

# %sql
# drop table kgsonedatadb_badrecords.trusted_hist_gdc_onboarding_master_bad

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master

# COMMAND ----------

