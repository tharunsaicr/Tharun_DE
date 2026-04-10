# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table  kgsonedatadb.raw_curr_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/talent_acquisition/kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.talent_acquisition_kgs_joiners_report_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/talent_acquisition/kgs_joiners_report_bad