# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_admin_qlikview_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_admin_qlikview_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_admin_qlikview_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_qlikview_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_qlikview_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_qlikview_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/admin

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/admin

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/admin

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin

# COMMAND ----------

# %fs
# rm -r dbfs:/mnt/badfiles/admin/qlikview_report_bad

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table kgsonedatadb_badrecords.trusted_hist_admin_qlikview_report_bad