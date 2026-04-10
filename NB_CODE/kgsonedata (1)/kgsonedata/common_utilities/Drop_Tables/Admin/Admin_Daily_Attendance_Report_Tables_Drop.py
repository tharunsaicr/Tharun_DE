# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin/daily_attendance_report

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.admin_daily_attendance_report_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/admin/daily_attendance_report_bad

# COMMAND ----------

