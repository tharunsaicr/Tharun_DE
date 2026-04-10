# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Rock
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_employee_engagement_rock

# COMMAND ----------

# DBTITLE 1,Year_end
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_employee_engagement_year_end

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_employee_engagement_year_end

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_employee_engagement_year_end

# COMMAND ----------

# DBTITLE 1,GPS
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_employee_engagement_gps

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_employee_engagement_gps

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_employee_engagement_gps

# COMMAND ----------

# DBTITLE 1,Encore_output
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_employee_engagement_encore_output

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_employee_engagement_encore_output

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_employee_engagement_encore_output

# COMMAND ----------

# DBTITLE 1,Thanks_dump
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_employee_engagement_thanks_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/employee_engagement

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/employee_engagement

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/employee_engagement

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/employee_engagement

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/employee_engagement

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/employee_engagement