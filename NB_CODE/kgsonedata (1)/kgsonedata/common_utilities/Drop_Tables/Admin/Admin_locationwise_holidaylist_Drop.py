# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin/locationwise_holidaylist

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.admin_locationwise_holidaylist_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/admin/locationwise_holidaylist_bad

# COMMAND ----------

