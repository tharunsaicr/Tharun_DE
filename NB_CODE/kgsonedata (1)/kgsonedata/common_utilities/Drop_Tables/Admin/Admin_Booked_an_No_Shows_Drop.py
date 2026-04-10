# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_booked_and_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_booked_and_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_booked_and_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin/booked_and_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin/booked_and_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin/booked_and_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.admin_booked_and_no_shows_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/admin/booked_and_no_shows_bad

# COMMAND ----------

