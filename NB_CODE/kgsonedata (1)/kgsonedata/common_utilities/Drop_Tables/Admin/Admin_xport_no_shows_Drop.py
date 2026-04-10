# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_xport_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_xport_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_xport_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin/xport_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin/xport_no_shows

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin/xport_no_shows

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.admin_xport_no_shows_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/admin/xport_no_shows_bad

# COMMAND ----------

