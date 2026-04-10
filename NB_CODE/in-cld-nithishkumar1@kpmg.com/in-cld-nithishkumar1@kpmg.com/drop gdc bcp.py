# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_bcp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_gdc_bcp_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/gdc/bcp_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/bcp

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/bcp

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/bcp

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/bcp

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/bcp

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/bcp

# COMMAND ----------




# COMMAND ----------

