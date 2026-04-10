# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_gdc_newjoiners_status

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_gdc_newjoiners_status_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/gdc/newjoiners_status_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/gdc/newjoiners_status

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/gdc/newjoiners_status

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/gdc/newjoiners_status

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/gdc/newjoiners_status

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/gdc/newjoiners_status

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/gdc/newjoiners_status

# COMMAND ----------

