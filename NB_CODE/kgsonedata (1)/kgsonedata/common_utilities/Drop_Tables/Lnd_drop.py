# Databricks notebook source
1+1

# COMMAND ----------

# DBTITLE 1,GLMS
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_lnd_glms_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_lnd_glms_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_lnd_glms_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_lnd_glms_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_lnd_glms_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_lnd_glms_details

# COMMAND ----------

# DBTITLE 1,KVA
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_lnd_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_lnd_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_lnd_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_lnd_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_lnd_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_lnd_kva_details

# COMMAND ----------

# DBTITLE 1,GLMS_KVA
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/lnd

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/lnd

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/lnd

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/lnd

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/lnd

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/lnd

# COMMAND ----------

