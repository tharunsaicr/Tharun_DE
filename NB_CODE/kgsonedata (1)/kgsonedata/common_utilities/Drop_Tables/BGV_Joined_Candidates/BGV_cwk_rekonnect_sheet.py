# Databricks notebook source
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv/cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_bgv_cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_bgv_cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_bgv_cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_cwk_rekonnect_sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_bgv_cwk_rekonnect_sheet