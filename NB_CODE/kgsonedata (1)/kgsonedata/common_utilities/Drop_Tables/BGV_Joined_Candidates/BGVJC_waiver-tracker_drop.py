# Databricks notebook source
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv/waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_bgv_waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_bgv_waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_bgv_waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_waiver_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_bgv_waiver_tracker

# COMMAND ----------

# %sql
# drop table kgsonedatadb_badrecords.bgv_waiver_tracker_bad

# COMMAND ----------

# %fs-
# rm -r dbfs:/mnt/badfiles/bgv/waiver_tracker_bad