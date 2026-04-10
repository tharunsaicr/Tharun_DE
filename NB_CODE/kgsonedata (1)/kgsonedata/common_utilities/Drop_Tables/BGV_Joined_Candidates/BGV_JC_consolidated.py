# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_bgv_joined_candidate_consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv_joined_candidate/consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.bgv_joined_candidate_consolidated_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/bgv_joined_candidate/consolidated_bad