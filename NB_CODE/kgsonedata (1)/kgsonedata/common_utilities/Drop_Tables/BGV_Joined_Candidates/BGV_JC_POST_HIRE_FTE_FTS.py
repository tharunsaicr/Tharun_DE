# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_jc_post_hire_fts

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv_jc_post_hire/fts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.bgv_jc_post_hire_fts_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/bgv_jc_post_hire/fts_bad