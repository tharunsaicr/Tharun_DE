# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv/ki_loaned_new_joiners_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.bgv_ki_loaned_new_joiners_data_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/bgv/ki_loaned_new_joiners_data_bad