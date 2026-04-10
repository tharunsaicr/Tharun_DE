# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_bgv_jc_costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv_jc/costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/trustedlayermount/history/bgv_jc/
# MAGIC

# COMMAND ----------

# dbutils.fs.ls("/mnt/trustedlayermount/staging/bgv_jc/")

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv_jc/costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv_jc/costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv_jc/costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv_jc/costcentre_bu

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv_jc/costcentre_bu

# COMMAND ----------

# %sql
# select count(*),Dated_On from kgsonedatadb.trusted_hist_bgv_jc_costcentre_bu group by 

# COMMAND ----------

