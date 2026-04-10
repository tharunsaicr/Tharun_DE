# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_legal_clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_legal_clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_legal_clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_legal_clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_legal_clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_legal_clra_data

# COMMAND ----------

# %sql
# drop table kgsonedatadb_badrecords.trusted_hist_legal_clra_data_bad

# COMMAND ----------

# %fs
# rm -r dbfs:/mnt/badfiles/legal/clra_data_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/legal/clra_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/legal/clra_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/legal/clra_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/legal/clra_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/legal/clra_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/legal/clra_data

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from kgsonedatadb.trusted_hist_legal_clra_data where CONTRACTOR_S_NAME like '%Team Computers%'
# MAGIC
# MAGIC --update kgsonedatadb.trusted_hist_legal_clra_data set CONTRACTOR_S_NAME='Team Computers' where BU='IT' and CONTRACTOR_S_NAME ='Team Computers Pvt. Ltd.'
# MAGIC
# MAGIC --update kgsonedatadb.trusted_hist_legal_clra_data set BU='IT' where BU='Admin' and CONTRACTOR_S_NAME ='Team Computers Pvt. Ltd.'

# COMMAND ----------

