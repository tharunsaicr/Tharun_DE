# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Germany_joiner
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_germany_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_germany_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_germany_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_germany_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_germany_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_germany_joiner

# COMMAND ----------

# DBTITLE 1,Germany_leaver
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_germany_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_germany_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_germany_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_germany_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_germany_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_germany_leaver

# COMMAND ----------

# DBTITLE 1,Germany_mover
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_germany_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_germany_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_germany_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_germany_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_germany_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_germany_mover

# COMMAND ----------

# DBTITLE 1,Nl_joiner
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_nl_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_nl_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_nl_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_nl_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_nl_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_nl_joiner

# COMMAND ----------

# DBTITLE 1,Nl_leaver
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_nl_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_nl_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_nl_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_nl_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_nl_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_nl_leaver

# COMMAND ----------

# DBTITLE 1,Nl_mover
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_nl_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_nl_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_nl_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_nl_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_nl_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_nl_mover

# COMMAND ----------

# DBTITLE 1,Uk_joiner
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_uk_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_uk_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_uk_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_uk_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_uk_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_uk_joiner

# COMMAND ----------

# DBTITLE 1,Uk_leaver
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_uk_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_uk_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_uk_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_uk_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_uk_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_uk_leaver

# COMMAND ----------

# DBTITLE 1,Uk_mover
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_uk_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_uk_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_uk_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_uk_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_uk_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_uk_mover

# COMMAND ----------

# DBTITLE 1,Us_joiner
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_us_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_us_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_us_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_us_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_us_joiner

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_us_joiner

# COMMAND ----------

# DBTITLE 1,Us_leaver
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_us_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_us_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_us_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_us_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_us_leaver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_us_leaver

# COMMAND ----------

# DBTITLE 1,Us_mover
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_jml_us_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_jml_us_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_jml_us_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_jml_us_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_jml_us_mover

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_jml_us_mover

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/jml

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/jml

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/jml

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/jml

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/jml

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/jml