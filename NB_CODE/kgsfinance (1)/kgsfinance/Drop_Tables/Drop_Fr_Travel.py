# Databricks notebook source
# MAGIC %sql
# MAGIC use kgsfinancedb;
# MAGIC show tables

# COMMAND ----------

# raw_curr_fr_travel_actual
# raw_curr_fr_travel_plan
# raw_curr_fr_travel_dump

# raw_hist_fr_travel_actual
# raw_hist_fr_travel_dump
# raw_hist_fr_travel_plan

# trusted_curr_fr_travel_actual
# trusted_curr_fr_travel_plan
# trusted_curr_fr_travel_dump

# trusted_hist_fr_travel_actual
# trusted_hist_fr_travel_plan
# trusted_hist_fr_travel_dump

# COMMAND ----------

# DBTITLE 1,Raw Actual - Travel
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_travel_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_travel_actual;

# COMMAND ----------

# DBTITLE 1,Raw - Travel Dump
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_travel_travel_dump;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_travel_travel_dump;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_travel_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_travel_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_travel_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_travel_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_travel_dump;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_travel_dump;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_actual;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_dim_country_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_dim_country_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_travel_dim_slab_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_travel_dim_slab_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/travel/actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/travel/actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/actual

# COMMAND ----------

# DBTITLE 1,ADLS Drop Travel Dump
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/travel/travel_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/travel/travel_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/travel_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/travel_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/travel/plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/travel/plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/travel/forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/travel/forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/dim_country_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/dim_geo_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/travel/dim_slab_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/dim_country_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/dim_geo_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/travel/dim_slab_mapping

# COMMAND ----------

