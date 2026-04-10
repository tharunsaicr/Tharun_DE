# Databricks notebook source
# MAGIC %sql
# MAGIC use kgsfinancedb;
# MAGIC show tables

# COMMAND ----------

# DBTITLE 1,Raw Actual Cost
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_actual_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_actual_cost;

# COMMAND ----------

# DBTITLE 1,Raw Plan Cost
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_plan_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_plan_cost;

# COMMAND ----------

# DBTITLE 1,Raw Plan Cost Full Year
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_plan_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_plan_cost_full_year;

# COMMAND ----------

# DBTITLE 1,Raw Forecast cost
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_forecast_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_forecast_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_forecast_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_forecast_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_actual_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_actual_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_plan_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_plan_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_plan_cost_full_year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_plan_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_forecast_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_forecast_cost;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_forecast_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_forecast_cost_full_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_no_of_training

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_no_of_training

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_no_of_training

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_no_of_training

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_training_cost_per_head

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_lnd_training_cost_per_head

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_training_cost_per_entity

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_lnd_training_cost_per_entity

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_training_cost_per_head

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_training_cost_per_head

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_training_cost_per_entity

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_training_cost_per_entity

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_account_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_account_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_account_code_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_account_code_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_account_name_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_account_name_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_client_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_client_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_training_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_training_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_lnd_dim_designation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_lnd_dim_designation

# COMMAND ----------

# DBTITLE 1,Drop ADLS Raw
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/actual_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/actual_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/plan_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/plan_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/plan_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/plan_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/forecast_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/forecast_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/forecast_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/forecast_cost_full_year

# COMMAND ----------

# DBTITLE 1,Drop ADLS Trusted
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/plan_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/plan_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/actual_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/actual_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/plan_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/plan_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/forecast_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/forecast_cost

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/forecast_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/forecast_cost_full_year

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/no_of_training

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/no_of_training

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/no_of_training

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/no_of_training

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/training_cost_per_head

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/training_cost_per_head

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/lnd/training_cost_per_entity

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/lnd/training_cost_per_entity

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/training_cost_per_head

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/training_cost_per_head

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/training_cost_per_entity

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/training_cost_per_entity

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_training_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_training_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_account_category

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_account_category

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_designation

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_designation

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_account_code_category

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_account_code_category

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_account_name_category;

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_account_name_category

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_geo_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_geo_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/lnd/dim_client_geo_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/lnd/dim_client_geo_mapping

# COMMAND ----------

