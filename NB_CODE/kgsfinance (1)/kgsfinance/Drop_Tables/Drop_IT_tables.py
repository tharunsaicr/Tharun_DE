# Databricks notebook source
# DBTITLE 1,Base data
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_base_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_base_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_base_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_base_data;

# COMMAND ----------

# DBTITLE 1,CFIT Cost Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_cost_actual;

# COMMAND ----------

# DBTITLE 1,cfit_cost_forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_cost_forecast;

# COMMAND ----------

# DBTITLE 1,CFIT Cost Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_cost_plan;

# COMMAND ----------

# DBTITLE 1,CFIT HC Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_hc_actual;

# COMMAND ----------

# DBTITLE 1,CFIT HC Forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_hc_forecast;

# COMMAND ----------

# DBTITLE 1,CFIT HC Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_hc_plan;

# COMMAND ----------

# DBTITLE 1,Cost Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cost_actual;

# COMMAND ----------

# DBTITLE 1,Cost Forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cost_forecast;

# COMMAND ----------

# DBTITLE 1,Cost Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cost_plan;

# COMMAND ----------

# DBTITLE 1,From Niraj
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_from_niraj;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_from_niraj;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_from_niraj;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_from_niraj;

# COMMAND ----------

# DBTITLE 1,HC Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_hc_actual;

# COMMAND ----------

# DBTITLE 1,Hc Forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_hc_forecast;

# COMMAND ----------

# DBTITLE 1,HC Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_hc_plan;

# COMMAND ----------

# DBTITLE 1,Out look tables starts - cfit_cost_outlook
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_cost_outlook;

# COMMAND ----------

# DBTITLE 1,cfit_hc_outlook
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cfit_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cfit_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cfit_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cfit_hc_outlook;

# COMMAND ----------

# DBTITLE 1,cost_outlook
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_cost_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_cost_outlook;

# COMMAND ----------

# DBTITLE 1,hc_outlook
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_it_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_it_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_hc_outlook;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_hc_outlook;

# COMMAND ----------

# DBTITLE 1,Dimension Lookups
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_dim_lookup_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_dim_lookup_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_dim_lookup_cfit_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_dim_lookup_cfit_category

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_dim_lookup_cfit_designation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_dim_lookup_cfit_designation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_it_dim_month_lookup_usd

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_it_dim_month_lookup_usd

# COMMAND ----------

# DBTITLE 1,Dim look up delete folder
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/dim_lookup_category/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/dim_lookup_category/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/dim_month_lookup_usd/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/dim_month_lookup_usd/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/dim_lookup_cfit_designation/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/dim_lookup_cfit_designation/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/dim_lookup_cfit_category/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/dim_lookup_cfit_category/

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Fact Tables
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/base_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/base_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/base_data
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/base_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_cost_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_cost_plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_hc_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_hc_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_hc_plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cost_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cost_plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/from_niraj
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/from_niraj
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/from_niraj
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/from_niraj

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/hc_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/hc_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/hc_plan

# COMMAND ----------

# DBTITLE 1,out look data starts
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/hc_outlook

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cost_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cost_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cost_outlook
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cost_outlook

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_hc_outlook
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/it/cfit_cost_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/it/cfit_cost_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/it/cfit_cost_outlook
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/it/cfit_cost_outlook
# MAGIC