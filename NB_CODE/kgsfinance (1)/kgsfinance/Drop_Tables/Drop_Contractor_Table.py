# Databricks notebook source
# DBTITLE 1,HC Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_hc_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_hc_plan;

# COMMAND ----------

# DBTITLE 1,HC Forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_hc_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_hc_forecast;

# COMMAND ----------

# DBTITLE 1,HC Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_hc_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_hc_actual;

# COMMAND ----------

# DBTITLE 1,Cost Plan
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_cost_plan;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_cost_plan;

# COMMAND ----------

# DBTITLE 1,Cost Forecast
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_cost_forecast;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_cost_forecast;

# COMMAND ----------

# DBTITLE 1,Cost Actual
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_contractor_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_contractor_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_cost_actual;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_cost_actual;

# COMMAND ----------

# DBTITLE 1,Dim Geo
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_dim_geo;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_dim_geo;

# COMMAND ----------

# DBTITLE 1,Dim Vendor
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_dim_vendor;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_dim_vendor

# COMMAND ----------

# DBTITLE 1,Dim Designation
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_dim_designation_mapping;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_dim_designation_mapping;

# COMMAND ----------

# DBTITLE 1,Dim Employee
# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_contractor_dim_employee_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_contractor_dim_employee_type;

# COMMAND ----------

# DBTITLE 1,Dim Tables
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/dim_Designation_Mapping
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/dim_Designation_Mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/dim_Employee_Type
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/dim_Employee_Type

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/dim_geo
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/dim_geo

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/dim_vendor
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/dim_vendor

# COMMAND ----------

# DBTITLE 1,Fact Tables
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/cost_actual/
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/cost_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/cost_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/cost_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/cost_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/cost_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/cost_plan

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/hc_actual
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/hc_actual

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/hc_forecast
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/hc_forecast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/contractor/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/contractor/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/contractor/hc_plan
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/contractor/hc_plan