# Databricks notebook source
# DBTITLE 1,Account Lookup
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_account_lookup

# COMMAND ----------

# DBTITLE 1,Accounts
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_accounts

# COMMAND ----------

# DBTITLE 1,CH Leads Mapping
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_ch_leads_mapping

# COMMAND ----------

# DBTITLE 1,Default RLS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_default_rls

# COMMAND ----------

# DBTITLE 1,Geo
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_geo

# COMMAND ----------

# DBTITLE 1,Incremental RLS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_incremental_rls

# COMMAND ----------

# DBTITLE 1,Business Category
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_business_category

# COMMAND ----------

# DBTITLE 1,Employee Type
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_employee_type

# COMMAND ----------

# DBTITLE 1,Entity
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_entity

# COMMAND ----------

# DBTITLE 1,Operating Unit
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_operating_unit

# COMMAND ----------

# DBTITLE 1,Projects
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_projects

# COMMAND ----------

# DBTITLE 1,Years
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_years

# COMMAND ----------

# DBTITLE 1,Scenario
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_scenario

# COMMAND ----------

# DBTITLE 1,Reporting View RLS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_reporting_view_rls

# COMMAND ----------

# DBTITLE 1,BU View RLS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_bu_view_rls

# COMMAND ----------

# DBTITLE 1,Console
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_console

# COMMAND ----------

# DBTITLE 1,KGS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_kgs

# COMMAND ----------

# DBTITLE 1,UK
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_uk

# COMMAND ----------

# DBTITLE 1,US
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_us

# COMMAND ----------

# DBTITLE 1,KDN
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_kdn

# COMMAND ----------

# DBTITLE 1,KRC Mapping
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_krc_mapping

# COMMAND ----------

# DBTITLE 1,hyperion
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_hyperion

# COMMAND ----------

# DBTITLE 1,Location
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_location

# COMMAND ----------

# DBTITLE 1,Period
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_period

# COMMAND ----------

# DBTITLE 1,RLS
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_rls

# COMMAND ----------

# DBTITLE 1,CF Function - CF Pack
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_cf_pack_cf_function

# COMMAND ----------

# DBTITLE 1,CF Headcount Remarks - CF Pack
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_cf_pack_cf_headcount_remarks

# COMMAND ----------

# DBTITLE 1,Year Classificaition - CF Pack
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_cf_pack_year_classification

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_hrpandl_pack_ee_and_iandd_cost

# COMMAND ----------

# %sql
# describe table kgsfinancedb.trusted_hist_bu_report_bu_pack_accounts

# COMMAND ----------

