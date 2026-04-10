# Databricks notebook source
# MAGIC %sql
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'CF' where `Function` = 'CF';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'CF' where `Function` = 'Corporate Functions';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Cap-Hubs' where `Function` = 'RAK';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Cap-Hubs' where `Function` = 'KGS Capability Hubs';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Cap-Hubs' where `Function` = 'CH';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Cap-Hubs' where `Function` = 'Cap-Hubs';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Consulting' where `Function` = 'Consulting';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Consulting' where `Function` = 'MC';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'DAS' where `Function` = 'DAS';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'DAS' where `Function` = 'DA';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'DAS' where `Function` = 'DA&S';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Digital Nexus' where `Function` = 'Digital Nexus';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'GDC' where `Function` = 'GDC';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'KRC' where `Function` = 'KRC';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'MS' where `Function` = 'MS';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'RS' where `Function` = 'RS';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'RS' where `Function` = 'Risk Services';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'RS' where `Function` = 'RC';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'RS' where `Function` = 'RAS';
# MAGIC update kgsonedatadb.trusted_hist_employee_engagement_year_end set `Function` = 'Tax' where `Function` = 'Tax';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_employee_engagement_year_end where `Function` in (
# MAGIC 'Corporate Functions',
# MAGIC 'RAK',
# MAGIC 'KGS Capability Hubs',
# MAGIC 'CH',
# MAGIC 'MC',
# MAGIC 'DA',
# MAGIC 'DA&S',
# MAGIC 'Risk Services',
# MAGIC 'RC',
# MAGIC 'RAS')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Cost_centre,BU,file_date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where cost_centre = 'MC-GDN-Audit Tech Ops-KCW'
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.config_cost_center_business_unit;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_cost_center_business_unit where cost_centre in ('Forensic-Inv','Nexus-Digital Adoption & Change','Capability Hubs-Research-GCM Platinum Support','MC-Advisory Solutions Enablement', 'RC Core-Modeling Tech', 'MC-PMO-Connected', 'MC-IT Advisory-Testing QA', 'Finance', 'KGS Core ESG-Management', 'M&A Consulting', 'Audit-Corporates Listed and Regulated')
# MAGIC
# MAGIC -- Forensic-Inv
# MAGIC -- M&A Consulting
# MAGIC -- Finance
# MAGIC