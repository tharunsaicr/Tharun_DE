-- Databricks notebook source
drop table kgsonedatadb.raw_hist_risk_independence_violations

-- COMMAND ----------

drop table kgsonedatadb.raw_stg_risk_independence_violations

-- COMMAND ----------

drop table kgsonedatadb.raw_curr_risk_independence_violations

-- COMMAND ----------

drop table kgsonedatadb.trusted_hist_risk_independence_violations

-- COMMAND ----------

drop table kgsonedatadb.trusted_stg_risk_independence_violations

-- COMMAND ----------

drop table kgsonedatadb.trusted_risk_independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/current/risk/independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/staging/risk/independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/rawlayermount/history/risk/independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/current/risk/independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/risk/independence_violations

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/trustedlayermount/history/risk/independence_violations

-- COMMAND ----------

drop table kgsonedatadb_badrecords.risk_independence_violations_bad

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/badfiles/risk/independence_violations_bad

-- COMMAND ----------

