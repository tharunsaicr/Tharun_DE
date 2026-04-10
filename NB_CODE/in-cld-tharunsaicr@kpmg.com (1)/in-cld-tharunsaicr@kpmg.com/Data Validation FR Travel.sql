-- Databricks notebook source
select count(Financial_Year),Financial_Year from kgsfinancedb.trusted_hist_fr_travel_travel_dump group by Financial_Year

-- COMMAND ----------

select count(Financial_Year),Financial_Year from kgsfinancedb.raw_hist_fr_travel_travel_dump group by Financial_Year

-- COMMAND ----------

