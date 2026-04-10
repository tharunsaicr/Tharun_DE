-- Databricks notebook source
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()

-- COMMAND ----------

-- DBTITLE 1,Input Parameters
CREATE WIDGET TEXT curr_table_name DEFAULT ""

-- COMMAND ----------

CREATE WIDGET TEXT hist_table_name DEFAULT ""

-- COMMAND ----------

CREATE WIDGET TEXT processName DEFAULT ""

-- COMMAND ----------

CREATE WIDGET TEXT tableName DEFAULT ""

-- COMMAND ----------

CREATE WIDGET TEXT ReportName DEFAULT ""

-- COMMAND ----------

-- MAGIC %run /kgsfinance/common_utilities/connection_configuration

-- COMMAND ----------

-- MAGIC %run /kgsfinance/common_utilities/common_components

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.functions import col, lit,to_timestamp
-- MAGIC import re,string
-- MAGIC import pytz
-- MAGIC
-- MAGIC
-- MAGIC currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
-- MAGIC print(currentdatetime)
-- MAGIC
-- MAGIC processName = dbutils.widgets.get("processName")
-- MAGIC tableName = dbutils.widgets.get("tableName")
-- MAGIC reportName= dbutils.widgets.get("ReportName")
-- MAGIC curr_table_name = dbutils.widgets.get("curr_table_name")
-- MAGIC hist_table_name = dbutils.widgets.get("hist_table_name")
-- MAGIC
-- MAGIC print(hist_table_name)
-- MAGIC print(curr_table_name)

-- COMMAND ----------

-- DBTITLE 1,Format Headers Data
-- MAGIC %python
-- MAGIC currentDf = spark.sql("select * from "+curr_table_name)
-- MAGIC currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
-- MAGIC
-- MAGIC display(currentDf)

-- COMMAND ----------

delete from $hist_table_name where financial_year in (select distinct Financial_year from $curr_table_name) 

-- COMMAND ----------

-- DBTITLE 1,Load History Data
-- MAGIC %python
-- MAGIC currentDf.write \
-- MAGIC .mode("append") \
-- MAGIC .format("delta") \
-- MAGIC .option("mergeschema","true")  \
-- MAGIC .option("path",finance_trusted_hist_savepath_url+reportName+"/"+processName+"/"+tableName) \
-- MAGIC .option("compression","snappy") \
-- MAGIC .saveAsTable(hist_table_name)