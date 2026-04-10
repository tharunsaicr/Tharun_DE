# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number,when
from pyspark.sql.window import Window

# COMMAND ----------

data = [("John","B",25),("Alice","C",25),("Bob","A",25),("Emma","A",25),("Emma","B",25),("Emma","C",25)]

# COMMAND ----------

df = spark.createDataFrame(data,["Name","Category","Age"])

# COMMAND ----------

custom_order = {"A":3,"B":2,"C":1}

# order_expr= [df["Category"] == cat for cat, _ in sorted(custom_order.items(),key=lambda x:x[1])]

# window_spec = Window.partitionBy("Name").orderBy(*order_expr)

window_spec = Window.partitionBy("Name").orderBy(when(df["Category"] =="A",custom_order["A"]).when(df["Category"] =="B",custom_order["B"]).when(df["Category"] =="C",custom_order["C"]).otherwise(float('inf')))

df =df.withColumn("Row_number", row_number().over(window_spec))

# COMMAND ----------

display(df)

# COMMAND ----------

