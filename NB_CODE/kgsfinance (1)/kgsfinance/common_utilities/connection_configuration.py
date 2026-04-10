# Databricks notebook source
finance_landing_path_url='/mnt/finance/'

finance_raw_hist_savepath_url="/mnt/finance/rawlayer/history/"
finance_raw_curr_savepath_url="/mnt/finance/rawlayer/current/"
finance_raw_stg_savepath_url="/mnt/finance/rawlayer/staging/"

finance_trusted_hist_savepath_url="/mnt/finance/trustedlayer/history/"
finance_trusted_curr_savepath_url="/mnt/finance/trustedlayer/current/"
finance_trusted_stg_savepath_url="/mnt/finance/trustedlayer/staging/"

# COMMAND ----------

databaseName = 'kgsfinancedb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")