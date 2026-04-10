# Databricks notebook source
edw_landing_path_url='/mnt/edw/'

config_path_url = "/mnt/configmount/edw"
config_hist_path_url = "/mnt/configmount/history/edw"


edw_raw_hist_savepath_url="/mnt/edw/rawlayer/history/"
edw_raw_curr_savepath_url="/mnt/edw/rawlayer/current/"
#edw_raw_stg_savepath_url="/mnt/edw/rawlayer/staging/"

edw_trusted_hist_savepath_url="/mnt/edw/trustedlayer/history/"
edw_trusted_curr_savepath_url="/mnt/edw/trustedlayer/current/"
#edw_trusted_stg_savepath_url="/mnt/edw/trustedlayer/staging/"

bad_filepath_url = "/mnt/badfiles/edw/"

# COMMAND ----------

databaseName = 'kgsonedatadbedw'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")