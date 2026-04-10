# Databricks notebook source
# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(filePath)
print(tableName)
print(processName)


# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate)

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,Import libraries
from datetime import datetime
from pyspark.sql.functions import col, lit,lower,udf
import io
import pandas as pd
import msoffcrypto
import urllib
import requests
import pyspark.sql.functions as f
from bs4 import BeautifulSoup
from pyspark.sql.types import ArrayType,StringType,BooleanType
from pyspark.sql import functions as F
import re
currentdatetime= datetime.now()

# COMMAND ----------

def replace(column, value):
    return when(column == value, lit(None)).otherwise(column)

# COMMAND ----------

filePath="dbfs:"+landing_path_url+filePath
print(filePath)

# COMMAND ----------

# DBTITLE 1,REading the password txt file
# html_file_path=landing_path_url+'pending/TA/TA_passwd.txt'
# html_content_df=spark.read.text(html_file_path)

# COMMAND ----------

# DBTITLE 1,Func to convert html tags to array

# def extract_data_from_html(html_content):
#     soup=BeautifulSoup(html_content,"html.parser")
#     paragraphs=soup.find_all("p")
#     extracted_data=[p.get_text() for p in paragraphs]
#     return extracted_data

# COMMAND ----------

# DBTITLE 1,Fetch file password into variable
# extract_data_udf=udf(extract_data_from_html,ArrayType(StringType()))
# extracted_data_df=html_content_df.withColumn("extracted_data",extract_data_udf(col("value")))
# # extracted_data_df.display()
# pattern = ".*Password to access the file.*"  
# extracted_data_df = extracted_data_df.filter(expr(f"exists(extracted_data, x -> regexp_like(x, '{pattern}'))"))
# extracted_data_df = extracted_data_df.withColumn("index", expr(f"array_position(extracted_data, '{pattern}')"))
# array_column_name = "extracted_data" 
# array_values = extracted_data_df.select(array_column_name).rdd.flatMap(lambda x: x[0]).collect()
# matches = [value for value in array_values if re.search(pattern, value)]
# print(matches)

# pattern = r'--- (\w+)'
# extracted_password = None
# for element in matches:
#     match = re.search(pattern, element)
#     if match:
#         extracted_password = match.group(1)

# if extracted_password:
#     pwd = extracted_password
#     print(pwd)

# else:
#     print("Pattern not found in the array.")


# COMMAND ----------

# DBTITLE 1,To extract password from file name
# import re
# file_name ="pending/talent_acquisition/2023/Firmed_Up_Joiners_List_20230901.xlsx"
date_pattern=r'(\d{8})'
match=re.search(date_pattern,filePath)
if match:
    date=match.group(1)
    year=date[:4]
    month=date[4:6]
    day=date[6:]
    month_names={'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
    month_name=month_names.get(month,'unknown')
    result=f"{month_name}{day}"
    print("Extracted password:",result)
else:
    print("Date not found in file name")    

# COMMAND ----------

pwd = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="compensationSecret")
# pwd = dbutils.secrets.get(scope="scopekeyvault",key="excel-comp-pwd")

# COMMAND ----------

sheetName= "'" +tableName+"'"
print(sheetName)

# COMMAND ----------

if(processName.upper() == 'COMPENSATION' and tableName.upper() == 'PD_RATING'):
    currentDf = spark.read.format("com.crealytics.spark.excel")\
                    .option("inferschema","false")\
                    .option("header","true")\
                    .option("dataAddress",sheetName+"!A1")\
                    .load(filePath)
    for col in currentDf.columns:
            currentDf=currentDf.withColumnRenamed(col,replacechar(col))                
else:
    print("under fts/cwk")
    sheetName = tableName.replace("post_hire_","")
    try:
        currentDf = spark.read.format("com.crealytics.spark.excel")\
                        .option("inferschema","false")\
                        .option("header","true")\
                        .option("dataAddress",sheetName+"!A1")\
                        .option("workbookPassword",result)\
                        .option("maxByteArraySize",150000000)\
                        .option("inferDateTimeFormat","false")\
                        .load(filePath)
    except (Exception, RuntimeError) as e:
        if "Unknown sheet "+tableName in str(e):
            print("If")
            # If the error is due to an unknown sheet, exit the notebook
            dbutils.notebook.exit("Exiting notebook as sheet '"+tableName+"' not present in the xlsx workbook.")
        else:
            print("else")
            # If it's a different AnalysisException, let it propagate
            raise e


for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))



# COMMAND ----------

# DBTITLE 1,Trim and Convert Column name to Upper Case
for col in currentDf.columns:
        currentDf=currentDf.withColumnRenamed(col,toUpper((trimchar(col))))

# COMMAND ----------

# DBTITLE 1,To remove ASCII in column values
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,ascii_ignore(col))

# COMMAND ----------

# DBTITLE 1,old_Reading file using Pyspark 
# print("File Path : ",filePath)

# currentDf = spark.read.format("com.crealytics.spark.excel")\
#                     .option("inferschema","false")\
#                     .option("header","true")\
#                     .option("dataAddress",sheetName+"!A1")\
#                     .option("tempFileThreshold", 10000000)\
#                     .option("workbookPassword",pwd)\
#                     .option("maxByteArraySize",150000000)\
#                     .option("inferDateTimeFormat","false")\
#                     .load(filePath)


# for col in currentDf.columns:
#     currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

# DBTITLE 1,Pandas Approach
# if ((processName.lower()== 'compensation')):
#     decrypted_workbook = io.BytesIO()
#     with open(filePath, 'rb') as file:
#         office_file = msoffcrypto.OfficeFile(file)
#         office_file.load_key(password=pwd)
#         office_file.decrypt(decrypted_workbook)

#     df = pd.read_excel(decrypted_workbook, sheet_name= tableName,dtype=str)
#     df=df.fillna('')
#     # df = df.astype(str)  
#     currentDf = spark.createDataFrame(df)
#     for col in currentDf.columns:
#         currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

# for col in currentDf.columns:
#     currentDf = currentDf.withColumn(col, replace(currentDf[col], "NaT"))
#     currentDf = currentDf.withColumn(col, replace(currentDf[col], "NaN"))
#     currentDf = currentDf.withColumn(col, replace(currentDf[col], "nan"))

# COMMAND ----------

# Typecast every column to String
currentDf=colcaststring(currentDf,currentDf.columns)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))

if 'File_Date' not in currentDf.columns:
    print("File_Date does not exists")
    currentDf=currentDf.withColumn("File_Date", lit(fileDate))
else:
    print("File_Date exists")
# currentDf.display()


# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

currentDf.display()

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

print(tableName)

# COMMAND ----------

# DBTITLE 1,Load Raw Staging Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from kgsonedatadb.raw_stg_talent_acquisition_CWK
# MAGIC
# MAGIC -- select * from kgsonedatadb.raw_hist_talent_acquisition_cwk

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

saveTableName = "kgsonedatadb.raw_stg_"+processName + "_"+ tableName
#print(tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from kgsonedatadb.raw_stg_talent_acquisition_FTE

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})

# COMMAND ----------

