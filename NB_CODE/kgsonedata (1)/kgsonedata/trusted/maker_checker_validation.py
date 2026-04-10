# Databricks notebook source
zoneName = "trusted_stg"

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")
# tableName = "employee_details"
# tableName = "resigned_and_left"

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")
# processName = "headcount"


print(tableName)
print(processName)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from dateutil.parser import parse
import itertools

# COMMAND ----------

df_actual_table = spark.sql("select * from kgsonedatadb."+zoneName+"_"+processName+"_"+tableName)
display(df_actual_table)

# COMMAND ----------

# print("Count Before Validation: ",df_actual_table.count())

actual_table_columnList = df_actual_table.rdd.flatMap(lambda x: x).collect()

if "Remarks" not in actual_table_columnList:
    df_actual_table = df_actual_table.withColumn("Remarks",lit(""))
else:
    df_actual_table = df_actual_table.withColumn("Remarks", when(df_actual_table.Remarks.isNull(), lit("")).otherwise(df_actual_table.Remarks))

df_config_maker_checker_columns = spark.sql("select distinct param_process_table_value_columns from kgsonedatadb.config_maker_checker_validation where processName = \""+processName+"\" and tableName = \""+tableName+"\"")

df_config_maker_checker_columnsList = df_config_maker_checker_columns.select("param_process_table_value_columns").rdd.flatMap(lambda x: x).collect()

print(df_config_maker_checker_columnsList)

i=0


for row in df_config_maker_checker_columnsList:

    df_config_maker_checker = spark.sql("select * from kgsonedatadb.config_maker_checker_validation where processName = \""+processName+"\" and tableName = \""+tableName+"\" and param_process_table_value_columns = \""+str(row)+"\"")

    processName = processName #"headcount"
    tableName = tableName #"employee_details"
    process_table_key_columnList = df_config_maker_checker.select("process_table_key_column").rdd.flatMap(lambda x: x).collect() #"Cost_centre"
    
#     print("process_table_key_columnList: ", process_table_key_columnList)
    
    if(process_table_key_columnList):

        process_table_key_column = process_table_key_columnList[0]
        
        process_table_columnList = df_config_maker_checker.select("param_process_table_value_columns").rdd.flatMap(lambda x: x).collect()
        
        param_process_table_value_column = process_table_columnList[0].split("|")
        
        config_table_nameList = df_config_maker_checker.select("config_table_name").rdd.flatMap(lambda x: x).collect() 
        config_table_name = config_table_nameList[0]
       
        config_table_key_columnList = df_config_maker_checker.select("config_table_key_column").rdd.flatMap(lambda x: x).collect() #"Cost_centre"
        config_table_key_column = config_table_key_columnList[0]

        config_table_columnList = df_config_maker_checker.select("param_config_table_value_columns").rdd.flatMap(lambda x: x).collect()
        param_config_table_value_column = config_table_columnList[0].split("|")
    
        config_key_column = ""
        config_value_column = ""
        
        i = i+1
        j=0
        
        for a,b in itertools.zip_longest(param_process_table_value_column,param_config_table_value_column):
           
            j=j+1
            process_table_value_column = a
            config_table_value_column = b
            
            if(len(param_process_table_value_column) >1):
                
                config_key_column = "config_key_column" + str(i) + "_" + str(j)
                
                config_value_column = "config_value_column" + str(i) + "_" + str(j)
            
            else:
                config_key_column = "config_key_column" + str(i)
                
                config_value_column = "config_value_column" + str(i)

            query = "select upper(" + config_table_key_column +") as " + config_key_column+", upper("+ config_table_value_column +") as "+config_value_column+ " from kgsonedatadb."+config_table_name

            df_config_table = spark.sql(query)
            df_config_table = df_config_table.groupBy(col(config_key_column)) \
            .agg(collect_list(col(config_value_column)))
            
            df_actual_table = df_actual_table.join(df_config_table, upper(df_actual_table[process_table_key_column]) == upper(df_config_table[config_key_column]), "left")
                       
            df_actual_table = df_actual_table.withColumn("Remarks", \
                                                         when(((df_actual_table[process_table_key_column].isNull()) & (~(df_actual_table.Remarks.contains(process_table_key_column+" is null")))) | (upper(df_actual_table[process_table_key_column]) == "NA"),concat(df_actual_table.Remarks,lit(" | "+process_table_key_column+" is null in "+tableName))).\
                                                         when(((df_actual_table[config_key_column].isNull()) & (~(df_actual_table.Remarks.contains(config_table_key_column+" is invalid")))),concat(df_actual_table.Remarks,lit(" | "+config_table_key_column+" is invalid "))).\
                                                         when((df_actual_table[config_key_column].isNotNull()) & (~(array_contains(df_actual_table["collect_list("+config_value_column+")"], upper(col(process_table_value_column))))),concat(df_actual_table.Remarks,lit(" | "+config_table_value_column+" mismatch"))).\
                                                         otherwise(concat(df_actual_table.Remarks,lit(""))))
            
            #10/6/2022
            df_actual_table = df_actual_table.drop(config_key_column,"collect_list("+config_value_column+")")
                        
    else:
        print("No entry available for table "+tableName +" in config_maker_checker_validation table")

     
# print("Count After Validation",df_actual_table.count())
# display(df_actual_table.select("Remarks").distinct())

# COMMAND ----------

columnList = df_actual_table.columns
if(tableName == "employee_details" or tableName == "sabbatical" or tableName == "secondee_outward"):
    columnList.remove("End_Date")
    columnList.remove("Remarks")
else:
    columnList.remove("Remarks")
print(columnList)

for columnName in columnList:
    df_actual_table = df_actual_table.withColumn("Remarks", when((((col(columnName) == " ") | (col(columnName) == "-") | (col(columnName) == "NA") | (col(columnName).isNull()) | (col(columnName) == "") | (col(columnName) == "#N/A")) & (~(df_actual_table.Remarks.contains(columnName + " is null or NA")))),concat(df_actual_table.Remarks,lit(" | " + columnName + " is null or NA"))).otherwise(df_actual_table.Remarks))

# COMMAND ----------

# display(df_actual_table.select("Remarks"))

# COMMAND ----------

# DBTITLE 1,Randomization -1/2
def randomizeStringData(stringval):
    vowelsList = ["a","e","i","o","u","A","E","I","O","U"]
    consonantList = ["b","c","d","g","h","k","l","m","n","p","r","s","t","B","C","D","G","H","K","L","M","N","P","R","S","T"]
    i=0
    for charval1 in vowelsList:
        stringval=stringval.replace(charval1,"")
    
    for charval2 in consonantList:
        stringval=stringval.replace(consonantList[i],consonantList[len(consonantList)-1-i])
        i = i + 1
    return stringval

# COMMAND ----------

df_actual_table.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# To handle GC problem with cluster
# spark.catalog.clearCache()