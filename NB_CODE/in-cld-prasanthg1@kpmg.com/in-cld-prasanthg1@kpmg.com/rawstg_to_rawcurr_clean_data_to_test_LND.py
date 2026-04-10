# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(tableName)
print(processName)

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get Computational Date Column
dateDf = spark.sql("select * from kgsonedatadb.config_convert_column_to_date")
dateDf = dateDf.select("ColumnName").where((dateDf.ProcessName == processName) & (dateDf.DeltaTableName == tableName ))
display(dateDf)

columnList = dateDf.rdd.flatMap(lambda x: x).collect()
print("List ", columnList)

# COMMAND ----------

# DBTITLE 1,Get Computational Serial Date Columns
sdateDf = spark.sql("select * from kgsonedatadb.config_SerialDateConversionColumns")
sdateDf = sdateDf.select("SerialDate_ColumnName","KeyColumn").where((sdateDf.ProcessName == processName) & (sdateDf.DeltaTableName == tableName ))
display(sdateDf)

serialcolumnList = sdateDf.select("SerialDate_ColumnName").rdd.flatMap(lambda x: x).collect()
print("List ", serialcolumnList)

keycolumnList = sdateDf.select("KeyColumn").rdd.flatMap(lambda x: x).collect()
print("Key List ", keycolumnList)

if(keycolumnList):
    keyColumn = keycolumnList[0]
    print("Key Column : ",keyColumn)
else:
    print("No column with serial date")

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.test_raw_stg_"+processName + "_"+ tableName)
print(currentDf.count())

# COMMAND ----------

# DBTITLE 1,Detect Serial Date column and convert them to YYYY-MM-DD
serialformattedColumn = []
serialDateLenght = 5 #Standard length for Serial Dates

for columnName in currentDf.columns:
     if (columnName in serialcolumnList):
            
            dateCol = currentDf.select(columnName).rdd.flatMap(lambda x: x).collect()
            dateValLength = len(dateCol[0])
            
            if(dateValLength == serialDateLenght):
                print("ColumnName ",columnName)
                df = spark.sql("select date_add('1899-12-30',cast("+ columnName +" as Integer)) as Converted_"+columnName+", "+keyColumn+" as Config_"+keyColumn+" from kgsonedatadb.raw_stg_"+processName + "_"+ tableName)

                display(df)

                joindf = currentDf.join(df,currentDf[keyColumn] == df["Config_"+keyColumn],"left").select(currentDf["*"],df["Converted_"+columnName])
                joindf = joindf.withColumn(columnName,col("Converted_"+columnName))
                joindf = joindf.drop("Converted_"+columnName)

                serialformattedColumn.append(columnName)
                currentDf = joindf
            
print("serialformattedColumn: ",serialformattedColumn)            
print(currentDf.count())  

# COMMAND ----------

# DBTITLE 1,Detect Date column and convert them to YYYY-MM-DD
dateKeyword = "Date"
lwdKeyword = "LWD"


nullColumns = get_null_column_names(currentDf)

formattedColumn = []

for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)
        
        currentDf = currentDf.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName).isNull()) | (col(columnName) == "") | (col(columnName) == "#N/A")),"1900-01-01").otherwise(col(columnName)))
                     
        currentDf = currentDf.withColumn(columnName,changeDateFormat(col(columnName)))
        formattedColumn.append(columnName)
        

print(currentDf.count())

# COMMAND ----------

# DBTITLE 1,Display Updated Date Column
if formattedColumn:
    print("Formatted Columns", formattedColumn)
    # currentDf.select([col for col in formattedColumn]).show()
    print(currentDf.count())
else:
    print("None of the column was formatted")

# COMMAND ----------

if (tableName == "contingent" or tableName == "contingent_worker" or tableName == "contingent_worker_resigned"):
    currentDf = currentDf.withColumn("Candidate_Id",regexp_replace(col("Candidate_Id"), "[^a-zA-Z0-9]", ""))
    

# COMMAND ----------

emp_col = "Employee_Number"
if emp_col in currentDf.columns:
    currentDf = currentDf.withColumn("Employee_Number", currentDf["Employee_Number"].cast(StringType()))

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.startswith("_c1") or column.startswith("_c2") or column.startswith("_c3") or column.startswith("_c4") or column.startswith("_c5") or column.startswith("_c6") or column.startswith("_c7") or column.startswith("_c8") or column.startswith("_c9") or column.startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
# buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
# buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

# columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# DBTITLE 1,Standardize BU across all Process
# for columnName in currentDf.columns:
#     if (columnName in columnList):
#         print(columnName)


#         joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

#         joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
#         .otherwise(joinDf[columnName]))
        
#         joinDf = joinDf.drop(*buList.columns)

#         currentDf = joinDf

# COMMAND ----------

# DBTITLE 1,Load into Raw Current Table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.test_raw_curr_"+ processName + "_" +tableName)

# COMMAND ----------

