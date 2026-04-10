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
dateDf = spark.sql("select * from kgsonedatadb.config_adhoc_convert_column_to_date")
dateDf = dateDf.select("ColumnName").where((dateDf.ProcessName == processName) & (dateDf.DeltaTableName == tableName ))
display(dateDf)

columnList = dateDf.rdd.flatMap(lambda x: x).collect()
# print("List ", columnList)

# COMMAND ----------

# DBTITLE 1,Get Computational Serial Date Columns
sdateDf = spark.sql("select * from kgsonedatadb.config_SerialDateConversionColumns")
sdateDf = sdateDf.select("SerialDate_ColumnName","KeyColumn").where((sdateDf.ProcessName == processName) & (sdateDf.DeltaTableName == tableName ))
display(sdateDf)

serialcolumnList = sdateDf.select("SerialDate_ColumnName").rdd.flatMap(lambda x: x).collect()
# print("List ", serialcolumnList)

keycolumnList = sdateDf.select("KeyColumn").rdd.flatMap(lambda x: x).collect()
# print("Key List ", keycolumnList)

if(keycolumnList):
    keyColumn = keycolumnList[0]
    print("Key Column : ",keyColumn)
else:
    print("No column with serial date")

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_stg_"+processName + "_"+ tableName)
# print(currentDf.count())

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

                # display(df)

                joindf = currentDf.join(df,currentDf[keyColumn] == df["Config_"+keyColumn],"left").select(currentDf["*"],df["Converted_"+columnName])
                joindf = joindf.withColumn(columnName,col("Converted_"+columnName))
                joindf = joindf.drop("Converted_"+columnName)

                serialformattedColumn.append(columnName)
                currentDf = joindf
            
# print("serialformattedColumn: ",serialformattedColumn)            
# print(currentDf.count())   

# COMMAND ----------

# DBTITLE 1,Detect Date column and convert them to YYYY-MM-DD
dateKeyword = "Date"
lwdKeyword = "LWD"


nullColumns = get_null_column_names(currentDf)

formattedColumn = []

for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)
        
        currentDf = currentDf.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "-") | (col(columnName) == "0-Jan-00") | (col(columnName).isNull()) | (trim(col(columnName)) == "") | (col(columnName) == "#N/A")),"1900-01-01").otherwise(col(columnName)))
                     
        currentDf = currentDf.withColumn(columnName,changeDateFormat(col(columnName)))
        formattedColumn.append(columnName)
        

# print(currentDf.count())

# COMMAND ----------

# DBTITLE 1,Display Updated Date Column
if formattedColumn:
    print("Formatted Columns", formattedColumn)
    currentDf.select([col for col in formattedColumn]).show()
else:
    print("None of the column was formatted")

# COMMAND ----------

if (tableName == "contingent" or tableName == "contingent_worker" or tableName == "contingent_worker_resigned"):
    currentDf = currentDf.withColumn("Candidate_Id",regexp_replace(col("Candidate_Id"), "[^a-zA-Z0-9]", ""))
    

# COMMAND ----------

headcount_list = ['employee_details','resigned_and_left','loaned_staff_from_ki','sabbatical','contingent_worker','maternity_cases','secondee_outward','loaned_contingent_resigned','loaned_resigned']

if tableName in headcount_list:
   
    if "Remarks" not in currentDf.columns:
        currentDf = currentDf.withColumn("Remarks",lit(""))
        print("Added Remarks column to ", tableName)
    else:
        currentDf = currentDf.withColumn("Remarks", currentDf.Remarks)
        print("Remarks column is already available in ", tableName)

# COMMAND ----------

# DBTITLE 1,Get BU List and Tables to be Updated
buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")

columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# DBTITLE 1,Standardize BU across all Process
for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
        .otherwise(joinDf[columnName]))
        
        joinDf = joinDf.drop(*buList.columns)

        currentDf = joinDf

# COMMAND ----------

# print(currentDf.count())
if ((tableName != 'loaned_staff_resigned') & (tableName != 'academic_trainee')):
    print("inside loaned_staff_resigned")
    actual_table_columnList = currentDf.rdd.flatMap(lambda x: x).collect()

    if "Remarks" not in actual_table_columnList:
        currentDf = currentDf.withColumn("Remarks",lit(""))
    else:
        currentDf = currentDf.withColumn("Remarks", when(currentDf.Remarks.isNull(), lit("")).otherwise(currentDf.Remarks))

# COMMAND ----------

# DBTITLE 1,Load into Trusted Current Table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load into Trusted History Table
currentDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true") \
.option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/Delta_to_SQL_with_Select",6000, {'DeltaTableName':tableName, 'ProcessName':processName})