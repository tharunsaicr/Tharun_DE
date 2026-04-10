# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "LayerName", defaultValue = "")
layerName = dbutils.widgets.get("LayerName")

print(tableName)
print(processName)
print(layerName)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_data_type_cast

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bad_record_check

# COMMAND ----------

# DBTITLE 1,Get Computational Column for Primary Key, NULL, Duplicates
badRecordCheckDf = spark.sql("select * from kgsonedatadb.config_bad_record_check")

# Primary Key Column List
pkcheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('PK_CHECK')))
# display(pkcheckDf)

primaryKeyColumnList = pkcheckDf.rdd.flatMap(lambda x: x).collect()
print("primaryKeyColumnList ", primaryKeyColumnList)

# Check Not Null COlumn List
notNullcheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('NOT_NULL_CHECK')))
# display(notNullcheckDf)

notNullColumnList = notNullcheckDf.rdd.flatMap(lambda x: x).collect()
print("notNullColumnList ", notNullColumnList)

# Check Duplicates Column List
duplicateCheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('DUPLICATE_CHECK')))
# display(duplicateCheckDf)

checkForDuplicateList = duplicateCheckDf.rdd.flatMap(lambda x: x).collect()
print("checkForDuplicateList ", checkForDuplicateList)

# COMMAND ----------

# DBTITLE 1,Get Complutational Column to check for TypeCast
typeCastCheckDf = spark.sql("select * from kgsonedatadb.config_data_type_cast")

# Int Column List
intcheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('INT')))

intList = intcheckDf.rdd.flatMap(lambda x: x).collect()
print("intList ", intList)

# Long Column List
longcheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('LONG')))

longList = longcheckDf.rdd.flatMap(lambda x: x).collect()
print("longList ", longList)

# float Column List
floatcheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('FLOAT')))

floatList = floatcheckDf.rdd.flatMap(lambda x: x).collect()
print("floatList ", floatList)

# Date Column List
datecheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('DATE')))

dateList = datecheckDf.rdd.flatMap(lambda x: x).collect()
print("dateList ", dateList)

# Timestamp Column List
timestampCheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('TIMESTAMP')))

timestampList = timestampCheckDf.rdd.flatMap(lambda x: x).collect()
print("timestampList ", timestampList)

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.trusted_hist_"+processName + "_"+ tableName)

# COMMAND ----------

# DBTITLE 1,Check if Primary Key Column is Null or Duplicate
currentDf = currentDf.withColumn("Is_Primary_Key_Null",lit(""))
currentDf = currentDf.withColumn("Is_Primary_Duplicate_Record",lit(""))

display(currentDf)

for columnName in currentDf.columns:
    if (columnName in primaryKeyColumnList):
        print(columnName)

        # Check for Primary Key Null records

        currentDf = currentDf.withColumn("Is_Primary_Key_Null",when((col(columnName).isNull()) | (col(columnName) == '') | (col(columnName) == '-'),"PK is Null")\
        .when(((col(columnName).isNotNull()) & (col(columnName) != '') & (col(columnName) != '-')) & (upper(currentDf.Is_Primary_Key_Null)  != "PK IS NULL"),"")\
        .otherwise(currentDf.Is_Primary_Key_Null))


        # Check for Duplicate Records
        duplicateRecords = currentDf\
                            .groupby(primaryKeyColumnList) \
                            .count() \
                            .where('count > 1') \
                            .sort('count', ascending=False)

        duplicateId = duplicateRecords.select(primaryKeyColumnList)

        print("Duplicate Records")
        display(duplicateRecords)
        
        duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

        currentDf = currentDf.withColumn("Is_Primary_Duplicate_Record",when(col(columnName).isin(duplicateIdList),"PK Duplicate Record")\
        .otherwise(currentDf.Is_Primary_Duplicate_Record))
        
        
goodDf = currentDf.filter((~(upper(currentDf.Is_Primary_Key_Null) == 'PK IS NULL')) & (~(upper(currentDf.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD')))
                             
badDf = currentDf.filter((upper(currentDf.Is_Primary_Key_Null) == 'PK IS NULL') |(upper(currentDf.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD') )         

display(goodDf)
display(badDf)

# COMMAND ----------

# DBTITLE 1,Check if column value is null and move that to Bad record
currentDf = currentDf.withColumn("Is_Column_Null",lit(""))

for columnName in currentDf.columns:
    if (columnName in notNullColumnList):
        print(columnName)

        currentDf = currentDf.withColumn("Is_Column_Null",when((col(columnName).isNull()) | (col(columnName) == ''),"Source is Null")\
        .when(((col(columnName).isNotNull()) & (col(columnName) != '')) & (upper(currentDf.Is_Column_Null)  != "SOURCE IS NULL"),"")\
        .otherwise(currentDf.Is_Column_Null))
        
        
goodDf = currentDf.filter(~(upper(currentDf.Is_Column_Null) == 'SOURCE IS NULL'))
                             
badDf = currentDf.filter(upper(currentDf.Is_Column_Null) == 'SOURCE IS NULL')        

display(goodDf)
display(badDf)

# COMMAND ----------

# DBTITLE 1,Check if duplicate based on Key and move that to bad record
currentDf = currentDf.withColumn("Is_Duplicate_Record",lit(""))

for columnName in currentDf.columns:
    if (columnName in checkForDuplicateList):
        print(columnName)

        duplicateRecords = currentDf\
                            .groupby(checkForDuplicateList) \
                            .count() \
                            .where('count > 1') \
                            .sort('count', ascending=False)

        print("Duplicate Records")
        display(duplicateRecords)

    duplicateId = duplicateRecords.select(checkForDuplicateList)
    
    duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

    currentDf = currentDf.withColumn("Is_Duplicate_Record",when(col(columnName).isin(duplicateIdList),"Duplicate Record")\
    .otherwise(currentDf.Is_Duplicate_Record))


goodDf = currentDf.filter(~(upper(currentDf.Is_Duplicate_Record) == 'DUPLICATE RECORD'))
                            
badDf = currentDf.filter(upper(currentDf.Is_Duplicate_Record) == 'DUPLICATE RECORD')  

display(goodDf)
display(badDf)


# COMMAND ----------

# DBTITLE 1,Typecast issue move that to Bad record
from pyspark.sql.types import *
from pyspark.sql.functions import col, unix_timestamp, to_date

currentDf = currentDf.withColumn("IntConversion",lit(""))
currentDf = currentDf.withColumn("DateConversion",lit(""))
currentDf = currentDf.withColumn("FloatConversion",lit(""))
currentDf = currentDf.withColumn("LongConversion",lit(""))
currentDf = currentDf.withColumn("TimestampConversion",lit(""))


print("Int: ",intList)
print("Date: ",dateList)
print("Float: ",floatList)
print("Long: ",longList)
print("TimeStamp: ",timestampList)


for columnName in currentDf.columns:

    if (columnName in intList):
        
        currentDf = currentDf.withColumn("IntConversion",when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(IntegerType()).isNull()),lit("Int Type Cast Issue"))\
        .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") ) & currentDf[columnName].cast(IntegerType()).isNull()),lit("Source is Null"))\
        .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(IntegerType()).isNotNull()),lit(""))\
        .otherwise(currentDf[columnName]))

    if (columnName in dateList):
        
        currentDf=currentDf.withColumn("DateConversion",\
        when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd MMMM, yyyy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),lit(""))\
        .when(to_date(currentDf[columnName], 'yyyy/MM/dd').isNotNull(),lit(""))\
        .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") ) & currentDf[columnName].cast(DateType()).isNull()),lit("Source is Null"))\
        .otherwise(lit("Date invalid"))
    )
     

    if (columnName in floatList):
        
        currentDf= currentDf.withColumn("FloatConversion",when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(FloatType()).isNull()),lit("Float Type Cast Issue"))\
        .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") ) & currentDf[columnName].cast(FloatType()).isNull()),lit("Source is Null"))\
        .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(FloatType()).isNotNull()),lit(""))\
        .otherwise(currentDf[columnName]))



    if (columnName in longList):
        
        currentDf = currentDf.withColumn("LongConversion",when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(LongType()).isNull()),lit("Long Type Cast Issue"))\
        .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") ) & currentDf[columnName].cast(LongType()).isNull()),lit("Source is Null"))\
        .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(LongType()).isNotNull()),lit(""))\
        .otherwise(currentDf[columnName]))



    if (columnName in timestampList):
        
        currentDf = currentDf.withColumn("TimeStampConversion",when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast("Timestamp").isNull()),lit("Timestamp Cast Issue"))\
        .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") ) & currentDf[columnName].cast("Timestamp").isNull()),lit("Source is Null"))\
        .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast("Timestamp").isNotNull()),lit(""))\
        .otherwise(currentDf[columnName]))
 
        
display(currentDf)

# COMMAND ----------

goodDf = currentDf.filter((~(upper(currentDf.Is_Duplicate_Record) == 'DUPLICATE RECORD')) & (~(upper(currentDf.Is_Primary_Key_Null) == 'PK IS NULL')) & (~(upper(currentDf.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD')) & (~(upper(currentDf.IntConversion) == 'INT TYPE CAST ISSUE')) & (~(upper(currentDf.FloatConversion) == 'FLOAT TYPE CAST ISSUE')) & (~(upper(currentDf.LongConversion) == 'LONG TYPE CAST ISSUE'))& (~(upper(currentDf.TimestampConversion) == 'TIMESTAMP TYPE CAST ISSUE')) & (~(upper(currentDf.DateConversion) == 'DATE INVALID')) & (~(upper(currentDf.Is_Column_Null) == 'SOURCE IS NULL')))
                            
badDf = currentDf.filter((upper(currentDf.Is_Duplicate_Record) == 'DUPLICATE RECORD') | (upper(currentDf.Is_Primary_Key_Null) == 'PK IS NULL') | (upper(currentDf.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD') | (upper(currentDf.IntConversion) == 'INT TYPE CAST ISSUE') | (upper(currentDf.FloatConversion) == 'FLOAT TYPE CAST ISSUE') | (upper(currentDf.LongConversion) == 'LONG TYPE CAST ISSUE')| (upper(currentDf.TimestampConversion) == 'TIMESTAMP TYPE CAST ISSUE') | (upper(currentDf.DateConversion) == 'DATE INVALID') | (upper(currentDf.Is_Column_Null) == 'SOURCE NOT NULL') )

# COMMAND ----------

print("Good DF: ")
display(goodDf)

print("Bad DF: ")
display(badDf)

# COMMAND ----------

# from pyspark.sql.types import *
# from pyspark.sql.functions import col, unix_timestamp, to_date


# print("Int: ",intList)
# print("Date: ",dateList)
# print("Float: ",floatList)
# print("Long: ",longList)
# print("TimeStamp: ",timestampList)

# currentDf = goodDf

# for columnName in currentDf.columns:

#     if (columnName in intList):
#         currentDf = currentDf.withColumn(columnName,currentDf[columnName].cast(IntegerType()))

#     if (columnName in dateList):
#         currentDf=currentDf.withColumn(columnName,\
#         when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),to_date(currentDf[columnName], 'yyyy-MM-dd'))\
#         .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),to_date(currentDf[columnName], 'dd/MM/yyyy'))\
#         .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),to_date(currentDf[columnName], 'MM/dd/yyyy'))\
#         .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MM-yyyy'))\
#         .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMM-yyyy'))\
#         .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMMM-yyyy'))\
#         .when(to_date(currentDf[columnName], 'dd MMMM, yyyy').isNotNull(),to_date(currentDf[columnName], 'dd MMMM,yyyy'))\
#         .when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),to_date(currentDf[columnName], 'dd-MM-yy'))\
#         .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),to_date(currentDf[columnName], 'dd MMM yy'))\
#         .when(to_date(currentDf[columnName], 'yyyy/MM/dd').isNotNull(),to_date(currentDf[columnName], 'yyyy/MM/dd'))\
#         .otherwise(currentDf[columnName])
#     )

       

#     if (columnName in floatList):
#         currentDf= currentDf.withColumn(columnName,currentDf[columnName].cast(FloatType()))



#     if (columnName in longList):
#         currentDf = currentDf.withColumn(columnName,currentDf[columnName].cast(LongType()))



#     if (columnName in timestampList):
#         currentDf = currentDf.withColumn(columnName,when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast("Timestamp").isNotNull()),currentDf[columnName].cast("Timestamp"))\
#         .otherwise(currentDf[columnName]))

# dropColumns = ["Is_Primary_Key_Null","Is_Primary_Duplicate_Record","Is_Column_Null","Is_Duplicate_Record","IntConversion","DateConversion","FloatConversion","LongConversion","TimestampConversion"]

# currentDf = currentDf.drop(*dropColumns)
        
# display(currentDf)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, unix_timestamp, to_date


print("Int: ",intList)
print("Date: ",dateList)
print("Float: ",floatList)
print("Long: ",longList)
print("TimeStamp: ",timestampList)

currentDf = goodDf

for columnName in currentDf.columns:

    if (columnName in intList):
        # currentDf = currentDf.withColumn(columnName,currentDf[columnName].cast(IntegerType()))
        currentDf = currentDf.withColumn(columnName,when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(IntegerType()).isNotNull()),currentDf[columnName].cast(IntegerType()))\
        .otherwise(currentDf[columnName]))
        

    if (columnName in dateList):
        currentDf=currentDf.withColumn(columnName,\
        when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),to_date(currentDf[columnName], 'yyyy-MM-dd'))\
        .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),to_date(currentDf[columnName], 'dd/MM/yyyy'))\
        .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),to_date(currentDf[columnName], 'MM/dd/yyyy'))\
        .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MM-yyyy'))\
        .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMM-yyyy'))\
        .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),to_date(currentDf[columnName], 'dd-MMMM-yyyy'))\
        .when(to_date(currentDf[columnName], 'dd MMMM, yyyy').isNotNull(),to_date(currentDf[columnName], 'dd MMMM,yyyy'))\
        .when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),to_date(currentDf[columnName], 'dd-MM-yy'))\
        .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),to_date(currentDf[columnName], 'dd MMM yy'))\
        .when(to_date(currentDf[columnName], 'yyyy/MM/dd').isNotNull(),to_date(currentDf[columnName], 'yyyy/MM/dd'))\
        .otherwise(currentDf[columnName])
    )

       

    if (columnName in floatList):
        # currentDf= currentDf.withColumn(columnName,currentDf[columnName].cast(FloatType()))
        currentDf= currentDf.withColumn(columnName,when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(FloatType()).isNotNull()),currentDf[columnName].cast(FloatType()))\
        .otherwise(currentDf[columnName]))



    if (columnName in longList):
        # currentDf = currentDf.withColumn(columnName,currentDf[columnName].cast(LongType()))
        currentDf = currentDf.withColumn(columnName,when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(LongType()).isNotNull()),currentDf[columnName].cast(LongType()))\
        .otherwise(currentDf[columnName]))



    if (columnName in timestampList):
        currentDf = currentDf.withColumn(columnName,when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast("Timestamp").isNotNull()),currentDf[columnName].cast("Timestamp"))\
        .otherwise(currentDf[columnName]))

dropColumns = ["Is_Primary_Key_Null","Is_Primary_Duplicate_Record","Is_Column_Null","Is_Duplicate_Record","IntConversion","DateConversion","FloatConversion","LongConversion","TimestampConversion"]

currentDf = currentDf.drop(*dropColumns)
        
display(currentDf)

# COMMAND ----------

dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,If Raw Layer load data to Raw_Curr and Raw_Hist
if(layerName.upper() == "RAW"):
    
    # Load into Raw Current Table
    currentDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True") \
    .option("path",raw_curr_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.raw_curr_"+ processName + "_" +tableName)

    # Load into Raw History Table
    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)


# COMMAND ----------

# DBTITLE 1,If Trusted Layer load data to Trusted and Trusted_Hist
if(layerName.upper() == "TRUSTED"):
    
    # Load into Trusted Current Table
    currentDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True") \
    .option("path",trusted_curr_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.trusted_"+ processName + "_" +tableName)

    # Load into Trusted History Table
    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",trusted_hist_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.trusted_hist_"+ processName + "_" + tableName)

# COMMAND ----------

databaseName = 'kgsonedatadb_badrecords'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# DBTITLE 1,Load to Bad Records
badDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true") \
.option("path",bad_filepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb_badrecords.trusted_hist_"+ processName + "_" + tableName+"_bad")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details whre File_Date

# COMMAND ----------

