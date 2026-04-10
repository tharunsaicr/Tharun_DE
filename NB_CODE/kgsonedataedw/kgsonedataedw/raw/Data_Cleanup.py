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

# MAGIC %run /Workspace/kgsonedataedw/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /Workspace/kgsonedataedw/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import pytz
from pyspark.sql import functions
from pyspark.sql.functions import dense_rank,expr,concat,to_timestamp,length, regexp_replace
from pyspark.sql.window import Window

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,Get Computational Column for Primary Key, NULL, Duplicates
# badRecordCheckDf = spark.sql("select * from kgsonedatadb.config_bad_record_check")

# # Primary Key Column List
# pkcheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('PK_CHECK')))


# primaryKeyColumnList = pkcheckDf.rdd.flatMap(lambda x: x).collect()
# print("primaryKeyColumnList ", primaryKeyColumnList)

# # Composite Key Column List
# ckcheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('CK_CHECK')))


# compositeKeyColumnList = ckcheckDf.rdd.flatMap(lambda x: x).collect()
# print("compositeKeyColumnList ", compositeKeyColumnList)

# # Check Not Null COlumn List
# notNullcheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('NOT_NULL_CHECK')))


# notNullColumnList = notNullcheckDf.rdd.flatMap(lambda x: x).collect()
# print("notNullColumnList ", notNullColumnList)

# # Check Duplicates Column List
# duplicateCheckDf = badRecordCheckDf.select("Column_Name").where((badRecordCheckDf.Process_Name == processName) & (badRecordCheckDf.Delta_Table_Name == tableName) & (upper(badRecordCheckDf.Validation_Type) == ('DUPLICATE_CHECK')))

# checkForDuplicateList = duplicateCheckDf.rdd.flatMap(lambda x: x).collect()
# print("checkForDuplicateList ", checkForDuplicateList)

# COMMAND ----------

# DBTITLE 1,Get Complutational Column to check for TypeCast
typeCastCheckDf = spark.sql("select * from kgsonedatadbedw.config_data_type_cast")

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

# double Column List
doublecheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('DOUBLE')))

doubleList = doublecheckDf.rdd.flatMap(lambda x: x).collect()
print("doubleList ", doubleList)

# Date Column List
datecheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('DATE')))

dateList = datecheckDf.rdd.flatMap(lambda x: x).collect()
print("dateList ", dateList)

# Timestamp Column List
timestampCheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('TIMESTAMP')))

timestampList = timestampCheckDf.rdd.flatMap(lambda x: x).collect()
print("timestampList ", timestampList)

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadbedw.raw_curr_"+ tableName)

# COMMAND ----------

# if tableName.lower() == 'talent_connect_daily_exit_report' and processName.lower() == 'it':
#     currentDf = spark.sql("select * from kgsonedatadb.raw_stg_"+processName + "_"+ tableName +" order by EMPLOYEENUMBER,RESIGNATIONINITIATEDDATE desc")

# if tableName.lower() == 'talent_connect_daily_transfer_report' and processName.lower() == 'it':
#     currentDf = spark.sql("select * from kgsonedatadb.raw_stg_"+processName + "_"+ tableName +" order by EMPLOYEE_NUMBER,TRANSFER_INITIATION_DATE desc")

# COMMAND ----------

# DBTITLE 1,Check if Primary Key Column is Null or Duplicate
# currentDf = currentDf.withColumn("Is_Primary_Key_Null",lit(""))
# currentDf = currentDf.withColumn("Is_Primary_Duplicate_Record",lit(""))


# for columnName in currentDf.columns:

#     if (columnName in primaryKeyColumnList):
#         print(columnName)

#         rankColumn = columnName+"_Rank"

#         # To find duplicates adding Row Number here 
#         w = Window.partitionBy(columnName).orderBy(columnName)
#         currentDf = currentDf.withColumn(rankColumn,row_number().over(w))

#         # Check for Primary Key Null records
#         currentDf = currentDf.withColumn("Is_Primary_Key_Null",when((col(columnName).isNull()) | (col(columnName) == '') | (col(columnName) == '-'),"PK is Null")\
#         .when(((col(columnName).isNotNull()) & (col(columnName) != '') & (col(columnName) != '-')) & (upper(currentDf.Is_Primary_Key_Null)  != "PK IS NULL"),"")\
#         .otherwise(currentDf.Is_Primary_Key_Null))


#         # Check for Duplicate Records
#         duplicateRecords = currentDf\
#                             .groupby(primaryKeyColumnList) \
#                             .count() \
#                             .where('count > 1') \
#                             .sort('count', ascending=False)

#         duplicateId = duplicateRecords.select(primaryKeyColumnList)

#         duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

#         currentDf = currentDf.withColumn("Is_Primary_Duplicate_Record",when((col(columnName).isin(duplicateIdList)) & ((col(columnName).isNotNull()) & (col(columnName) != '') & (col(columnName) != '-') & (col(rankColumn) >1)),"PK Duplicate Record")\
#             .otherwise(currentDf.Is_Primary_Duplicate_Record))

#         # For Null records adding NULL_<RandomeNumber> based on DateTime and for Duplicates DUP_<Key_Column>_<Random_Number
#         currentDf = currentDf.withColumn(columnName,when(upper(currentDf.Is_Primary_Key_Null) == 'PK IS NULL',concat(lit('NULL_'),lit(randomNumber(col(columnName))),lit('_'),lit(monotonically_increasing_id())))\
#             .when(((upper(currentDf.Is_Primary_Key_Null) != 'PK IS NULL') & (upper(currentDf.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD') & (col(rankColumn) >1)),concat(lit('DUP_'),lit(col(columnName)),lit("_"),lit(randomNumber(col(columnName))),lit('_'),lit(monotonically_increasing_id())))\
#             .otherwise(col(columnName)))
     

#         currentDf = currentDf.drop(rankColumn)                    



# COMMAND ----------

# DBTITLE 1,Check if Composite Key Column is Duplicate (ignore NULL)
# import re
# def atoi(text):
#     return int(text) if text.isdigit() else text
# def natural_keys(text):
#     return [ atoi(c) for c in re.split('(\d+)',text) ]


# print(compositeKeyColumnList)
# currentDf = currentDf.withColumn("Is_Composite_Duplicate_Record",lit(""))

# if len(compositeKeyColumnList) > 0:

#     rankColumn = columnName+"_Rank"

#     # To find duplicates adding Row Number here 
#     w = Window.partitionBy(compositeKeyColumnList).orderBy(compositeKeyColumnList)
#     currentDf = currentDf.withColumn(rankColumn,row_number().over(w))

#     # Check for Duplicate Records
#     duplicateRecords = currentDf\
#                         .groupby(compositeKeyColumnList) \
#                         .count() \
#                         .where('count > 1') \
#                         .sort('count', ascending=False)

#     # display(duplicateRecords)

#     duplicateId = duplicateRecords.select(compositeKeyColumnList)

#     duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()
#     # print(duplicateIdList)

#     currentDf = currentDf.withColumn("Is_Composite_Duplicate_Record",lit(""))

#     for columnName in currentDf.columns:
        
#         if (columnName in compositeKeyColumnList):

#             currentDf = currentDf.withColumn("Is_Composite_Duplicate_Record",when((col(columnName).isin(duplicateIdList)) & ((col(columnName).isNotNull()) & (col(columnName) != '') & (col(columnName) != '-') & (col(rankColumn) >1)),"CK Duplicate Record")\
#             .otherwise(currentDf.Is_Composite_Duplicate_Record))


#             # For for Duplicates DUP_<Key_Column>_<RandomeNumber> based on DateTime
            
#             currentDf = currentDf.withColumn(columnName,when(((upper(currentDf.Is_Composite_Duplicate_Record) == 'CK DUPLICATE RECORD') & (col(rankColumn) >1)),concat(lit('DUP_'),lit(col(columnName)),lit("_"),lit(randomNumber(col(columnName))),lit('_'),lit(monotonically_increasing_id())))\
#             .otherwise(col(columnName)))
    

#     currentDf = currentDf.drop(rankColumn)  

# dropCKColumnList = [ele for ele in currentDf.columns if((ele.startswith('Is_Composite_Key_Null')))]

# COMMAND ----------

# DBTITLE 1,Check if column value is null and move that to Bad record
# currentDf = currentDf.withColumn("Is_Column_Null",lit(""))

# for columnName in currentDf.columns:
#     if (columnName in notNullColumnList):
#         print(columnName)

#         currentDf = currentDf.withColumn("Is_Column_Null",when((col(columnName).isNull()) | (col(columnName) == ''),"Source is Null")\
#         .when(((col(columnName).isNotNull()) & (col(columnName) != '')) & (upper(currentDf.Is_Column_Null)  != "SOURCE IS NULL"),"")\
#         .otherwise(currentDf.Is_Column_Null))
        


# COMMAND ----------

# DBTITLE 1,Check if duplicate based on Key and move that to bad record
# currentDf = currentDf.withColumn("Is_Duplicate_Record",lit(""))

# for columnName in currentDf.columns:
#     if (columnName in checkForDuplicateList):

#         rankColumn = columnName+"_Rank"

#         # To find duplicates adding Row Number here 
#         w = Window.partitionBy(columnName).orderBy(columnName)
#         currentDf = currentDf.withColumn(rankColumn,row_number().over(w))

#         print(columnName)

#         duplicateRecords = currentDf\
#                             .groupby(checkForDuplicateList) \
#                             .count() \
#                             .where('count > 1') \
#                             .sort('count', ascending=False)

#         print("Duplicate Records")
#         # display(duplicateRecords)

#         duplicateId = duplicateRecords.select(checkForDuplicateList)
    
#         duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

#         currentDf = currentDf.withColumn("Is_Duplicate_Record",when(col(columnName).isin(duplicateIdList) & (col(rankColumn) >1) ,"Duplicate Record")\
#         .otherwise(currentDf.Is_Duplicate_Record))




# COMMAND ----------

# DBTITLE 1,Typecast issue move that to Bad record
from pyspark.sql.types import *
from pyspark.sql.functions import col, unix_timestamp, to_date



if len(dateList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in dateList):

            
            varDateConversion=f"DateConversion_{dateList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varDateConversion,lit(""))
    
            currentDf=currentDf.withColumn(varDateConversion,\
            when(to_date(currentDf[columnName], 'yyyy-MM-dd').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd/MM/yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'MM/dd/yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd-MM-yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd-MMM-yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd-MMMM-yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd MMMM, yyyy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'yyyy/MM/dd').isNotNull(),lit(""))\
            .when(to_date(currentDf[columnName], 'MMMM dd, yyyy').isNotNull(),lit(""))\
            .when((((currentDf[columnName].isNull()) | (currentDf[columnName] == "#N/A") | (currentDf[columnName] == "NA") | (currentDf[columnName] == "null") | (currentDf[columnName] == "0-Jan-00") | (currentDf[columnName] == "00 January 1900") | (upper(trim(currentDf[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(currentDf[columnName])) == "TO BE SCHEDULED") | (trim(currentDf[columnName]) == "") | (currentDf[columnName] == " ") | (trim(currentDf[columnName]) == "-") | (trim(currentDf[columnName]) == "_")) & currentDf[columnName].cast(DateType()).isNull()),lit("Source is Null"))\
            .otherwise(lit("Date invalid")))

            




if len(doubleList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in doubleList):

            
            
            varDoubleConversion=f"DoubleConversion_{doubleList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varDoubleConversion,lit(""))

            currentDf=currentDf.withColumn(columnName,when(trim(currentDf[columnName]) =='-',lit(None)).otherwise(currentDf[columnName]))

            #replacing ',' if it present in any amount related  columns to convert it to double
            currentDf=currentDf.withColumn(columnName,regexp_replace(currentDf[columnName],',',''))

            # replacing next line character
            currentDf = currentDf.withColumn(columnName, regexp_replace(currentDf[columnName], "[\\n\\r]", ''))

            currentDf= currentDf.withColumn(varDoubleConversion,when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") & (trim(currentDf[columnName]) != "NA") & (trim(currentDf[columnName]) != "null")) & currentDf[columnName].cast(DoubleType()).isNull()),lit("Double Type Cast Issue"))\
            .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") | (trim(currentDf[columnName]) == "NA") |(trim(currentDf[columnName]) == "null")) & currentDf[columnName].cast(DoubleType()).isNull()),lit("Source is Null"))\
            .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(DoubleType()).isNotNull()),lit(""))\
            .otherwise(currentDf[columnName]))

            
    


if len(intList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in intList):

            
           
            varIntConversion=f"IntConversion_{intList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varIntConversion,lit(""))  

            currentDf=currentDf.withColumn(columnName,when(trim(currentDf[columnName]) =='-',lit(None)).otherwise(currentDf[columnName]))
            
            currentDf = currentDf.withColumn(columnName,regexp_replace(col(columnName), "[^a-zA-Z0-9-]", ""))

            # replacing next line character
            currentDf = currentDf.withColumn(columnName, regexp_replace(currentDf[columnName], "[\\n\\r]", ''))
            
            currentDf = currentDf.withColumn(varIntConversion,when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "null") &  (trim(currentDf[columnName]) != "") & (trim(currentDf[columnName]) != "NA")) &  currentDf[columnName].cast(IntegerType()).isNull()),lit("Int Type Cast Issue"))\
            .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") | (trim(currentDf[columnName]) == "NA") | (trim(currentDf[columnName]) == "null")) & currentDf[columnName].cast(IntegerType()).isNull()),lit("Source is Null"))\
            .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(IntegerType()).isNotNull()),lit(""))\
            .otherwise(currentDf[columnName]))



if len(floatList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in floatList):

            
           
            varFloatConversion=f"FloatConversion_{floatList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varFloatConversion,lit(""))
            
            #update - values in float columns as null:
            currentDf=currentDf.withColumn(columnName,when(trim(currentDf[columnName]) =='-',lit(None)).otherwise(currentDf[columnName]))

            #replacing ',' if it present in any amount related  columns to convert it to float
            currentDf=currentDf.withColumn(columnName,regexp_replace(currentDf[columnName],',',''))

            # replacing next line character
            currentDf = currentDf.withColumn(columnName, regexp_replace(currentDf[columnName], "[\\n\\r]", ''))
            
            currentDf= currentDf.withColumn(varFloatConversion,when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") & (trim(currentDf[columnName]) != "NA") & (trim(currentDf[columnName]) != "null")) & currentDf[columnName].cast(FloatType()).isNull()),lit("Float Type Cast Issue"))\
            .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") | (trim(currentDf[columnName]) == "NA") | (trim(currentDf[columnName]) == "null")) & currentDf[columnName].cast(FloatType()).isNull()),lit("Source is Null"))\
            .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(FloatType()).isNotNull()),lit(""))\
            .otherwise(currentDf[columnName]))



if len(longList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in longList):

            
           
            varLongConversion=f"LongConversion_{longList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varLongConversion,lit(""))
            
            currentDf=currentDf.withColumn(columnName,when(trim(currentDf[columnName]) =='-',lit(None)).otherwise(currentDf[columnName]))

            currentDf = currentDf.withColumn(columnName,regexp_replace(col(columnName), "[^a-zA-Z0-9-]", ""))

            # replacing next line character
            currentDf = currentDf.withColumn(columnName, regexp_replace(currentDf[columnName], "[\\n\\r]", ''))
        
            currentDf = currentDf.withColumn(varLongConversion,when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") & (trim(currentDf[columnName]) != "NA") & (trim(currentDf[columnName]) != "null")) & currentDf[columnName].cast(LongType()).isNull()),lit("Long Type Cast Issue"))\
            .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") | (trim(currentDf[columnName]) == "NA") | (trim(currentDf[columnName]) == "null")) & currentDf[columnName].cast(LongType()).isNull()),lit("Source is Null"))\
            .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast(LongType()).isNotNull()),lit(""))\
            .otherwise(currentDf[columnName]))


if len(timestampList) > 0:

    for columnName in currentDf.columns:
    
        if (columnName in timestampList):

            
           
            varTimeStampConversion=f"TimeStampConversion_{timestampList.index(columnName) + 1}"
            currentDf = currentDf.withColumn(varTimeStampConversion,lit(""))

            currentDf=currentDf.withColumn(columnName,when(trim(currentDf[columnName]) =='-',lit(None)).otherwise(currentDf[columnName]))
        
            currentDf = currentDf.withColumn(varTimeStampConversion,when((((currentDf[columnName].isNotNull()) &  (trim(currentDf[columnName]) != "") & (trim(currentDf[columnName]) != "NA") & (trim(currentDf[columnName]) != "null")) & currentDf[columnName].cast("Timestamp").isNull()),lit("Timestamp Cast Issue"))\
            .when((((currentDf[columnName].isNull()) | (trim(currentDf[columnName]) == "") | (trim(currentDf[columnName]) == "NA") | (trim(currentDf[columnName]) == "null")) & currentDf[columnName].cast("Timestamp").isNull()),lit("Source is Null"))\
            .when((((currentDf[columnName].isNotNull())  & (trim(currentDf[columnName]) != "") ) & currentDf[columnName].cast("Timestamp").isNotNull()),lit(""))\
            .otherwise(currentDf[columnName]))
                        

dateconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("DateConversion")]
doubleconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("DoubleConversion")]
intconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("IntConversion")]
floatconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("FloatConversion")]
longconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("LongConversion")]
timestampconversion_columns = [col_name for col_name in currentDf.columns if col_name.startswith("TimeStampConversion")]
castconversion_columns=[item for sublist in[dateconversion_columns,doubleconversion_columns,intconversion_columns,floatconversion_columns,longconversion_columns,timestampconversion_columns] for item in sublist]


# # Sort the column names based on their numeric part
sorted_dateconversion_columns = sorted(dateconversion_columns, key=lambda x: int(x.split("_")[-1]))
sorted_doubleconversion_columns = sorted(doubleconversion_columns, key=lambda y: int(y.split("_")[-1]))
sorted_intconversion_columns = sorted(intconversion_columns, key=lambda x: int(x.split("_")[-1]))
sorted_floatconversion_columns = sorted(floatconversion_columns, key=lambda x: int(x.split("_")[-1]))
sorted_longconversion_columns = sorted(longconversion_columns, key=lambda x: int(x.split("_")[-1]))
sorted_timestampconversion_columns = sorted(timestampconversion_columns, key=lambda x: int(x.split("_")[-1]))


 
# Select the DataFrame columns dynamically
selected_columns = [col_name for col_name in currentDf.columns if col_name not in castconversion_columns]  + sorted_dateconversion_columns + sorted_doubleconversion_columns + sorted_intconversion_columns + sorted_floatconversion_columns + sorted_longconversion_columns + sorted_timestampconversion_columns 

currentDf = currentDf.select(*selected_columns)

dropDateConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('DateConversion')))]
print(dropDateConversionColumnList)
dropDoubleConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('DoubleConversion')))]
print(dropDoubleConversionColumnList)
dropIntConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('IntConversion')))]
print(dropIntConversionColumnList)
dropFloatConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('FloatConversion')))]
print(dropFloatConversionColumnList)
dropLongConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('LongConversion')))]
print(dropLongConversionColumnList)
dropTimeStampConversionColumnList = [ele for ele in currentDf.columns if((ele.startswith('TimeStampConversion')))]
print(dropTimeStampConversionColumnList)
print(castconversion_columns)

# COMMAND ----------

# # File Year is in INT in Bad Records for below
# if processName.lower() == "headcount_monthly" and tableName in ('contingent_worker','contingent_worker_resigned','employee_details','loaned_staff_from_ki','loaned_staff_resigned'):

#     columnList = ('File_Year')
    
#     for columnName in currentDf.columns:
#         if (columnName in columnList):
#             currentDf = currentDf.withColumn(columnName,currentDf[columnName].cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Creating View to segregate Good and Bad DF
currentDf.createOrReplaceTempView("CurrentDfTempView")

# COMMAND ----------

# DBTITLE 1,Filter Condition for GoodDf
str1=""
goodFilterCondition= '''select * from CurrentDfTempView where'''
# (upper(Is_Duplicate_Record) != "DUPLICATE RECORD") and (upper(Is_Primary_Key_Null) != "PK IS NULL") and (upper(Is_Primary_Duplicate_Record) != "PK DUPLICATE RECORD")  and (upper(Is_Column_Null) != "SOURCE NOT NULL") and (upper(Is_Primary_Duplicate_Record) != "PK DUPLICATE RECORD") and (upper(Is_Composite_Duplicate_Record) != "CK DUPLICATE RECORD")'''


if len(dropDateConversionColumnList) > 0:   
    
        for columnName in dropDateConversionColumnList:
            str2 = ' (upper('+columnName+') != "DATE INVALID") and'
            str1 = str1+str2


if len(dropDoubleConversionColumnList) > 0:

        for columnName in dropDoubleConversionColumnList:
            str2 = ' (upper('+columnName+') != "DOUBLE TYPE CAST ISSUE") and'
            str1 = str1+str2

if len(dropIntConversionColumnList) > 0:   
    
        for columnName in dropIntConversionColumnList:
            str2 = ' (upper('+columnName+') != "INT TYPE CAST ISSUE") and'
            str1 = str1+str2

if len(dropFloatConversionColumnList) > 0:

        for columnName in dropFloatConversionColumnList:
            str2 = ' (upper('+columnName+') != "FLOAT TYPE CAST ISSUE") and'
            str1 = str1+str2


if len(dropLongConversionColumnList) > 0:   
    
        for columnName in dropLongConversionColumnList:
            str2 = ' (upper('+columnName+') != "LONG TYPE CAST ISSUE") and'
            str1 = str1+str2


if len(dropTimeStampConversionColumnList) > 0:

        for columnName in dropTimeStampConversionColumnList:
            str2 = ' (upper('+columnName+') != "TIMESTAMP CAST ISSUE") and'
            str1 = str1+str2


goodFilterCondition=goodFilterCondition+str1
goodFilterCondition=goodFilterCondition[:-4]
print(goodFilterCondition)

goodDf = spark.sql(goodFilterCondition)

display(goodDf)



# COMMAND ----------

# DBTITLE 1,Filter Condition for BadDf
str1=""


badFilterCondition= '''select * from CurrentDfTempView where'''
# (upper(Is_Duplicate_Record) = "DUPLICATE RECORD") or (upper(Is_Primary_Key_Null) = "PK IS NULL") or (upper(Is_Primary_Duplicate_Record) = "PK DUPLICATE RECORD")  or (upper(Is_Column_Null) = "SOURCE NOT NULL") or (upper(Is_Primary_Duplicate_Record) = "PK DUPLICATE RECORD") or (upper(Is_Composite_Duplicate_Record) = "CK DUPLICATE RECORD")'''


if len(dropDateConversionColumnList) > 0:   
    
        for columnName in dropDateConversionColumnList:
            str2 = ' (upper('+columnName+') = "DATE INVALID") or'
            str1 = str1+str2


if len(dropDoubleConversionColumnList) > 0:

        for columnName in dropDoubleConversionColumnList:
            str2 = ' (upper('+columnName+') = "DOUBLE TYPE CAST ISSUE") or'
            str1 = str1+str2

if len(dropIntConversionColumnList) > 0:   
    
        for columnName in dropIntConversionColumnList:
            str2 = ' (upper('+columnName+') = "INT TYPE CAST ISSUE") or'
            str1 = str1+str2


if len(dropFloatConversionColumnList) > 0:

        for columnName in dropFloatConversionColumnList:
            str2 = ' (upper('+columnName+') = "FLOAT TYPE CAST ISSUE") or'
            str1 = str1+str2

if len(dropLongConversionColumnList) > 0:   
    
        for columnName in dropLongConversionColumnList:
            str2 = ' (upper('+columnName+') = "LONG TYPE CAST ISSUE") or'
            str1 = str1+str2


if len(dropTimeStampConversionColumnList) > 0:

        for columnName in dropTimeStampConversionColumnList:
            str2 = ' (upper('+columnName+') = "TIMESTAMP CAST ISSUE") or'
            str1 = str1+str2

badFilterCondition=badFilterCondition+str1
badFilterCondition=badFilterCondition[:-3]
print(badFilterCondition)

badDf = spark.sql(badFilterCondition)

display(badDf)

# COMMAND ----------

# if processName.lower() == "headcount_monthly":
#     columnList = ('File_Year')
    
#     for columnName in goodDf.columns:
#         if (columnName in columnList):
#             goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(IntegerType()))

# COMMAND ----------

wave1List = ['bgv','employee_engagement','global_mobility','headcount','headcount_monthly','jml','talent_acquisition']

# Run for Wave1 process
if (processName in wave1List) and (tableName!='attrition_data'):
    print("inside")
    from pyspark.sql.types import *
    from pyspark.sql.functions import col, unix_timestamp, to_date


    print("Int: ",intList)
    print("Date: ",dateList)
    print("Float: ",floatList)
    print("Long: ",longList)
    print("TimeStamp: ",timestampList)

    # currentDf = goodDf

    for columnName in goodDf.columns:

        if (columnName in intList):
            # goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(IntegerType()))
            goodDf = goodDf.withColumn(columnName,when((((goodDf[columnName].isNotNull())  & (trim(goodDf[columnName]) != "") ) & goodDf[columnName].cast(IntegerType()).isNotNull()),goodDf[columnName].cast(IntegerType()))\
            .otherwise(goodDf[columnName]))
            

        if (columnName in dateList):
            goodDf=goodDf.withColumn(columnName,regexp_replace(regexp_replace(goodDf[columnName],'th ','-'),'\'','-'))

            goodDf=goodDf.withColumn(columnName,\
            when(to_date(goodDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MM-yy'),'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd MM yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd MM yy'),'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MMM-yy'),'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd-MMMM-yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MMMM-yy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd MMM yy'),'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd/MM/yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd/MM/yy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'MM/dd/yy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'MM/dd/yy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'yyyy-MM-dd').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'yyyy-MM-dd'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd/MM/yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd/MM/yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'MM/dd/yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'MM/dd/yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd-MM-yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd-MMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MMM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd-MMMM-yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd-MMMM-yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd MMMM,yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd MMMM,yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'yyyy/MM/dd').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'yyyy/MM/dd'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'MMMM d,yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'MMMM d,yyyy'), 'yyyy-MM-dd'))\
            .when(to_date(goodDf[columnName], 'dd.mm.yyyy').isNotNull(),from_unixtime(unix_timestamp(goodDf[columnName], 'dd.mm.yyyy'), 'yyyy-MM-dd'))\
            .otherwise(goodDf[columnName]))
            # display(goodDf)

        

        if (columnName in floatList):
            # goodDf= goodDf.withColumn(columnName,goodDf[columnName].cast(FloatType()))
            goodDf= goodDf.withColumn(columnName,when((((goodDf[columnName].isNotNull())  & (trim(goodDf[columnName]) != "") ) & goodDf[columnName].cast(FloatType()).isNotNull()),goodDf[columnName].cast(FloatType()))\
            .otherwise(goodDf[columnName]))

    # Added during LND history load 5/19/2023

        if (columnName in doubleList):
            # goodDf= goodDf.withColumn(columnName,goodDf[columnName].cast(FloatType()))
            goodDf= goodDf.withColumn(columnName,when((((goodDf[columnName].isNotNull())  & (trim(goodDf[columnName]) != "") ) & goodDf[columnName].cast(DoubleType()).isNotNull()),goodDf[columnName].cast(DoubleType()))\
            .otherwise(goodDf[columnName]))



        if (columnName in longList):
            # goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(LongType()))
            goodDf = goodDf.withColumn(columnName,when((((goodDf[columnName].isNotNull())  & (trim(goodDf[columnName]) != "") ) & goodDf[columnName].cast(LongType()).isNotNull()),goodDf[columnName].cast(LongType()))\
            .otherwise(goodDf[columnName]))



        if (columnName in timestampList):
            goodDf = goodDf.withColumn(columnName,when((((goodDf[columnName].isNotNull())  & (trim(goodDf[columnName]) != "") ) & goodDf[columnName].cast("Timestamp").isNotNull()),goodDf[columnName].cast("Timestamp"))\
            .otherwise(goodDf[columnName]))

    dropColumns = ["Is_Primary_Key_Null","Is_Primary_Duplicate_Record","Is_Column_Null","Is_Duplicate_Record","Is_Composite_Duplicate_Record"]

    # Adding both list into one
    finalDropColumnList = dropColumns + castconversion_columns

    goodDf = goodDf.drop(*finalDropColumnList)

# COMMAND ----------

# DBTITLE 1,Column Typecast
wave1List = ['bgv','employee_engagement','global_mobility','headcount','headcount_monthly','jml','talent_acquisition']

# Run if not in Wave1 process
if (processName not in wave1List) or (tableName=='attrition_data'):
    from pyspark.sql.types import *
    from pyspark.sql.functions import col, unix_timestamp, to_date


    print("Int: ",intList)
    print("Date: ",dateList)
    print("Float: ",floatList)
    print("Double: ",doubleList)
    print("Long: ",longList)
    print("TimeStamp: ",timestampList)
    
    # currentDf = goodDf

    for columnName in goodDf.columns:

        if (columnName in intList):
            goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(IntegerType()))

            

        if (columnName in dateList):
            # print("Date Column: ",columnName)
            # goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(DateType()))
            
            goodDf = goodDf.withColumn(columnName, when(((col(columnName) == " ") | (col(columnName) == "0-Jan-00") | (col(columnName) == "00 January 1900") | (col(columnName).isNull()) | (upper(trim(currentDf[columnName])) == "DATA NOT AVAILABLE") | (upper(trim(currentDf[columnName])) == "TO BE SCHEDULED") | (trim(col(columnName)) == "") | (trim(col(columnName)) == "-") | (trim(col(columnName)) == "_") | (col(columnName) == "#N/A") | (col(columnName) == "NA") |(col(columnName) == "null")),"1900-01-01").otherwise(col(columnName)))

            # display(goodDf)
                     
            goodDf = goodDf.withColumn(columnName,changeDateFormat(col(columnName)))
            goodDf = goodDf.withColumn(columnName, when(col(columnName) == to_date(lit("1900-01-01")), None).otherwise(col(columnName)))


        

        if (columnName in floatList):
            goodDf= goodDf.withColumn(columnName,goodDf[columnName].cast(FloatType()))


        if (columnName in longList):
            goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(LongType()))


        if (columnName in timestampList):
            goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(TimestampType()))

        if (columnName in doubleList):
            goodDf = goodDf.withColumn(columnName,goodDf[columnName].cast(DoubleType()))


    dropColumns = ["Is_Primary_Key_Null","Is_Primary_Duplicate_Record","Is_Column_Null","Is_Duplicate_Record","Is_Composite_Duplicate_Record"]
    
    # Adding both list into one
    finalDropColumnList = dropColumns + castconversion_columns

    goodDf = goodDf.drop(*finalDropColumnList)

# COMMAND ----------

goodDf=goodDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

# DBTITLE 1,Create Database if not Exist
databaseName = 'kgsonedatadbedw_badrecords'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

print(bad_filepath_url)

# COMMAND ----------

badDfCount=badDf.count()
print(badDfCount)

# COMMAND ----------

# DBTITLE 1,Write Bad records to Trusted Bad Database tables
#Write bad dataframe if its not empty
if ~(badDf.rdd.isEmpty()):
    if (layerName.upper() != "RAW_ADHOC" and tableName.upper() != "GLMS_KVA_DETAILS"):
        badDf.write \
        .mode("append") \
        .format("delta") \
        .option("mergeSchema","true") \
        .option("path",bad_filepath_url+processName+"/"+tableName+"_bad") \
        .option("compression","snappy") \
        .saveAsTable("kgsonedatadbedw_badrecords.trusted_hist_"+tableName+"_bad")
    

# COMMAND ----------

# DBTITLE 1,If Raw Layer load data to Raw_Curr and Raw_Hist
if(layerName.upper() == "RAW" or layerName.upper() == "RAW_ADHOC"):
    goodDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True") \
    .option("path",edw_raw_curr_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadbedw.raw_curr_"+ tableName)   



# COMMAND ----------

# DBTITLE 1,If Trusted Layer load data to Trusted and Trusted_Hist
if(layerName.upper() == "TRUSTED"):
    print("inside trusted")

    # Load into Trusted Current Table
    goodDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True") \
    .option("path",edw_trusted_curr_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadbedw.trusted_"+tableName)

    # Load into Trusted History Table
    goodDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",edw_trusted_hist_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadbedw.trusted_hist_"+ tableName)


    dbutils.notebook.run("/Workspace/kgsonedataedw/trusted/Delta_to_SQL_with_Select",8000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# if (layerName.upper() == "RAW"):
#     dbutils.notebook.run("/kgsonedata/raw/rawstg_to_rawcurr_clean_data",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# if (layerName.upper() == "RAW_ADHOC"):
#     dbutils.notebook.run("/kgsonedata/raw/adhoc_raw_to_trusted_clean_data",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

