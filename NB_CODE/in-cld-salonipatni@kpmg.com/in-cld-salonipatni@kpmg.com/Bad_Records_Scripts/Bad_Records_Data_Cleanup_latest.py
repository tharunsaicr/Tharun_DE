# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# primaryKeyColumn - validation type PK_Check , null and duplicate check
# NotNullColumn - validation type Not_Null_Check

primaryKeyColumnList = ['id']
notNullColumnList = ['middlename','EntryTime']
checkForDuplicateList = ['firstname']
intList = ['id']
longList = ['salary']
floatList = ['rating']
dateList = ['dob']
timestampList = ['EntryTime']

# COMMAND ----------

# DBTITLE 1,Test DataFrame
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","12-01-2001","36636","4.5",3000000,'2022-11-09T13:16:40.757+00:00'),
    ("James","","Smith","12-01-2001","36636","2.6",3000000,'11-Feb-2020'),
    ("Michael","Rose","","12-Jan-2001","40288","4",40000,''),
    ("Robert","","Williams","12-01-2001","42114","6.8",400000,'11-12-2021'),
    ("Maria","Anne","Jones","12-01-2001","id1","3",4000000,''),
    ("Martin","Ben","Jones","12-01-2001","5","3",4000000,'2022-11-09T13:16:40.757+00:00'),
    ("Jen","Mary","Brown","","","no rating","No salary",'2025-10-19')
  ]
data_date = [("2023-03-21"),("21/03/2023"),("03/21/2023"),"21-Mar-2023","21-March-2023","21 March, 2023"]
# data2 = [("Jen","Mary","Brown","","","F",-1)]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("dob",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("rating", StringType(), True), \
    StructField("salary", StringType(), True), \
    StructField("EntryTime", StringType(), True) \
])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check if Primary Key Column is Null or Duplicate
df = df.withColumn("Is_Primary_Key_Null",lit(""))
df = df.withColumn("Is_Primary_Duplicate_Record",lit(""))

for columnName in df.columns:
    if (columnName in primaryKeyColumnList):
        print(columnName)

        # Check for Primary Key Null records

        df = df.withColumn("Is_Primary_Key_Null",when((col(columnName).isNull()) | (col(columnName) == ''),"PK is Null")\
        .when(((col(columnName).isNotNull()) & (col(columnName) != '')) & (upper(df.Is_Primary_Key_Null)  != "PK IS NULL"),"PK is not Null")\
        .otherwise(df.Is_Primary_Key_Null))


        # Check for Duplicate Records
        duplicateRecords = df\
                            .groupby(primaryKeyColumnList) \
                            .count() \
                            .where('count > 1') \
                            .sort('count', ascending=False)

        duplicateId = duplicateRecords.select(primaryKeyColumnList)
        
        duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

        df = df.withColumn("Is_Primary_Duplicate_Record",when(col(columnName).isin(duplicateIdList),"PK Duplicate Record")\
        .otherwise(df.Is_Primary_Duplicate_Record))
        
        
goodDf = df.filter((upper(df.Is_Primary_Key_Null) == 'PK IS NOT NULL') & (~(upper(df.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD')))
                             
badDf = df.filter((upper(df.Is_Primary_Key_Null) == 'PK IS NULL') |(upper(df.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD') )         

display(goodDf)
display(badDf)

# COMMAND ----------

# DBTITLE 1,Check if column value is null and move that to Bad record
df = df.withColumn("Is_Column_Null",lit(""))

for columnName in df.columns:
    if (columnName in notNullColumnList):
        print(columnName)

        df = df.withColumn("Is_Column_Null",when((col(columnName).isNull()) | (col(columnName) == ''),"Source is Null")\
        .when(((col(columnName).isNotNull()) & (col(columnName) != '')) & (upper(df.Is_Column_Null)  != "SOURCE IS NULL"),"Source is not Null")\
        .otherwise(df.Is_Column_Null))
        
        
goodDf = df.filter(upper(df.Is_Column_Null) == 'SOURCE IS NOT NULL')
                             
badDf = df.filter(upper(df.Is_Column_Null) == 'SOURCE IS NULL')        

display(goodDf)
display(badDf)

# COMMAND ----------

# DBTITLE 1,Check if duplicate based on Key and move that to bad record
df = df.withColumn("Is_Duplicate_Record",lit(""))

for columnName in df.columns:
    if (columnName in checkForDuplicateList):
        print(columnName)

        duplicateRecords = df\
                            .groupby(checkForDuplicateList) \
                            .count() \
                            .where('count > 1') \
                            .sort('count', ascending=False)

    duplicateId = duplicateRecords.select(checkForDuplicateList)
    
    duplicateIdList = duplicateId.rdd.flatMap(lambda x: x).collect()

    df = df.withColumn("Is_Duplicate_Record",when(col(columnName).isin(duplicateIdList),"Duplicate Record")\
    .otherwise(df.Is_Duplicate_Record))


goodDf = df.filter(~(upper(df.Is_Duplicate_Record) == 'DUPLICATE RECORD'))
                            
badDf = df.filter(upper(df.Is_Duplicate_Record) == 'DUPLICATE RECORD')  

display(goodDf)
display(badDf)


# COMMAND ----------

# DBTITLE 1,Typecast issue move that to Bad record
from pyspark.sql.types import *
from pyspark.sql.functions import col, unix_timestamp, to_date

df = df.withColumn("IntConversion",lit(""))
df = df.withColumn("DateConversion",lit(""))
df = df.withColumn("FloatConversion",lit(""))
df = df.withColumn("LongConversion",lit(""))
df = df.withColumn("TimestampConversion",lit(""))


for columnName in df.columns:

    if (columnName in intList):
        print("Int: ",columnName)

        # df = df.withColumn("IntConverstion",df[columnName].cast(IntegerType()))
        
        df = df.withColumn("IntConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & df[columnName].cast(IntegerType()).isNull()),lit("Int Type Cast Issue"))\
        .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & df[columnName].cast(IntegerType()).isNull()),lit("Source is Null"))\
        .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & df[columnName].cast(IntegerType()).isNotNull()),lit("Good record"))\
        .otherwise(df[columnName]))

    if (columnName in dateList):
        print("Date: ",columnName)

        # df = df.withColumn("DateConverstion",to_date(unix_timestamp(col(columnName), 'dd-MM-yyyy').cast("timestamp")))

        # df = df.withColumn("DateConverstion",df[columnName].cast(DateType()))

        df=df.withColumn("DateConversion",\
        when(to_date(df[columnName], 'yyyy-MM-dd').isNotNull(),to_date(df[columnName], 'yyyy-MM-dd'))\
        .when(to_date(df[columnName], 'dd/MM/yyyy').isNotNull(),to_date(df[columnName], 'dd/MM/yyyy'))\
        .when(to_date(df[columnName], 'MM/dd/yyyy').isNotNull(),to_date(df[columnName], 'MM/dd/yyyy'))\
        .when(to_date(df[columnName], 'dd-MM-yyyy').isNotNull(),to_date(df[columnName], 'dd-MM-yyyy'))\
        .when(to_date(df[columnName], 'dd-MMM-yyyy').isNotNull(),to_date(df[columnName], 'dd-MMM-yyyy'))\
        .when(to_date(df[columnName], 'dd-MMMM-yyyy').isNotNull(),to_date(df[columnName], 'dd-MMMM-yyyy'))\
        .when(to_date(df[columnName], 'dd MMMM, yyyy').isNotNull(),to_date(df[columnName], 'dd MMMM,yyyy'))\
        .when(to_date(df[columnName], 'dd-MM-yy').isNotNull(),to_date(df[columnName], 'dd-MM-yy'))\
        .when(to_date(df[columnName], 'dd MMM yy').isNotNull(),to_date(df[columnName], 'dd MMM yy'))\
        .when(to_date(df[columnName], 'yyyy/MM/dd').isNotNull(),to_date(df[columnName], 'yyyy/MM/dd'))\
        .otherwise(lit("Date invalid"))
    )

       

    if (columnName in floatList):
        print("Float: ",columnName)

        # df = df.withColumn("FloatConversion",df[columnName].cast(FloatType()))
        df = df.withColumn("FloatConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & df[columnName].cast(FloatType()).isNull()),lit("Float Type Cast Issue"))\
        .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & df[columnName].cast(FloatType()).isNull()),lit("Source is Null"))\
        .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & df[columnName].cast(FloatType()).isNotNull()),lit("Good record"))\
        .otherwise(df[columnName]))



    if (columnName in longList):
        print("Long: ",columnName)

        df = df.withColumn("LongConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & df[columnName].cast(LongType()).isNull()),lit("Long Type Cast Issue"))\
        .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & df[columnName].cast(LongType()).isNull()),lit("Source is Null"))\
        .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & df[columnName].cast(LongType()).isNotNull()),lit("Good record"))\
        .otherwise(df[columnName]))



    if (columnName in timestampList):
        print("Long: ",columnName)

        # df = df.withColumn("TimeStampConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & (to_timestamp(df[columnName], "yyyy-MM-dd HH:mm:ss").cast("Timestamp").isNull())),lit("Timestamp Cast Issue"))\
        # .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & (to_timestamp(df[columnName], "yyyy-MM-dd HH:mm:ss").cast("Timestamp").isNull())),lit("Source is Null"))\
        # .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & (to_timestamp(df[columnName], "yyyy-MM-dd HH:mm:ss").cast("Timestamp").isNotNull())),lit("Good record"))\
        # .otherwise(df[columnName]))

        df = df.withColumn("TimeStampConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & df[columnName].cast("Timestamp").isNull()),lit("Timestamp Cast Issue"))\
        .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & df[columnName].cast("Timestamp").isNull()),lit("Source is Null"))\
        .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & df[columnName].cast("Timestamp").isNotNull()),lit("Good record"))\
        .otherwise(df[columnName]))
 
        
# df = df.withColumn("TypeMismatch", when())

display(df)

# COMMAND ----------

goodDf = df.filter((~(upper(df.Is_Duplicate_Record) == 'DUPLICATE RECORD')) & (upper(df.Is_Primary_Key_Null) == 'PK IS NOT NULL') & (~(upper(df.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD')) & (upper(df.IntConversion) == 'GOOD RECORD') & (upper(df.FloatConversion) == 'GOOD RECORD') & (upper(df.LongConversion) == 'GOOD RECORD')& (upper(df.TimeStampConversion) == 'GOOD RECORD') & (~(upper(df.IntConversion) == 'DATE INVALID')) & (upper(df.Is_Column_Null) == 'SOURCE IS NOT NULL'))
                            
badDf = df.filter((upper(df.Is_Duplicate_Record) == 'DUPLICATE RECORD') | (upper(df.Is_Primary_Key_Null) == 'PK IS NULL') | (upper(df.Is_Primary_Duplicate_Record) == 'PK DUPLICATE RECORD') | (upper(df.IntConversion) == 'INT TYPE CAST ISSUE') | (upper(df.FloatConversion) == 'FLOAT TYPE CAST ISSUE') | (upper(df.LongConversion) == 'LONG TYPE CAST ISSUE')| (upper(df.TimeStampConversion) == 'TIMESTAMP TYPE CAST ISSUE') | (upper(df.IntConversion) == 'DATE INVALID') | (upper(df.Is_Column_Null) == 'SOURCE NOT NULL') )

# COMMAND ----------

display(goodDf)
display(badDf)