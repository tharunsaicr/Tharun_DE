# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bad_record_check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_data_type_cast

# COMMAND ----------

# primaryKeyColumn - validation type PK_Check , null and duplicate check
# NotNullColumn - validation type Not_Null_Check
# intList = []
# longList = []
# floatList = []
# dateList = []
# timestampList = []


# COMMAND ----------

keyColumn = ['id','lastname','middlename']
primaryKeyColumn = ['id','firstname']
intList = ['id']
dateList = ['dob']
floatList = ['salary']
doubleList = []
timestampList = []



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","12-01-2001","36636","M",3000),
    ("James","","Smith","12-01-2001","36636","M",3000),
    ("Michael","Rose","","12-Jan-2001","40288","M",4000),
    ("Robert","","Williams","12-01-2001","42114","M",4000),
    ("Maria","Anne","Jones","12-01-2001","id1","F",4000),
    ("Jen","Mary","Brown","","","F","No salary")
  ]
data_date = [("2023-03-21"),("21/03/2023"),("03/21/2023"),"21-Mar-2023","21-March-2023","21 March, 2023"]
# data2 = [("Jen","Mary","Brown","","","F",-1)]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("dob",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", StringType(), True) \
])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# badDf = df.filter((df.id.isNull())| (df.id == ''))
# display(badDf)

# goodDf = df.filter(~((df.id.isNull())| (df.id == '')))
# display(goodDf)

# COMMAND ----------

# DBTITLE 1,Check if column value is null and move that to Bad record
# df = df.withColumn("Data_Record_Type",lit(""))

# for columnName in df.columns:
#     if (columnName in keyColumn):
#         print(columnName)

#         df = df.withColumn("Data_Record_Type",when((col(columnName).isNull()) | (col(columnName) == ''),"Bad Record")\
#         .when(((col(columnName).isNotNull()) | (col(columnName) != '')) & (upper(df.Data_Record_Type)  != "BAD RECORD"),"Good Record")\
#         .otherwise(df.Data_Record_Type))
        
        
# goodDf = df.filter(upper(df.Data_Record_Type) == 'GOOD RECORD')
                             
# badDf = df.filter(upper(df.Data_Record_Type) == 'BAD RECORD')        

# display(goodDf)
# display(badDf)

# COMMAND ----------

df = df.withColumn("Is_Primary_Key_Null",lit(""))

for columnName in df.columns:
    if (columnName in keyColumn):
        print(columnName)

        df = df.withColumn("Is_Primary_Key_Null",when((col(columnName).isNull()) | (col(columnName) == ''),"PK is Null")\
        .when(((col(columnName).isNotNull()) & (col(columnName) != '')) & (upper(df.Is_Primary_Key_Null)  != "PK IS NULL"),"PK is not Null")\
        .otherwise(df.Is_Primary_Key_Null))
        
        
goodDf = df.filter(upper(df.Is_Primary_Key_Null) == 'PK IS NOT NULL')
                             
badDf = df.filter(upper(df.Is_Primary_Key_Null) == 'PK IS NULL')        

display(goodDf)
display(badDf)

# COMMAND ----------

# DBTITLE 1,Check if duplicate based on Key and move that to bad record
df = df.withColumn("Is_Duplicate_Record",lit(""))

for columnName in df.columns:
    if (columnName in primaryKeyColumn):
        print(columnName)

        duplicateRecords = df\
                            .groupby(primaryKeyColumn) \
                            .count() \
                            .where('count > 1') \
                            .sort('count', ascending=False)

    duplicateId = duplicateRecords.select(primaryKeyColumn)
    
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



    if (columnName in doubleList):
        print("Float: ",columnName)

        df = df.withColumn("DoubleConversion",when((((df[columnName].isNotNull()) &  (trim(df[columnName]) != "") ) & df[columnName].cast(DoubleType()).isNull()),lit("Double Type Cast Issue"))\
        .when((((df[columnName].isNull()) | (trim(df[columnName]) == "") ) & df[columnName].cast(DoubleType()).isNull()),lit("Source is Null"))\
        .when((((df[columnName].isNotNull())  & (trim(df[columnName]) != "") ) & df[columnName].cast(DoubleType()).isNotNull()),lit("Good record"))\
        .otherwise(df[columnName]))
 
        
# df = df.withColumn("TypeMismatch", when())

display(df)

# COMMAND ----------

# display(df.select("firstname").where(df['id']!=""))

df = df.withColumn("TestColumn",when(((df.id.isNull())|(df['id'] == "")) & (df.id.cast(IntegerType()).isNull()),lit("Source is null")).when(((df.id.isNotNull()) & (df['id'] != "")) & (df.id.cast(IntegerType()).isNull()),lit("Id value is not int")).otherwise("Good record"))
display(df)

# COMMAND ----------



# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

data_date = [("ABC","21-03-23"),("ABC","2023-03-21"),("MNO","21-March-2023"),("DEF","21/03/2023"),("GHI","03/21/2023"),("JKL","21-Mar-2023"),("STU","2023-03-21"),("PQR","21 March, 2023"),("TUV","12 Dec 2011"),("QWV","05-12-2022")]


# data2 = [("Jen","Mary","Brown","","","F",-1)]

schema = StructType([StructField("lastname",StringType(),True), \
StructField("date_col",StringType(),True)])
 
df_date = spark.createDataFrame(data=data_date,schema=schema)
df_date.printSchema()
display(df_date)

# df_date = df_date.withColumn("DateConversion",to_date(unix_timestamp(df_date['date_col'], 'yyyy-MM-dd').cast("timestamp")))


# 2023-05-01
# 12 Dec 2011
# 17-AUG-2023
# 2023/02/17
# 05-12-2022


df_date=df_date.withColumn("DateConversion",\
when(to_date(df_date['date_col'], 'yyyy-MM-dd').isNotNull(),to_date(df_date['date_col'], 'yyyy-MM-dd'))\
.when(to_date(df_date['date_col'], 'dd/MM/yyyy').isNotNull(),to_date(df_date['date_col'], 'dd/MM/yyyy'))\
.when(to_date(df_date['date_col'], 'MM/dd/yyyy').isNotNull(),to_date(df_date['date_col'], 'MM/dd/yyyy'))\
.when(to_date(df_date['date_col'], 'dd-MM-yyyy').isNotNull(),to_date(df_date['date_col'], 'dd-MM-yyyy'))\
.when(to_date(df_date['date_col'], 'dd-MMM-yyyy').isNotNull(),to_date(df_date['date_col'], 'dd-MMM-yyyy'))\
.when(to_date(df_date['date_col'], 'dd-MMMM-yyyy').isNotNull(),to_date(df_date['date_col'], 'dd-MMMM-yyyy'))\
.when(to_date(df_date['date_col'], 'dd MMMM, yyyy').isNotNull(),to_date(df_date['date_col'], 'dd MMMM,yyyy'))\
.when(to_date(df_date['date_col'], 'dd-MM-yy').isNotNull(),to_date(df_date['date_col'], 'dd-MM-yy'))\
.when(to_date(df_date['date_col'], 'dd MMM yy').isNotNull(),to_date(df_date['date_col'], 'dd MMM yy'))\
.when(to_date(df_date['date_col'], 'yyyy/MM/dd').isNotNull(),to_date(df_date['date_col'], 'yyyy/MM/dd'))\

)

# df_date = df_date.withColumn("date",changeDateFormat(col('date_col')))
display(df_date)

# COMMAND ----------

from pyspark.sql.functions import to_date, date_format
# create a sample dataframe
# df = spark.createDataFrame([("1", "21-March-2023"),("ABC","2023-03-21")], ["id", "date_string"])
df = spark.createDataFrame([("ABC","2023-03-21"),("MNO","21-March-2023"),("DEF","21/03/2023"),("GHI","03/21/2023"),("JKL","21-Mar-2023"),("STU","2023-03-21")], ["id", "date_string"])
# convert the date format
df = df.withColumn("date",changeDateFormat(col('date_string')))
# .drop("date")
# show the result
df.show()

# COMMAND ----------

# 2023-05-01
# 12 Dec 2011
# 17-AUG-2023
# 2023/02/17
# 05-12-2022


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from 
# MAGIC
# MAGIC select * from 
# MAGIC --kgsonedatadb.trusted_hist_jml_uk_leaver
# MAGIC --kgsonedatadb.trusted_hist_jml_uk_mover
# MAGIC
# MAGIC kgsonedatadb.trusted_hist_jml_uk_joiner

# COMMAND ----------

