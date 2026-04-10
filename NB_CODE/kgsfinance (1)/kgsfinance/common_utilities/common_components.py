# Databricks notebook source
#Type casting
#Handling column headers - replace ' ' & replace special characters
#Handling empty columns
#Handling nulls for date columns while loading into raw
# Date functions
# Remove any extra character/spaces e.g. '  1234'
# Null check in Key Columns (Employee_Number, Candidate_ID, Cost_Center,BU,Client_Geography etc.)
# Column count check b/w file and table before Df is loaded
# Row count check b/w file and table after Df is loaded




# With Dates need to on what value to be filled if it is empty 

# COMMAND ----------

from dateutil.parser import parse
from pyspark.sql.functions import col,when,lit,date_sub,to_date,count
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import trim, col
from pyspark.sql import functions as f

# COMMAND ----------

# DBTITLE 1,Replace Special Character/Space in ColumnName with '_'
 lst_rchar=[' ','/','&','(',')','.',"'",';','{','}','\n','\t','=','%',':','+','$','-']
def replacechar(colvalue):
    for rval in lst_rchar:
        colvalue=colvalue.replace(rval, '_')
    return colvalue

# COMMAND ----------

# DBTITLE 1,Cast columns to type String
def colcaststring(df,collist):
    for x in collist:
        df = df.withColumn(x, df[x].cast(StringType()))
    #df = df.select([col(x).cast("String") for x in df.columns])
    return df

# COMMAND ----------

# def leadtrailremove(df1):
#     df2 = df1.select([f.trim(col(c)).alias(c) for c in df1.columns])
#     return df2

# COMMAND ----------

# DBTITLE 1,Change Any Date Format to YYYY-MM-DD
from dateutil.parser import parse
from pyspark.sql.types import *

changeDateFormat = udf(lambda coldate: parse(coldate), DateType())

# COMMAND ----------

# DBTITLE 1,Detect Null Column i.e. Column with no values
import pyspark.sql.functions as F

def get_null_column_names(df):
   column_names = []

   for col_name in df.columns:

       min_ = df.select(F.min(col_name)).first()[0]
       max_ = df.select(F.max(col_name)).first()[0]

       if min_ is None and max_ is None:
           column_names.append(col_name)

   return column_names

# COMMAND ----------

# To find columns with no value

def get_null_column_names1(df):
    nullColumns = []

    for k in currentDf.columns:
        if currentDf.agg(countDistinct(currentDf[k])).take(1)[0][0] == 0:
            nullColumns.append(k)
            
    return nullColumns

# COMMAND ----------

# Do not un-comment until function is created
# from pyspark.sql import functions as f
def leadtrailremove(df1):
    for colname in df1.columns:
        df1 = df1.withColumn(colname, f.trim(f.col(colname)))
    
    return df1

# COMMAND ----------

# Do not un-comment until function is created 
# Replacing #N/A to Null value for further computation
# currentmonthDF = currentmonthDF.withColumn("Status", \
#         when(col("Status")=="#N/A" ,None) \
#            .otherwise(col("Status")))

# display(currentmonthDF.select("Status").distinct())

# COMMAND ----------

# DBTITLE 1,To strip the column names
#it will remove the space or unwanted character at the beginning and the end the column names
def colNameTrim(colvalue,TrimValue):
    colvalue=colvalue.strip(TrimValue)
    return colvalue

# COMMAND ----------

# DBTITLE 1,Renaming the Same Named columns
# #this function will rename the same named columns by appending the numbers in asc order from the 2nd column
# def colNameRepeating(df,len):
#     y=[]
#     for i in range(0,len):
#         x=1
#         if(i not in y):
#             for j in range(1,len):
#                 if (i != j)&(j not in y):
#                     if (df.columns[i].strip(string.digits) == df.columns[j].strip('_'))or(df.columns[i].strip(string.digits) == df.columns[j].strip(string.digits)):
#                         y.append(j)
#                         print("ij:"+str(i)+str(j))
#                         print(df.columns[i])
#                         print(df.columns[j])
#                         df=df.withColumnRenamed(df.columns[i], df.columns[i].strip(string.digits))
#                         df=df.withColumnRenamed(df.columns[j], df.columns[j].strip(string.digits)+"_"+str(x))
#                         x+=1
#                         print(x)
#     return df
# #print(y)
            

# COMMAND ----------

#this function will rename the same named columns by appending the numbers in asc order from the 2nd column
def colNameRepeating(df,len):
    y=[]
    for i in range(0,len):
        x=1
        if(i not in y):
            for j in range(1,len):
                if (i != j) & (j not in y):
                    if (df.columns[i].strip(string.digits).lower() == df.columns[j].strip('_').lower() or (df.columns[i].strip(string.digits).lower() == df.columns[j].strip(string.digits).lower()) ):
                        y.append(j)
                        print("ij:"+str(i)+str(j))
                        print(df.columns[i])
                        print(df.columns[j])
                        df=df.withColumnRenamed(df.columns[i], df.columns[i].strip(string.digits))
                        df=df.withColumnRenamed(df.columns[j], df.columns[j].strip(string.digits)+"_"+str(x))
                        x+=1
                        print(x)
    return df
#print(y)
            

# COMMAND ----------

#this function will rename the same named columns by appending the numbers in asc order from the 2nd column
def colNameRepeating1(df,len):
    y=[]
    for i in range(0,len):
        x=1
        print("print i "+str(i))
        if(i not in y):
            for j in range(1,len):
                if (i != j) & (j not in y) & (i not in y):
                    if ( (df.columns[i].strip(string.digits).lower() == df.columns[j].strip(string.digits).strip('_').lower()) or (df.columns[i].strip(string.digits).strip('_').lower() == df.columns[j].strip(string.digits).strip('_').lower()) or (df.columns[i].strip(string.digits).lower() == df.columns[j].strip(string.digits).lower()) or (df.columns[i].strip(string.digits).lower() == df.columns[j].strip(string.digits).lower())):
                        y.append(j)
                        print("ij:"+str(i)+str(j))
                        print(df.columns[i])
                        print(df.columns[j])
                        df=df.withColumnRenamed(df.columns[i], df.columns[i].strip(string.digits).strip('_'))
                        print("after i "+df.columns[i])
                        df=df.withColumnRenamed(df.columns[j], (df.columns[j].strip(string.digits).strip('_')+"_"+str(x)))
                        print("after j "+df.columns[j])
                        x+=1
                        print(x)
    display(df)
    return df
#print(y)
            