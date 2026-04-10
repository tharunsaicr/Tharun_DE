# Databricks notebook source
from dateutil.parser import parse
from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,expr, add_months, date_format, regexp_replace, substring, concat,from_unixtime, unix_timestamp
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as f
import pyodbc
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Replace Special Character/Space in ColumnName with '_'
lst_rchar=[' ','/','&','(',')','.',"'",';','{','}','\n','\t','=','-','[',']',',','%']
def replacechar(colvalue):
    for rval in lst_rchar:
        colvalue=colvalue.replace(rval, '_')
        # colvalue=colvalue.strip().replace(rval, '_')
    return colvalue

# COMMAND ----------

# DBTITLE 1,To strip the column names
#it will remove the space or unwanted character at the beginning and the end the column names
def colNameTrim(colvalue,TrimValue):
    colvalue=colvalue.strip(TrimValue)
    return colvalue

# COMMAND ----------

# DBTITLE 1,Change Case to Upper
def toUpper(colvalue):
    colvalue=colvalue.upper()
    return colvalue

# COMMAND ----------

# DBTITLE 1,Change List Case to Upper
def changeList_to_upper(oldList):
    newList = []
    for element in oldList:
        newList.append(element.upper())
    return newList

# COMMAND ----------

# DBTITLE 1,Trim spaces
def trimchar(colvalue):
    colvalue=colvalue.strip()
    return colvalue

# COMMAND ----------

# DBTITLE 1,Change Any Date Format to YYYY-MM-DD
from dateutil.parser import parse
from pyspark.sql.types import *

changeDateFormat = udf(lambda coldate: parse(coldate), DateType())

# COMMAND ----------

from datetime import *
from pyspark.sql.types import *
# randomNumber = int(round(datetime.now().timestamp()))

randomNumber = udf(lambda col: int(datetime.now().timestamp()), StringType())


# COMMAND ----------

# DBTITLE 1,CAST COLUMN TO STRING
def colcaststring(df,collist):
    for x in collist:
        df = df.withColumn(x, df[x].cast(StringType()))
    #df = df.select([col(x).cast("String") for x in df.columns])
    return df

# COMMAND ----------

# Do not un-comment until function is created
# from pyspark.sql import functions as f
def leadtrailremove(df1):
    for colname in df1.columns:
        df1 = df1.withColumn(colname, f.trim(f.col(colname)))
    
    return df1

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

# DBTITLE 1,Calculate Business Days including Start Date and End Date
import pandas as pd
from pyspark.sql.types import IntegerType

def get_count_working_days(startDate,endDate):

    # busniness days including Start date and End Date

    businessDayCount = pd.bdate_range(startDate,endDate)
    return len(businessDayCount)

get_count_working_days_udf = udf(get_count_working_days,IntegerType())

# COMMAND ----------

# Function to calculate business days using NumPy
import numpy as np
def calculate_business_days(start_date, end_date):
    try:
        start_date = np.datetime64(start_date)
        end_date = np.datetime64(end_date)
        business_days = np.busday_count(start_date, end_date)
        return int(business_days)
    except ValueError:
        return None
business_days_udf = udf(calculate_business_days, IntegerType())

# COMMAND ----------

# DBTITLE 1,ADD Working days
def add_business_days(start_date, days_to_add):
    try:
        # start_date_np = np.datetime64(start_date)
        start_date_np = str(start_date)
        result_date_np = np.busday_offset(start_date_np, int(days_to_add), roll='forward')
 
        # Convert result_date_np to Python datetime
        result_date = np.datetime_as_string(result_date_np, unit='D').astype('datetime64[D]').tolist()
 
        return result_date
    except Exception as e:
        print(f"Error: {e}")
        return None
working_days_udf = udf(add_business_days, DateType())

# COMMAND ----------

# DBTITLE 1,Renaming same named columns
#this function will rename the same named columns by appending the numbers in asc order from the 2nd column 
import string

def colNameRepeating(df,len):
    y=[]
    for i in range(0,len):
        x=1
        if(i not in y):
            for j in range(1,len):
                if (i != j) & (j not in y):
                    if (df.columns[i].strip(string.digits).lower() == df.columns[j].strip('_').lower() or (df.columns[i].strip(string.digits).lower() == df.columns[j].strip(string.digits).lower()) ):
                        y.append(j)
                        print(df.columns[i])
                        print(df.columns[j])
                        df=df.withColumnRenamed(df.columns[i], df.columns[i].strip(string.digits))
                        df=df.withColumnRenamed(df.columns[j], df.columns[j].strip(string.digits)+"_"+str(x))
                        x+=1
                        print(" ")
    return df


# COMMAND ----------

# DBTITLE 1,Ignore ASCII values
from pyspark.sql.functions import udf

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

ascii_udf = udf(ascii_ignore)

# COMMAND ----------

# DBTITLE 1,To Standardize MonthYear Format
from dateutil.parser import parse
from pyspark.sql.types import *

def year_month_date(date_str):
    month_mapping={"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06","jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
    try:
        year=int(date_str[-2:])
        month=date_str[:3]
        month_one=date_str[3:6]
        if month.lower() in month_mapping:
           month_num=month_mapping[month]
           if month_num in ['Feb']:
               formatted_date=f"20{year}{month_num}28"
           else:
              formatted_date=f"20{year}{month_num}{'31' if month.lower() in ['jan','mar','may','july','aug','oct','dec'] else '30'}"
          
           return formatted_date
      
        if month_one.lower() in month_mapping:
           month_num=month_mapping[month_one]
           if month_num in ['Feb']:
               formatted_date=f"20{year}{month_num}28"
           else:
              formatted_date=f"20{year}{month_num}{'31' if month_one.lower() in ['jan','mar','may','july','aug','oct','dec'] else '30'}"
          
           return formatted_date
           
        else:
            parseDate = str(parse(date_str))
            convertedDate = datetime.strptime(parseDate,"%Y-%m-%d %H:%M:%S").date()
            # convertedDate = datetime.strptime(parseDate,"%Y-%m-%d").date()
            convertedDate = convertedDate.strftime("%Y%m%d")
            return convertedDate
    except:
        return None
    
year_month_date_udf=udf(year_month_date)


# COMMAND ----------

# DBTITLE 1,Convert String to Camel Case
def camel_case(inputString):
    convertedString = inputString.capitalize()
    return convertedString
    
camel_case_udf = udf(camel_case,StringType())

# COMMAND ----------

# DBTITLE 1,Unpivot function
from pyspark.sql.functions import col, ltrim
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr

def unpivotdf(df, pivotColumn, valueColumn):
    
    #overall columns - spaces removed from column names
    overalldf = df.select([col(c).alias(c.replace(' ', '')) for c in df.columns])

    #list of columns for unpivoting
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    unpivot_cols = [x for x in overalldf.columns if any(x in y or y in x for y in months)]

    #cast the month columns to string type 
    overalldf = colcaststring(overalldf,unpivot_cols)

    #other columns apart from unpivot columns
    othercolsdf = overalldf.drop(*unpivot_cols)
    other_cols = othercolsdf.columns

    #find number of columns for unpivoting
    columnCount = str(len(unpivot_cols))

    #preparing dynamic string for stack function - unpivot string
    stackcols = ",".join(["'" + x + "'," + x for x in unpivot_cols])
    finalstackstring = columnCount + "," + stackcols
    #finalstackstring = "stack({}) as ("+Month+","+Amount+")".format(finalstackstring,pivotColumn,valueColumn)
    finalstackstring = "stack({}) as ({},{})".format(finalstackstring,pivotColumn,valueColumn)

    #unpivot function using the finalstackstring created
    unpivotdf = overalldf.select(*other_cols, expr(finalstackstring))
    
    return unpivotdf


# COMMAND ----------

# DBTITLE 1,Pivot
def pivotdf(df, columns, pivotCol, aggcol):
  final_df = df.groupBy(*columns).pivot(pivotCol).sum(aggcol)
  return final_df

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql.types import *

def calculateNextMonth(date,monthstoadd,dateformat,convertedDateFormat):
    
    datetime_object = datetime.strptime(date, dateformat)
    n = int(monthstoadd)
    future_date = datetime_object + relativedelta(months=n)
    next_Month_Name = future_date.strftime(convertedDateFormat)

    return next_Month_Name

calculateNextMonthUDF = udf(calculateNextMonth,StringType())

# COMMAND ----------

from unidecode import unidecode
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

#Define a UDF to apply unidecode to each element in a column
unidecode_udf = udf(lambda x: unidecode(str(x)), StringType())

# COMMAND ----------

# DBTITLE 1,Append String to Column Name
def Append_String_to_ColumnName(df, stringToBeAppened):
    for c in df.columns:
        df=df.withColumnRenamed(c,stringToBeAppened+"_"+c)
    return df

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
            