# Databricks notebook source
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run 
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, ltrim
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr

def unpivotdf(df):
    #overall columns - spaces removed from column names
    overalldf = df.select([col(c).alias(c.replace(' ', '')) for c in df.columns])

    #list of columns for unpivoting
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
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
    finalstackstring = "stack({}) as (Month,Amount)".format(finalstackstring)

    #unpivot function using the finalstackstring created
    unpivotdf = overalldf.select(*other_cols, expr(finalstackstring))

    unpivotdf.groupBy("Entity").pivot('Entity').agg(F.first("Amount"))
    
    return unpivotdf