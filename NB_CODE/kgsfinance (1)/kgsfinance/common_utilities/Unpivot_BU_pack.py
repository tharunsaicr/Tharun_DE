# Databricks notebook source
# MAGIC %run 
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, ltrim
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr

# COMMAND ----------

def unpivotdf_bu(df,other_cols):
    
    #list of cols NOT to be unpivoted
    #other_cols = ["BusinessCategory", "Entity", "Employee_Type", "Role", "Operating_Unit", "Geo", "Location", "CostCenter", "Version", "Years", "Scenario", "Currency", "Period", "Dated_On", "Month", "Calendar_Year", "Financial_Year"]

    #list of columns for unpivoting 
    unpivotcolsdf = df.drop(*other_cols)
    unpivot_cols = unpivotcolsdf.columns

    #find number of columns for unpivoting
    columnCount = str(len(unpivot_cols))

    #preparing dynamic string for stack function - unpivot string
    stackcols = ",".join(["'" + x + "'," + x for x in unpivot_cols])
    finalstackstring = columnCount + "," + stackcols
    finalstackstring = "stack({}) as (Attribute,Value)".format(finalstackstring)

    #unpivot function using the finalstackstring created
    unpivotdf = df.select(*other_cols, expr(finalstackstring))
    #display(unpivotdf)
    
    return unpivotdf