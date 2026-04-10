# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
import string
from pyspark.sql.functions import *

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

currentDf.count()

# COMMAND ----------

# DBTITLE 1,Get Levelwise Mapping for CSR Data
#Lnd Level wise data
df_lnd_level_wise = spark.sql("select distinct Position,Mapping from kgsonedatadb.config_dim_level_wise")

# If Position is Manager, mapping should be manager as well but in config table for Manger Mapping in Associate Director
df_lnd_level_wise = df_lnd_level_wise.withColumn('Mapping', \
    when(upper(col('Position')) == 'MANAGER', lit('Manager'))\
    .otherwise(col('Mapping')))

df_lnd_level_wise = df_lnd_level_wise.withColumn('Mapping',\
    when(trim(upper(df_lnd_level_wise.Mapping)) == 'ANALYSTS TO SENIORS/TEAM LEADERS',lit('TL and Below'))\
    .otherwise(col('Mapping')))


# Impact related Levelwise Data
columns = ['Position', 'Mapping']

rows = [
        ['Functional Mailbox','TL and Below']
        ,['Partner-KGS Legal & RM Leader','Director +']
        ,['Receptionist','TL and Below']
        ,['Third Party - ROD','Contractor']
        ,['Director-GDCOPP','Director +']
        ,['Senior Executive Assistant','TL and Below']
        ,['Executive Director - Finance','Director +']
        ,['Partner - KGS Consulting Leader','Director +']
        ,['Third Party','Contractor']
        ,['Partner-KGS Tax Leader','Director +']
        ]

df_impact_level_wise = spark.createDataFrame(rows, columns)

# Combining Lnd and Impact Mapping
df_level_wise = df_lnd_level_wise.union(df_impact_level_wise)

df_level_wise=df_level_wise.withColumn('Mapping',\
    when(col('Mapping').contains('Senior Associate Director') | col('Mapping').contains('Associate Director') | col('Mapping').contains('Director +'),'AD and Above')\
    .otherwise(col('Mapping')))

df_level_wise = df_level_wise.dropDuplicates()

# COMMAND ----------

if tableName.lower() == 'csr_data':
    currentDf =currentDf.join(df_level_wise, currentDf.DESIGNATION == df_level_wise.Position,"left").select(currentDf['*'],df_level_wise['Mapping'])

    currentDf =currentDf.withColumnRenamed("Mapping","LEVELWISE_MAPPING")


# COMMAND ----------

if (processName.lower() == 'impact') & (tableName.lower() == 'air_travel_data'):

    # Filter only KGS Entities
    Entity_List = ['KGS','KGDCPL','KGSMPL','KGSPL','KRCPL']

    Entity_List = [entity.lower() for entity in Entity_List]

    # Filter  required entities
    currentDf = currentDf.filter(lower(currentDf.ENTITY).isin(Entity_List))
    
    currentDf = currentDf.withColumnRenamed("NEW_DEFINITION_HAUL_MARKING__SHORT_HAUL___LESS_THAN_1200_KMS__MEDIUM_HAUL___1200___3700_KMS__LONG_HAUL___ABOVE_3700_KMS__2019_DEF:_HAUL_MARKING_BASIS_AIR_KMS:_SHORT_HAUL_BELOW__499_KMS_MEDIUM_HAUL_BETWEEN___500_KMS___1349_KMS_LONG_HAUL___ABOVE_1350_KMS_","HAUL_MARKING__BASIS_AIR_KMS")

    #air_travel_data replacing next line character
    currentDf = currentDf.withColumn('TICKET_NO', regexp_replace(col('TICKET_NO'), "[\\n\\r]", ''))

# COMMAND ----------

# DBTITLE 1,Unpivoting  the Month columns &  pivoting Metrics column

if (processName.lower() == 'impact') & (tableName.lower() == 'electricity_and_dg_data'):
    
    currentDf=unpivotdf(currentDf,"MONTH","UNIT_OR_AMOUNT")
    currentDf=currentDf.drop("Total").withColumn("UNIT_OR_AMOUNT",currentDf.UNIT_OR_AMOUNT.cast('double'))

    currentDf = pivotdf(currentDf, ["Dated_On","File_date","STATE","LOCATION","FY","SQUARE_FEET_AREA","ENTITY","TYPE","MONTH"], "METRIC","UNIT_OR_AMOUNT")
    currentDf=currentDf.select("STATE","LOCATION","FY","SQUARE_FEET_AREA","ENTITY","TYPE","MONTH","SPENDS","UNITS","Dated_On","File_date")
    

# COMMAND ----------

# DBTITLE 1,Column formatting
FYList = ['waste_management_data','csr_data','it_purchased_goods_consumption_data','air_travel_data','integrity_training_data','fuel_reimbursement_leased_cars','kgs_rec_purchases','electricity_and_dg_data']

MonthList = ['waste_management_data','admin_data','csr_data','air_travel_data','accident_within_office_premises','fatality_within_office_premises','one_to_one_status_data','locationwise_seat_allocation']

if tableName.lower() in FYList:
    if(tableName.lower() == 'integrity_training_data'):
        currentDf = currentDf.withColumn("Financial_Year",concat(upper(regexp_replace('Financial_Year', "[^a-zA-Z]", "")), lit(" "), regexp_extract('Financial_Year','[0-9]+', 0) ))

    else:
        currentDf = currentDf.withColumn("FY",concat(upper(regexp_replace('FY', "[^a-zA-Z]", "")), lit(" "), regexp_extract('FY','[0-9]+', 0) ))


if tableName.lower() in MonthList:

    print(tableName.lower())

    if(tableName.lower() == 'waste_management_data'):
        currentDf = currentDf.withColumn("Month",concat(lit("20"),regexp_extract('Month','[0-9]+', 0) , lit(" "), substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3)))

    elif(tableName.lower() == 'air_travel_data'):

        #Extracting only numbers & appending with "20" and then only 3 characters to get like "2023 Mar" format

        currentDf=currentDf.withColumn('INVOICE_MONTH',when(to_date(currentDf['INVOICE_MONTH'], 'yyyy-MM-dd').isNotNull(),date_format(col("INVOICE_MONTH"), "MMM-yy")).otherwise(currentDf['INVOICE_MONTH']))

        currentDf = currentDf.withColumn("INVOICE_MONTH",concat(lit("20"),regexp_extract('INVOICE_MONTH','[0-9]+', 0) , lit(" "), substring(regexp_replace('INVOICE_MONTH', "[^a-zA-Z]", ""),1,3)))

    elif(tableName.lower() in ('accident_within_office_premises','fatality_within_office_premises','one_to_one_status_data','locationwise_seat_allocation')):
        currentDf = currentDf.withColumn("Month", substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3))

    else:
        
        currentDf = currentDf.withColumn("Month",concat(regexp_extract('Month','[0-9]+', 0) , lit(" "), substring(regexp_replace('Month', "[^a-zA-Z]", ""),1,3)))

# COMMAND ----------

if(tableName.lower() == 'integrity_training_data'):
    currentDf = currentDf.withColumn("FULL_NAME",regexp_replace(col("FULL_NAME"), "[^a-zA-Z0-9-\s,]", ""))

# COMMAND ----------

if (processName.lower() == 'impact') & (tableName.lower() == 'locationwise_seat_allocation'):
    currentDf = currentDf.withColumn("LOCATION",\
        when(upper(col('LOCATION')) == 'BANGALORE',lit('Bengaluru'))\
        .when(upper(col('LOCATION')) == 'GURGAON',lit('Gurugram'))\
        .otherwise(col('LOCATION')))

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
currentDf = currentDf.withColumn("Dated_On",lit(currentdatetime))

# COMMAND ----------

currentDf.count()

# COMMAND ----------

# DBTITLE 1, Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

