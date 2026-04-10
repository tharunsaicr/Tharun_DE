# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

# COMMAND ----------

# DBTITLE 1,Import Required Functions
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,Read from raw current Delta Table
# df=spark.sql("select * from kgsfinancedb.raw_stg_fr_facility_bu_geo_actual")
raw_curr_facility_url=finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName
print(raw_curr_facility_url)
read_df= spark.read.format("delta").load(raw_curr_facility_url)
display(read_df)

# COMMAND ----------

# DBTITLE 1,List of Columns to be dropped
# colslist=['Key_1','Ledger_Name','Operating_Unit','Business_Category','GDC_Future','GDC_Future_1','Inter_Operating_Unit','User_Type','Source','Function','Sub_Function','Service_Line','Profit_Centre_Cost_Centre','Profit_Cost_Centre_Code','Function_Code','Location','Project_ID','Account_Code','Account_Name','Account_Flex_Field','Category','Account_Grouping','Project_Org','Project_Name','Project_Description','Vendor_Name','Customer_Name','Invoice_No_ER_No_Adjustment_No','Description_Full','Description_Full','GL_Description','Batch_Name','Journal_Desc','Journal_Name','Justification_from_ER', 'PO_Number', 'Payment_Doc_No', 'Payment_Date', 'Created_By', 'CREATED_BY__DESC', 'Creation_Date', 'Creation_Time', 'GL_Date', 'DD_UTR_Number', 'Receipt_Creator_Name', 'Receipt_Number', 'Vendor_PAN_No', 'Customer_PAN_No', 'Vendor_Address', 'Customer_Address', 'Vendor_Service_Tax_RegNo', 'Customer_Service_Tax_RegNo', 'Requisition_Number', 'Requisition_Description','Source_1', 'Spoc', 'Reviewer', 'Remarks', 'Date', 'Type', 'CC_Geo', 'Affliate_Entity', 'Affliate_MI_Grouping', 'Inter_Co', 'Common_Cost_Entity', 'Common_Cost_Location','BU__KI', 'Res','Geo___BU__KI','Geo', 'MI_Mapping', 'Mapping_2']

# COMMAND ----------

# DBTITLE 1,Drop Columns
# read_df=read_df.drop(*colslist)
# display(read_df)
# read_df.printSchema()

# COMMAND ----------

month_list=['oct','nov','dec']

# COMMAND ----------

#casting columns to required datatype
cast_df=read_df.select("*").withColumn("Debit_Amt",f.col("Debit_Amt").cast("double")) \
                           .withColumn("Credit_Amt",f.col("Credit_Amt").cast("double")) \
                           .withColumn("Net_Amount",f.col("Net_Amount").cast("double")) \
                           .withColumn("Period",when(F.to_date(col("Period"), "MMM yyyy").isNotNull(),F.to_date(col("period"), "MMM yyyy")).when(F. 
                            to_date(col("period"), "MMMM yyyy").isNotNull(),F.to_date(col("period"), "MMMM yyyy")).otherwise(None)) \
                           .withColumn("Calendar_Year",F.year(F.to_date("Calendar_Year","yyyy"))) \
                           .withColumn("Financial_Year",F.year(F.to_date("Financial_Year","yyyy"))) \
                           .withColumn("Dated_On",F.to_timestamp("Dated_On","yyyy-MM-dd HH:mm:ss.SSSSSS"))
cast_df.printSchema()
display(cast_df)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = cast_df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(cast_df.columns)
print("Column ",column)

# COMMAND ----------

cast_df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ reportName + "_" +processName + "_"+ tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName)

# COMMAND ----------

curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ reportName + "_" +processName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Calling Trusted Current to Trusted History Notebook
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

if(spark._jsparkSession.catalog().tableExists(saveTableName)):
    tableDf = spark.sql("select * from "+saveTableName)

    tableDf_row = tableDf.count()
    print("Row ",tableDf_row)

    tableDf_col = len(tableDf.columns)
    print("Column ",tableDf_col)

    if((row == tableDf_row) & (column == tableDf_col)):
        print("Row and Column Count is Matching!!")
    else:
        print("Row Count is NOT Matching!!")
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})