-- Databricks notebook source
ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_leaver SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
)

-- COMMAND ----------

describe table kgsonedatadb.trusted_hist_jml_uk_leaver

-- COMMAND ----------

-- DBTITLE 1,trusted_hist_jml_uk_joiner
ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Grade to Pay_Grade;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Employee_Group to Employee_Class;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Work_Schedule to Standard_Weekly_Hours;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Position to Job_Title;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Capability to Retain_Capability;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Function to Capability;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Service_Line_Code to Retain_Service_Line_Code;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN Service_Line_Description to Retain_Service_Line_Description;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
RENAME COLUMN VDI_Image to Notes;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_joiner
ADD COLUMN Position_ID STRING

-- COMMAND ----------

-- DBTITLE 1,trusted_hist_jml_uk_mover
ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN New_Capability to New_Retain_Capability;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN New_Function to New_Capability;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Service_Line_Code to New_Retain_Service_Line_Code;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Service_Line_Description to New_Retain_Service_Line_Description;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN New_Grade to New_Pay_Grade;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Employee_Group to New_Employee_Class;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Work_Schedule to New_Standard_Weekly_Hours;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Contract_Activity_Type to New_Work_Contract;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
RENAME COLUMN Office to New_Location;

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
ADD COLUMN Modified STRING

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
ADD COLUMN Existing_Position_ID STRING

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
ADD COLUMN New_Position_ID STRING

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
ADD COLUMN Existing_Cost_Centre STRING

ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_mover
ADD COLUMN Capability STRING

-- COMMAND ----------

-- DBTITLE 1,trusted_hist_jml_uk_leaver
ALTER TABLE kgsonedatadb.trusted_hist_jml_uk_leaver
ADD COLUMN Position_ID STRING

-- COMMAND ----------

