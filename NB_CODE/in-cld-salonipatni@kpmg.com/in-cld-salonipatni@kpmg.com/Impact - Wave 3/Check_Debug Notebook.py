# Databricks notebook source
# DBTITLE 1,BR 05 F01 Access_Card_Data_20230719
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_access_card_data 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_access_card_data --

# COMMAND ----------

# DBTITLE 1,F-02 Admin Data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_admin_data --where file_date='20230803'

# COMMAND ----------

# MAGIC %sql
# MAGIC select ount(* from  kgsonedatadb.trusted_hist_impact_admin_data --where expense_category='Paper'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_admin_data --where file_date='20230803'

# COMMAND ----------

# DBTITLE 1,F-03	Air Travel Data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_air_travel_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select `INVOICE_#` from kgsonedatadb.trusted_hist_impact_air_travel_data where fy ='FY 22'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_air_travel_data

# COMMAND ----------

# DBTITLE 1,F-04	Car Lease data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_car_lease_data order by 5 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_car_lease_data

# COMMAND ----------

# DBTITLE 1,F-05	Electricity and DG data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_electricity_and_dg_data

# COMMAND ----------

# DBTITLE 1,F-06	ID&E _ Gender Target Vs Actual
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_IDnE_Gender_Target_vs_Actuals 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_IDnE_Gender_Target_vs_Actuals -- %as float?

# COMMAND ----------

# DBTITLE 1,F-07	Official travel Personal Car
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_Official_travel_Personal_Car --datatype issue

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_Official_travel_Personal_Car --datatype issue

# COMMAND ----------

# DBTITLE 1,F-08	Purchaged Goods and Services
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_Purchased_Goods_and_Services

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_Purchased_Goods_and_Services

# COMMAND ----------

# DBTITLE 1,F-09	CSR Data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_csr_data --

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_csr_data --39036

# COMMAND ----------

# DBTITLE 1,IT_PURCHASED_GOODS_CONSUMPTION
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_it_purchased_goods_consumption_data-- where FY='FY22'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_it_purchased_goods_consumption_data

# COMMAND ----------

# DBTITLE 1,Health and Wellbeing
#7 tables

# COMMAND ----------

# DBTITLE 1,sick_wellbeing_data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_sick_wellbeing_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_sick_wellbeing_data

# COMMAND ----------

# DBTITLE 1,one_to_one_status_data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_one_to_one_status_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_one_to_one_status_data

# COMMAND ----------

# DBTITLE 1,gps_wellbeing
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_gps_wellbeing

# COMMAND ----------

# DBTITLE 1,wlb_attrition_data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_wlb_attrition_data

# COMMAND ----------

# DBTITLE 1,fatality_within_office_premises
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_fatality_within_office_premises

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_fatality_within_office_premises

# COMMAND ----------

# DBTITLE 1,office_safety_compliance
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_office_safety_compliance

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_office_safety_compliance

# COMMAND ----------

# DBTITLE 1,accident_within_office_premises
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_accident_within_office_premises

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsonedatadb.trusted_hist_impact_accident_within_office_premises

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details

# COMMAND ----------

# DBTITLE 1,Duplicate check Access Card
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_access_card_data where cost_centre ='Tax' and LOCATION ='Bangalore' and EMPLOYEE_ID ='118832' and LEVEL ='Senior Associate' and BU ='Tax' and date ='2023-05-08' and AS_OF_DATE ='2023-05-15' and EMPLOYEE_NAME ='JAIN POOJA'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),cost_centre,LOCATION,EMPLOYEE_ID,LEVEL,BU,date, AS_OF_DATE,EMPLOYEE_NAME from kgsonedatadb.trusted_hist_impact_access_card_data group by cost_centre,LOCATION,EMPLOYEE_ID,LEVEL,BU,date,AS_OF_DATE,EMPLOYEE_NAME Having count(*)>1

# COMMAND ----------

# DBTITLE 1,CSR Data
# MAGIC %sql
# MAGIC select count(*), EMPLOYEE_NAME,EMPLOYEE_ID,EMAIL_ID,LOCATION,DESIGNATION,SUB_FUNCTION_1,ACTIVITY_NAME,VIRTUAL_OR_NON_VIRTUAL,TYPE,HOURS,OUT_REACH_OF_BENEFICIERIES,MONTH,FY, Dated_On from kgsonedatadb.trusted_hist_impact_csr_data group by EMPLOYEE_NAME,EMPLOYEE_ID,EMAIL_ID,LOCATION,DESIGNATION,SUB_FUNCTION_1,ACTIVITY_NAME,VIRTUAL_OR_NON_VIRTUAL,TYPE,HOURS,OUT_REACH_OF_BENEFICIERIES,MONTH,FY ,Dated_On Having count(*)>1

# COMMAND ----------

# DBTITLE 1,Electricity  dg data
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_electricity_and_dg_data where ELECTRICITY_CONSUMPTION_TYPE ='GRID Consumption' and Entity ='KGS' AND AREA ='67736'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_kgs_rec_purchases

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_fuel_reimbursement_leased_cars

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_gps_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_impact_gps_data --datatype issue