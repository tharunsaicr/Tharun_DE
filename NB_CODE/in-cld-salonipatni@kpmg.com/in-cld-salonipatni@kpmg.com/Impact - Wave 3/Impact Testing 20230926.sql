-- Databricks notebook source
-- select count(1) from kgsonedatadb.trusted_hist_impact_electricity_and_dg_data --1560
-- select count(1) from kgsonedatadb.raw_hist_impact_electricity_and_dg_data --260
select * from kgsonedatadb.trusted_impact_electricity_and_dg_data 

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_impact_access_card_data;
select count(*) from kgsonedatadb.trusted_hist_impact_access_card_data;




-- COMMAND ----------



-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_impact_admin_data;
-- select * from kgsonedatadb.trusted_hist_impact_car_lease_data;
-- select * from kgsonedatadb.trusted_hist_impact_electricity_and_dg_data;
-- select * from kgsonedatadb.trusted_hist_impact_air_travel_data;
-- select * from kgsonedatadb.trusted_hist_impact_official_travel_personal_car;
-- select * from kgsonedatadb.trusted_hist_impact_purchased_goods_and_services;
-- select * from kgsonedatadb.trusted_hist_impact_idne_gender_target_vs_actuals;
-- select * from kgsonedatadb.trusted_hist_impact_csr_data;
-- select * from kgsonedatadb.trusted_hist_impact_it_purchased_goods_consumption_data;
-- select * from kgsonedatadb.trusted_hist_impact_integrity_training_data;
-- select * from kgsonedatadb.trusted_hist_impact_waste_management_data;
-- select * from kgsonedatadb.trusted_hist_impact_employee_commute_data;
-- select * from kgsonedatadb.trusted_hist_impact_gps_data;
-- select * from kgsonedatadb.trusted_hist_impact_sick_wellbeing_data;
-- select * from kgsonedatadb.trusted_hist_impact_one_to_one_status_data;
-- select * from kgsonedatadb.trusted_hist_impact_gps_wellbeing;
-- select * from kgsonedatadb.trusted_hist_impact_wlb_attrition_data;
-- select * from kgsonedatadb.trusted_hist_impact_fatality_within_office_premises;
-- select * from kgsonedatadb.trusted_hist_impact_office_safety_compliance;
-- select * from kgsonedatadb.trusted_hist_impact_accident_within_office_premises;
-- select * from kgsonedatadb.trusted_hist_impact_kgs_rec_purchases;
--select * from kgsonedatadb.trusted_hist_impact_fuel_reimbursement_leased_cars;