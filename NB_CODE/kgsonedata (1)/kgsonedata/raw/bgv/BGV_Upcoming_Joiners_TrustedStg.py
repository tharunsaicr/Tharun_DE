# Databricks notebook source
tableName = "upcoming_joiners"
processName = "bgv"

print(tableName)
print(processName)

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,import packages and files

from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat, current_date
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from datetime import *
import pytz


# COMMAND ----------


fileYear = FileDate[:4]
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
# print(convertedFileDate)
convertedFileDate = datetime.strptime(convertedFileDate, '%Y-%m-%d').date()

print(convertedFileDate)

# COMMAND ----------

from datetime import datetime
fileDate_new = datetime.strptime(FileDate, '%Y%m%d').strftime('%Y-%m-%d')
print(fileDate_new)

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Reading Waiver Tracker 

if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_waiver_tracker')):
    # df_waiver_tracker = spark.read.table("kgsonedatadb.trusted_bgv_waiver_tracker")
    # df_waiver_tracker = spark.sql("select * from (select rank() over(partition by file_date order by report_date desc,file_date desc) as rank, * from kgsonedatadb.trusted_hist_bgv_waiver_tracker where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_waiver_tracker where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

    df_waiver_tracker = spark.sql("select * from (select ROW_NUMBER() over(partition by Candidate_Email_ID order by Report_date desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_waiver_tracker where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_waiver_tracker where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

    # display(df_waiver_tracker)

else:
    print("table doesn't exist")


# COMMAND ----------

# DBTITLE 1,Reading K  check
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_kcheck')):
    # df_kcheck = spark.read.table("kgsonedatadb.trusted_bgv_kcheck")
    # df_kcheck = spark.sql("select * from (select rank() over(partition by file_date order by file_date desc) as rank, * from kgsonedatadb.trusted_hist_bgv_kcheck where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_kcheck where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

    df_kcheck = spark.sql("select * from (select rank() over(partition by Email order by Initiated_On desc, `Inv#` desc) as rank, * from kgsonedatadb.trusted_hist_bgv_kcheck where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_kcheck where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")
    
    # df_kcheck = spark.sql("select * from (select rank() over(partition by Email order by Initiated_On desc) as rank, * from kgsonedatadb.trusted_hist_bgv_kcheck where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_kcheck where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")
    
    # display(df_kcheck)

else:
    print("table doesn't exist")




# COMMAND ----------

# DBTITLE 1,Reading Offer Release
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_offer_release')):
    # df_offer_release = spark.read.table("kgsonedatadb.trusted_bgv_offer_release")
    df_offer_release = spark.sql("select * from (select rank() over(partition by Candidate_Email order by Start_Date desc, Offer_Extended_Date desc, Dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_offer_release where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_offer_release where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

#   Commenting below 3 lines of code to incorporate latest change from BGV to accommodate updated values
    # df_prev_upcoming_joiners = spark.sql("select `Candidate_Email/Candidate_Email_Personal_Email` as Prev_File_Candidate_Mail from (select rank() over(partition by `Candidate_Email/Candidate_Email_Personal_Email` order by File_Date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners) hist where rank = 1")

    # df_offer_release = df_offer_release.join(df_prev_upcoming_joiners,df_offer_release["Candidate_Email"] == df_prev_upcoming_joiners["Prev_File_Candidate_Mail"],"left").select(df_offer_release["*"],df_prev_upcoming_joiners["Prev_File_Candidate_Mail"])

    # df_offer_release = df_offer_release.filter(col("Prev_File_Candidate_Mail").isNull()).drop("Prev_File_Candidate_Mail")

    # display(df_offer_release)

else:
    print("table doesn't exist")

# COMMAND ----------

# DBTITLE 1,Reading Progress Sheet
if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_bgv_progress_sheet')):
    # df_progress_sheet = spark.read.table("kgsonedatadb.trusted_bgv_progress_sheet")  
    # df_progress_sheet = spark.sql("select * from (select rank() over(partition by file_date order by file_date desc) as rank, * from kgsonedatadb.trusted_hist_bgv_progress_sheet where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_progress_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

     df_progress_sheet = spark.sql("select * from (select rank() over(partition by Personal_Email_ID order by Case_Initiation_Date desc, Dated_On desc) as rank, * from kgsonedatadb.trusted_hist_bgv_progress_sheet where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_bgv_progress_sheet where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")
    


else:
    print("table doesn't exist")
# display(df_progress_sheet)

# COMMAND ----------

# DBTITLE 1,Sorts offer extended date, start date and accepted date desc
df_or_sorted = df_offer_release.orderBy(df_offer_release.Start_Date.desc(),
                                        df_offer_release.Offer_Extended_Date.desc(),
                                        df_offer_release.Offer_Accepted_Date.desc())



# COMMAND ----------


df_or_updated = df_or_sorted.withColumn("current_date", current_date())



# COMMAND ----------

# DBTITLE 1,Only consider offer if status is Accepted or Extended(add lower logic)
df_or_consider = df_offer_release.filter(df_offer_release.Offer_Status.isin ("Offer, Accepted","Offer, Extended"))

print(df_or_consider.count())


# COMMAND ----------

# DBTITLE 1,Filtering data based on current date
# df_upcoming_candidates# only consider Upcoming Candidate based on Offer Extended Date, Offer Accepted date, Start date =today and future dates.

# df_upcoming_candidates = df_or_consider.filter((df_or_consider.Start_Date >= fileDate_new) &\
#     (df_or_consider.Offer_Extended_Date >= fileDate_new) & \
#     (df_or_consider.Offer_Accepted_Date >= fileDate_new))

df_upcoming_candidates = df_or_consider.filter(df_or_consider.Start_Date >= fileDate_new)

print(df_upcoming_candidates.count())

# COMMAND ----------

# DBTITLE 1,Remove duplicate data
# Then Remove the Duplicate Data from the list – only consider the latest date 
df_dist_upcoming_candidates = df_upcoming_candidates.distinct()


# COMMAND ----------

# DBTITLE 1,CC BU mapping file based on Cost Centre.

df_config_bu_cc = spark.sql("select * from kgsonedatadb.config_cost_center_business_unit")

df_dist_upcoming_candidates = df_dist_upcoming_candidates.join(df_config_bu_cc,df_dist_upcoming_candidates.Cost_Center == df_config_bu_cc.Cost_centre,"left").select(df_dist_upcoming_candidates["*"],df_config_bu_cc["BU"])



# COMMAND ----------

# DBTITLE 1,Considered columns from offer release
# 1.Candidate Identifier, 2.BU, 3.Candidate name, 4.Candidate Email, 5. Candidate Alternate Email- not there,  6. Start Date, 7. Recruiter Name, 8. Work Location, 9. Cost Center, 10. Client Geography, 11. Designation


or_final_df = df_dist_upcoming_candidates.select("Candidate_Identifier","BU","Candidate_Full_Name","Candidate_Email","Start_Date","Recruiter_Name","Work_Location","Cost_Center","Client_Geography","Designation")



# COMMAND ----------

# DBTITLE 1,Considered columns from KCheck and applied left join with offer release
kcheck_filtered_df = or_final_df.join(df_kcheck,or_final_df.Candidate_Email == df_kcheck.Email,"left").select(or_final_df["*"],df_kcheck["KPMG_Ref_No"],df_kcheck["External_Status"],df_kcheck["Email"])


# COMMAND ----------

kcheck_filtered_df = kcheck_filtered_df.withColumn("Offer_Release_Email",col('Candidate_Email'))\
    .withColumn("Kcheck_Email",col('Email'))

# COMMAND ----------

# MAGIC %md
# MAGIC Required selected column

# COMMAND ----------

# DBTITLE 1,Considered columns from Progress-sheet and applied left join with Kcheck
progress_filter_df = kcheck_filtered_df.join(df_progress_sheet,kcheck_filtered_df.Candidate_Email == df_progress_sheet.Personal_Email_ID,"left").select(kcheck_filtered_df["*"],df_progress_sheet["Candidate_Official_email_"],df_progress_sheet["Status"],df_progress_sheet["CRI1"],df_progress_sheet["CRI2"],df_progress_sheet["CRI3"],df_progress_sheet["CRI4"],df_progress_sheet["CRI5"],df_progress_sheet["Database1"],df_progress_sheet["Database2"],df_progress_sheet["Database_3"],df_progress_sheet["Database_4"],df_progress_sheet["Open_Insuff"],df_progress_sheet["UTV_Inaccessible_remarks"],df_progress_sheet["EDU_EMP_Name__Suspicious_"],df_progress_sheet["Discrepancy_remarks"],df_progress_sheet["latest_color_code"],df_progress_sheet["Reference_No_"],df_progress_sheet["Over_All_Status_Drop_Down_"],df_progress_sheet["Final_Report_Color_Code_Drop_Down_"],df_progress_sheet["Case_Initiation_Date"],df_progress_sheet["Personal_Email_ID"])

#renaming column because of redundancy
progress_filter_df = progress_filter_df.withColumnRenamed("Status","ps_status")

# progress_filter_df.display()


# COMMAND ----------

progress_filter_df = progress_filter_df.withColumn("Progress_Sheet_Email",col('Personal_Email_ID'))

# COMMAND ----------

# DBTITLE 1,BGV Status Logic
# Status column depends on KPMG Ref No
# If KPMG Ref No is Null :
# Get Column D('External Status') from K Check
# Else  if  KPMG Ref No is available:
# Get Column Over_All_Status_Drop_Down_  from Progress Sheet
#  for few cases 2 ref number can be generated due to technical glitch from kcheck that's why we're not getting the proper data from ps and kcheck data 


# COMMAND ----------

# DBTITLE 1,BGV Status Column Implementation

status_filter_df = progress_filter_df.withColumn("BGV_Status",
                    when(((progress_filter_df.KPMG_Ref_No.isNull()) | (progress_filter_df.KPMG_Ref_No == '-') | (progress_filter_df.KPMG_Ref_No == '')), col("External_Status"))
                    .otherwise(col("Over_All_Status_Drop_Down_")))
# status_filter_df.display()


# COMMAND ----------

# DBTITLE 1,Testing BGV_Status
# status_filter_df.select('KPMG_Ref_No','External_status','Over_All_Status_Drop_Down_','BGV_Status').distinct().display()

# COMMAND ----------

# DBTITLE 1,Check if status colulmn is populated correctly

# status_filter_df.select('KPMG_Ref_No','External_status','Over_All_Status_Drop_Down_','BGV_Status').filter((col("BGV_Status").isNull()) | (col("BGV_Status") == "")).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding columns on bgv_upcoming_joiner and applying rules on it

# COMMAND ----------

# DBTITLE 1,Mandatory_Checks_Completed logic
# if 'CRI1' is 'Verified' and 'CRI2' or 'CRI3' or 'CRI4' or 'CRI5' is 'Verified' or 'NA' or 'null' and 'Database1' and  'Database2' is verified and 'Database_3' or 'Database_4', is 'Verified' or 'NA' or 'Null' then 'Mandatory Checks Completed' is 'Yes'

# if 'CRI1' or 'CRI2' or 'CRI3' or 'CRI4' or 'CRI5' is 'Discrepancy' or 'Database1', 'Database2','Database_3','Database_4', is 'Discrepancy' then 'Mandatory Checks Completed' is 'Yes-Discrepancy'

# if 'CRI1' or 'CRI2' or 'CRI3' or 'CRI4' or 'CRI5' is "'Discrepancy' and 'WIP'" or  'Database1' or 'Database2' or 'Database_3' or 'Database_4' is "'Discrepancy' or 'WIP'" then 'Mandatory Checks Completed' is 'WIP'

# if 'CRI1' or 'CRI2' or 'CRI3' or 'CRI4' or 'CRI5' is 'Insufficiency' or 'Database1', 'Database2','Database_3','Database_4', is 'Insufficiency' then 'Mandatory Checks Completed' is 'No- Insufficiency'

# else  'Mandatory Checks Completed' is 'No'
# Note:- Put mandatory status ""-"" for the cases wherin the reference ID not generated."

# COMMAND ----------

# MAGIC %md
# MAGIC Following are the constant string checks for Mandatory Checks Completed - 
# MAGIC verified -  ver%, 
# MAGIC discrepancy - disc%, 
# MAGIC stop check - stop%, 
# MAGIC insufficiency - ins%

# COMMAND ----------

# DBTITLE 1,MCC column
bgv_upcoming_joiners_ac1 = status_filter_df.withColumn("Mandatory_Checks_Completed", \
when((lower(status_filter_df.CRI1).like("ver%")) & \
    ((lower(status_filter_df.CRI2).like("ver%")) | (lower(status_filter_df.CRI2 )== "na") | (status_filter_df.CRI2.isNull()))&\
    ((lower(status_filter_df.CRI3).like("ver%")) | (lower(status_filter_df.CRI3) == "na") | (status_filter_df.CRI3.isNull()))&\
    ((lower(status_filter_df.CRI4).like("ver%")) | (lower(status_filter_df.CRI4) == "na") | (status_filter_df.CRI4.isNull()))&\
    ((lower(status_filter_df.CRI5).like("ver%")) | (lower(status_filter_df.CRI5) == "na") | (status_filter_df.CRI5.isNull()))&\
    (lower(status_filter_df.Database1).like("ver%")) &\
    (lower(status_filter_df.Database2).like("ver%")) &\
    ((lower(status_filter_df.Database_3).like("ver%"))| (lower(status_filter_df.Database_3) == "na") | (status_filter_df.Database_3.isNull())) & \
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_4) == "na") | (status_filter_df.Database_4.isNull())) \
    ,lit("Yes")) \
    # ----------
.when(
    (lower(status_filter_df.CRI1) == "wip") & \
    (lower(status_filter_df.CRI2) == "wip") & \
    (lower(status_filter_df.CRI3) == "wip") & \
    (lower(status_filter_df.CRI4) == "wip") & \
    (lower(status_filter_df.CRI5) == "wip") & \
    (lower(status_filter_df.Database1) == "wip") &\
    (lower(status_filter_df.Database2) == "wip") &\
    (lower(status_filter_df.Database_3) == "wip") &\
    (lower(status_filter_df.Database_4) == "wip")
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1) == "wip") & \
    (lower(status_filter_df.CRI2) == "wip") & \
    (lower(status_filter_df.CRI3) == "wip") & \
    (lower(status_filter_df.CRI4) == "wip") & \
    (lower(status_filter_df.CRI5) == "wip") & \
    (lower(status_filter_df.Database1).like("stop%")) &\
    (lower(status_filter_df.Database2).like("stop%")) &\
    (lower(status_filter_df.Database_3).like("stop%")) &\
    (lower(status_filter_df.Database_4).like("stop%"))
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1) == "wip") & \
    (lower(status_filter_df.CRI2) == "wip") & \
    (lower(status_filter_df.CRI3) == "wip") & \
    (lower(status_filter_df.CRI4) == "wip") & \
    (lower(status_filter_df.CRI5) == "wip") & \
    (lower(status_filter_df.Database1).like("ver%")) &\
    (lower(status_filter_df.Database2).like("ver%")) &\
    ((lower(status_filter_df.Database_3).like("ver%")) | (lower(status_filter_df.Database_3) == "na")) &\
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_4) == "na"))
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1) == "wip") & \
    (lower(status_filter_df.CRI2) == "wip") & \
    (lower(status_filter_df.CRI3) == "wip") & \
    (lower(status_filter_df.CRI4) == "wip") & \
    (lower(status_filter_df.CRI5) == "wip") & \
    (lower(status_filter_df.Database1).like("disc%")) &\
    (lower(status_filter_df.Database2).like("disc%")) &\
    (lower(status_filter_df.Database_3).like("disc%")) &\
    (lower(status_filter_df.Database_4).like("disc%"))
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1).like("stop%")) & \
    (lower(status_filter_df.CRI2).like("stop%")) & \
    (lower(status_filter_df.CRI3).like("stop%")) & \
    (lower(status_filter_df.CRI4).like("stop%")) & \
    (lower(status_filter_df.CRI5).like("stop%")) & \
    (lower(status_filter_df.Database1) == "wip") &\
    (lower(status_filter_df.Database2) == "wip") &\
    (lower(status_filter_df.Database_3) == "wip") &\
    (lower(status_filter_df.Database_4) == "wip")
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1).like("ver%")) & \
    ((lower(status_filter_df.CRI2).like("ver%")) | (lower(status_filter_df.CRI2) == "na")) & \
    ((lower(status_filter_df.CRI3).like("ver%")) | (lower(status_filter_df.CRI3) == "na")) & \
    ((lower(status_filter_df.CRI4).like("ver%")) | (lower(status_filter_df.CRI4) == "na")) & \
    ((lower(status_filter_df.CRI5).like("ver%")) | (lower(status_filter_df.CRI5) == "na")) & \
    ((lower(status_filter_df.Database1) == "wip") &\
    (lower(status_filter_df.Database2) == "wip") &\
    (lower(status_filter_df.Database_3) == "wip") &\
    (lower(status_filter_df.Database_4) == "wip"))
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1).like("disc%")) & \
    (lower(status_filter_df.CRI2).like("disc%")) & \
    (lower(status_filter_df.CRI3).like("disc%")) & \
    (lower(status_filter_df.CRI4).like("disc%")) & \
    (lower(status_filter_df.CRI5).like("disc%")) & \
    (lower(status_filter_df.Database1) == "wip") &\
    (lower(status_filter_df.Database2) == "wip") &\
    (lower(status_filter_df.Database_3) == "wip") &\
    (lower(status_filter_df.Database_4) == "wip")
    , lit("WIP")) \
.when(
    (lower(status_filter_df.CRI1).like("disc%")) & \
    (lower(status_filter_df.CRI2).like("disc%")) & \
    (lower(status_filter_df.CRI3).like("disc%")) & \
    (lower(status_filter_df.CRI4).like("disc%")) & \
    (lower(status_filter_df.CRI5).like("disc%")) & \
    (lower(status_filter_df.Database1).like("disc%")) &\
    (lower(status_filter_df.Database2).like("disc%")) &\
    (lower(status_filter_df.Database_3).like("disc%")) &\
    (lower(status_filter_df.Database_4).like("disc%"))
    , lit("Yes-Discrepancy")) \
.when(
    (lower(status_filter_df.CRI1).like("disc%")) & \
    (lower(status_filter_df.CRI2).like("disc%")) & \
    (lower(status_filter_df.CRI3).like("disc%")) & \
    (lower(status_filter_df.CRI4).like("disc%")) & \
    (lower(status_filter_df.CRI5).like("disc%")) & \
    (lower(status_filter_df.Database1).like("ver%")) &\
    (lower(status_filter_df.Database2).like("ver%")) &\
    ((lower(status_filter_df.Database_3).like("ver%")) | (lower(status_filter_df.Database_3) == "na")) &\
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_3) == "na"))
    , lit("Yes-Discrepancy")) \
.when(
    (lower(status_filter_df.CRI1).like("ver%")) & \
    ((lower(status_filter_df.CRI2).like("ver%")) | (lower(status_filter_df.CRI2) == "na")) & \
    ((lower(status_filter_df.CRI3).like("ver%")) | (lower(status_filter_df.CRI3) == "na")) & \
    ((lower(status_filter_df.CRI4).like("ver%")) | (lower(status_filter_df.CRI4) == "na")) & \
    ((lower(status_filter_df.CRI5).like("ver%")) | (lower(status_filter_df.CRI5) == "na")) & \
    ((lower(status_filter_df.Database1).like("disc%")) &\
    (lower(status_filter_df.Database2).like("disc%")) &\
    (lower(status_filter_df.Database_3).like("disc%")) &\
    (lower(status_filter_df.Database_4).like("disc%")))
    , lit("Yes-Discrepancy")) \
.when(
    (lower(status_filter_df.CRI1).like("stop%")) & \
    (lower(status_filter_df.CRI2).like("stop%")) & \
    (lower(status_filter_df.CRI3).like("stop%")) & \
    (lower(status_filter_df.CRI4).like("stop%")) & \
    (lower(status_filter_df.CRI5).like("stop%")) & \
    ((lower(status_filter_df.Database1).like("disc%")) &\
    (lower(status_filter_df.Database2).like("disc%")) &\
    (lower(status_filter_df.Database_3).like("disc%")) &\
    (lower(status_filter_df.Database_4).like("disc%")))
    , lit("No-Discrepancy")) \
.when(
    (lower(status_filter_df.CRI1).like("stop%")) & \
    (lower(status_filter_df.CRI2).like("stop%")) & \
    (lower(status_filter_df.CRI3).like("stop%")) & \
    (lower(status_filter_df.CRI4).like("stop%")) & \
    (lower(status_filter_df.CRI5).like("stop%")) & \
    (lower(status_filter_df.Database1).like("stop%")) &\
    (lower(status_filter_df.Database2).like("stop%")) &\
    (lower(status_filter_df.Database_3).like("stop%")) &\
    (lower(status_filter_df.Database_4).like("stop%"))
    , lit("Stop Check")) \
.when(
    (lower(status_filter_df.CRI1).like("stop%")) & \
    (lower(status_filter_df.CRI2).like("stop%")) & \
    (lower(status_filter_df.CRI3).like("stop%")) & \
    (lower(status_filter_df.CRI4).like("stop%")) & \
    (lower(status_filter_df.CRI5).like("stop%")) & \
    (lower(status_filter_df.Database1).like("ver%")) &\
    (lower(status_filter_df.Database2).like("ver%")) &\
    ((lower(status_filter_df.Database_3).like("ver%")) | (lower(status_filter_df.Database_3) == "na")) &\
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_4) == "na"))
    , lit("Stop Check")) \
.when(
    (lower(status_filter_df.CRI1).like("ver%")) & \
    ((lower(status_filter_df.CRI2).like("ver%")) | (lower(status_filter_df.CRI2) == "na")) & \
    ((lower(status_filter_df.CRI3).like("ver%")) | (lower(status_filter_df.CRI3) == "na")) & \
    ((lower(status_filter_df.CRI4).like("ver%")) | (lower(status_filter_df.CRI4) == "na")) & \
    ((lower(status_filter_df.CRI5).like("ver%")) | (lower(status_filter_df.CRI5) == "na")) & \
    ((lower(status_filter_df.Database1).like("stop%")) &\
    (lower(status_filter_df.Database2).like("stop%")) &\
    (lower(status_filter_df.Database_3).like("stop%")) &\
    (lower(status_filter_df.Database_4).like("stop%")))
    , lit("Stop Check")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air"))& \
    ((lower(status_filter_df.CRI2).like("ins%")) | (lower(status_filter_df.CRI2) == "air")) & \
    ((lower(status_filter_df.CRI3).like("ins%")) | (lower(status_filter_df.CRI3) == "air")) & \
    ((lower(status_filter_df.CRI4).like("ins%")) | (lower(status_filter_df.CRI4) == "air")) & \
    ((lower(status_filter_df.CRI5).like("ins%")) | (lower(status_filter_df.CRI5) == "air")) & \
    ((lower(status_filter_df.Database1).like("stop%")) | (lower(status_filter_df.Database1) == "air"))&\
    ((lower(status_filter_df.Database2).like("stop%")) | (lower(status_filter_df.Database2) == "air")) &\
    ((lower(status_filter_df.Database_3).like("stop%")) | (lower(status_filter_df.Database_3) == "air")) &\
    ((lower(status_filter_df.Database_4).like("stop%")) | (lower(status_filter_df.Database_4) == "air"))
    , lit("No-Insufficiency")) \
.when(
    (lower(status_filter_df.CRI1) == "wip") & \
    (lower(status_filter_df.CRI2) == "wip") & \
    (lower(status_filter_df.CRI3) == "wip") & \
    (lower(status_filter_df.CRI4) == "wip") & \
    (lower(status_filter_df.CRI5) == "wip") & \
    ((lower(status_filter_df.Database1).like("ins%")) | (lower(status_filter_df.Database1) == "air"))&\
    ((lower(status_filter_df.Database2).like("ins%")) | (lower(status_filter_df.Database2) == "air")) &\
    ((lower(status_filter_df.Database_3).like("ins%")) | (lower(status_filter_df.Database_3) == "air")) &\
    ((lower(status_filter_df.Database_4).like("ins%")) | (lower(status_filter_df.Database_4) == "air"))
    , lit("No-Insufficiency")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air"))& \
    ((lower(status_filter_df.CRI2).like("ins%")) | (lower(status_filter_df.CRI2) == "air")) & \
    ((lower(status_filter_df.CRI3).like("ins%")) | (lower(status_filter_df.CRI3) == "air")) & \
    ((lower(status_filter_df.CRI4).like("ins%")) | (lower(status_filter_df.CRI4) == "air")) & \
    ((lower(status_filter_df.CRI5).like("ins%")) | (lower(status_filter_df.CRI5) == "air")) & \
    (lower(status_filter_df.Database1) == "wip") &\
    (lower(status_filter_df.Database2) == "wip") &\
    (lower(status_filter_df.Database_3) == "wip") &\
    (lower(status_filter_df.Database_4) == "wip")
    , lit("No-Insufficiency")) \
.when(
    (lower(status_filter_df.CRI1).like("stop%")) & \
    (lower(status_filter_df.CRI2).like("stop%")) & \
    (lower(status_filter_df.CRI3).like("stop%")) & \
    (lower(status_filter_df.CRI4).like("stop%")) & \
    (lower(status_filter_df.CRI5).like("stop%")) & \
    ((lower(status_filter_df.Database1).like("ins%")) | (lower(status_filter_df.Database1) == "air"))&\
    ((lower(status_filter_df.Database2).like("ins%")) | (lower(status_filter_df.Database2) == "air")) &\
    ((lower(status_filter_df.Database_3).like("ins%")) | (lower(status_filter_df.Database_3) == "air")) &\
    ((lower(status_filter_df.Database_4).like("ins%")) | (lower(status_filter_df.Database_4) == "air"))
    , lit("No-Insufficiency")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air"))& \
    ((lower(status_filter_df.CRI2).like("ins%")) | (lower(status_filter_df.CRI2) == "air")) & \
    ((lower(status_filter_df.CRI3).like("ins%")) | (lower(status_filter_df.CRI3) == "air")) & \
    ((lower(status_filter_df.CRI4).like("ins%")) | (lower(status_filter_df.CRI4) == "air")) & \
    ((lower(status_filter_df.CRI5).like("ins%")) | (lower(status_filter_df.CRI5) == "air")) & \
    (lower(status_filter_df.Database1).like("stop%")) &\
    (lower(status_filter_df.Database2).like("stop%")) &\
    (lower(status_filter_df.Database_3).like("stop%")) &\
    (lower(status_filter_df.Database_4).like("stop%"))
    , lit("No-Insufficiency")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air"))& \
    ((lower(status_filter_df.CRI2).like("ins%")) | (lower(status_filter_df.CRI2) == "air")) & \
    ((lower(status_filter_df.CRI3).like("ins%")) | (lower(status_filter_df.CRI3) == "air")) & \
    ((lower(status_filter_df.CRI4).like("ins%")) | (lower(status_filter_df.CRI4) == "air")) & \
    ((lower(status_filter_df.CRI5).like("ins%")) | (lower(status_filter_df.CRI5) == "air")) & \
    (lower(status_filter_df.Database1).like("ver%")) &\
    (lower(status_filter_df.Database2).like("ver%")) &\
    ((lower(status_filter_df.Database_3).like("ver%")) | (lower(status_filter_df.Database_3) == "na")) &\
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_4) == "na"))
    , lit("No-Insufficiency")) \
.when(
    (lower(status_filter_df.CRI1).like("ver%")) & \
    ((lower(status_filter_df.CRI2).like("ver%")) | (lower(status_filter_df.CRI2) == "na")) & \
    ((lower(status_filter_df.CRI3).like("ver%")) | (lower(status_filter_df.CRI3) == "na")) & \
    ((lower(status_filter_df.CRI4).like("ver%")) | (lower(status_filter_df.CRI4) == "na")) & \
    ((lower(status_filter_df.CRI5).like("ver%")) | (lower(status_filter_df.CRI5) == "na")) & \
    ((lower(status_filter_df.Database1).like("ins%")) | (lower(status_filter_df.Database1) == "air"))&\
    ((lower(status_filter_df.Database2).like("ins%")) | (lower(status_filter_df.Database2) == "air")) &\
    ((lower(status_filter_df.Database_3).like("ins%")) | (lower(status_filter_df.Database_3) == "air")) &\
    ((lower(status_filter_df.Database_4).like("ins%")) | (lower(status_filter_df.Database_4) == "air"))
    , lit("No-Insufficiency")) \
.when(
    (lower(status_filter_df.CRI1).like("disc%")) & \
    (lower(status_filter_df.CRI2).like("disc%")) & \
    (lower(status_filter_df.CRI3).like("disc%")) & \
    (lower(status_filter_df.CRI4).like("disc%")) & \
    (lower(status_filter_df.CRI5).like("disc%")) & \
    ((lower(status_filter_df.Database1).like("ins%")) | (lower(status_filter_df.Database1) == "air"))&\
    ((lower(status_filter_df.Database2).like("ins%")) | (lower(status_filter_df.Database2) == "air")) &\
    ((lower(status_filter_df.Database_3).like("ins%")) | (lower(status_filter_df.Database_3) == "air")) &\
    ((lower(status_filter_df.Database_4).like("ins%")) | (lower(status_filter_df.Database_4) == "air"))
    , lit("No-Insufficiency")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air"))& \
    ((lower(status_filter_df.CRI2).like("ins%")) | (lower(status_filter_df.CRI2) == "air")) & \
    ((lower(status_filter_df.CRI3).like("ins%")) | (lower(status_filter_df.CRI3) == "air")) & \
    ((lower(status_filter_df.CRI4).like("ins%")) | (lower(status_filter_df.CRI4) == "air")) & \
    ((lower(status_filter_df.CRI5).like("ins%")) | (lower(status_filter_df.CRI5) == "air")) & \
    (lower(status_filter_df.Database1).like("disc%")) &\
    (lower(status_filter_df.Database2).like("disc%")) &\
    (lower(status_filter_df.Database_3).like("disc%")) &\
    (lower(status_filter_df.Database_4).like("disc%"))
    , lit("No-Insufficiency")) \

    # ------------
.when(
    (lower(status_filter_df.CRI1) == "wip") | \
    (lower(status_filter_df.CRI2) == "wip") | \
    (lower(status_filter_df.CRI3) == "wip") | \
    (lower(status_filter_df.CRI4) == "wip") | \
    (lower(status_filter_df.CRI5) == "wip") & \
    (lower(status_filter_df.Database1).like("ver%")) |\
    (lower(status_filter_df.Database2).like("ver%")) |\
    ((lower(status_filter_df.Database_3).like("ver%")) | (lower(status_filter_df.Database_3) == "na")) |\
    ((lower(status_filter_df.Database_4).like("ver%")) | (lower(status_filter_df.Database_4) == "na"))
    , lit("WIP")) \
.when(
    (((lower(status_filter_df.CRI1).like("ver%"))|(lower(status_filter_df.CRI1)=="wip"))&((lower(status_filter_df.CRI2).like("ver%"))|(lower(status_filter_df.CRI2) == "na")))&\
    ((lower(status_filter_df.Database1) == "wip") |\
    (lower(status_filter_df.Database2) == "wip"))
    , lit("WIP")) \
.when(
    ((lower(status_filter_df.CRI1).like("ins%")) | (lower(status_filter_df.CRI1) == "air")) & \
    ((lower(status_filter_df.Database1).like("ins%")) | (lower(status_filter_df.Database1) == "air") |\
    (lower(status_filter_df.Database2).like("ins%")) | (lower(status_filter_df.Database2) == "air"))
    , lit("No-Insufficiency")) \
.when((status_filter_df.KPMG_Ref_No.isNull()) & \
    (status_filter_df.Reference_No_.isNull())
    , lit("-")) \
    .otherwise(lit("No")) \
  )
# bgv_upcoming_joiners_ac1.display()

# COMMAND ----------

# DBTITLE 1,Testing MCC
# bgv_upcoming_joiners_ac1.select('CRI1','CRI2','CRI3','CRI4','CRI5','Database1','Database2','Database_3','Database_4','KPMG_Ref_No','Reference_No_','Mandatory_Checks_Completed').distinct().display()

# COMMAND ----------

# DBTITLE 1,logic for waiver applicable new
# If 'Report Color'("latest_color_code" from Progress Sheet)  is  "Yellow", "Red" then 'Waiver Applicable' is Yes

# If 'Report Color'("latest_color_code" from Progress Sheet) is "Amber" and "UTV/Inaccessible remarks mentioned"('(UTV/Inaccessible remarks)' from Progress sheet) is not null or '-' or 'NA' (if some UTV remarks are there)then Waiver applicable is Yes 

# If BGV status('Over All Status(Drop Down)' from Progress Sheet) - "WIP' or "Insufficiency" or "Report in Process" or "CEA report in Process" then Waiver applicable is "No"

# If 'External Status'(from kcheck) is  "Pending at Candidate/Pending at Client/Pending at KPMG/ Lapsed/ Send back by Client/ Send back by KPMG" then Waiver Applicable will be "-"
# If latest_color_code is green then Waiver_Applicable is No
# other wise Waiver Applicable will be No 

# COMMAND ----------

# MAGIC %md
# MAGIC Following are the constant string checks for Waiver_Applicable - 
# MAGIC cea report in process -  cea%, 
# MAGIC report in process - report%,
# MAGIC pending at candidate - pending%, 
# MAGIC pending at client - pending%, 
# MAGIC lapsed - lap%, 
# MAGIC sent back by client - sen%, 
# MAGIC sent back by KPMG - sen%, 
# MAGIC insufficiency - ins%

# COMMAND ----------

# DBTITLE 1,waiver_applicable column 
bgv_upcoming_joiners_ac2 = bgv_upcoming_joiners_ac1.withColumn("Waiver_Applicable",
                            when((lower(bgv_upcoming_joiners_ac1.latest_color_code) == "yellow") | (lower(bgv_upcoming_joiners_ac1.latest_color_code) == "red"), lit("Yes")) \
                            .when((lower(bgv_upcoming_joiners_ac1.latest_color_code) == "amber") & \
                                ((lower(bgv_upcoming_joiners_ac1.UTV_Inaccessible_remarks) != "-")| \
                                (lower(bgv_upcoming_joiners_ac1.UTV_Inaccessible_remarks) != "na")| \
                                (lower(bgv_upcoming_joiners_ac1.UTV_Inaccessible_remarks) != "")| \
                                (lower(bgv_upcoming_joiners_ac1.UTV_Inaccessible_remarks).isNotNull())), lit("Yes")) \
                            .when(lower(bgv_upcoming_joiners_ac1.latest_color_code) == 'green',lit("No"))\
                            .when((lower(bgv_upcoming_joiners_ac1.Over_All_Status_Drop_Down_) == "wip") |\
                                (lower(bgv_upcoming_joiners_ac1.Over_All_Status_Drop_Down_).like("ins%")) | \
                                (lower(bgv_upcoming_joiners_ac1.Over_All_Status_Drop_Down_).like("report%")) | \
                                (lower(bgv_upcoming_joiners_ac1.Over_All_Status_Drop_Down_).like("cea%")), lit("No"))
                            .when((lower(bgv_upcoming_joiners_ac1.External_Status).like("pending%")) |\
                                (lower(bgv_upcoming_joiners_ac1.External_Status).like("lap%")) | \
                                (lower(bgv_upcoming_joiners_ac1.External_Status).like("sen%")), lit("-"))
                            .otherwise(lit("No"))
  )
# bgv_upcoming_joiners_ac2.display()

# COMMAND ----------

# DBTITLE 1,Testing Waiver_Applicable column
# bgv_upcoming_joiners_ac2.select('latest_color_code','UTV_Inaccessible_remarks','Over_All_Status_Drop_Down_','External_Status','Waiver_Applicable').distinct().display()

# COMMAND ----------

# DBTITLE 1,Waiver status logic

# If "Waiver Applicable(calculated)" is "-" then Waiver status is "NA"

# If BGV status('Over All Status(Drop Down)' from progress sheet) is  "WIP" or "Insufficiency" or "Report in Process/ CEA report in Process" and  'Waiver applicable' is "No" then 'Waiver status' will be "NA"

# In 'Waiver Tracker file' Based on 'Candidate Email ID' refer column "Waiver(Open/Closed)", If "Waiver(Open/Closed)" is "closed" then "waiver status" is "Yes", otherwise "waiver status" is "No"


# COMMAND ----------

# MAGIC %md
# MAGIC Following are the constant string checks for Waiver_Status_Closed_Yes_or_No - 
# MAGIC cea report in process -  cea report%, 
# MAGIC report in process - report%,
# MAGIC insufficiency - ins%

# COMMAND ----------

# DBTITLE 1,waiver status implementation
status_df = bgv_upcoming_joiners_ac2.join(df_waiver_tracker,bgv_upcoming_joiners_ac2.Candidate_Email == df_waiver_tracker.Candidate_Email_ID,"left").select(bgv_upcoming_joiners_ac2["*"],df_waiver_tracker["Waiver_Open_Closed"])

bgv_upcoming_joiners_ac3 = status_df.withColumn("Waiver_Status_Closed_Yes_or_No",
    when(((lower(col("Waiver_Applicable")) == "-") | (lower(col("Waiver_Applicable")) == "no")), "NA")                             
   .when(
        ((lower(col("Over_All_Status_Drop_Down_")) == "wip") |\
         (lower(col("Over_All_Status_Drop_Down_")).like("ins%"))|\
         (lower(col("Over_All_Status_Drop_Down_")).like("report%")) |\
         (lower(col("Over_All_Status_Drop_Down_")).like("cea report%"))) & \
        (lower(col("Waiver_Applicable")) == "no"), "NA")
   .when((lower(col("Waiver_Open_Closed")) == "closed"), "Yes")
   .otherwise("No"))
   
# bgv_upcoming_joiners_ac3.display()


# COMMAND ----------

# DBTITLE 1,Testing Waiver_Status_Closed_Yes_or_No
# bgv_upcoming_joiners_ac3.select('Waiver_Applicable','Over_All_Status_Drop_Down_','Waiver_Open_Closed','Waiver_Status_Closed_Yes_or_No').distinct().display()

# COMMAND ----------

# DBTITLE 1,can be onboarded logic
# •	If "Mandatory checks Completed"  is "Yes" and ‘latest color code’(from PS) is “Green/CEA/WIP” then ‘Can be on-boarded’ is Yes
# •	If "Mandatory checks Completed"  is "Yes" and ‘latest color code’(from PS) is “Amber/Yellow/Red”  and waiver status is "yes/na” then ‘Can be on-boarded’ is Yes
# •	If "Mandatory checks Completed  is "Yes-Discrepancy" and waiver status is "Yes/na" then Can be on-boarded – Yes
# •	If "Mandatory checks Completed"  is "Yes" and ‘latest color code’(from PS) is “Amber/Yellow/Red”  and waiver status is "No” then ‘Can be on-boarded’ is No
# •	Otherwise No


# COMMAND ----------

# DBTITLE 1,16-05-22
# If the BGV_Status is “WIP” and Mandatory_Checks_Completed is “Yes” then Can_be_Onboarded_Yes_or_No should be “Yes”

# COMMAND ----------

# DBTITLE 1,can be onboarded implementation
bgv_upcoming_joiners_ac4 = bgv_upcoming_joiners_ac3.withColumn("Can_be_Onboarded_Yes_or_No",
    when((lower(col("Mandatory_Checks_Completed")) == "yes") &\
        ((lower(col("latest_color_code")) == "green") | (lower(col("latest_color_code")) == "cea") | (lower(col("latest_color_code")) == "wip")), "Yes")\

    

    .when((lower(col("Mandatory_Checks_Completed")) == "yes") &\
        ((lower(col("latest_color_code")) == "amber") | (lower(col("latest_color_code")) == "yellow") | (lower(col("latest_color_code")) == "red")) &\
        ((lower(col("Waiver_Status_Closed_Yes_or_No")) == "yes") | (lower(col("Waiver_Status_Closed_Yes_or_No")) == "na")), "Yes")\

    .when((lower(col("Mandatory_Checks_Completed")) == "yes-discrepancy") &\
        ((lower(col("Waiver_Status_Closed_Yes_or_No")) == "yes") | (lower(col("Waiver_Status_Closed_Yes_or_No")) == "na")), "Yes")\
    
    .when((lower(col("Mandatory_Checks_Completed")) == "yes") &\
        ((lower(col("BGV_Status")) == "wip")), "Yes")\

    .when((lower(col("Mandatory_Checks_Completed")) == "yes") &\
        ((lower(col("latest_color_code")) == "amber") | (lower(col("latest_color_code")) == "yellow") | (lower(col("latest_color_code")) == "red")) &\
        (lower(col("Waiver_Status_Closed_Yes_or_No")) == "no"), "No")\

   .otherwise("No"))

bgv_upcoming_joiners_ac4.display()

# COMMAND ----------

# DBTITLE 1,Testing Can_be_Onboarded_Yes_or_No
# bgv_upcoming_joiners_ac4.select('Waiver_Status_Closed_Yes_or_No','latest_color_code','Mandatory_Checks_Completed','BGV_Status','Can_be_Onboarded_Yes_or_No').distinct().display()

# COMMAND ----------

# DBTITLE 1,All_Checks_Completed

# Logic : •	If 'BGV_Status' is 'completed' then 'All_Checks_Completed' is 'Yes' 
#        •	If 'BGV_Status' is 'CEA' then 'All_Checks_Completed' is 'Yes-Except CEA' 
#            otherwise it is 'No'
# (lower(col("latest_color_code")) == "green")
bgv_upcoming_joiners_final_df = bgv_upcoming_joiners_ac4.withColumn("All_Checks_Completed", \
                                                    when(((lower(bgv_upcoming_joiners_ac4.Over_All_Status_Drop_Down_).like("compl%")) & \
                                                        (lower(col("latest_color_code")) == "green")), lit("Yes")) \
                                                    .when((lower(bgv_upcoming_joiners_ac4.Over_All_Status_Drop_Down_) == "cea"), lit("Yes-Except CEA")) \
                                                    .otherwise(lit("No")))

# bgv_upcoming_joiners_final_df.display()

# COMMAND ----------

# DBTITLE 1,Testing All_Checks_Completed
# bgv_upcoming_joiners_final_df.select('Over_All_Status_Drop_Down_','latest_color_code','All_Checks_Completed').distinct().display()

# COMMAND ----------

# DBTITLE 1,Reanaming column as per names of final template
bgv_upcoming_joiners_final_df_1  = bgv_upcoming_joiners_final_df.withColumnRenamed("Candidate_Full_Name","Candidate_Name")\
                                                        .withColumnRenamed("Candidate_Email","Candidate_Email/Candidate_Email_Personal_Email")\
                                                        .withColumnRenamed("Candidate_Official_email_","Candidate_Alternate_Email_Candiddate_Official_Email")\
                                                        .withColumnRenamed("KPMG_Ref_No","BGV_Reference_No")\
                                                        .withColumnRenamed("Case_Initiation_Date","BGV_Initiation_Date")\
                                                        .withColumnRenamed("latest_color_code","Report_Colour")\
                                                        .withColumnRenamed("Open_Insuff","Insuff_Remarks")\
                                                        .withColumnRenamed("UTV_Inaccessible_remarks","UTV_Remarks")\
                                                        .withColumnRenamed("EDU_EMP_Name__Suspicious_","Found_in_Suspicious_List")\
                                                        .withColumnRenamed("Database1","DTB_1")\
                                                        .withColumnRenamed("Database2","DTB_2")\
                                                        .withColumnRenamed("Database_3","DTB_3")\
                                                        .withColumnRenamed("Database_4","DTB_4")
# bgv_upcoming_joiners_final_df_1.display()



# COMMAND ----------

bgv_upcoming_joiners_final_df_test = bgv_upcoming_joiners_final_df_1.withColumnRenamed("ps_status","progress_sheet_status")\
.withColumn("progress_sheet_ref_no",col('Reference_No_'))

# COMMAND ----------

bgv_upcoming_joiners_testing = bgv_upcoming_joiners_final_df_test.select('Candidate_Identifier','BU','Candidate_Name','Candidate_Email/Candidate_Email_Personal_Email','Candidate_Alternate_Email_Candiddate_Official_Email','Start_Date','Recruiter_Name','BGV_Reference_No','Reference_No_','Progress_Sheet_Email','Offer_Release_Email','Kcheck_Email','BGV_Initiation_Date','External_Status','progress_sheet_status','BGV_Status','Report_Colour','All_Checks_Completed','Waiver_Status_Closed_Yes_or_No','Can_be_Onboarded_Yes_or_No','Final_Report_Color_Code_Drop_Down_','Over_All_Status_Drop_Down_','Waiver_Applicable','Waiver_Open_Closed','Waiver_Status_Closed_Yes_or_No','Insuff_Remarks','UTV_Remarks','Discrepancy_remarks','Found_in_Suspicious_List','Work_Location','Cost_Center','Client_Geography','Designation','progress_sheet_ref_no','CRI1','CRI2','CRI3','CRI4','CRI5','DTB_1','DTB_2','DTB_3','DTB_4','Mandatory_Checks_Completed')
# bgv_upcoming_joiners_testing.display()

# COMMAND ----------

# DBTITLE 1,Selecting columns in sequence
# Candidate Identifier
# BU
# Candidate name
# Candidate Email/Candidate_Email(Personal Email)
# Candidate Alternate Email(Candiddate Official Email)
# Start Date
# Recruiter Name
# BGV Reference No.
# BGV Initiation Date
# BGV status
# Report Colour
# Mandatory Checks Completed
# All Checks Completed
# "Can be onboarded Yes/No"
# Waiver Applicable
# "Waiver Status - Closed Yes/No"
# Insuff Remarks
# UTV Remarks
# Discrepancy remarks
# Found in Suspicious List
# Work Location
# Cost Center
# Client Geography
# Designation
# CRI-1
# Cri-2
# Cri-3
# CRI-4
# CRI-5
# DTB-1
# DTB-2
# DTB-3
# DTB-4
bgv_upcoming_joiners = bgv_upcoming_joiners_final_df_1.select('Candidate_Identifier','BU','Candidate_Name','Candidate_Email/Candidate_Email_Personal_Email','Candidate_Alternate_Email_Candiddate_Official_Email','Start_Date','Recruiter_Name','BGV_Reference_No','BGV_Initiation_Date','BGV_Status','Report_Colour','Mandatory_Checks_Completed','All_Checks_Completed','Can_be_Onboarded_Yes_or_No','Waiver_Applicable','Waiver_Status_Closed_Yes_or_No','Insuff_Remarks','UTV_Remarks','Discrepancy_remarks','Found_in_Suspicious_List','Work_Location','Cost_Center','Client_Geography','Designation','CRI1','CRI2','CRI3','CRI4','CRI5','DTB_1','DTB_2','DTB_3','DTB_4')
# bgv_upcoming_joiners.display()


# COMMAND ----------

bgv_upcoming_joiners = bgv_upcoming_joiners.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
bgv_upcoming_joiners = bgv_upcoming_joiners.withColumn("Dated_On",lit(currentdatetime))

# COMMAND ----------

bgv_upcoming_joiners \
    .groupby(['Candidate_Email/Candidate_Email_Personal_Email','Start_Date']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# COMMAND ----------

# display(bgv_upcoming_joiners.filter(col("Candidate_Email/Candidate_Email_Personal_Email") == '19poonamrawat@gmail.com'))
# display(bgv_upcoming_joiners.select("File_Date").distinct())
print(bgv_upcoming_joiners.count())

# COMMAND ----------

bgv_upcoming_joiners.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True")\
.option("path",trusted_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsOneDataDb.trusted_stg_"+ processName + "_" + tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

