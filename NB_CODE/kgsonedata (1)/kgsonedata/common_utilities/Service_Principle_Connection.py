# Databricks notebook source
# adlsContainerName = "landinglayer"
# mountPoint = "/mnt/landinglayermount"

# adlsContainerName = "landing1"
# mountPoint = "/mnt/landing1mount"

# adlsContainerName = "test"
# mountPoint = "/mnt/testmount"

# adlsContainerName = "rawlayer"
# mountPoint = "/mnt/rawlayermount"

# adlsContainerName = "trustedlayer"
# mountPoint = "/mnt/trustedlayermount"

# adlsContainerName = "config"
# mountPoint = "/mnt/configmount"

# adlsContainerName = "outputfiles"
# mountPoint = "/mnt/outputfiles"

# adlsContainerName = "badfiles"
# mountPoint = "/mnt/badfiles"

# COMMAND ----------

adlsAccountName = "goprdadlskgsdata01"
# adlsContainerName = "landinglayer"
adlsFolderName = ""
# mountPoint = "/mnt/datalakemount"

# Application (Client) ID
applicationId = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="applicationId")

# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="appregkgs1datasecret")

# Directory (Tenant) ID
tenantId = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="tenantId")

endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any (mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = source,
        mount_point = mountPoint,
        extra_configs = configs)
    print("Mount Point created :", mountPoint)
else:
    print("Mount Point already exists : ", mountPoint)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))
# display(dbutils.fs.ls("/mnt/testmount/lnd/"))

# COMMAND ----------

# dbutils.fs.unmount("/mnt/rawlayermount")

# dbutils.fs.unmount("/mnt/configmount")

# COMMAND ----------

landing_path_url = "/mnt/landinglayermount/"
config_path_url = "/mnt/configmount/"

raw_hist_savepath_url="/mnt/rawlayermount/history/"
raw_curr_savepath_url="/mnt/rawlayermount/current/"
raw_stg_savepath_url="/mnt/rawlayermount/staging/"

trusted_hist_savepath_url="/mnt/trustedlayermount/history/"
trusted_curr_savepath_url="/mnt/trustedlayermount/current/"
trusted_stg_savepath_url="/mnt/trustedlayermount/staging/"

test_path_url = "/mnt/outputfiles/"

bad_filepath_url = "/mnt/badfiles/"