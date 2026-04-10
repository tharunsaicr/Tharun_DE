# Databricks notebook source
adlsContainerName = "finance"
mountPoint = "/mnt/finance/"

# COMMAND ----------

# DBTITLE 1,Prod
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

display(dbutils.fs.ls("/mnt/finance/"))
# display(dbutils.fs.ls("/mnt/testmount/lnd/"))

# COMMAND ----------

