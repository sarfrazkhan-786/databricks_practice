# Databricks notebook source
#Listing Scopes Defined
dbutils.secrets.listScopes()


# COMMAND ----------

# Selecting Scope defined in create Scope to check secrets stored in Az Key Vaults
dbutils.secrets.list(scope='secret01')

# COMMAND ----------

#LOading Scope Secret name into a varible to cross verify the secrets stored in Key Vaults
x= dbutils.secrets.get("scope01","username")


# COMMAND ----------

#Checking the secrets and verify using loop
for y in x:
    print(y,end=',')

# COMMAND ----------

#Configuring the Mount point to ADLS Gen2 locaiton and accessing to read and write the files respectively
containerName = "azdlscontainer"
storageAccntName = "azdatalakestoragegen2"
folderName = "Input"
mountPoint = "/mnt/staging"

# Accesss application id from scope to key vault
applicationId = dbutils.secrets.get(scope="scope02_azkvdataB03", key="appid")
# Accesss client secret from scope to key vault
authenticationKey = dbutils.secrets.get(scope="scope02_azkvdataB03", key="clientSecret")

# Accesss tenantid id from scope to key vault
tenant_id = dbutils.secrets.get(scope="scope02_azkvdataB03", key="tenantId")

print(applicationId, authenticationKey, tenant_id)
# end_point="https://login.microsoftonline.com/"+ tenant_id + "/oauth2/token"
end_point = "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"
url = (
    "abfss://"
    + containerName
    + "@"
    + storageAccntName
    + ".dfs.core.windows.net/"
    + folderName
)
config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": applicationId,
    "fs.azure.account.oauth2.client.secret": authenticationKey,
    "fs.azure.account.oauth2.client.endpoint": end_point,
}
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source=url, mount_point=mountPoint, extra_configs=config)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

'''
This Code can be configured using spark configurations
#containerName = "databricksparkassignment"
containerName="azdlscontainer"
 #storageAccntName="databrickssessionstorage"
storageAccntName="azdatalakestoragegen2"
folderName="Input"
mountPoint="/mnt/staging"

 #Accesss application id from scope to key vault
applicationId = dbutils.secrets.get(scope="scope02_azkvdataB03",key="appid")
 #Accesss client secret from scope to key vault
authenticationKey= dbutils.secrets.get(scope="scope02_azkvdataB03",key="clientSecret")

 #Accesss tenantid id from scope to key vault
tenant_id= dbutils.secrets.get(scope="scope02_azkvdataB03",key="tenantId")
print("tenant_id"+tenant_id)

service_credential = dbutils.secrets.get(scope="scope02_azkvdataB03",key="clientSecret")

spark.conf.set("fs.azure.account.auth.type.azdatalakestoragegen2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azdatalakestoragegen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azdatalakestoragegen2.dfs.core.windows.net", "{applicationId}")
spark.conf.set("fs.azure.account.oauth2.client.secret.azdatalakestoragegen2.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azdatalakestoragegen2.dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")
dbutils.fs.ls("abfss://azdlscontainer@azdatalakestoragegen2.dfs.core.windows.net/Input/")

spark.read.format("csv").load("abfss://azdlscontainer@azdatalakestoragegen2.dfs.core.windows.net/external-location/Input/Sample.csv")

spark.sql("SELECT * FROM parquet.`abfss://azdlscontainer@azdatalakestoragegen2.dfs.core.windows.net/external-location/Input/Sample.csv`")'''

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/"

# COMMAND ----------


# Reading and Writing from ADLS Gen2 
import pandas as pd
df = pd.read_csv("/dbfs/mnt/staging/Input/Sample.csv",sep=",")
#printing columns
print(df.columns)

#renaming columns
df.columns=["name","ag","contact"]

print(df.columns)

# Replace column name new name
df.rename(columns = {'contact':'Ph_No','ag':'age'},inplace="true")
print(df.columns)

#writing the file in adls gen2 with new column names

df.to_csv("/dbfs/mnt/staging/Input/Sample_Modified")

