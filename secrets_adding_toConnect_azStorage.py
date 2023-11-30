# Databricks notebook source
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

end_point="https://login.microsoftonline.com"+ tenant_id +"/oauth2/token"
url = "abfss://"+ containerName + "@" + storageAccntName +".dfs.core.windows.net/".format(containerName,storageAccntName)
#url = "wasbs://"+ containerName + "@" + storageAccntName +".blob.core.windows.net/"
#url = "wasbs://{containerName}@{storageAccntName}.blob.core.windows.net".format(blobContainerName, storageAccountName)"
config= {"fs.azure.account.auth.type":"OAuth",
        "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": applicationId,
        "fs.azure.account.oauth2.client.secret":authenticationKey,
        "fs.azure.account.oauth2.client.endpoint":end_point}
print("End Point:- "+ end_point)
print("source point:"+ url)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=url,
        mount_point=mountPoint,
        extra_configs=config)

        




# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/staging"

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='secret01')

# COMMAND ----------

x= dbutils.secrets.get("scope01","username")

# COMMAND ----------

for y in x:
    print(y,end=',')

# COMMAND ----------

'''from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


key_vault_name = "azkvdatab03"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
secret_name = "username"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_uri, credential=credential)
retrieved_secret = client.get_secret(secret_name)

print(f"The value of secret '{secret_name}' in '{key_vault_name}' is: '{retrieved_secret.value}'")'''
