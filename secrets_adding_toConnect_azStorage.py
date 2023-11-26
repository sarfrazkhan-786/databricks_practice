# Databricks notebook source
 containerName = "databricksparkassignment"
 storageAccntName="databrickssessionstorage"
 mount_point=/mnt/staging2

 #Accesss application id from scope to key vault
application_id=dbutils.scerets.get(scope="secret01",key="appid")

 #Accesss client secret from scope to key vault
application_id=dbutils.scerets.get(scope="secret01",key="clientsecrets")

 #Accesss tenantid id from scope to key vault
application_id=dbutils.scerets.get(scope="secret01",key="tenantid")

end_point="https://login.microsoftonline.com"+ tenantid+"/oauth2/token"

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='secret01')

# COMMAND ----------

x= dbutils.secrets.get("secret01","appid")

# COMMAND ----------

for y in x:
    print(y,end=',')

# COMMAND ----------


