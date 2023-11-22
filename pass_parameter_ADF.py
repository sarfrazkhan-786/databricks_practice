# Databricks notebook source
import pandas as pd

# COMMAND ----------

storage_account_name="databrickssessionstorage"
storage_account_key="?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-11-21T05:10:09Z&st=2023-11-20T21:10:09Z&spr=https&sig=BOhBNFmRhjXS0NQxKG226jyO3BwiPeQL5eps9Wmpgl0%3D"
containerName = "databricksparkassignment"
url="wasbs://" + containerName + "@" + storage_account_name + ".blob.core.windows.net/"
#config="fs.azure.account.key." + containerName + "." + storage_account_name + ".blob.core.windows.net"
config="fs.azure.account.key."+ storage_account_name +".blob.core.windows.net:"+storage_account_key
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

print(url)
print(config)


# COMMAND ----------

ref=pd.read_csv('https://databrickssessionstorage.blob.core.windows.net/databricksparkassignment/Sample.csv')

# COMMAND ----------

print(ref)

# COMMAND ----------

# getting file input from adf pipeline
dbutils.widgets.text("input",",")
y=dbutils.widgets.get("input")
print(y)
ref=pd.read_csv(y)
print(ref)

# COMMAND ----------


