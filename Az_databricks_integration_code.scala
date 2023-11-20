// Databricks notebook source
val containerName = "databricksparkassignment"
val storageName="databrickssessionstorage"
val sas="?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-11-21T05:10:09Z&st=2023-11-20T21:10:09Z&spr=https&sig=BOhBNFmRhjXS0NQxKG226jyO3BwiPeQL5eps9Wmpgl0%3D"
val url="wasbs://" + containerName + "@" + storageName + ".blob.core.windows.net/"
val config="fs.azure.sas." + containerName + "." + storageName + ".blob.core.windows.net"

dbutils.fs.mount(
  source=url,
  mountPoint="/mnt/staging",
  extraConfigs=Map(config -> sas))

// COMMAND ----------

val df = spark.read.format("csv").option("header","true")
                                 .option("inferSchema","true").option("mode","failfast")
                                 .load("/mnt/staging/Sample.csv")
                  

// COMMAND ----------

df.show()

// COMMAND ----------

df.createOrReplaceTempView("CustomerData")
spark.sql("select sum(age) from customerdata").show

// COMMAND ----------

spark.sql("select avg(age) from customerdata").show

// COMMAND ----------

df.write.mode("overwrite").csv("/mnt/staging/output/customerdata.csv")
