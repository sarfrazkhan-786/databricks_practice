// Databricks notebook source
val containerName = "databricksparkassignment"
val storageName="databrickssessionstorage"
val sas="?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-11-20T07:38:47Z&st=2023-11-19T23:38:47Z&spr=https&sig=xo%2Blgd7apjcfVfWmmCI99iBFpz4V30ljyCOy7UqlnAw%3D"
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
