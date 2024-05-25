// Databricks notebook source
println(spark.version)
display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

val flightData2015 = spark
.read
.option("inferSchema", "true")
.option("header", "true")
.csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")
flightData2015.show()

// COMMAND ----------

flightData2015.take(3)

// COMMAND ----------

flightData2015.sort("count").explain()

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)

// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")
// in Scala
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(*)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY count(*) DESC 
""")

// COMMAND ----------

sqlWay.show()

// COMMAND ----------

spark.sql("SELECT max(count) from flight_data_2015").take(1)

// COMMAND ----------

val dest = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

// COMMAND ----------

dest.explain()

// COMMAND ----------

import org.apache.spark.sql.functions.desc
flightData2015
.groupBy("DEST_COUNTRY_NAME")
.sum("count")
.withColumnRenamed("sum(count)", "destination_total")
.sort(desc("destination_total"))
.limit(5)
.show()
//.explain()
