// Databricks notebook source
import org.apache.spark.sql.types._
val myManualSchema = new StructType(Array(
new StructField("DEST_COUNTRY_NAME", StringType, true),
new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
new StructField("count", IntegerType, false) ))

val df = spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("/databricks-datasets/definitive-guide/data/flight-data/csv/2010-summary.csv")


// COMMAND ----------

df.write
.format("csv")
.mode("overwrite")
.option("sep", "\t")
.save("/tmp/my-tsv-file.tsv")

// COMMAND ----------

val df = spark.read.format("parquet")
.load("/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet")

// COMMAND ----------

df.write
.format("parquet")
.mode("overwrite")
.save("/tmp/my-parquet-file.parquet")
