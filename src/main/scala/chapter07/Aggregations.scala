// Databricks notebook source
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/databricks-datasets/definitive-guide/data/retail-data/all/*.csv")
.coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")


// COMMAND ----------

import org.apache.spark.sql.functions.{count, countDistinct}
df.select(count("country"), countDistinct("country"))
.show()

// COMMAND ----------

import org.apache.spark.sql.functions.{collect_list, collect_set}
df.select(collect_set("country"), collect_list("country")).printSchema

// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId")

// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count().show()

// COMMAND ----------

import org.apache.spark.sql.functions.expr
df
.groupBy("InvoiceNo", "CustomerId")
.agg(
  count("*").alias("total_count"),
  expr("max(quantity)").as("highest")
)
.show()

// COMMAND ----------

import org.apache.spark.sql.functions.{col, to_date}
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
"MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
display(dfWithDate)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
val windowSpec = Window
.partitionBy("CustomerId", "date")
.orderBy(col("Quantity").desc)
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

import org.apache.spark.sql.functions.max
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// COMMAND ----------

import org.apache.spark.sql.functions.{dense_rank, rank}
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
import org.apache.spark.sql.functions.col
dfWithDate
//.drop("InvoiceDate")
.where("CustomerId IS NOT NULL")
.orderBy("CustomerId")
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity"))
//.sort("CustomerId", "date", "Quantity")
.show()

// COMMAND ----------

import org.apache.spark.sql.functions.sum
val dfNoNull = dfWithDate.drop()
val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
.selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
.orderBy("Date")

// COMMAND ----------

display(rolledUpDF)

// COMMAND ----------

display(rolledUpDF.where("date is null"))

// COMMAND ----------

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
.select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

// COMMAND ----------

import org.apache.spark.sql.functions.{grouping_id, sum, expr}
dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
.orderBy(expr("grouping_id()").desc)
.show()

// COMMAND ----------

val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
