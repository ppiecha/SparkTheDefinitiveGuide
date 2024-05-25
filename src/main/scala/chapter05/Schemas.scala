// Databricks notebook source
  val flights2015 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

 flights2015.printSchema()

// COMMAND ----------

  println(flights2015.schema)

// COMMAND ----------

  val flights2015 = spark
    .read
    .option("header", "true")
    //.option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

flights2015.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{col, column}
val doubled = flights2015
.withColumn("doubled", col("count") * 2)

// COMMAND ----------

doubled.show()

// COMMAND ----------

val df = spark.read.format("json")
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
val myManualSchema = StructType(Array(
StructField("DEST_COUNTRY_NAME", StringType, true),
StructField("ORIGIN_COUNTRY_NAME", StringType, true),
StructField("count", LongType, false,
Metadata.fromJson("{\"hello\":\"world\"}"))
))

// COMMAND ----------

val df = spark.read.format("json").schema(myManualSchema)
.load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

// COMMAND ----------

import org.apache.spark.sql.functions.{col, column}
val c1 = col("c1")
val c2 = 'c2
println(c2)

// COMMAND ----------

df.columns.foreach(println)

// COMMAND ----------

df.first

// COMMAND ----------

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

val myManualSchema = new StructType(Array(
new StructField("some", StringType, true),
new StructField("col", StringType, true),
new StructField("names", LongType, false)))
val myRows = Seq(Row("Hello", null, 1L))
val myRDD = spark.sparkContext.parallelize(myRows)
val myDf = spark.createDataFrame(myRDD, myManualSchema)
myDf.show()

// COMMAND ----------

val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
myDF.show()

// COMMAND ----------

  val flights2015 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

val ext = flights2015.selectExpr("*", "1 as oneLit", "count <= oneLit", "current_timestamp", "current_date", "sum(count) over()")
ext.printSchema()
display(ext)

// COMMAND ----------

val oneRow = flights2015.selectExpr("sum(count) as sumOfAll", "count(distinct DEST_COUNTRY_NAME) as dist_dest", "'1' as oneString")
oneRow.printSchema()
display(oneRow)

// COMMAND ----------

//import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.expr
val withCol = flights2015
.withColumn("one", expr("1.0"))
.withColumnRenamed("dest_COUNTRY_NAME", "destination")
withCol.printSchema()
withCol.show()

// COMMAND ----------

import org.apache.spark.sql.functions.expr
val withCol2 = flights2015
.withColumn("Super-long column name", expr("'Very very very long'"))
.withColumnRenamed("Super-long column name", "short one")
.withColumn("substr", expr("substring(`short one`, 0, 3)"))
.withColumn("casted", expr("cast(count as long) as count_long"))
withCol2.printSchema()
withCol2.show()

// COMMAND ----------

flights2015
.where("count < 2")
.limit(2)
.show()

// COMMAND ----------

  val flights2015 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

flights2015
.select("DEST_COUNTRY_NAME")
.distinct()
.orderBy("DEST_COUNTRY_NAME")
.limit(10)
.show()

// COMMAND ----------

flights2015.createOrReplaceTempView("flights2015")
spark.sql("""
select count(distinct DEST_COUNTRY_NAME, origin_COUNTRY_NAME)
  from flights2015
 order by 1 desc
 limit 20
""")
.show()

// COMMAND ----------

flights2015.sample(false, 0.5).count // with replacement returns item into population before next sample

// COMMAND ----------

val List(x, y) = flights2015.randomSplit(Array(0.25, 0.75)).toList
println(x.count, y.count)

// COMMAND ----------

  val df = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

import org.apache.spark.sql.Row
val schema = df.schema
val newRows = Seq(
Row("New Country", "Other Country", 5),
Row("New Country 2", "Other Country 3", 1)
)
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF)
.where("count = 1")
.where($"ORIGIN_COUNTRY_NAME" =!= "United States")
.show() // get all of them and we'll see our new rows at the end


// COMMAND ----------

  val df = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

import org.apache.spark.sql.functions.col
val opt = df.repartition(5, col("DEST_COUNTRY_NAME"))
opt.coalesce(2).explain

// COMMAND ----------

df.show(5, false)
