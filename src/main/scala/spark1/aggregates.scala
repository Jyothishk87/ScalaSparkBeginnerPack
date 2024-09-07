package spark1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, count, countDistinct, expr, sum}

object aggregates extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "AGR")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().
    config(sparkConf)
    // appName("AGR").
    //   master("local[*]").
    .getOrCreate()
  val invoiceDf = spark.read.option("header", true)
    .format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "/Users/jyothishk/Desktop/Big data training/week12datasets/order_data.csv")
    .load()
  // simple aggregation in multiple ways.
  // 1. column object expression

  invoiceDf.select(
    count("*") as ("RowCOunt"),
    sum ("Quantity") as ("TotalQuanity"),
    avg ("UnitPrice") as ("AvgPrice"),
    countDistinct("InvoiceNo") as ("countDistinct")
   ).show()
// 2. string expression
  invoiceDf.selectExpr(
    "count(*) as RowCOunt",
     "sum(Quantity) as TotalQuanity",
     "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo)) as countDistinct"
  ).show()

  // 3. spark-sql
   invoiceDf.createOrReplaceTempView("sales")
  spark.sql("select count(*) ,sum(Quantity),avg(UnitPrice), count(Distinct(InvoiceNo)) from sales" ).show()

  // grouped aggregates
  // 1. column expression

  val agg1Df= invoiceDf.groupBy("Country","InvoiceNo")
    .agg(sum("Quantity") as ("TotalQuantity"),
         sum (expr("Quantity*UnitPrice")) as ("InvoiceValue")
    )
  agg1Df.show()
 // 2. string expression
  val agg2df = invoiceDf.groupBy("Country","InvoiceNo")
    .agg(expr("sum(Quantity) as TotalQuantity" ),
      expr("sum(Quantity*UnitPrice) as InoiceValue")
     )
  agg2df.show()
 // 3. SQL (long sql statments can enclose in """ """
  invoiceDf.createOrReplaceTempView("sales1")
  spark.sql("select Country,InvoiceNo , sum(Quantity) as TotalQuantity , " +
    "sum(Quantity*UnitPrice) as InvoiceValue " +
    "from sales1 group by Country , InvoiceNo")
    .show()
  spark.stop()
}
