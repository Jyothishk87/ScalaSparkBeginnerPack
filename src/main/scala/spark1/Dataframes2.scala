package spark1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Dataframes2  extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name" ,"Spark API1")
  sparkConf.set("spark.master","local[*]")

  val spark= SparkSession.builder().
    config(sparkConf)
    // appName("Spark API1").
    //   master("local[*]").
    .getOrCreate()

  // programatic way of explicit schema
  val orderSchema = StructType(List
  (
    StructField("orderId", IntegerType),
    StructField("orderdate", TimestampType),
    StructField("customerId", IntegerType),
    StructField("orderStatus", StringType),

  ))
  // DDL way of schema
  val orderSchemaDDL = "orderID Int,orderDate String,customerId Int,orderstatus String"

  val orderDf = spark.read.option("header",true)
    .format("csv")
    // infer schema is an action
    .schema(orderSchemaDDL)
    .option("header",true)
    .option("path","/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
    // path and format can be given as below also.
    // .csv("/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
    .load()

 orderDf.show()
  val groupdorderDF= orderDf
    .repartition(4)
    .select("orderID","customerId")
    .where("customerId > 10000")
    .groupBy("customerId")
    .count()

  // groupdorderDF.show()

  Logger.getLogger(getClass.getName).info("app processed")

  // orderDf.printSchema()
  spark.stop()

}
